package bolt

import (
	"bytes"
	"fmt"
	"unsafe"
)

const (
	// MaxKeySize is the maximum length of a key, in bytes.
	MaxKeySize = 32768  // key的最大长度:32kb

	// MaxValueSize is the maximum length of a value, in bytes.
	MaxValueSize = (1 << 31) - 2 // value的最大长度，2^32bytes
)

const (
	maxUint = ^uint(0)
	minUint = 0
	maxInt  = int(^uint(0) >> 1)
	minInt  = -maxInt - 1
)

const bucketHeaderSize = int(unsafe.Sizeof(bucket{}))

const (
	minFillPercent = 0.1
	maxFillPercent = 1.0
)

// DefaultFillPercent is the percentage that split pages are filled.
// This value can be changed by setting Bucket.FillPercent.
const DefaultFillPercent = 0.5

// Bucket represents a collection of key/value pairs inside the database.
// 类似于表的一个概念，在boltdb相关数据必须存在一个Bucket下，不同Bucket下的数据相互隔离，
// 每个Bucket下有一个单调递增的sequence，类似于自增主键。
// 一个Bucket代表一个完整的B+树
type Bucket struct {
	*bucket   // Bucket的头部
	// 事务，每一次对数据库的操作是一次事务，每次事务都是一次探索定位，修改或读取数据的过程
	tx       *Tx                // 当前Bucket所属的Transaction。Transaction和Bucket均是动态的概念，即内存中的对象，Bucket最终表示为BoltDB的KV记录
	// 探索过程，缓存记录下已经探索过的buckets
	buckets  map[string]*Bucket // subbucket cache（子Bucket）
	page     *page              // inline Bucket的页引用，内置Bucket只有一个节点，即它的根节点，且节点不存在独立的页面中，而是作为Bucket的Value存在父Bucket所在页上，页引用就是指向Value中的内置页
	rootNode *node              // Bucket的根节点，也是对应B+树的根节点
	// 探索过程中，缓存记录下已经探索过的node
	nodes    map[pgid]*node     // Bucket中的node集合（缓存）

	// Sets the threshold for filling nodes when they split. By default,
	// the bucket will fill to 50% but it can be useful to increase this
	// amount if you know that your write workloads are mostly append-only.
	//
	// This is non-persisted across transactions so it must be set in every Tx.
	FillPercent float64  // Bucket中节点的填充百分比(或者占空比)。
	// 该值与B+Tree中节点的分裂有关系，当节点中Key的个数或者size超过整个node容量的某个百分比后，
	// 节点必须分裂为两个节点，这是为了防止B+Tree中插入K/V时引发频繁的再平衡操作，所以注释中提到只有当确定大数多写入操作是向尾添加时，
	// 这个值调大才有帮助。该值的默认值是50%.
}

// bucket represents the on-file representation of a bucket.
// This is stored as the "value" of a bucket key. If the bucket is small enough,
// then its root page can be stored inline in the "value", after the bucket
// header. In the case of inline buckets, the "root" will be 0.
type bucket struct {
	// root就是bucket所在的根
	// subbucket的root结点的page id
	root     pgid   // Bucket根节点的页号
	// boltdb支持每bucket的自增id，即这里的sequence
	sequence uint64 // 序列号，可以用于Bucket的序号生成器
}

// newBucket returns a new bucket associated with a transaction.
func newBucket(tx *Tx) Bucket {
	var b = Bucket{tx: tx, FillPercent: DefaultFillPercent}
	if tx.writable {
		b.buckets = make(map[string]*Bucket)
		b.nodes = make(map[pgid]*node)
	}
	return b
}

// Tx returns the tx of the bucket.
func (b *Bucket) Tx() *Tx {
	return b.tx
}

// Root returns the root of the bucket.
func (b *Bucket) Root() pgid {  // 树的根
	return b.root
}

// Writable returns whether the bucket is writable.
func (b *Bucket) Writable() bool {
	return b.tx.writable
}

// Cursor creates a cursor associated with the bucket.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
// cursor与bucket相关联
// 只要事务打开，cursor就有效
// 不要在事务关闭之后使用cursor
func (b *Bucket) Cursor() *Cursor {
	// Update transaction statistics.
	b.tx.stats.CursorCount++

	// Allocate and return a cursor.
	return &Cursor{
		bucket: b,
		stack:  make([]elemRef, 0),
	}
}

// Bucket retrieves a nested bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
// 根据名字查找Bucket
// Bucket实例仅在事务的生命周期内有效
func (b *Bucket) Bucket(name []byte) *Bucket {
	if b.buckets != nil {
		// 根据名字查找
		// 查看Buckets缓存记录中有没有，有则直接返回
		// 在父Bucket缓存的子Bucket中查找，若有则返回
		if child := b.buckets[string(name)]; child != nil {
			return child
		}
	}

	// Move cursor to key.
	// 通过游标查找定位
	c := b.Cursor()
	// 准确查找，seek函数细节之前已经讲过
	k, v, flags := c.seek(name)  // 移动游标

	// Return nil if the key doesn't exist or it is not a bucket.
	// 判断类型是否匹配
	// key不匹配 || 不是叶节点
	if !bytes.Equal(name, k) || (flags&bucketLeafFlag) == 0 {
		return nil
	}

	// Otherwise create a bucket and cache it.
	// 查找到后，要做一次转化成bucket，并将其放入到buckets记录缓存中。
	// 将查找到的Bucket记录的Value传入openBucket()得到子Bucket
	var child = b.openBucket(v)  // 创建一个新的bucket
	if b.buckets != nil {
		b.buckets[string(name)] = child  // 缓存
	}

	return child
}

// Helper method that re-interprets a sub-bucket value
// from a parent into a Bucket
func (b *Bucket) openBucket(value []byte) *Bucket {
	var child = newBucket(b.tx)

	// If unaligned load/stores are broken on this arch and value is
	// unaligned simply clone to an aligned byte array.
	// brokenUnaligned默认为false

	// 下面这段逻辑相当于可以忽略
	unaligned := brokenUnaligned && uintptr(unsafe.Pointer(&value[0]))&3 != 0

	if unaligned {
		value = cloneBytes(value)  // 没对齐，然后重新建一个一模一样的变量？
	}

	// If this is a writable transaction then we need to copy the bucket entry.
	// Read-only transactions can point directly at the mmap entry.
	// 在读写Transaction中，将Value深度拷贝到新Bucket的头部；在只读Transaction，将新Bucket的头部指向Value即可；
	// 前面我们介绍过，普通Bucket记录的Value就是Bucket头部，内置Bucket记录的Value是Bucket头和内置page的序列化结果;
	if b.tx.writable && !unaligned {
		// 都走到这个分支
		child.bucket = &bucket{}
		*child.bucket = *(*bucket)(unsafe.Pointer(&value[0]))
	} else {
		child.bucket = (*bucket)(unsafe.Pointer(&value[0]))
	}

	// Save a reference to the inline page if the bucket is inline.
	// 如果bucket属于inline，把内容也存到value里面
	// 如果是一个内置Bucket,将新Bucket的page字段指向Value中内置page的起始位置。
	if child.root == 0 {
		child.page = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	}

	return &child
}

// CreateBucket creates a new bucket at the given key and returns the new bucket.
// Returns an error if the key already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
// 根据key创建bucket
func (b *Bucket) CreateBucket(key []byte) (*Bucket, error) {
	if b.tx.db == nil {
		return nil, ErrTxClosed
	} else if !b.tx.writable {
		return nil, ErrTxNotWritable
	} else if len(key) == 0 {
		return nil, ErrBucketNameRequired
	}

	// Move cursor to correct position.
	// 1.通过游标进行查找，查找到所对应key的node数据结构
	// 为当前Bucket创建游标Cursor
	c := b.Cursor()
	k, _, flags := c.seek(key)  // 移动游标

	// Return an error if there is an existing key.
	// 2.找到了，对应的node结构是不是bucket类型，是则返回
	// 在当前Bucket中查找key并移动游标，确定其应该插入的位置
	if bytes.Equal(key, k) {  // 已经存在该key
		// 叶节点
		if (flags & bucketLeafFlag) != 0 {
			// 返回已经存在的error
			return nil, ErrBucketExists
		}
		// 返回类型不匹配的error
		return nil, ErrIncompatibleValue
	}

	// Create empty, inline bucket.
	// 创建空的，内联的bucket
	// 没有找到，则创建一个bucket，并将rootNode设置为isLeaf
	// 创建一个空的，内置的Bucket
	var bucket = Bucket{
		bucket:      &bucket{},
		rootNode:    &node{isLeaf: true},
		FillPercent: DefaultFillPercent,
	}
	// 其中write是将bucket结构数据生成page内存数据
	// 将刚创建的Bucket序列化成[]byte,以作为与key对应的value插入到B+树中
	var value = bucket.write()

	// Insert into node.
	// 对key进行深拷贝，防止它引用的[]byte被GC回收
	key = cloneBytes(key)
	// 将bucket所对应的node数据结构放到游标所对应的node中。
	// 代表子Bucket的K/V插入到游标位置，其中Key是子Bucket的名字，Value是子Bucket的序列化结果
	c.node().put(key, key, value, 0, bucketLeafFlag)

	// Since subbuckets are not allowed on inline buckets, we need to
	// dereference the inline page, if it exists. This will cause the bucket
	// to be treated as a regular, non-inline bucket for the rest of the tx.
	// 将当前Bucket的page字段置空，因为当前Bucket已经包含了刚创建的子Bucket，它不会是inline bucket，而page字段代表inline bucket的页引用
	b.page = nil

	// 通过b.Bucket()方法按子Bucket的名字查找子Bucket并返回结果
	// todo:为什么这里并没有将刚创建的子Bucket直接返回而是通过查找的方式返回?
	// 为了缓存节点
	return b.Bucket(key), nil
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist and returns a reference to it.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (b *Bucket) CreateBucketIfNotExists(key []byte) (*Bucket, error) {
	child, err := b.CreateBucket(key)
	// bucket已经存在
	if err == ErrBucketExists {
		return b.Bucket(key), nil
	} else if err != nil {
		return nil, err
	}
	return child, nil
}

// DeleteBucket deletes a bucket at the given key.
// Returns an error if the bucket does not exists, or if the key represents a non-bucket value.
func (b *Bucket) DeleteBucket(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if bucket doesn't exist or is not a bucket.
	if !bytes.Equal(key, k) {
		return ErrBucketNotFound
	} else if (flags & bucketLeafFlag) == 0 {
		return ErrIncompatibleValue
	}

	// Recursively delete all child buckets.
	// 递归删除所有的子bucket
	child := b.Bucket(key)
	err := child.ForEach(func(k, v []byte) error {
		if v == nil {
			if err := child.DeleteBucket(k); err != nil {
				return fmt.Errorf("delete bucket: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	// Remove cached copy.
	delete(b.buckets, string(key))

	// Release all bucket pages to freelist.
	child.nodes = nil
	child.rootNode = nil
	child.free()

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// Get retrieves the value for a key in the bucket.
// Returns a nil value if the key does not exist or if the key is a nested bucket.
// The returned value is only valid for the life of the transaction.
func (b *Bucket) Get(key []byte) []byte {
	// 通过游标进行定位
	k, v, flags := b.Cursor().seek(key)

	// Return nil if this is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return nil
	}

	// If our target node isn't the same key as what's passed in then return nil.
	if !bytes.Equal(key, k) {
		return nil
	}
	return v
}

// Put sets the value for a key in the bucket.
// If the key exist then its previous value will be overwritten.
// Supplied value must remain valid for the life of the transaction.
// Returns an error if the bucket was created from a read-only transaction, if the key is blank, if the key is too large, or if the value is too large.
func (b *Bucket) Put(key []byte, value []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	} else if len(key) == 0 {
		return ErrKeyRequired
	} else if len(key) > MaxKeySize {
		return ErrKeyTooLarge
	} else if int64(len(value)) > MaxValueSize {
		return ErrValueTooLarge
	}

	// Move cursor to correct position.
	// 通过游标查找key所在的node，然后判断node类型，
	c := b.Cursor()
	k, _, flags := c.seek(key)

	// Return an error if there is an existing key with a bucket value.
	// 当欲插入的Key与当前Bucket中已有的一个子Bucket的Key相同时，会拒绝写入
	// 从而保护嵌套的子Bucket的引用不会被擦除，防止子Bucket变成孤儿。
	if bytes.Equal(key, k) && (flags&bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Insert into node.
	key = cloneBytes(key)
	// 将key，value写入到node中
	c.node().put(key, key, value, 0, 0)

	return nil
}

// Delete removes a key from the bucket.
// If the key does not exist then nothing is done and a nil error is returned.
// Returns an error if the bucket was created from a read-only transaction.
func (b *Bucket) Delete(key []byte) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Move cursor to correct position.
	c := b.Cursor()
	_, _, flags := c.seek(key)

	// Return an error if there is already existing bucket value.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}

	// Delete the node if we have a matching key.
	c.node().del(key)

	return nil
}

// Sequence returns the current integer for the bucket without incrementing it.
func (b *Bucket) Sequence() uint64 { return b.bucket.sequence }

// SetSequence updates the sequence number for the bucket.
func (b *Bucket) SetSequence(v uint64) error {
	if b.tx.db == nil {
		return ErrTxClosed
	} else if !b.Writable() {
		return ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence = v
	return nil
}

// NextSequence returns an autoincrementing integer for the bucket.
func (b *Bucket) NextSequence() (uint64, error) {
	if b.tx.db == nil {
		return 0, ErrTxClosed
	} else if !b.Writable() {
		return 0, ErrTxNotWritable
	}

	// Materialize the root node if it hasn't been already so that the
	// bucket will be saved during commit.
	if b.rootNode == nil {
		_ = b.node(b.root, nil)
	}

	// Increment and return the sequence.
	b.bucket.sequence++
	return b.bucket.sequence, nil
}

// ForEach executes a function for each key/value pair in a bucket.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller. The provided function must not modify
// the bucket; this will result in undefined behavior.
func (b *Bucket) ForEach(fn func(k, v []byte) error) error {
	if b.tx.db == nil {
		return ErrTxClosed
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}
// 看到这里

// Stat returns stats on a bucket.
func (b *Bucket) Stats() BucketStats {
	var s, subStats BucketStats
	pageSize := b.tx.db.pageSize
	s.BucketN += 1
	if b.root == 0 {
		s.InlineBucketN += 1
	}
	b.forEachPage(func(p *page, depth int) {
		if (p.flags & leafPageFlag) != 0 {
			s.KeyN += int(p.count)

			// used totals the used bytes for the page
			used := pageHeaderSize

			if p.count != 0 {
				// If page has any elements, add all element headers.
				used += leafPageElementSize * int(p.count-1)

				// Add all element key, value sizes.
				// The computation takes advantage of the fact that the position
				// of the last element's key/value equals to the total of the sizes
				// of all previous elements' keys and values.
				// It also includes the last element's header.
				lastElement := p.leafPageElement(p.count - 1)
				used += int(lastElement.pos + lastElement.ksize + lastElement.vsize)
			}

			if b.root == 0 {
				// For inlined bucket just update the inline stats
				s.InlineBucketInuse += used
			} else {
				// For non-inlined bucket update all the leaf stats
				s.LeafPageN++
				s.LeafInuse += used
				s.LeafOverflowN += int(p.overflow)

				// Collect stats from sub-buckets.
				// Do that by iterating over all element headers
				// looking for the ones with the bucketLeafFlag.
				for i := uint16(0); i < p.count; i++ {
					e := p.leafPageElement(i)
					if (e.flags & bucketLeafFlag) != 0 {
						// For any bucket element, open the element value
						// and recursively call Stats on the contained bucket.
						subStats.Add(b.openBucket(e.value()).Stats())
					}
				}
			}
		} else if (p.flags & branchPageFlag) != 0 {
			s.BranchPageN++
			lastElement := p.branchPageElement(p.count - 1)

			// used totals the used bytes for the page
			// Add header and all element headers.
			used := pageHeaderSize + (branchPageElementSize * int(p.count-1))

			// Add size of all keys and values.
			// Again, use the fact that last element's position equals to
			// the total of key, value sizes of all previous elements.
			used += int(lastElement.pos + lastElement.ksize)
			s.BranchInuse += used
			s.BranchOverflowN += int(p.overflow)
		}

		// Keep track of maximum page depth.
		if depth+1 > s.Depth {
			s.Depth = (depth + 1)
		}
	})

	// Alloc stats can be computed from page counts and pageSize.
	s.BranchAlloc = (s.BranchPageN + s.BranchOverflowN) * pageSize
	s.LeafAlloc = (s.LeafPageN + s.LeafOverflowN) * pageSize

	// Add the max depth of sub-buckets to get total nested depth.
	s.Depth += subStats.Depth
	// Add the stats for all sub-buckets
	s.Add(subStats)
	return s
}

// forEachPage iterates over every page in a bucket, including inline pages.
func (b *Bucket) forEachPage(fn func(*page, int)) {
	// If we have an inline page then just use that.
	if b.page != nil {
		fn(b.page, 0)
		return
	}

	// Otherwise traverse the page hierarchy.
	b.tx.forEachPage(b.root, 0, fn)
}

// forEachPageNode iterates over every page (or node) in a bucket.
// This also includes inline pages.
func (b *Bucket) forEachPageNode(fn func(*page, *node, int)) {
	// If we have an inline page or root node then just use that.
	if b.page != nil {
		fn(b.page, nil, 0)
		return
	}
	b._forEachPageNode(b.root, 0, fn)
}

func (b *Bucket) _forEachPageNode(pgid pgid, depth int, fn func(*page, *node, int)) {
	var p, n = b.pageNode(pgid)

	// Execute function.
	fn(p, n, depth)

	// Recursively loop over children.
	if p != nil {
		if (p.flags & branchPageFlag) != 0 {
			for i := 0; i < int(p.count); i++ {
				elem := p.branchPageElement(uint16(i))
				b._forEachPageNode(elem.pgid, depth+1, fn)
			}
		}
	} else {
		if !n.isLeaf {
			for _, inode := range n.inodes {
				b._forEachPageNode(inode.pgid, depth+1, fn)
			}
		}
	}
}

// spill writes all the nodes for this bucket to dirty pages.
// 分裂结点(spill):将大小超过page size * FillPercent的node分解为多个node
func (b *Bucket) spill() error {
	// Spill all child buckets first.
	for name, child := range b.buckets {
		// If the child bucket is small enough and it has no child buckets then
		// write it inline into the parent bucket's page. Otherwise spill it
		// like a normal bucket and make the parent value a pointer to the page.
		/*
		子Bucket不满足inlineable()条件时，如果子Bucket原来是一个内置Bucket，则它将通过spill()变成一个普通的Bucket，
		即它的B+Tree有一个根节点和至少两个叶子节点；如果子Bucket原本是一个普通Bucket，则spill()可能会更新它的根节点。
		 */
		var value []byte
		// 是否满足inlineable bucket的情形
		if child.inlineable() {
			// 如果符合，直接把子bucket写到value里面
			// 如果一个普通的子Bucket由于K/V记录减少而满足了inlineable()条件时，
			// 它将变成一个内置Bucket，即它的B+Tree只有一个根节点，并将根节点上的所有inodes作为Value写入父Bucket;
			child.free()
			value = child.write()
		} else {
			// 如果子Bucket不满足inlineable()条件时，如果子Bucket原来是一个内置Bucket，则它将通过spill()变成一个普通的Bucket
			// 即它的B+树有一个根节点和至少两个叶子节点；如果子Bucket原本是一个普通Bucket，则spill()可能会更新它的根节点。
			if err := child.spill(); err != nil {
				return err
			}

			// Update the child bucket header in this bucket.
			// value执行子Bucket
			value = make([]byte, unsafe.Sizeof(bucket{}))
			var bucket = (*bucket)(unsafe.Pointer(&value[0]))
			*bucket = *child.bucket
		}

		// Skip writing the bucket if there are no materialized nodes.
		if child.rootNode == nil {
			continue
		}

		// Update parent node.
		var c = b.Cursor()
		// 定位游标Cursor
		k, _, flags := c.seek([]byte(name))
		if !bytes.Equal([]byte(name), k) {
			panic(fmt.Sprintf("misplaced bucket header: %x -> %x", []byte(name), k))
		}
		if flags&bucketLeafFlag == 0 {
			panic(fmt.Sprintf("unexpected bucket header flag: %x", flags))
		}
		// 将子Bucket的新Value更新到父Bucket中
		c.node().put([]byte(name), []byte(name), value, 0, bucketLeafFlag)
	}

	// Ignore if there's not a materialized root node.
	if b.rootNode == nil {
		return nil
	}

	// Spill nodes.
	// 更新完子Bucket之后，开始spill自己，从rootNode开始
	if err := b.rootNode.spill(); err != nil {
		return err
	}
	// Bucket spill之后，其根节点可能有所变化，更新根节点的引用
	b.rootNode = b.rootNode.root()

	// Update the root node for this bucket.
	if b.rootNode.pgid >= b.tx.meta.pgid {
		panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", b.rootNode.pgid, b.tx.meta.pgid))
	}
	// 更新Bucket头中的根节点页号
	b.root = b.rootNode.pgid

	return nil
}

// inlineable returns true if a bucket is small enough to be written inline
// and if it contains no subbuckets. Otherwise returns false.
// 通常情况下，父bucket中只保存了subbucket的bucket header,每个subbucket都会占据至少一个page，
// 若subpage中的数据很少，会造成磁盘空间的浪费。inline bucket是将小的subbucket的值完整放在父bucket的
// leaf node上，从而减少page的个数。

// inline bucket的限制如下：
// 1. 没有subbucket；
// 2. 整个Bucket的大小不能超过page size / 4
func (b *Bucket) inlineable() bool {
	var n = b.rootNode

	// Bucket must only contain a single leaf node.
	if n == nil || !n.isLeaf {
		return false
	}

	// Bucket is not inlineable if it contains subbuckets or if it goes beyond
	// our threshold for inline bucket size.
	var size = pageHeaderSize
	for _, inode := range n.inodes {
		size += leafPageElementSize + len(inode.key) + len(inode.value)

		if inode.flags&bucketLeafFlag != 0 {
			return false
		} else if size > b.maxInlineBucketSize() {
			return false
		}
	}

	return true
}

// Returns the maximum total size of a bucket to make it a candidate for inlining.
func (b *Bucket) maxInlineBucketSize() int {
	return b.tx.db.pageSize / 4
}

// write allocates and writes a bucket to a byte slice.
// b.bucket转[]byte
// 将bucket的头部dump到page中
func (b *Bucket) write() []byte {
	// Allocate the appropriate size.
	var n = b.rootNode
	var value = make([]byte, bucketHeaderSize+n.size())

	// Write a bucket header.
	var bucket = (*bucket)(unsafe.Pointer(&value[0]))
	*bucket = *b.bucket

	// Convert byte slice to a fake page and write the root node.
	// 在b.rootNode中写入b.bucket
	// 从实现上看，内置Bucket就是将Bucket头和其根节点作为Value存于页框上，而其头中字段均为0。
	var p = (*page)(unsafe.Pointer(&value[bucketHeaderSize]))
	n.write(p)

	return value
}

// rebalance attempts to balance all nodes.
// 合并结点：会将任何删除过key且大小和key数量不满足要求的node和sibiling合并
func (b *Bucket) rebalance() {
	// 先对Bucket中缓存的node进行再平衡
	for _, n := range b.nodes {
		n.rebalance()
	}
	// 然后对所有子Bucket递归调用rebalance()
	for _, child := range b.buckets {
		child.rebalance()
	}
}

// node creates a node from a page and associates it with a given parent.
func (b *Bucket) node(pgid pgid, parent *node) *node {
	_assert(b.nodes != nil, "nodes map expected")

	// Retrieve node if it's already been created.
	// 先在bucket中查找，查找到了则直接返回pgid对应的node
	if n := b.nodes[pgid]; n != nil {
		return n
	}

	// Otherwise create a node and cache it.
	// 如果没找到，则进行初始化
	// 初始化bucket，还有parent
	n := &node{bucket: b, parent: parent}
	if parent == nil {
		b.rootNode = n
	} else {
		parent.children = append(parent.children, n)
	}

	// Use the inline page if this is an inline bucket.
	// 定位到对应的page上
	var p = b.page
	if p == nil {
		p = b.tx.page(pgid)
	}

	// Read the page into the node and cache it.
	// 把page的内容加载到内存中的node，并将其记录在bucket中的node信息中
	n.read(p)
	b.nodes[pgid] = n

	// Update statistics.
	b.tx.stats.NodeCount++

	return n
}

// free recursively frees all pages in the bucket.
func (b *Bucket) free() {
	if b.root == 0 {
		return
	}

	var tx = b.tx
	b.forEachPageNode(func(p *page, n *node, _ int) {
		if p != nil {
			tx.db.freelist.free(tx.meta.txid, p)
		} else {
			n.free()
		}
	})
	b.root = 0
}

// dereference removes all references to the old mmap.
func (b *Bucket) dereference() {
	if b.rootNode != nil {
		b.rootNode.root().dereference()
	}

	for _, child := range b.buckets {
		child.dereference()
	}
}

// pageNode returns the in-memory node, if it exists.
// Otherwise returns the underlying page.
// 根据pgid在Bucket里面查找出对应的底层page和node
// 根据pgid找到node或者page的过程
func (b *Bucket) pageNode(id pgid) (*page, *node) {
	// Inline buckets have a fake page embedded in their value so treat them
	// differently. We'll return the rootNode (if available) or the fake page.
	// inlinebucket的特殊处理
	if b.root == 0 {
		if id != 0 {
			panic(fmt.Sprintf("inline bucket non-zero page access(2): %d != 0", id))
		}
		if b.rootNode != nil {
			return nil, b.rootNode
		}
		return b.page, nil
	}

	// Check the node cache for non-inline buckets.
	// 从Bucket缓存的nodes中查找pgid对应的node，若有则返回node，page为空
	if b.nodes != nil {
		if n := b.nodes[id]; n != nil {
			return nil, n
		}
	}

	// Finally lookup the page from the transaction if no node is materialized.
	// 如果node没有构造出来，从事务中找到那个page
	return b.tx.page(id), nil
}

// BucketStats records statistics about resources used by a bucket.
// Bucket的一些统计信息
type BucketStats struct {
	// Page count statistics.
	BranchPageN     int // number of logical branch pages
	BranchOverflowN int // number of physical branch overflow pages
	LeafPageN       int // number of logical leaf pages
	LeafOverflowN   int // number of physical leaf overflow pages

	// Tree statistics.
	KeyN  int // number of keys/value pairs
	Depth int // number of levels in B+tree

	// Page size utilization.
	BranchAlloc int // bytes allocated for physical branch pages
	BranchInuse int // bytes actually used for branch data
	LeafAlloc   int // bytes allocated for physical leaf pages
	LeafInuse   int // bytes actually used for leaf data

	// Bucket statistics
	BucketN           int // total number of buckets including the top bucket
	InlineBucketN     int // total number on inlined buckets
	InlineBucketInuse int // bytes used for inlined buckets (also accounted for in LeafInuse)
}

func (s *BucketStats) Add(other BucketStats) {
	s.BranchPageN += other.BranchPageN
	s.BranchOverflowN += other.BranchOverflowN
	s.LeafPageN += other.LeafPageN
	s.LeafOverflowN += other.LeafOverflowN
	s.KeyN += other.KeyN
	if s.Depth < other.Depth {
		s.Depth = other.Depth
	}
	s.BranchAlloc += other.BranchAlloc
	s.BranchInuse += other.BranchInuse
	s.LeafAlloc += other.LeafAlloc
	s.LeafInuse += other.LeafInuse

	s.BucketN += other.BucketN
	s.InlineBucketN += other.InlineBucketN
	s.InlineBucketInuse += other.InlineBucketInuse
}

// cloneBytes returns a copy of a given slice.
func cloneBytes(v []byte) []byte {
	var clone = make([]byte, len(v))
	copy(clone, v)
	return clone
}
