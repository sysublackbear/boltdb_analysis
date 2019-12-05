package bolt

import (
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"
)

// txid represents the internal transaction identifier.
// 事务id
type txid uint64

// Tx represents a read-only or read/write transaction on the database.
// Read-only transactions can be used for retrieving values for keys and creating cursors.
// Read/write transactions can create and remove buckets and create and remove keys.
//
// IMPORTANT: You must commit or rollback transactions when you are done with
// them. Pages can not be reclaimed by the writer until no more transactions
// are using them. A long running read transaction can cause the database to
// quickly grow.
// boltdb的事务数据结构，在boltdb中，全部的对数据的操作必须发生在一个事务之内，boltdb的并发读取都在此实现
type Tx struct {
	writable       bool             // 指示是否是可读写的transaction
	managed        bool             // 指示当前transaction是否被db托管，即通过db.Update()或者db.View()来写或者读数据库。
	                                // 因为BoltDB还支持直接调用Tx的相关方法进行读写，这时managed字段为false;
	db             *DB              // 指向当前db对象
	meta           *meta            // transaction初始化时从db中数到的meta信息
	root           Bucket           // transaction的根Bucket,所有的transaction均从根Bucket开始进行查找
	pages          map[pgid]*page   // 当前transaction读或写的page
	stats          TxStats          // 与transaction操作统计相关
	commitHandlers []func()         // transaction在Commit时的回调函数

	// WriteFlag specifies the flag for write-related methods like WriteTo().
	// Tx opens the database file with the specified flag to copy the data.
	//
	// By default, the flag is unset, which works well for mostly in-memory
	// workloads. For databases that are much larger than available RAM,
	// set the flag to syscall.O_DIRECT to avoid trashing the page cache.
	WriteFlag int                   // 复制或移动数据库文件时，指定的文件打开模式
}

// init initializes the transaction.
func (tx *Tx) init(db *DB) {
	// 将tx.db初始化为传入的db，将tx.pages初始化为空
	tx.db = db
	tx.pages = nil

	// Copy the meta page since it can be changed by the writer.
	// 创建一个空的meta对象，并初始化为tx.meta，再将db中的meta复制到刚创建的meta对象中（对象拷贝而不是指针拷贝）
	tx.meta = &meta{}
	// db.meta()返回的meta相当于数据库最新的状态
	db.meta().copy(tx.meta)

	// Copy over the root bucket.
	// 创建一个Bucket，并将其设为根Bucket
	tx.root = newBucket(tx)  // 根据tx对象创建bucket
	tx.root.bucket = &bucket{}
	// 同时用meta中保存的根Bucket的头部来初始化transaction的根Bucket头部。
	/*
	在没有介绍Bucket之前，这里稍微有点不好理解，简单地说，Bucket包括头部(bucket)和一些正文字段，
	头部(bucket)中包括了Bucket的根节点所在的页的页号和一个序列号，
	所以tx.init()中对tx.root的初始化其实主要就是将meta中存的根Bucket(它也是整个db的根Bucket)的头部(bucket)拷贝给当前transaction的根Bucket
	 */
	*tx.root.bucket = tx.meta.root

	// Increment the transaction id and add a page cache for writable transactions.
	// 如果是可读写的transaction，就将meta中的txid加1，当可读写transaction commit之后，meta就会更新到数据库文件中，
	// 数据库的修改版本号就增加了。
	if tx.writable {
		tx.pages = make(map[pgid]*page)  // 再搞一个page cache
		tx.meta.txid += txid(1)  // txid自增1
	}
}

// ID returns the transaction id.
func (tx *Tx) ID() int {
	return int(tx.meta.txid)  // 专门弄了一个meta结构
}

// DB returns a reference to the database that created the transaction.
func (tx *Tx) DB() *DB {
	return tx.db
}

// Size returns current database size in bytes as seen by this transaction.
func (tx *Tx) Size() int64 {
	// Size() = tx.meta.pgid * tx.db.pageSize
	return int64(tx.meta.pgid) * int64(tx.db.pageSize)
}

// Writable returns whether the transaction can perform write operations.
func (tx *Tx) Writable() bool {
	// 判断事务是否支持写操作
	return tx.writable
}

// Cursor creates a cursor associated with the root bucket.
// All items in the cursor will return a nil value because all root bucket keys point to buckets.
// The cursor is only valid as long as the transaction is open.
// Do not use a cursor after the transaction is closed.
func (tx *Tx) Cursor() *Cursor {
	// todo:返回Bucket的Cursor，逻辑不太懂
	return tx.root.Cursor()
}

// Stats retrieves a copy of the current transaction statistics.
func (tx *Tx) Stats() TxStats {
	// stats数据
	return tx.stats
}

// Bucket retrieves a bucket by name.
// Returns nil if the bucket does not exist.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) Bucket(name []byte) *Bucket {
	return tx.root.Bucket(name)
}

// CreateBucket creates a new bucket.
// Returns an error if the bucket already exists, if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateBucket(name []byte) (*Bucket, error) {
	// todo:又是Bucket的操作
	// 通过tx的根Bucket来创建新Bucket的
	return tx.root.CreateBucket(name)
}

// CreateBucketIfNotExists creates a new bucket if it doesn't already exist.
// Returns an error if the bucket name is blank, or if the bucket name is too long.
// The bucket instance is only valid for the lifetime of the transaction.
func (tx *Tx) CreateBucketIfNotExists(name []byte) (*Bucket, error) {
	// todo:又是Bucket的操作
	return tx.root.CreateBucketIfNotExists(name)
}

// DeleteBucket deletes a bucket.
// Returns an error if the bucket cannot be found or if the key represents a non-bucket value.
func (tx *Tx) DeleteBucket(name []byte) error {
	// todo:又是Bucket的操作
	return tx.root.DeleteBucket(name)
}

// ForEach executes a function for each bucket in the root.
// If the provided function returns an error then the iteration is stopped and
// the error is returned to the caller.
func (tx *Tx) ForEach(fn func(name []byte, b *Bucket) error) error {
	// todo: Bucket操作
	return tx.root.ForEach(func(k, v []byte) error {
		if err := fn(k, tx.root.Bucket(k)); err != nil {
			return err
		}
		return nil
	})
}

// OnCommit adds a handler function to be executed after the transaction successfully commits.
func (tx *Tx) OnCommit(fn func()) {
	// 往tx.commitHandlers新增加操作函数fn
	tx.commitHandlers = append(tx.commitHandlers, fn)
}

// Commit writes all changes to disk and updates the meta page.
// Returns an error if a disk write error occurs, or if Commit is
// called on a read-only transaction.
// 1.从根Bucket开始，对访问过的Bucket进行合并和分裂，让进行过插入和删除操作的B+树重新达到平衡状态；
// 2.更新freeList页；
// 3.将由当前Transaction分配的页缓存写入磁盘，需要分配页缓存的地方有：a.节点分裂时产生新的节点；b.freeList页重新分配
// 4.将meta页写入磁盘
func (tx *Tx) Commit() error {
	// tx.managed必须为false
	_assert(!tx.managed, "managed tx commit not allowed")
	if tx.db == nil {
		return ErrTxClosed  // db对象为空
	} else if !tx.writable {
		return ErrTxNotWritable  // 事务不可写
	}

	// TODO(benbjohnson): Use vectorized I/O to write out dirty pages.

	// Rebalance nodes which have had deletions.
	var startTime = time.Now()
	// 对根Bucket进行再平衡，这里的根Bucket也是整个DB的根Bucket
	// 然而从根Bucket进行再平衡并不是对DB所有节点进行操作，而是对当前读写Transaction访问过的Bucket中的有删除操作的节点进行再平衡
	tx.root.rebalance()
	if tx.stats.Rebalance > 0 {
		tx.stats.RebalanceTime += time.Since(startTime)  // 更新rebalance操作的耗时
	}

	// spill data onto dirty pages.
	startTime = time.Now()
	// 对根Bucket进行溢出操作，同样地，也是对访问过的子Bucket进行溢出操作，而且只有当节点中key数量确实超限时才会分裂节点
	if err := tx.root.spill(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.SpillTime += time.Since(startTime)  // 更新spill操作的耗时

	// Free the old root bucket.
	// 在进行再旋转与分裂后，根Bucket的根节点可能发生了变化，因此将根Bucket的根节点的页号更新，且最终会写入DB的meta page
	tx.meta.root.root = tx.root.root

	opgid := tx.meta.pgid

	// Free the freelist and allocate new pages for it. This will overestimate
	// the size of the freelist but not underestimate the size (which would be bad).
	// 释放tx.db.freelist.free的txid为tx.meta.txid，到tx.db.page(tx.meta.freelist)
	// 更新db的freelist page，作了先释放后重新分配页框并写入的操作，因为在下面tx.write写磁盘的时候，只会向磁盘写入由当前Transaction
	// 分配并写入过的脏页，由于freelist最初是在db初始化过程中分配的页，如果不在transaction内部释放并重新分配，那么freelist Page将没有
	// 机会被更新到DB文件中。
	tx.db.freelist.free(tx.meta.txid, tx.db.page(tx.meta.freelist))
	// 分配回相应的页数
	p, err := tx.allocate((tx.db.freelist.size() / tx.db.pageSize) + 1)
	if err != nil {
		// 分配失败，回滚
		tx.rollback()
		return err
	}
	if err := tx.db.freelist.write(p); err != nil {
		// tx.db.freelist的内容写入p失败，回滚
		tx.rollback()
		return err
	}
	tx.meta.freelist = p.id

	// If the high water mark has moved up then attempt to grow the database.
	/*
	这里主要做这样的逻辑：只有当映射入内存的页数增加时，才调用db.grow()来刷新磁盘文件的元数据，以及时更新文件大小信息。
	这里需要解释一下: 我们前面介绍过，windows平台下db.mmap()调用会通过ftruncate系统调用来增加文件大小，
	而linux平台则没有，但linux平台会在db.grow()中调用ftruncate更新文件大小。
	我们前面介绍过，BoltDB写数据时不是通过mmap内存映射写文件的，而是直接通过fwrite和fdatesync系统调用向文件系统写文件。
	当向文件写入数据时，文件系统上该文件结点的元数据可能不会立即刷新，导致文件的size不会立即更新，
	当进程crash时，可能会出现写文件结束但文件大小没有更新的情况，所以为了防止这种情况出现，
	在写DB文件之前，无论是windows还是linux平台，都会通过ftruncate系统调用来增加文件大小；
	但是linux平台为什么不在每次mmap的时候调用ftruncate来更新文件大小呢？
	这里是一个优化措施，因为频繁地ftruncate系统调用会影响性能，这里的优化方式是: 只有当：
	1) 重新分配freeListPage时，没有空闲页，这里大家可能会有疑惑，freeListPage不是刚刚通过freelist的free()方法释放过它所占用的页吗，
	还会有重新分配时没有空闲页的情况吗？实际上，free()过并不是真正释放页，而是将页标记为pending，
	要等创建下一次读写Transaction时才会被真正回收(大家可以查看freeist的free()和release()以及DB的beginRWTx()方法中最后一节代码了解具体逻辑)；
	2) remmap的长度大于文件实际大小时，才会调用ftruncate来增加文件大小，且当映射文件大小大于16M后，每次增加文件大小时会比实际需要的文件大小多增加16M。
	这里的优化比较隐晦
	 */
	if tx.meta.pgid > opgid {
		if err := tx.db.grow(int(tx.meta.pgid+1) * tx.db.pageSize); err != nil {
			tx.rollback()
			return err
		}
	}

	// Write dirty pages to disk.
	// 将当前Transaction分配的脏页写入磁盘
	startTime = time.Now()
	if err := tx.write(); err != nil {
		tx.rollback()
		return err
	}

	// If strict mode is enabled then perform a consistency check.
	// Only the first consistency error is reported in the panic.
	if tx.db.StrictMode {
		// 一致性检查
		ch := tx.Check()
		var errs []string
		for {
			err, ok := <-ch
			if !ok {
				break
			}
			errs = append(errs, err.Error())
		}
		// 等待接收结果
		if len(errs) > 0 {
			panic("check fail: " + strings.Join(errs, "\n"))
		}
	}

	// Write meta to disk.
	// 把meta数据写到磁盘上面
	// 将当前transaction的meta写入DB的meta页，因为进行读写操作后，meta中的txid已经改变，root、freelist和pgid也有可能已经更新了
	// 将脏页写入磁盘后，tx.Commit()随后将meta写入磁盘
	if err := tx.writeMeta(); err != nil {
		tx.rollback()
		return err
	}
	tx.stats.WriteTime += time.Since(startTime)  // 记录整个事务写的耗时

	// Finalize the transaction.
	// 关闭当前Transaction，清空相关字段
	tx.close()

	// Execute commit handlers now that the locks have been removed.
	// 回调执行commitHandlers
	for _, fn := range tx.commitHandlers {
		fn()  // 执行事务提交后的commitHandlers
	}

	return nil
}

// Rollback closes the transaction and ignores all previous updates. Read-only
// transactions must be rolled back and not committed.
func (tx *Tx) Rollback() error {
	// tx.managed必须为false
	_assert(!tx.managed, "managed tx rollback not allowed")
	if tx.db == nil {
		return ErrTxClosed
	}
	tx.rollback()
	return nil
}

func (tx *Tx) rollback() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		tx.db.freelist.rollback(tx.meta.txid)  // 从freelist中删除这个txid对应的所有pgid
		tx.db.freelist.reload(tx.db.page(tx.db.meta().freelist))  // 重新刷新freelist
	}
	tx.close()
}

func (tx *Tx) close() {
	if tx.db == nil {
		return
	}
	if tx.writable {
		// Grab freelist stats.
		var freelistFreeN = tx.db.freelist.free_count()
		var freelistPendingN = tx.db.freelist.pending_count()
		var freelistAlloc = tx.db.freelist.size()

		// Remove transaction ref & writer lock.
		tx.db.rwtx = nil
		tx.db.rwlock.Unlock()  // 读写锁解锁

		// Merge statistics.
		// 记录Stats
		tx.db.statlock.Lock()
		tx.db.stats.FreePageN = freelistFreeN
		tx.db.stats.PendingPageN = freelistPendingN
		tx.db.stats.FreeAlloc = (freelistFreeN + freelistPendingN) * tx.db.pageSize
		tx.db.stats.FreelistInuse = freelistAlloc
		tx.db.stats.TxStats.add(&tx.stats)
		tx.db.statlock.Unlock()
	} else {
		// todo:db操作，待会再看
		tx.db.removeTx(tx)
	}

	// Clear all references.
	// 清除所有的引用
	tx.db = nil
	tx.meta = nil
	tx.root = Bucket{tx: tx}
	tx.pages = nil
}

// Copy writes the entire database to a writer.
// This function exists for backwards compatibility.
//
// Deprecated; Use WriteTo() instead.
func (tx *Tx) Copy(w io.Writer) error {
	_, err := tx.WriteTo(w)  // 把内容写到w句柄里
	return err
}

// WriteTo writes the entire database to a writer.
// If err == nil then exactly tx.Size() bytes will be written into the writer.
func (tx *Tx) WriteTo(w io.Writer) (n int64, err error) {
	// Attempt to open reader with WriteFlag
	// 打开一个文件
	f, err := os.OpenFile(tx.db.path, os.O_RDONLY|tx.WriteFlag, 0)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Generate a meta page. We use the same page data for both meta pages.
	// 生成一个meta page
	buf := make([]byte, tx.db.pageSize)
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = metaPageFlag
	*page.meta() = *tx.meta  // 把tx.meta的内容写到page中

	// Write meta 0.
	page.id = 0
	page.meta().checksum = page.meta().sum64()
	nn, err := w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 0 copy: %s", err)
	}

	// Write meta 1 with a lower transaction id.
	page.id = 1
	page.meta().txid -= 1
	page.meta().checksum = page.meta().sum64()
	nn, err = w.Write(buf)
	n += int64(nn)
	if err != nil {
		return n, fmt.Errorf("meta 1 copy: %s", err)
	}

	// Move past the meta pages in the file.
	// 先写两页meta page
	if _, err := f.Seek(int64(tx.db.pageSize*2), os.SEEK_SET); err != nil {
		return n, fmt.Errorf("seek: %s", err)
	}

	// Copy data pages.
	wn, err := io.CopyN(w, f, tx.Size()-int64(tx.db.pageSize*2))
	n += wn
	if err != nil {
		return n, err
	}

	return n, f.Close()
}

// CopyFile copies the entire database to file at the given path.
// A reader transaction is maintained during the copy so it is safe to continue
// using the database while a copy is in progress.
func (tx *Tx) CopyFile(path string, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return err
	}

	err = tx.Copy(f)
	if err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// Check performs several consistency checks on the database for this transaction.
// An error is returned if any inconsistency is found.
//
// It can be safely run concurrently on a writable transaction. However, this
// incurs a high cost for large databases and databases with a lot of subbuckets
// because of caching. This overhead can be removed if running on a read-only
// transaction, however, it is not safe to execute other writer transactions at
// the same time.
func (tx *Tx) Check() <-chan error {
	ch := make(chan error)
	go tx.check(ch)
	return ch
}

// todo:这里的检查细则需要回头再看
func (tx *Tx) check(ch chan error) {
	// Check if any pages are double freed.
	freed := make(map[pgid]bool)
	all := make([]pgid, tx.db.freelist.count())
	tx.db.freelist.copyall(all)  // tx.db.freelist的内容转移到all
	for _, id := range all {
		if freed[id] {
			ch <- fmt.Errorf("page %d: already freed", id)
		}
		freed[id] = true
	}

	// Track every reachable page.
	reachable := make(map[pgid]*page)
	reachable[0] = tx.page(0) // meta0
	reachable[1] = tx.page(1) // meta1
	for i := uint32(0); i <= tx.page(tx.meta.freelist).overflow; i++ {
		reachable[tx.meta.freelist+pgid(i)] = tx.page(tx.meta.freelist)
	}

	// Recursively check buckets.
	// 递归检查buckets
	tx.checkBucket(&tx.root, reachable, freed, ch)

	// Ensure all pages below high water mark are either reachable or freed.
	for i := pgid(0); i < tx.meta.pgid; i++ {
		_, isReachable := reachable[i]
		if !isReachable && !freed[i] {  // 不可达且未被释放
			ch <- fmt.Errorf("page %d: unreachable unfreed", int(i))
		}
	}

	// Close the channel to signal completion.
	close(ch)
}

func (tx *Tx) checkBucket(b *Bucket, reachable map[pgid]*page, freed map[pgid]bool, ch chan error) {
	// Ignore inline buckets.
	if b.root == 0 {
		return
	}

	// Check every page used by this bucket.
	b.tx.forEachPage(b.root, 0, func(p *page, _ int) {
		if p.id > tx.meta.pgid {
			ch <- fmt.Errorf("page %d: out of bounds: %d", int(p.id), int(b.tx.meta.pgid))
		}

		// Ensure each page is only referenced once.
		for i := pgid(0); i <= pgid(p.overflow); i++ {
			var id = p.id + i
			if _, ok := reachable[id]; ok {
				ch <- fmt.Errorf("page %d: multiple references", int(id))
			}
			reachable[id] = p
		}

		// We should only encounter un-freed leaf and branch pages.
		if freed[p.id] {
			ch <- fmt.Errorf("page %d: reachable freed", int(p.id))
		} else if (p.flags&branchPageFlag) == 0 && (p.flags&leafPageFlag) == 0 {
			ch <- fmt.Errorf("page %d: invalid type: %s", int(p.id), p.typ())
		}
	})

	// Check each bucket within this bucket.
	_ = b.ForEach(func(k, v []byte) error {
		if child := b.Bucket(k); child != nil {
			tx.checkBucket(child, reachable, freed, ch)
		}
		return nil
	})
}

// allocate returns a contiguous block of memory starting at a given page.
func (tx *Tx) allocate(count int) (*page, error) {
	p, err := tx.db.allocate(count)
	if err != nil {
		return nil, err
	}

	// Save to our page cache.
	// 这里分配的页缓存将被加入到tx的pages字段
	tx.pages[p.id] = p

	// Update statistics.
	tx.stats.PageCount++
	tx.stats.PageAlloc += count * tx.db.pageSize

	return p, nil
}

// write writes any dirty pages to disk.
func (tx *Tx) write() error {
	// Sort pages by id.
	// 将当前tx中的脏页引用保存到本地slice变量中，并释放原来的引用。请注意，Tx对象并不是线程安全的，
	// 而接下来的写文件操作会比较耗时，此时应该避免tx.pages被修改。
	pages := make(pages, 0, len(tx.pages))
	for _, p := range tx.pages {
		pages = append(pages, p)
	}
	// Clear out page cache early.
	tx.pages = make(map[pgid]*page)
	// 对页按其pgid排序，保证在随后按页顺序写文件中,一定程度上提高写文件效率
	sort.Sort(pages)  // map[pgid]*page转[]pages

	// Write pages to disk in order.
	// 开始将各页循环写入文件，循环体中的代码通过fwrite系统调用写文件
	for _, p := range pages {
		size := (int(p.overflow) + 1) * tx.db.pageSize
		offset := int64(p.id) * int64(tx.db.pageSize)

		// Write out page in "max allocation" sized chunks.
		ptr := (*[maxAllocSize]byte)(unsafe.Pointer(p))
		for {
			// Limit our write to our max allocation size.
			sz := size
			if sz > maxAllocSize-1 {
				sz = maxAllocSize - 1
			}

			// Write chunk to disk.
			buf := ptr[:sz]
			if _, err := tx.db.ops.writeAt(buf, offset); err != nil {
				return err
			}

			// Update statistics.
			tx.stats.Write++

			// Exit inner for loop if we've written all the chunks.
			size -= sz
			if size == 0 {
				break
			}

			// Otherwise move offset forward and move pointer to next chunk.
			offset += int64(sz)
			ptr = (*[maxAllocSize]byte)(unsafe.Pointer(&ptr[sz]))
		}
	}

	// Ignore file sync if flag is set on DB.
	if !tx.db.NoSync || IgnoreNoSync {
		// 需要同步落盘
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// Put small pages back to page pool.
	for _, p := range pages {  // 把小的页对象(p.overflow == 0)放回sync.Pool
		// Ignore page sizes over 1 page.
		// These are allocated using make() instead of the page pool.
		if int(p.overflow) != 0 {
			continue
		}

		buf := (*[maxAllocSize]byte)(unsafe.Pointer(p))[:tx.db.pageSize]

		// See https://go.googlesource.com/go/+/f03c9202c43e0abb130669852082117ca50aa9b1
		for i := range buf {
			buf[i] = 0
		}
		tx.db.pagePool.Put(buf)
	}

	return nil
}

// writeMeta writes the meta to the disk.
func (tx *Tx) writeMeta() error {
	// Create a temporary buffer for the meta page.
	// 先创建一个临时分配的页缓存
	buf := make([]byte, tx.db.pageSize)
	p := tx.db.pageInBuffer(buf, 0)
	// 写入序列化后的meta页
	tx.meta.write(p)

	// Write the meta page to file.
	if _, err := tx.db.ops.writeAt(buf, int64(p.id)*int64(tx.db.pageSize)); err != nil {
		return err
	}
	if !tx.db.NoSync || IgnoreNoSync {
		if err := fdatasync(tx.db); err != nil {
			return err
		}
	}

	// Update statistics.
	tx.stats.Write++

	return nil
}

// page returns a reference to the page with a given id.
// If page has been written to then a temporary buffered page is returned.
func (tx *Tx) page(id pgid) *page {
	// Check the dirty pages first.
	// 先在脏页中查找

	// 再从Bucket所属的Transaction申请过的page的集合(Tx的pages字段)中查找，有则返回该page，node为空；
	if tx.pages != nil {
		if p, ok := tx.pages[id]; ok {
			return p
		}
	}

	// Otherwise return directly from the mmap.
	// 直接返回共享内存数据
	// 若Transaction中的page缓存中仍然没有，则直接从DB的内存映射中查找该页，并返回
	return tx.db.page(id)
}

// forEachPage iterates over every page within a given page and executes a function.
func (tx *Tx) forEachPage(pgid pgid, depth int, fn func(*page, int)) {
	p := tx.page(pgid)

	// Execute function.
	fn(p, depth)

	// Recursively loop over children.
	if (p.flags & branchPageFlag) != 0 {
		for i := 0; i < int(p.count); i++ {
			elem := p.branchPageElement(uint16(i))
			tx.forEachPage(elem.pgid, depth+1, fn)
		}
	}
}

// Page returns page information for a given page number.
// This is only safe for concurrent use when used by a writable transaction.
// 给定id，返回PageInfo，人为可读的page结构
func (tx *Tx) Page(id int) (*PageInfo, error) {
	if tx.db == nil {
		return nil, ErrTxClosed
	} else if pgid(id) >= tx.meta.pgid {
		return nil, nil
	}

	// Build the page info.
	p := tx.db.page(pgid(id))
	info := &PageInfo{
		ID:            id,
		Count:         int(p.count),
		OverflowCount: int(p.overflow),
	}

	// Determine the type (or if it's free).
	if tx.db.freelist.freed(pgid(id)) {
		info.Type = "free"
	} else {
		info.Type = p.typ()
	}

	return info, nil
}

// TxStats represents statistics about the actions performed by the transaction.
type TxStats struct {
	// Page statistics.
	PageCount int // number of page allocations
	PageAlloc int // total bytes allocated

	// Cursor statistics.
	CursorCount int // number of cursors created

	// Node statistics
	NodeCount int // number of node allocations
	NodeDeref int // number of node dereferences

	// Rebalance statistics.
	Rebalance     int           // number of node rebalances
	RebalanceTime time.Duration // total time spent rebalancing

	// Split/Spill statistics.
	Split     int           // number of nodes split
	Spill     int           // number of nodes spilled
	SpillTime time.Duration // total time spent spilling

	// Write statistics.
	Write     int           // number of writes performed
	WriteTime time.Duration // total time spent writing to disk
}

func (s *TxStats) add(other *TxStats) {
	s.PageCount += other.PageCount
	s.PageAlloc += other.PageAlloc
	s.CursorCount += other.CursorCount
	s.NodeCount += other.NodeCount
	s.NodeDeref += other.NodeDeref
	s.Rebalance += other.Rebalance
	s.RebalanceTime += other.RebalanceTime
	s.Split += other.Split
	s.Spill += other.Spill
	s.SpillTime += other.SpillTime
	s.Write += other.Write
	s.WriteTime += other.WriteTime
}

// Sub calculates and returns the difference between two sets of transaction stats.
// This is useful when obtaining stats at two different points and time and
// you need the performance counters that occurred within that time span.
func (s *TxStats) Sub(other *TxStats) TxStats {
	var diff TxStats
	diff.PageCount = s.PageCount - other.PageCount
	diff.PageAlloc = s.PageAlloc - other.PageAlloc
	diff.CursorCount = s.CursorCount - other.CursorCount
	diff.NodeCount = s.NodeCount - other.NodeCount
	diff.NodeDeref = s.NodeDeref - other.NodeDeref
	diff.Rebalance = s.Rebalance - other.Rebalance
	diff.RebalanceTime = s.RebalanceTime - other.RebalanceTime
	diff.Split = s.Split - other.Split
	diff.Spill = s.Spill - other.Spill
	diff.SpillTime = s.SpillTime - other.SpillTime
	diff.Write = s.Write - other.Write
	diff.WriteTime = s.WriteTime - other.WriteTime
	return diff
}
