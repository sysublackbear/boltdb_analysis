// +build !windows,!plan9,!solaris

package bolt

import (
	"fmt"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// flock acquires an advisory lock on a file descriptor.
func flock(db *DB, mode os.FileMode, exclusive bool, timeout time.Duration) error {
	var t time.Time
	for {
		// If we're beyond our timeout then return an error.
		// This can only occur after we've attempted a flock once.
		if t.IsZero() {
			t = time.Now()
		} else if timeout > 0 && time.Since(t) > timeout {
			return ErrTimeout
		}
		flag := syscall.LOCK_SH
		if exclusive {
			flag = syscall.LOCK_EX
		}

		// Otherwise attempt to obtain an exclusive lock.
		err := syscall.Flock(int(db.file.Fd()), flag|syscall.LOCK_NB)
		if err == nil {
			return nil
		} else if err != syscall.EWOULDBLOCK {
			return err
		}

		// Wait for a bit and try again.
		time.Sleep(50 * time.Millisecond)
	}
}

// funlock releases an advisory lock on a file descriptor.
func funlock(db *DB) error {
	return syscall.Flock(int(db.file.Fd()), syscall.LOCK_UN)
}

// mmap memory maps a DB's data file.
// boltdb 没有实现 page cache,而是调用mmap()将整个文件映射进来，并调用madvise(MADV_RANDOM)由操作系统
// 管理page cache，后续对磁盘上文件的所有读操作直接读取db.data即可，简化了实现。

// 关于mmap && madvise 的联合使用
// mmap的作用是将硬盘文件的内容映射到内存中，采用闭链哈希建立的索引文件非常适合利用mmap的方式进行内存映射，
// 利用mmap返回的地址指针就是索引文件在内存中的首地址，这样我们就可以放心大胆的访问这些内容了。

// 使用过mmap映射文件的同学会发现一个问题，search程序访问对应的内存映射时，处理query的时间会有延时的突增，
// 究其原因是因为mmap只是建立了一个逻辑地址，linux的内存分配测试都是采用延迟分配的形式，
// 也就是只有你真正去访问时采用分配物理内存页，并与逻辑地址建立映射，这也就是我们常说的缺页中断。

// 缺页中断分为两类:
// 1.内存缺页中断，这种的代表是malloc，利用malloc分配的内存只有在程序访问到得时候，内存才会分配；
// 2.硬盘缺页中断，这种中断的代表就是mmap，利用mmap映射后的只是逻辑地址，当我们的程序访问时，
// 内核会将硬盘中的文件内容读进物理内存页中，这里我们就会明白为什么mmap之后，访问内存中的数据延时会陡增。

// 出现问题解决问题，上述情况出现的原因本质上是mmap映射文件之后，实际并没有加载到内存中，
// 要解决这个文件，需要我们进行索引的预加载，这里就会引出本文讲到的另一函数madvise，这个函数会传入一个地址指针，
// 已经一个区间长度，madvise会向内核提供一个针对于于地址区间的I/O的建议，内核可能会采纳这个建议，会做一些预读的操作。
// 例如MADV_SEQUENTIAL这个就表明顺序预读。

// 如果感觉这样还不给力，可以采用read操作，从mmap文件的首地址开始到最终位置，顺序的读取一遍，这样可以完全保证mmap后的数据全部load到内存中。

// 我们这里选择了使用madvise(b, syscall.MADV_RANDOM),是指类以随机顺序引用页，因此，预读比通常会很有用。
func mmap(db *DB, sz int) error {
	// Map the data file to memory.
	b, err := syscall.Mmap(int(db.file.Fd()), 0, sz, syscall.PROT_READ, syscall.MAP_SHARED|db.MmapFlags)
	if err != nil {
		return err
	}

	// Advise the kernel that the mmap is accessed randomly.
	if err := madvise(b, syscall.MADV_RANDOM); err != nil {
		return fmt.Errorf("madvise: %s", err)
	}

	// Save the original byte slice and convert to a byte array pointer.
	db.dataref = b
	db.data = (*[maxMapSize]byte)(unsafe.Pointer(&b[0]))
	db.datasz = sz
	return nil
}

// munmap unmaps a DB's data file from memory.
func munmap(db *DB) error {
	// Ignore the unmap if we have no mapped data.
	if db.dataref == nil {
		return nil
	}

	// Unmap using the original byte slice.
	err := syscall.Munmap(db.dataref)
	db.dataref = nil
	db.data = nil
	db.datasz = 0
	return err
}

// NOTE: This function is copied from stdlib because it is not available on darwin.
func madvise(b []byte, advice int) (err error) {
	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&b[0])), uintptr(len(b)), uintptr(advice))
	if e1 != 0 {
		err = e1
	}
	return
}
