package raft

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"time"
)

// uint64Slice implements sort interface
type uint64Slice []uint64

func (p uint64Slice) Len() int           { return len(p) }
func (p uint64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p uint64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// WriteFile writes data to a file named by filename.
// If the file does not exist, WriteFile creates it with permissions perm;
// otherwise WriteFile truncates it before writing.
// This is copied from ioutil.WriteFile with the addition of a Sync call to
// ensure the data reaches the disk.

/*
writeFileSynced 将 data 同步写入到 filename 代表的文件中。

如果文件不存在，将按照 perm 权限来创建文件。
否则在写入之前对文件进行截断。
该方法从 ioutil.WriteFile 复制而来，其中增加了同步调用，以确保数据直接写入磁盘。
*/
func writeFileSynced(filename string, data []byte, perm os.FileMode) error {
	// 打开文件，如果不存在，则创建；否则对其进行截断
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close() // Idempotent 原子

	// 写入 data
	n, err := f.Write(data)

	// 检查写入错误和写入数据长度
	if err == nil && n < len(data) { // 写入成功，但是未全部写入
		return io.ErrShortWrite
	} else if err != nil { // 写入错误
		return err
	}

	// 同步刷新到文件中
	if err = f.Sync(); err != nil { // 刷入磁盘失败
		return err
	}

	// 关闭文件
	return f.Close()
}

// Waits for a random time between two durations and sends the current time on
// the returned channel.

// 在 min~max 两个间隔间生成一个随机数，并以 channel 方式返回
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	// 以当前时间为种子创建随机数
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 { 
		// 创建随机数
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	// 返回一个未来时间戳
	return time.After(d)
}

// TODO(xiangli): Remove assertions when we reach version 1.0

// _assert will panic with a given formatted message if the given condition is false.
func _assert(condition bool, msg string, v ...interface{}) {
	if !condition {
		panic(fmt.Sprintf("assertion failed: "+msg, v...))
	}
}
