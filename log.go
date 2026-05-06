package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/goraft/raft/protobuf"
)

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A log is a collection of log entries that are persisted to durable storage.

/*
Log 是 log entry 的集合，并且会被持久化到磁盘。
使用了读写锁保证并发场景下的安全性。
*/
type Log struct {
	ApplyFunc func(*LogEntry, Command) (interface{}, error)
	// 底层实际存储的 log entry 的文件
	file *os.File
	// 文件的路径
	path string
	// 其中所有的 log entry
	entries []*LogEntry
	// 最后提交的索引
	commitIndex uint64
	mutex       sync.RWMutex
	// log entry 中首个 entry 之前的索引，当出现 log 压缩时，该值为被压缩的最后一个日志的索引
	startIndex uint64 // the index before the first entry in the Log entries
	// log entry 中首个 entry 之前的任期，当出现 log 压缩时，该值为被压缩的最后一个日志的任期
	startTerm uint64
	// 表示是否完成从 log 读取 entry 的操作
	initialized bool
}

// The results of the applying a log entry.
type logResult struct {
	returnValue interface{}
	err         error
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new log.
// newLog 创建一个新的 Log
func newLog() *Log {
	return &Log{
		entries: make([]*LogEntry, 0),
	}
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

//--------------------------------------
// Log Indices
//--------------------------------------

// The last committed index in the log.
// CommitIndex 返回日志最后提交的索引
func (l *Log) CommitIndex() uint64 {
	// 使用读锁
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	return l.commitIndex
}

// The current index in the log.
// currentIndex 返回日志当前的索引
func (l *Log) currentIndex() uint64 {
	// 使用读锁
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	// 返回日志当前的索引（无锁）
	return l.internalCurrentIndex()
}

// The current index in the log without locking
// internalCurrentIndex 返回日志当前的索引（无锁）
func (l *Log) internalCurrentIndex() uint64 {
	// 检查是否存在 log entry
	if len(l.entries) == 0 { // 没有的话，返回 startIndex
		return l.startIndex
	}
	// 读取最后一条 log entry 的 Index
	return l.entries[len(l.entries)-1].Index()
}

// The next index in the log.
// nextIndex 返回该 log 的下一个 entry 的索引
func (l *Log) nextIndex() uint64 {
	return l.currentIndex() + 1
}

// Determines if the log contains zero entries.
// isEmpty 检查该 log 逻辑上是否没有任何 entry
func (l *Log) isEmpty() bool {
	// 使用读锁
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	// len(l.entries) == 0 代表当前 log 没有任何 entry
	// l.startIndex == 0 代表 log 没有被压缩过，这就是第一条 log
	return (len(l.entries) == 0) && (l.startIndex == 0)
}

// The name of the last command in the log.
// lastCommandName 返回 log 中最后的命令名称，也是最新的
func (l *Log) lastCommandName() string {
	// 使用读锁
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if len(l.entries) > 0 {
		// 读取最后一条 entry
		if entry := l.entries[len(l.entries)-1]; entry != nil {
			// 返回 CommandName
			return entry.CommandName()
		}
	}
	return ""
}

//--------------------------------------
// Log Terms
//--------------------------------------

// The current term in the log.
// currentTerm 返回 log 最新的任期
func (l *Log) currentTerm() uint64 {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// 检查是否存在 log entry
	if len(l.entries) == 0 { // 没有的话，返回 startTerm
		return l.startTerm
	}
	// 读取最后一条 log entry 的 Term
	return l.entries[len(l.entries)-1].Term()
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// State
//--------------------------------------

// Opens the log file and reads existing entries. The log can remain open and
// continue to append entries to the end of the log.
// open 打开 log 文件，并读取其中的 entry。log 可以保持开启状态，以便在文件末尾追加 entry。
func (l *Log) open(path string) error {
	// Read all the entries from the log if one exists.
	var readBytes int64

	var err error
	debugln("log.open.open ", path)
	// open log file
	// 打开文件
	l.file, err = os.OpenFile(path, os.O_RDWR, 0600)
	l.path = path

	// 处理文件打开错误的场景
	if err != nil {
		// if the log file does not exist before
		// we create the log file and set commitIndex to 0
		// 如果文件不存在，则创建文件，然后结束，因为没有任何内容可以读取
		if os.IsNotExist(err) {
			l.file, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0600)
			debugln("log.open.create ", path)
			if err == nil {
				l.initialized = true
			}
			return err
		}
		return err
	}
	debugln("log.open.exist ", path)

	// Read the file and decode entries.
	// 读取文件并解码 entry
	for {
		// Instantiate log entry and decode into it.
		// 初始化一个 log entry
		entry, _ := newLogEntry(l, nil, 0, 0, nil)
		// 将文件位置写入 Position
		entry.Position, _ = l.file.Seek(0, os.SEEK_CUR)

		// 从文件中读取一条 log entry
		n, err := entry.Decode(l.file)
		if err != nil {
			if err == io.EOF {
				debugln("open.log.append: finish ")
			} else {
				if err = os.Truncate(path, readBytes); err != nil {
					return fmt.Errorf("raft.Log: Unable to recover: %v", err)
				}
			}
			break
		}

		// 检查读取的 entry 的 Index 超过了前一个 entry 的 index
		if entry.Index() > l.startIndex { // 代表是一个合法的 entry
			// Append entry.
			// 将读取的 entry 追加到内存中
			l.entries = append(l.entries, entry)
			// 检查读取的 entry 的 Index 是否已经提交过了
			if entry.Index() <= l.commitIndex { // 已经提交过了
				// 使用该 entry 构造一个 command
				command, err := newCommand(entry.CommandName(), entry.Command())
				if err != nil {
					continue
				}
				// 执行命令，本质上是分发 commit 事件，执行 command#Apply，将命令应用到状态机
				l.ApplyFunc(entry, command)
			}
			debugln("open.log.append log index ", entry.Index())
		}

		// 累加读取的字节数
		readBytes += int64(n)
	}
	debugln("open.log.recovery number of log ", len(l.entries))
	// 初始化完成标识
	l.initialized = true
	return nil
}

// Closes the log file.
// close 关闭 log 文件
func (l *Log) close() {
	// 使用写锁
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// 关闭文件
	if l.file != nil {
		l.file.Close()
		l.file = nil
	}
	// 清空内存中的 entries
	l.entries = make([]*LogEntry, 0)
}

// sync to disk
// sync 将 log 同步刷新到磁盘
func (l *Log) sync() error {
	return l.file.Sync()
}

//--------------------------------------
// Entries
//--------------------------------------

// Creates a log entry associated with this log.
func (l *Log) createEntry(term uint64, command Command, e *ev) (*LogEntry, error) {
	return newLogEntry(l, e, l.nextIndex(), term, command)
}

// Retrieves an entry from the log. If the entry has been eliminated because
// of a snapshot then nil is returned.
func (l *Log) getEntry(index uint64) *LogEntry {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	if index <= l.startIndex || index > (l.startIndex+uint64(len(l.entries))) {
		return nil
	}
	return l.entries[index-l.startIndex-1]
}

// Checks if the log contains a given index/term combination.
func (l *Log) containsEntry(index uint64, term uint64) bool {
	entry := l.getEntry(index)
	return (entry != nil && entry.Term() == term)
}

// Retrieves a list of entries after a given index as well as the term of the
// index provided. A nil list of entries is returned if the index no longer
// exists because a snapshot was made.
func (l *Log) getEntriesAfter(index uint64, maxLogEntriesPerRequest uint64) ([]*LogEntry, uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// Return nil if index is before the start of the log.
	if index < l.startIndex {
		traceln("log.entriesAfter.before: ", index, " ", l.startIndex)
		return nil, 0
	}

	// Return an error if the index doesn't exist.
	if index > (uint64(len(l.entries)) + l.startIndex) {
		panic(fmt.Sprintf("raft: Index is beyond end of log: %v %v", len(l.entries), index))
	}

	// If we're going from the beginning of the log then return the whole log.
	if index == l.startIndex {
		traceln("log.entriesAfter.beginning: ", index, " ", l.startIndex)
		return l.entries, l.startTerm
	}

	traceln("log.entriesAfter.partial: ", index, " ", l.entries[len(l.entries)-1].Index)

	entries := l.entries[index-l.startIndex:]
	length := len(entries)

	traceln("log.entriesAfter: startIndex:", l.startIndex, " length", len(l.entries))

	if uint64(length) < maxLogEntriesPerRequest {
		// Determine the term at the given entry and return a subslice.
		return entries, l.entries[index-1-l.startIndex].Term()
	} else {
		return entries[:maxLogEntriesPerRequest], l.entries[index-1-l.startIndex].Term()
	}
}

//--------------------------------------
// Commit
//--------------------------------------

// Retrieves the last index and term that has been committed to the log.
func (l *Log) commitInfo() (index uint64, term uint64) {
	l.mutex.RLock()
	defer l.mutex.RUnlock()
	// If we don't have any committed entries then just return zeros.
	if l.commitIndex == 0 {
		return 0, 0
	}

	// No new commit log after snapshot
	if l.commitIndex == l.startIndex {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term from the last committed entry.
	debugln("commitInfo.get.[", l.commitIndex, "/", l.startIndex, "]")
	entry := l.entries[l.commitIndex-1-l.startIndex]
	return entry.Index(), entry.Term()
}

// Retrieves the last index and term that has been appended to the log.
// 返回日志中最新的 entry 的 index 和 term
func (l *Log) lastInfo() (index uint64, term uint64) {
	// 申请读锁
	l.mutex.RLock()
	defer l.mutex.RUnlock()

	// If we don't have any entries then just return zeros.
	// 检查是否有 entries，没有则使用初始值
	if len(l.entries) == 0 {
		return l.startIndex, l.startTerm
	}

	// Return the last index & term
	// 返回最新的 entry 的 index 和 term
	entry := l.entries[len(l.entries)-1]
	return entry.Index(), entry.Term()
}

// Updates the commit index
func (l *Log) updateCommitIndex(index uint64) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	if index > l.commitIndex {
		l.commitIndex = index
	}
	debugln("update.commit.index ", index)
}

// Updates the commit index and writes entries after that index to the stable storage.
// setCommitIndex 将上次 commit 和本地要 commit 之间的 index 提交到 log，并通知到对应的状态机
func (l *Log) setCommitIndex(index uint64) error {
	// 申请写锁
	l.mutex.Lock()
	defer l.mutex.Unlock()

	// this is not error any more after limited the number of sending entries
	// commit up to what we already have
	// 检查 index 是否合法
	if index > l.startIndex+uint64(len(l.entries)) { // 超过了当前已有数据的 index
		debugln("raft.Log: Commit index", index, "set back to ", len(l.entries))
		// 修正为最后一条
		index = l.startIndex + uint64(len(l.entries))
	}

	// Do not allow previous indices to be committed again.

	// This could happens, since the guarantee is that the new leader has up-to-dated
	// log entries rather than has most up-to-dated committed index

	// For example, Leader 1 send log 80 to follower 2 and follower 3
	// follower 2 and follow 3 all got the new entries and reply
	// leader 1 committed entry 80 and send reply to follower 2 and follower3
	// follower 2 receive the new committed index and update committed index to 80
	// leader 1 fail to send the committed index to follower 3
	// follower 3 promote to leader (server 1 and server 2 will vote, since leader 3
	// has up-to-dated the entries)
	// when new leader 3 send heartbeat with committed index = 0 to follower 2,
	// follower 2 should reply success and let leader 3 update the committed index to 80

	// 已提交了的不再提交，幂等
	if index < l.commitIndex {
		return nil
	}

	// Find all entries whose index is between the previous index and the current index.
	// 找到上一次提交和本次提交之前的 entries
	for i := l.commitIndex + 1; i <= index; i++ {
		entryIndex := i - 1 - l.startIndex
		entry := l.entries[entryIndex]

		// Update commit index.
		// 更新 commitIndex
		l.commitIndex = entry.Index()

		// Decode the command.
		// 反序列化为命令
		command, err := newCommand(entry.CommandName(), entry.Command())
		if err != nil {
			return err
		}

		// Apply the changes to the state machine and store the error code.
		// 执行命令，应用变更到状态机
		returnValue, err := l.ApplyFunc(entry, command)

		debugf("setCommitIndex.set.result index: %v, entries index: %v", i, entryIndex)
		// 事件写入返回值和错误
		if entry.event != nil {
			entry.event.returnValue = returnValue
			entry.event.c <- err
		}

		// 检查是否为 join 命令，如果是则直接跳过后续处理
		_, isJoinCommand := command.(JoinCommand)

		// we can only commit up to the most recent join command
		// if there is a join in this batch of commands.
		// after this commit, we need to recalculate the majority.
		// 如果这批命令中包含 join 命令，那么只能提交到该命令处
		// 完成该命令后，需要重新计算多数派
		if isJoinCommand {
			return nil
		}
	}
	return nil
}

// Set the commitIndex at the head of the log file to the current
// commit Index. This should be called after obtained a log lock
func (l *Log) flushCommitIndex() {
	l.file.Seek(0, os.SEEK_SET)
	fmt.Fprintf(l.file, "%8x\n", l.commitIndex)
	l.file.Seek(0, os.SEEK_END)
}

//--------------------------------------
// Truncation
//--------------------------------------

// Truncates the log to the given index and term. This only works if the log
// at the index has not been committed.
// truncate 将 log 截断到给定的 index 和 term，需要同时操作 file 和 entries(内存)
func (l *Log) truncate(index uint64, term uint64) error {
	// 申请写锁
	l.mutex.Lock()
	defer l.mutex.Unlock()
	debugln("log.truncate: ", index)

	// Do not allow committed entries to be truncated.
	// 已提交的 entry 不允许被截断
	if index < l.commitIndex {
		debugln("log.truncate.before")
		return fmt.Errorf("raft.Log: Index is already committed (%v): (IDX=%v, TERM=%v)", l.commitIndex, index, term)
	}

	// Do not truncate past end of entries.
	// 超过了长度的 index 无法被截断
	if index > l.startIndex+uint64(len(l.entries)) {
		debugln("log.truncate.after")
		return fmt.Errorf("raft.Log: Entry index does not exist (MAX=%v): (IDX=%v, TERM=%v)", len(l.entries), index, term)
	}

	// If we're truncating everything then just clear the entries.
	// 检查是否为 startIndex
	if index == l.startIndex { // 其为 startIndex，代表清空整个 entry
		debugln("log.truncate.clear")
		l.file.Truncate(0)
		// 置为0
		l.file.Seek(0, os.SEEK_SET)

		// notify clients if this node is the previous leader
		//
		for _, entry := range l.entries {
			if entry.event != nil {
				entry.event.c <- errors.New("command failed to be committed due to node failure")
			}
		}

		l.entries = []*LogEntry{}
	} else { // 需要按需截断
		// Do not truncate if the entry at index does not have the matching term.
		// 将逻辑的 index 映射到实际的数组 index上，并获取前一个位置的 entry
		entry := l.entries[index-l.startIndex-1]
		// 检查 entries 是否有值，且 leader 任期是否一致
		if len(l.entries) > 0 && entry.Term() != term { // entries 不同，或任期不一致
			debugln("log.truncate.termMismatch")
			return fmt.Errorf("raft.Log: Entry at index does not have matching term (%v): (IDX=%v, TERM=%v)", entry.Term(), index, term)
		}

		// Otherwise truncate up to the desired entry.
		// 检查 index 是否合法
		if index < l.startIndex+uint64(len(l.entries)) { // 合法
			debugln("log.truncate.finish")
			// 获取该位置的 entry 的文件中的 offset
			position := l.entries[index-l.startIndex].Position
			// 将位置截取到该位置，丢弃之后的 entry
			l.file.Truncate(position)
			l.file.Seek(position, os.SEEK_SET)

			// notify clients if this node is the previous leader
			// 对于内存中之后的 entry，进行通知
			for i := index - l.startIndex; i < uint64(len(l.entries)); i++ {
				entry := l.entries[i]
				if entry.event != nil {
					entry.event.c <- errors.New("command failed to be committed due to node failure")
				}
			}

			// 截断内存中之后的 entry
			l.entries = l.entries[0 : index-l.startIndex]
		}
	}

	return nil
}

//--------------------------------------
// Append
//--------------------------------------

// Appends a series of entries to the log.
// appendEntries 将 entries 追加到日志中
func (l *Log) appendEntries(entries []*protobuf.LogEntry) error {
	// 申请写锁
	l.mutex.Lock()
	defer l.mutex.Unlock()

	startPosition, _ := l.file.Seek(0, os.SEEK_CUR)

	w := bufio.NewWriter(l.file)

	var size int64
	var err error
	// Append each entry but exit if we hit an error.
	// 遍历待 append 的 entries
	for i := range entries {
		// 更新其中的 log 以及 Position 信息
		logEntry := &LogEntry{
			log:      l,
			Position: startPosition,
			pb:       entries[i],
		}

		// 写入 entry
		if size, err = l.writeEntry(logEntry, w); err != nil {
			return err
		}

		startPosition += size
	}
	w.Flush()
	err = l.sync()

	if err != nil {
		panic(err)
	}

	return nil
}

// Writes a single log entry to the end of the log.
func (l *Log) appendEntry(entry *LogEntry) error {
	l.mutex.Lock()
	defer l.mutex.Unlock()

	if l.file == nil {
		return errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	if len(l.entries) > 0 {
		lastEntry := l.entries[len(l.entries)-1]
		if entry.Term() < lastEntry.Term() {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		} else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() {
			return fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}
	}

	position, _ := l.file.Seek(0, os.SEEK_CUR)

	entry.Position = position

	// Write to storage.
	if _, err := entry.Encode(l.file); err != nil {
		return err
	}

	// Append to entries list if stored on disk.
	l.entries = append(l.entries, entry)

	return nil
}

// appendEntry with Buffered io
// writeEntry 将 entry 写入文件和内存
func (l *Log) writeEntry(entry *LogEntry, w io.Writer) (int64, error) {
	// 文件不存在，报错
	if l.file == nil {
		return -1, errors.New("raft.Log: Log is not open")
	}

	// Make sure the term and index are greater than the previous.
	// 内存中 entries 有值了
	if len(l.entries) > 0 {
		// 读取最后一条
		lastEntry := l.entries[len(l.entries)-1]

		if entry.Term() < lastEntry.Term() { // 该 entry 的任期不能小于上一条
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		} else if entry.Term() == lastEntry.Term() && entry.Index() <= lastEntry.Index() { // 当该 entry 的任期和上一个的任期一致时，其 index 不能小于上一条
			return -1, fmt.Errorf("raft.Log: Cannot append entry with earlier index in the same term (%x:%x <= %x:%x)", entry.Term(), entry.Index(), lastEntry.Term(), lastEntry.Index())
		}
	}

	// Write to storage.
	// 将 entry 写入文件
	size, err := entry.Encode(w)
	if err != nil {
		return -1, err
	}

	// Append to entries list if stored on disk.
	// 将 entry 写入内存
	l.entries = append(l.entries, entry)

	return int64(size), nil
}

//--------------------------------------
// Log compaction
//--------------------------------------

// compact the log before index (including index)
func (l *Log) compact(index uint64, term uint64) error {
	var entries []*LogEntry

	l.mutex.Lock()
	defer l.mutex.Unlock()

	if index == 0 {
		return nil
	}
	// nothing to compaction
	// the index may be greater than the current index if
	// we just recovery from on snapshot
	if index >= l.internalCurrentIndex() {
		entries = make([]*LogEntry, 0)
	} else {
		// get all log entries after index
		entries = l.entries[index-l.startIndex:]
	}

	// create a new log file and add all the entries
	new_file_path := l.path + ".new"
	file, err := os.OpenFile(new_file_path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		position, _ := l.file.Seek(0, os.SEEK_CUR)
		entry.Position = position

		if _, err = entry.Encode(file); err != nil {
			file.Close()
			os.Remove(new_file_path)
			return err
		}
	}
	file.Sync()

	old_file := l.file

	// rename the new log file
	err = os.Rename(new_file_path, l.path)
	if err != nil {
		file.Close()
		os.Remove(new_file_path)
		return err
	}
	l.file = file

	// close the old log file
	old_file.Close()

	// compaction the in memory log
	l.entries = entries
	l.startIndex = index
	l.startTerm = term
	return nil
}
