package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	"code.google.com/p/gogoprotobuf/proto"
	"github.com/goraft/raft/protobuf"
)

// A log entry stores a single item in the log.
// LogEntry 代表保存在 log 中的单个 entry
type LogEntry struct {
	pb *protobuf.LogEntry
	// 其位于 log 中的位置，也可以被称为 offset
	Position int64 // position in the log file
	// 所属的 log
	log   *Log
	event *ev
}

// Creates a new log entry associated with a log.
// newLogEntry 创建一个与 log 相关的 entry
func newLogEntry(log *Log, event *ev, index uint64, term uint64, command Command) (*LogEntry, error) {
	var buf bytes.Buffer
	var commandName string

	if command != nil {
		// 读取命名名称
		commandName = command.CommandName()
		if encoder, ok := command.(CommandEncoder); ok { // 使用 CommandEncoder 编码器
			if err := encoder.Encode(&buf); err != nil {
				return nil, err
			}
		} else { // 反之使用 json 编码器
			if err := json.NewEncoder(&buf).Encode(command); err != nil {
				return nil, err
			}
		}
	}

	// 构造支持 probobuf 的 LogEntry，
	pb := &protobuf.LogEntry{
		Index:       proto.Uint64(index),
		Term:        proto.Uint64(term),
		CommandName: proto.String(commandName),
		Command:     buf.Bytes(), // 以字节形式保存命令内容
	}

	e := &LogEntry{
		pb:    pb,
		log:   log,
		event: event,
	}

	return e, nil
}

// Index 返回 log entry 的 Index
func (e *LogEntry) Index() uint64 {
	return e.pb.GetIndex()
}

// Term 返回 log entry 的 Term
func (e *LogEntry) Term() uint64 {
	return e.pb.GetTerm()
}

// CommandName 返回 log entry 的 Command 的名称
func (e *LogEntry) CommandName() string {
	return e.pb.GetCommandName()
}

// Command 返回 log entry 的 Command
func (e *LogEntry) Command() []byte {
	return e.pb.GetCommand()
}

// Encodes the log entry to a buffer. Returns the number of bytes
// written and any error that may have occurred.
// Encode 将 log entry 编码到指定的 w ，并返回写入的字节数
// 使用长度前缀+换行符+数据体的方式，其中长度前缀的宽度至少为8个字符
func (e *LogEntry) Encode(w io.Writer) (int, error) {
	// 对内容进行序列化
	b, err := proto.Marshal(e.pb)
	if err != nil {
		return -1, err
	}

	// 先把 protobuf 序列化后的字节长度以 16 进制输出
	// 宽度最少8个字符，不足会左侧补空格
	if _, err = fmt.Fprintf(w, "%8x\n", len(b)); err != nil {
		return -1, err
	}

	// 写入真实的字节内容
	return w.Write(b)
}

// Decodes the log entry from a buffer. Returns the number of bytes read and
// any error that occurs.
// Decode 从 r 中解析指定的 log entry，返回读取的字节数和发生的错误
func (e *LogEntry) Decode(r io.Reader) (int, error) {

	var length int
	// 首先读取长度
	_, err := fmt.Fscanf(r, "%8x\n", &length)
	if err != nil {
		return -1, err
	}

	// 按照长度构造数组
	data := make([]byte, length)
	// 从 r 中读取数组大小的字节数组
	_, err = io.ReadFull(r, data)

	if err != nil {
		return -1, err
	}

	// 反编码
	if err = proto.Unmarshal(data, e.pb); err != nil {
		return -1, err
	}

	// 
	return length + 8 + 1, nil
}
