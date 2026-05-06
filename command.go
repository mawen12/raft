package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
)

/*
type -> Command

目前有三种 Command:

	NOPCommand
	DefaultJoinCommand
	DefaultLeaveCommand
*/
var commandTypes map[string]Command

func init() {
	commandTypes = map[string]Command{}
}

// Command represents an action to be taken on the replicated state machine.

/*
Command 代表可以在复制状态机上执行的操作
*/
type Command interface {
	// 命令名称
	CommandName() string
}

// CommandApply represents the interface to apply a command to the server.
/*
CommandApply 代表将命令应用到 Context 的接口
*/
type CommandApply interface {
	Apply(Context) (interface{}, error)
}

// deprecatedCommandApply represents the old interface to apply a command to the server.
/*
deprecatedCommandApply 代表将命令应用到 Server 的老接口
*/
type deprecatedCommandApply interface {
	Apply(Server) (interface{}, error)
}

/*
CommandEncoder 命令编码/解码器
*/
type CommandEncoder interface {
	// 编码
	Encode(w io.Writer) error
	// 解码
	Decode(r io.Reader) error
}

// Creates a new instance of a command by name.
// newCommand 根据名称创建一个新的命令实例
func newCommand(name string, data []byte) (Command, error) {
	// Find the registered command.
	// 查找对应的命令类型
	command := commandTypes[name]
	// 没找到报错
	if command == nil {
		return nil, fmt.Errorf("raft.Command: Unregistered command type: %s", name)
	}

	// Make a copy of the command.
	// 创建 command 的拷贝
	v := reflect.New(reflect.Indirect(reflect.ValueOf(command)).Type()).Interface()
	copy, ok := v.(Command)
	if !ok {
		panic(fmt.Sprintf("raft: Unable to copy command: %s (%v)", command.CommandName(), reflect.ValueOf(v).Kind().String()))
	}

	// If data for the command was passed in the decode it.
	// 解码数据
	if data != nil {
		if encoder, ok := copy.(CommandEncoder); ok {
			if err := encoder.Decode(bytes.NewReader(data)); err != nil {
				return nil, err
			}
		} else {
			if err := json.NewDecoder(bytes.NewReader(data)).Decode(copy); err != nil {
				return nil, err
			}
		}
	}

	return copy, nil
}

// Registers a command by storing a reference to an instance of it.
func RegisterCommand(command Command) {
	if command == nil {
		panic(fmt.Sprintf("raft: Cannot register nil"))
	} else if commandTypes[command.CommandName()] != nil {
		panic(fmt.Sprintf("raft: Duplicate registration: %s", command.CommandName()))
	}
	commandTypes[command.CommandName()] = command
}
