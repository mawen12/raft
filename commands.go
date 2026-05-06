package raft

import (
	"io"
)

// Join command interface
/* 代表新 server 加入集群的命令接口 */
type JoinCommand interface {
	Command
	NodeName() string
}

// Join command
type DefaultJoinCommand struct {
	// server name
	Name string `json:"name"`
	// server 的连接地址
	ConnectionString string `json:"connectionString"`
}

// Leave command interface
/* 代表有 sever 退出集群的命令接口 */
type LeaveCommand interface {
	Command
	// server name
	NodeName() string
}

// Leave command
type DefaultLeaveCommand struct {
	// server name
	Name string `json:"name"`
}

// NOP command
type NOPCommand struct {
}

// The name of the Join command in the log
func (c *DefaultJoinCommand) CommandName() string {
	return "raft:join"
}

func (c *DefaultJoinCommand) Apply(server Server) (interface{}, error) {
	// 将新的 node 添加到 server.peers 中
	err := server.AddPeer(c.Name, c.ConnectionString)

	return []byte("join"), err
}

func (c *DefaultJoinCommand) NodeName() string {
	return c.Name
}

// The name of the Leave command in the log
func (c *DefaultLeaveCommand) CommandName() string {
	return "raft:leave"
}

func (c *DefaultLeaveCommand) Apply(server Server) (interface{}, error) {
	// 将 node 从 server.peers 中移除
	err := server.RemovePeer(c.Name)

	return []byte("leave"), err
}
func (c *DefaultLeaveCommand) NodeName() string {
	return c.Name
}

// The name of the NOP command in the log
func (c NOPCommand) CommandName() string {
	return "raft:nop"
}

func (c NOPCommand) Apply(server Server) (interface{}, error) {
	// NOP
	return nil, nil
}

func (c NOPCommand) Encode(w io.Writer) error {
	return nil
}

func (c NOPCommand) Decode(r io.Reader) error {
	return nil
}
