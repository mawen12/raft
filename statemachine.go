package raft

// StateMachine is the interface for allowing the host application to save and
// recovery the state machine. This makes it possible to make snapshots
// and compact the log.

/*
StateMachine 是一个允许主机应用保存和恢复状态机的接口。
这使得创建 snapshot 和压缩日志成为可能。
*/
type StateMachine interface {
	// 保存状态机
	Save() ([]byte, error)
	// 恢复状态机
	Recovery([]byte) error
}
