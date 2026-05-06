package raft

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// Transporter is the interface for allowing the host application to transport
// requests to other nodes.

/*
Transporter 是一个允许主机应用发送请求到其他节点的接口，包含了该 server 不同状态下
与其他节点通信所涉及的所有 RPC
*/
type Transporter interface {
	// 当 Server#state=candidate时，发送投票请求，选举 leader
	SendVoteRequest(server Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse
	// 当 Server#state=leader时，同步 log entry 到 follower
	// 同时还被 leader 用于心跳检测，维护集群中的 leadership
	SendAppendEntriesRequest(server Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse
	// 当 Server#state=leader时，同步 log entry snapshot 到 follower
	SendSnapshotRequest(server Server, peer *Peer, req *SnapshotRequest) *SnapshotResponse
	//
	SendSnapshotRecoveryRequest(server Server, peer *Peer, req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse
}
