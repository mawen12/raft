package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"sync"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

// 代表 server 可能的状态
const (
	// 代表 server 已关闭
	Stopped = "stopped"
	// 代表 server 刚启动，正在初始化
	Initialized = "initialized"
	// 代表该 cluster 中已经有了一个 leader
	Follower = "follower"
	// 代表该 cluster 原先的 leader 已经故障，需要原先的 follower 进行投票选举出新的 leader
	Candidate = "candidate"
	// 代表该 server 为当前 cluster 中的唯一 leader
	Leader       = "leader"
	Snapshotting = "snapshotting"
)

const (
	MaxLogEntriesPerRequest         = 2000
	NumberOfLogEntriesAfterSnapshot = 200
)

const (
	// DefaultHeartbeatInterval is the interval that the leader will send
	// AppendEntriesRequests to followers to maintain leadership.

	// leader 发送到 follower 的 AppendEntriesRequests RPC 的请求间隔
	DefaultHeartbeatInterval = 50 * time.Millisecond

	// 选举超时的间隔
	DefaultElectionTimeout = 150 * time.Millisecond
)

// ElectionTimeoutThresholdPercent specifies the threshold at which the server
// will dispatch warning events that the heartbeat RTT is too close to the
// election timeout.
const ElectionTimeoutThresholdPercent = 0.8

//------------------------------------------------------------------------------
//
// Errors
//
//------------------------------------------------------------------------------

var NotLeaderError = errors.New("raft.Server: Not current leader")
var DuplicatePeerError = errors.New("raft.Server: Duplicate peer")
var CommandTimeoutError = errors.New("raft: Command timeout")
var StopError = errors.New("raft: Has been stopped")

//------------------------------------------------------------------------------
//
// Typedefs
//
//------------------------------------------------------------------------------

// A server is involved in the consensus protocol and can act as a follower,
// candidate or a leader.

/*
Server 参与共识协议，可充当 follower/candidate/leader。
*/
type Server interface {
	// 名称
	Name() string
	// 上下文
	Context() interface{}
	// 状态机
	StateMachine() StateMachine
	Leader() string
	// 当前状态，为 follower/candidate/leader 之一
	State() string
	Path() string
	LogPath() string
	SnapshotPath(lastIndex uint64, lastTerm uint64) string
	Term() uint64
	CommitIndex() uint64
	VotedFor() string
	MemberCount() int
	QuorumSize() int
	IsLogEmpty() bool
	LogEntries() []*LogEntry
	LastCommandName() string
	GetState() string
	// 选举超时间隔
	ElectionTimeout() time.Duration
	// 设置选举超时间隔
	SetElectionTimeout(duration time.Duration)
	// 心跳间隔
	HeartbeatInterval() time.Duration
	// 设置心跳间隔
	SetHeartbeatInterval(duration time.Duration)
	// 底层发送 HTTP 请求的传输层
	Transporter() Transporter
	// 设置 HTTP 传输层
	SetTransporter(t Transporter)

	AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse
	RequestVote(req *RequestVoteRequest) *RequestVoteResponse
	RequestSnapshot(req *SnapshotRequest) *SnapshotResponse
	SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse

	// 添加集群成员
	AddPeer(name string, connectiongString string) error
	// 移除集群成员
	RemovePeer(name string) error
	// 返回所有的集群成员
	Peers() map[string]*Peer
	// 初始化
	Init() error
	// 启动
	Start() error
	// 停止
	Stop()
	// 是否运行中
	Running() bool
	// 执行命令
	Do(command Command) (interface{}, error)
	TakeSnapshot() error
	LoadSnapshot() error
	AddEventListener(string, EventListener)
	FlushCommitIndex()
}

type server struct {
	// 支持注册事件监听器、事件分发
	*eventDispatcher

	name        string
	path        string
	state       string
	transporter Transporter
	context     interface{}
	currentTerm uint64

	votedFor   string
	log        *Log
	leader     string
	peers      map[string]*Peer
	mutex      sync.RWMutex
	syncedPeer map[string]bool

	stopped chan bool
	// event channel，处理投递过来的事件
	c                 chan *ev
	electionTimeout   time.Duration
	heartbeatInterval time.Duration

	snapshot *Snapshot

	// PendingSnapshot is an unfinished snapshot.
	// After the pendingSnapshot is saved to disk,
	// it will be set to snapshot and also will be
	// set to nil.
	pendingSnapshot *Snapshot

	stateMachine            StateMachine
	maxLogEntriesPerRequest uint64

	connectionString string

	routineGroup sync.WaitGroup
}

// An internal event to be processed by the server's event loop.
type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan error
}

//------------------------------------------------------------------------------
//
// Constructor
//
//------------------------------------------------------------------------------

// Creates a new server with a log at the given path. transporter must
// not be nil. stateMachine can be nil if snapshotting and log
// compaction is to be disabled. context can be anything (including nil)
// and is not used by the raft package except returned by
// Server.Context(). connectionString can be anything.

// NewServer 使用给定的日志路径创建一个新的 server。参数 transporter 不能为空。
// 如果禁用了 snapshot 和 log 压缩，那么 stateMachine 可以为空。
// ctx 不做限制，并且其没有被 raft 使用，除了作为 Server.Context() 方法返回以外。
// connectionString 不做限制。
func NewServer(name string, path string, transporter Transporter, stateMachine StateMachine, ctx interface{}, connectionString string) (Server, error) {
	// name 不能为空
	if name == "" {
		return nil, errors.New("raft.Server: Name cannot be blank")
	}
	// transporter 不能为空
	if transporter == nil {
		panic("raft: Transporter required")
	}

	s := &server{
		name:                    name,
		path:                    path,
		transporter:             transporter,
		stateMachine:            stateMachine,
		context:                 ctx,
		state:                   Stopped,
		peers:                   make(map[string]*Peer),
		log:                     newLog(),
		c:                       make(chan *ev, 256),      // 至多允许 256 个事件，满了之后会阻塞
		electionTimeout:         DefaultElectionTimeout,   // 默认 150ms
		heartbeatInterval:       DefaultHeartbeatInterval, // 默认 50ms
		maxLogEntriesPerRequest: MaxLogEntriesPerRequest,  // 默认 2000
		connectionString:        connectionString,
	}

	// 创建一个 source = server 的事件分发器
	s.eventDispatcher = newEventDispatcher(s)

	// Setup apply function.
	// 1.分发 commit 事件
	// 2.执行 command#Apply
	s.log.ApplyFunc = func(e *LogEntry, c Command) (interface{}, error) {
		// Dispatch commit event.
		// 分发一个 commit 事件，写入 command
		s.DispatchEvent(newEvent(CommitEventType, e, nil))

		// Apply command to the state machine.
		// 将命令应用到状态机
		switch c := c.(type) {
		case CommandApply:
			return c.Apply(&context{
				server:       s,
				currentTerm:  s.currentTerm,
				currentIndex: s.log.internalCurrentIndex(),
				commitIndex:  s.log.commitIndex,
			})
		case deprecatedCommandApply:
			return c.Apply(s)
		default:
			return nil, fmt.Errorf("Command does not implement Apply()")
		}
	}

	return s, nil
}

//------------------------------------------------------------------------------
//
// Accessors
//
//------------------------------------------------------------------------------

//--------------------------------------
// General
//--------------------------------------

// Retrieves the name of the server.
func (s *server) Name() string {
	return s.name
}

// Retrieves the storage path for the server.
func (s *server) Path() string {
	return s.path
}

// The name of the current leader.
func (s *server) Leader() string {
	return s.leader
}

// Retrieves a copy of the peer data.
func (s *server) Peers() map[string]*Peer {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	peers := make(map[string]*Peer)
	for name, peer := range s.peers {
		peers[name] = peer.clone()
	}
	return peers
}

// Retrieves the object that transports requests.
func (s *server) Transporter() Transporter {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.transporter
}

func (s *server) SetTransporter(t Transporter) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.transporter = t
}

// Retrieves the context passed into the constructor.
func (s *server) Context() interface{} {
	return s.context
}

// Retrieves the state machine passed into the constructor.
func (s *server) StateMachine() StateMachine {
	return s.stateMachine
}

// Retrieves the log path for the server.
func (s *server) LogPath() string {
	return path.Join(s.path, "log")
}

// Retrieves the current state of the server.
func (s *server) State() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.state
}

// Sets the state of the server.
func (s *server) setState(state string) {
	// 申请写锁
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// 保存之前的 state 和 leader
	// Temporarily store previous values.
	prevState := s.state
	prevLeader := s.leader

	// 更新新的 state 和 leader
	// Update state and leader.
	s.state = state
	if state == Leader { // 如果自己是 leader，则使用自己的名称，并构造同步 peer
		s.leader = s.Name()
		s.syncedPeer = make(map[string]bool)
	}

	// Dispatch state and leader change events.
	// 分发 stateChange 事件
	s.DispatchEvent(newEvent(StateChangeEventType, s.state, prevState))

	// 分发 leaderChange 事件
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

// Retrieves the current term of the server.
func (s *server) Term() uint64 {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.currentTerm
}

// Retrieves the current commit index of the server.
func (s *server) CommitIndex() uint64 {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.commitIndex
}

// Retrieves the name of the candidate this server voted for in this term.
func (s *server) VotedFor() string {
	return s.votedFor
}

// Retrieves whether the server's log has no entries.
func (s *server) IsLogEmpty() bool {
	return s.log.isEmpty()
}

// A list of all the log entries. This should only be used for debugging purposes.
func (s *server) LogEntries() []*LogEntry {
	s.log.mutex.RLock()
	defer s.log.mutex.RUnlock()
	return s.log.entries
}

// A reference to the command name of the last entry.
func (s *server) LastCommandName() string {
	return s.log.lastCommandName()
}

// Get the state of the server for debugging
func (s *server) GetState() string {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return fmt.Sprintf("Name: %s, State: %s, Term: %v, CommitedIndex: %v ", s.name, s.state, s.currentTerm, s.log.commitIndex)
}

// Check if the server is promotable
// promotable 检查服务器是否处于可晋升状态
func (s *server) promotable() bool {
	return s.log.currentIndex() > 0
}

//--------------------------------------
// Membership
//--------------------------------------

// Retrieves the number of member servers in the consensus.
func (s *server) MemberCount() int {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return len(s.peers) + 1
}

// Retrieves the number of servers required to make a quorum.
func (s *server) QuorumSize() int {
	return (s.MemberCount() / 2) + 1
}

//--------------------------------------
// Election timeout
//--------------------------------------

// Retrieves the election timeout.
func (s *server) ElectionTimeout() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.electionTimeout
}

// Sets the election timeout.
func (s *server) SetElectionTimeout(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.electionTimeout = duration
}

//--------------------------------------
// Heartbeat timeout
//--------------------------------------

// Retrieves the heartbeat timeout.
func (s *server) HeartbeatInterval() time.Duration {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	return s.heartbeatInterval
}

// Sets the heartbeat timeout.
func (s *server) SetHeartbeatInterval(duration time.Duration) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	s.heartbeatInterval = duration
	for _, peer := range s.peers {
		peer.setHeartbeatInterval(duration)
	}
}

//------------------------------------------------------------------------------
//
// Methods
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

// Reg the NOPCommand
func init() {
	RegisterCommand(&NOPCommand{})
	RegisterCommand(&DefaultJoinCommand{})
	RegisterCommand(&DefaultLeaveCommand{})
}

// Start the raft server
// If log entries exist then allow promotion to candidate if no AEs received.
// If no log entries exist then wait for AEs from another node.
// If no log entries exist and a self-join command is issued then
// immediately become leader and commit entry.
// 启动 raft 服务器
func (s *server) Start() error {
	// Exit if the server is already running.
	// 重复启动，会退出当前的启动操作，并返回错误
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	// 执行初始化操作
	if err := s.Init(); err != nil {
		return err
	}

	// stopped needs to be allocated each time server starts
	// because it is closed at `Stop`.
	// 创建 stop channel，支持异步关闭
	s.stopped = make(chan bool)
	// 初始状态为 follower
	s.setState(Follower)

	// If no log entries exist then
	// 1. wait for AEs from another node
	// 2. wait for self-join command
	// to set itself promotable
	//
	if !s.promotable() {
		s.debugln("start as a new raft server")

		// If log entries exist then allow promotion to candidate
		// if no AEs received.
	} else {
		s.debugln("start from previous saved state")
	}

	debugln(s.GetState())

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.loop()
	}()

	return nil
}

// Init initializes the raft server.
// If there is no previous log file under the given path, Init() will create an empty log file.
// Otherwise, Init() will load in the log entries from the log file.
// Init 初始化 raft server，创建 snapshot 目录，读取配置，打开 log
func (s *server) Init() error {
	// 重复初始化，返回错误
	if s.Running() {
		return fmt.Errorf("raft.Server: Server already running[%v]", s.state)
	}

	// Server has been initialized or server was stopped after initialized
	// If log has been initialized, we know that the server was stopped after
	// running.

	// 重复初始化，或已经走到 log 初始化完成的步骤，则无需重复触发
	if s.state == Initialized || s.log.initialized {
		s.state = Initialized
		return nil
	}

	// Create snapshot directory if it does not exist
	// 创建 snpshot 目录
	err := os.Mkdir(path.Join(s.path, "snapshot"), 0700)
	if err != nil && !os.IsExist(err) {
		s.debugln("raft: Snapshot dir error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// 读取 server 配置，将 commitIndex 更新 log
	if err := s.readConf(); err != nil {
		s.debugln("raft: Conf file error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Initialize the log and load it up.
	// 打开 log 文件，并读取其中的 entry。log 可以保持开启状态，以便在文件末尾追加 entry。
	if err := s.log.open(s.LogPath()); err != nil {
		s.debugln("raft: Log error: ", err)
		return fmt.Errorf("raft: Initialization error: %s", err)
	}

	// Update the term to the last term in the log.
	// 读取日志中最新的 term,
	_, s.currentTerm = s.log.lastInfo()

	// 状态变为 state
	s.state = Initialized
	return nil
}

// Shuts down the server.
func (s *server) Stop() {
	if s.State() == Stopped {
		return
	}

	close(s.stopped)

	// make sure all goroutines have stopped before we close the log
	s.routineGroup.Wait()

	s.log.close()
	s.setState(Stopped)
}

// Checks if the server is currently running.
// 检查当前 server 是否已经启动
func (s *server) Running() bool {
	// 申请读锁
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	// 状态即不为 stopped，也不为 Initialized
	return (s.state != Stopped && s.state != Initialized)
}

//--------------------------------------
// Term
//--------------------------------------

// updates the current term for the server. This is only used when a larger
// external term is found.
// updateCurrentTerm 更新当前 server 所属 leader 的任期信息
func (s *server) updateCurrentTerm(term uint64, leaderName string) {
	// 新的 leader 的 term 必须要比当前任期要大，因为 term 是自增的
	_assert(term > s.currentTerm,
		"upadteCurrentTerm: update is called when term is not larger than currentTerm")

	// 保存之前的 leader 信息
	// Store previous values temporarily.
	prevTerm := s.currentTerm
	prevLeader := s.leader

	// set currentTerm = T, convert to follower (§5.1)
	// stop heartbeats before step-down
	if s.state == Leader { // 当前为 leader
		for _, peer := range s.peers {
			peer.stopHeartbeat(false) // 停止心跳检测
		}
	}
	// update the term and clear vote for
	if s.state != Follower { // 当前为 candidate，降级为 follower
		s.setState(Follower) //
	}

	// 申请写锁，更新 leader 的任期信息，并清空投票
	s.mutex.Lock()
	s.currentTerm = term
	s.leader = leaderName
	s.votedFor = ""
	s.mutex.Unlock()

	// Dispatch change events.
	// 分发 termChange 事件
	s.DispatchEvent(newEvent(TermChangeEventType, s.currentTerm, prevTerm))

	// 如果 leader 不一致，分发 leaderChange 事件
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}
}

//--------------------------------------
// Event Loop
//--------------------------------------

//	   ________
//	--|Snapshot|                 timeout
//	|  --------                  ______
//
// recover    |       ^                   |      |
// snapshot / |       |snapshot           |      |
// higher     |       |                   v      |     recv majority votes
// term       |    --------    timeout    -----------                        -----------
//
//	|-> |Follower| ----------> | Candidate |--------------------> |  Leader   |
//	     --------               -----------                        -----------
//	        ^          higher term/ |                         higher term |
//	        |            new leader |                                     |
//	        |_______________________|____________________________________ |
//
// The main event loop for the server
func (s *server) loop() {
	defer s.debugln("server.loop.end")

	state := s.State()

	for state != Stopped {
		s.debugln("server.loop.run ", state)
		switch state {
		case Follower:
			s.followerLoop()
		case Candidate:
			s.candidateLoop()
		case Leader:
			s.leaderLoop()
		case Snapshotting:
			s.snapshotLoop()
		}
		state = s.State()
	}
}

// Sends an event to the event loop to be processed. The function will wait
// until the event is actually processed before returning.
// send 发送事件，并等待事件被处理后返回
func (s *server) send(value interface{}) (interface{}, error) {
	// 检查 server 状态
	if !s.Running() {
		return nil, StopError
	}

	// 构造 event
	event := &ev{target: value, c: make(chan error, 1)}
	// 要么将事件投递到 server 的 event channel，要么响应 server stop
	select {
	case s.c <- event:
	case <-s.stopped:
		return nil, StopError
	}

	// 要么响应 server stop，要么等待 event 的 c channel
	select {
	case <-s.stopped:
		return nil, StopError
	case err := <-event.c:
		return event.returnValue, err
	}
}

func (s *server) sendAsync(value interface{}) {
	if !s.Running() {
		return
	}

	event := &ev{target: value, c: make(chan error, 1)}
	// try a non-blocking send first
	// in most cases, this should not be blocking
	// avoid create unnecessary go routines
	select {
	case s.c <- event:
		return
	default:
	}

	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		select {
		case s.c <- event:
		case <-s.stopped:
		}
	}()
}

// The event loop that is run when the server is in a Follower state.
// Responds to RPCs from candidates and leaders.
// Converts to candidate if election timeout elapses without either:
//
//	1.Receiving valid AppendEntries RPC, or
//	2.Granting vote to candidate

// followerLoop 当 server.state = follower 时运行的事件循环
// 负责响应 candidate 和 leader 的 RPC 请求
// 当触发选举超时时，且未出现以下情况时，则转换为 candidate：
//
// - 收到合法的 AppendEntries RPC
// - 赋予候选人投票权
func (s *server) followerLoop() {
	since := time.Now()
	electionTimeout := s.ElectionTimeout()
	// 构造一个 [electionTimeout, electionTimeout * 2) 的超时通道
	timeoutChan := afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)

	// 仅在 follower 状态触发
	for s.State() == Follower {
		var err error
		update := false
		select {
		case <-s.stopped: // 响应 server stop
			s.setState(Stopped) // 更新 server 为 stopped 状态
			return              // 退出循环

		case e := <-s.c:
			switch req := e.target.(type) {
			case JoinCommand:
				//If no log entries exist and a self-join command is issued
				//then immediately become leader and commit entry.
				if s.log.currentIndex() == 0 && req.NodeName() == s.Name() {
					s.debugln("selfjoin and promote to leader")
					s.setState(Leader)
					s.processCommand(req, e)
				} else {
					err = NotLeaderError
				}
			case *AppendEntriesRequest: // 处理 AppendEntries RPC
				// If heartbeats get too close to the election timeout then send an event.
				// 读取距离上次通信时间的间隔
				elapsedTime := time.Now().Sub(since)
				// 检查距离上次通信时间的间隔是否超过了超时的80%
				if elapsedTime > time.Duration(float64(electionTimeout)*ElectionTimeoutThresholdPercent) {
					// 分发 electionTimeoutThreshold 事件
					s.DispatchEvent(newEvent(ElectionTimeoutThresholdEventType, elapsedTime, nil))
				}
				// server 处理 AppendEntries RPC
				e.returnValue, update = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, update = s.processRequestVoteRequest(req)
			case *SnapshotRequest:
				e.returnValue = s.processSnapshotRequest(req)
			default:
				err = NotLeaderError
			}
			// Callback to event.
			e.c <- err

		case <-timeoutChan:
			// only allow synced follower to promote to candidate
			if s.promotable() {
				s.setState(Candidate)
			} else {
				update = true
			}
		}

		// Converts to candidate if election timeout elapses without either:
		//   1.Receiving valid AppendEntries RPC, or
		//   2.Granting vote to candidate
		if update {
			since = time.Now()
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
		}
	}
}

// The event loop that is run when the server is in a Candidate state.
func (s *server) candidateLoop() {
	// Clear leader value.
	prevLeader := s.leader
	s.leader = ""
	if prevLeader != s.leader {
		s.DispatchEvent(newEvent(LeaderChangeEventType, s.leader, prevLeader))
	}

	lastLogIndex, lastLogTerm := s.log.lastInfo()
	doVote := true
	votesGranted := 0
	var timeoutChan <-chan time.Time
	var respChan chan *RequestVoteResponse

	for s.State() == Candidate {
		if doVote {
			// Increment current term, vote for self.
			s.currentTerm++
			s.votedFor = s.name

			// Send RequestVote RPCs to all other servers.
			respChan = make(chan *RequestVoteResponse, len(s.peers))
			for _, peer := range s.peers {
				s.routineGroup.Add(1)
				go func(peer *Peer) {
					defer s.routineGroup.Done()
					peer.sendVoteRequest(newRequestVoteRequest(s.currentTerm, s.name, lastLogIndex, lastLogTerm), respChan)
				}(peer)
			}

			// Wait for either:
			//   * Votes received from majority of servers: become leader
			//   * AppendEntries RPC received from new leader: step down.
			//   * Election timeout elapses without election resolution: increment term, start new election
			//   * Discover higher term: step down (§5.1)
			votesGranted = 1
			timeoutChan = afterBetween(s.ElectionTimeout(), s.ElectionTimeout()*2)
			doVote = false
		}

		// If we received enough votes then stop waiting for more votes.
		// And return from the candidate loop
		if votesGranted == s.QuorumSize() {
			s.debugln("server.candidate.recv.enough.votes")
			s.setState(Leader)
			return
		}

		// Collect votes from peers.
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return

		case resp := <-respChan:
			if success := s.processVoteResponse(resp); success {
				s.debugln("server.candidate.vote.granted: ", votesGranted)
				votesGranted++
			}

		case e := <-s.c:
			var err error
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}

			// Callback to event.
			e.c <- err

		case <-timeoutChan:
			doVote = true
		}
	}
}

// The event loop that is run when the server is in a Leader state.
func (s *server) leaderLoop() {
	logIndex, _ := s.log.lastInfo()

	// Update the peers prevLogIndex to leader's lastLogIndex and start heartbeat.
	s.debugln("leaderLoop.set.PrevIndex to ", logIndex)
	for _, peer := range s.peers {
		peer.setPrevLogIndex(logIndex)
		peer.startHeartbeat()
	}

	// Commit a NOP after the server becomes leader. From the Raft paper:
	// "Upon election: send initial empty AppendEntries RPCs (heartbeat) to
	// each server; repeat during idle periods to prevent election timeouts
	// (§5.2)". The heartbeats started above do the "idle" period work.
	s.routineGroup.Add(1)
	go func() {
		defer s.routineGroup.Done()
		s.Do(NOPCommand{})
	}()

	// Begin to collect response from followers
	for s.State() == Leader {
		var err error
		select {
		case <-s.stopped:
			// Stop all peers before stop
			for _, peer := range s.peers {
				peer.stopHeartbeat(false)
			}
			s.setState(Stopped)
			return

		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				s.processCommand(req, e)
				continue
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *AppendEntriesResponse:
				s.processAppendEntriesResponse(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			}

			// Callback to event.
			e.c <- err
		}
	}

	s.syncedPeer = nil
}

func (s *server) snapshotLoop() {
	for s.State() == Snapshotting {
		var err error
		select {
		case <-s.stopped:
			s.setState(Stopped)
			return

		case e := <-s.c:
			switch req := e.target.(type) {
			case Command:
				err = NotLeaderError
			case *AppendEntriesRequest:
				e.returnValue, _ = s.processAppendEntriesRequest(req)
			case *RequestVoteRequest:
				e.returnValue, _ = s.processRequestVoteRequest(req)
			case *SnapshotRecoveryRequest:
				e.returnValue = s.processSnapshotRecoveryRequest(req)
			}
			// Callback to event.
			e.c <- err
		}
	}
}

//--------------------------------------
// Commands
//--------------------------------------

// Attempts to execute a command and replicate it. The function will return
// when the command has been successfully committed or an error has occurred.

func (s *server) Do(command Command) (interface{}, error) {
	return s.send(command)
}

// Processes a command.
func (s *server) processCommand(command Command, e *ev) {
	s.debugln("server.command.process")

	// Create an entry for the command in the log.
	entry, err := s.log.createEntry(s.currentTerm, command, e)

	if err != nil {
		s.debugln("server.command.log.entry.error:", err)
		e.c <- err
		return
	}

	if err := s.log.appendEntry(entry); err != nil {
		s.debugln("server.command.log.error:", err)
		e.c <- err
		return
	}

	s.syncedPeer[s.Name()] = true
	if len(s.peers) == 0 {
		commitIndex := s.log.currentIndex()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Appends zero or more log entry from the leader to this server.
// AppendEntries 追加来自 leader 的 log entry 到当前 server
func (s *server) AppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*AppendEntriesResponse)
	return resp
}

// Processes the "append entries" request.
// processAppendEntriesRequest 处理 AppendEntries RPC
func (s *server) processAppendEntriesRequest(req *AppendEntriesRequest) (*AppendEntriesResponse, bool) {
	s.traceln("server.ae.process")

	// 检查 term 是否合法
	// 检查发出请求的 leader 的任期是否小于当前记忆中的 leader 的任期
	// 本质上是检查发过来的请求的 leader 是否发生变化
	if req.Term < s.currentTerm { // 这不是最新 leader 发出的请求，需要报错
		s.debugln("server.ae.error: stale term")
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), false
	}

	// 检查 leader 是否发生变化
	if req.Term == s.currentTerm { // leader 未变化
		// 二次检查，当前不应该为 leader 状态
		_assert(s.State() != Leader, "leader.elected.at.same.term.%d\n", s.currentTerm)

		// step-down to follower when it is a candidate
		// 如果当前节点为 candidate，则降级为 follower
		if s.state == Candidate {
			// change state to follower
			s.setState(Follower)
		}

		// discover new leader when candidate
		// save leader name when follower
		// 保存 leader 的名称
		s.leader = req.LeaderName
	} else { // leader 变化
		// Update term and leader.
		s.updateCurrentTerm(req.Term, req.LeaderName)
	}

	// Reject if log doesn't contain a matching previous entry.
	// 将 log 截断到给定的 index 和 term，需要同时操作 file 和 entries(内存)
	if err := s.log.truncate(req.PrevLogIndex, req.PrevLogTerm); err != nil {
		s.debugln("server.ae.truncate.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Append entries to the log.
	// 将 entries 追加到日志中
	if err := s.log.appendEntries(req.Entries); err != nil {
		s.debugln("server.ae.append.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// Commit up to the commit index.
	// 更新 commitIndex
	if err := s.log.setCommitIndex(req.CommitIndex); err != nil {
		s.debugln("server.ae.commit.error: ", err)
		return newAppendEntriesResponse(s.currentTerm, false, s.log.currentIndex(), s.log.CommitIndex()), true
	}

	// once the server appended and committed all the log entries from the leader

	return newAppendEntriesResponse(s.currentTerm, true, s.log.currentIndex(), s.log.CommitIndex()), true
}

// Processes the "append entries" response from the peer. This is only
// processed when the server is a leader. Responses received during other
// states are dropped.
func (s *server) processAppendEntriesResponse(resp *AppendEntriesResponse) {
	// If we find a higher term then change to a follower and exit.
	if resp.Term() > s.Term() {
		s.updateCurrentTerm(resp.Term(), "")
		return
	}

	// panic response if it's not successful.
	if !resp.Success() {
		return
	}

	// if one peer successfully append a log from the leader term,
	// we add it to the synced list
	if resp.append == true {
		s.syncedPeer[resp.peer] = true
	}

	// Increment the commit count to make sure we have a quorum before committing.
	if len(s.syncedPeer) < s.QuorumSize() {
		return
	}

	// Determine the committed index that a majority has.
	var indices []uint64
	indices = append(indices, s.log.currentIndex())
	for _, peer := range s.peers {
		indices = append(indices, peer.getPrevLogIndex())
	}
	sort.Sort(sort.Reverse(uint64Slice(indices)))

	// We can commit up to the index which the majority of the members have appended.
	commitIndex := indices[s.QuorumSize()-1]
	committedIndex := s.log.commitIndex

	if commitIndex > committedIndex {
		// leader needs to do a fsync before committing log entries
		s.log.sync()
		s.log.setCommitIndex(commitIndex)
		s.debugln("commit index ", commitIndex)
	}
}

// processVoteReponse processes a vote request:
//  1. if the vote is granted for the current term of the candidate, return true
//  2. if the vote is denied due to smaller term, update the term of this server
//     which will also cause the candidate to step-down, and return false.
//  3. if the vote is for a smaller term, ignore it and return false.
func (s *server) processVoteResponse(resp *RequestVoteResponse) bool {
	if resp.VoteGranted && resp.Term == s.currentTerm {
		return true
	}

	if resp.Term > s.currentTerm {
		s.debugln("server.candidate.vote.failed")
		s.updateCurrentTerm(resp.Term, "")
	} else {
		s.debugln("server.candidate.vote: denied")
	}
	return false
}

//--------------------------------------
// Request Vote
//--------------------------------------

// Requests a vote from a server. A vote can be obtained if the vote's term is
// at the server's current term and the server has not made a vote yet. A vote
// can also be obtained if the term is greater than the server's current term.
func (s *server) RequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*RequestVoteResponse)
	return resp
}

// Processes a "request vote" request.
func (s *server) processRequestVoteRequest(req *RequestVoteRequest) (*RequestVoteResponse, bool) {

	// If the request is coming from an old term then reject it.
	if req.Term < s.Term() {
		s.debugln("server.rv.deny.vote: cause stale term")
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// If the term of the request peer is larger than this node, update the term
	// If the term is equal and we've already voted for a different candidate then
	// don't vote for this candidate.
	if req.Term > s.Term() {
		s.updateCurrentTerm(req.Term, "")
	} else if s.votedFor != "" && s.votedFor != req.CandidateName {
		s.debugln("server.deny.vote: cause duplicate vote: ", req.CandidateName,
			" already vote for ", s.votedFor)
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// If the candidate's log is not at least as up-to-date as our last log then don't vote.
	lastIndex, lastTerm := s.log.lastInfo()
	if lastIndex > req.LastLogIndex || lastTerm > req.LastLogTerm {
		s.debugln("server.deny.vote: cause out of date log: ", req.CandidateName,
			"Index :[", lastIndex, "]", " [", req.LastLogIndex, "]",
			"Term :[", lastTerm, "]", " [", req.LastLogTerm, "]")
		return newRequestVoteResponse(s.currentTerm, false), false
	}

	// If we made it this far then cast a vote and reset our election time out.
	s.debugln("server.rv.vote: ", s.name, " votes for", req.CandidateName, "at term", req.Term)
	s.votedFor = req.CandidateName

	return newRequestVoteResponse(s.currentTerm, true), true
}

//--------------------------------------
// Membership
//--------------------------------------

// Adds a peer to the server.
func (s *server) AddPeer(name string, connectiongString string) error {
	s.debugln("server.peer.add: ", name, len(s.peers))

	// Do not allow peers to be added twice.
	if s.peers[name] != nil {
		return nil
	}

	// Skip the Peer if it has the same name as the Server
	if s.name != name {
		peer := newPeer(s, name, connectiongString, s.heartbeatInterval)

		if s.State() == Leader {
			peer.startHeartbeat()
		}

		s.peers[peer.Name] = peer

		s.DispatchEvent(newEvent(AddPeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()

	return nil
}

// Removes a peer from the server.
func (s *server) RemovePeer(name string) error {
	s.debugln("server.peer.remove: ", name, len(s.peers))

	// Skip the Peer if it has the same name as the Server
	if name != s.Name() {
		// Return error if peer doesn't exist.
		peer := s.peers[name]
		if peer == nil {
			return fmt.Errorf("raft: Peer not found: %s", name)
		}

		// Stop peer and remove it.
		if s.State() == Leader {
			// We create a go routine here to avoid potential deadlock.
			// We are holding log write lock when reach this line of code.
			// Peer.stopHeartbeat can be blocked without go routine, if the
			// target go routine (which we want to stop) is calling
			// log.getEntriesAfter and waiting for log read lock.
			// So we might be holding log lock and waiting for log lock,
			// which lead to a deadlock.
			// TODO(xiangli) refactor log lock
			s.routineGroup.Add(1)
			go func() {
				defer s.routineGroup.Done()
				peer.stopHeartbeat(true)
			}()
		}

		delete(s.peers, name)

		s.DispatchEvent(newEvent(RemovePeerEventType, name, nil))
	}

	// Write the configuration to file.
	s.writeConf()

	return nil
}

//--------------------------------------
// Log compaction
//--------------------------------------

func (s *server) TakeSnapshot() error {
	if s.stateMachine == nil {
		return errors.New("Snapshot: Cannot create snapshot. Missing state machine.")
	}

	// Shortcut without lock
	// Exit if the server is currently creating a snapshot.
	if s.pendingSnapshot != nil {
		return errors.New("Snapshot: Last snapshot is not finished.")
	}

	// TODO: acquire the lock and no more committed is allowed
	// This will be done after finishing refactoring heartbeat
	s.debugln("take.snapshot")

	lastIndex, lastTerm := s.log.commitInfo()

	// check if there is log has been committed since the
	// last snapshot.
	if lastIndex == s.log.startIndex {
		return nil
	}

	path := s.SnapshotPath(lastIndex, lastTerm)
	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot = &Snapshot{lastIndex, lastTerm, nil, nil, path}

	state, err := s.stateMachine.Save()
	if err != nil {
		return err
	}

	// Clone the list of peers.
	peers := make([]*Peer, 0, len(s.peers)+1)
	for _, peer := range s.peers {
		peers = append(peers, peer.clone())
	}
	peers = append(peers, &Peer{Name: s.Name(), ConnectionString: s.connectionString})

	// Attach snapshot to pending snapshot and save it to disk.
	s.pendingSnapshot.Peers = peers
	s.pendingSnapshot.State = state
	s.saveSnapshot()

	// We keep some log entries after the snapshot.
	// We do not want to send the whole snapshot to the slightly slow machines
	if lastIndex-s.log.startIndex > NumberOfLogEntriesAfterSnapshot {
		compactIndex := lastIndex - NumberOfLogEntriesAfterSnapshot
		compactTerm := s.log.getEntry(compactIndex).Term()
		s.log.compact(compactIndex, compactTerm)
	}

	return nil
}

// Retrieves the log path for the server.
func (s *server) saveSnapshot() error {
	if s.pendingSnapshot == nil {
		return errors.New("pendingSnapshot.is.nil")
	}

	// Write snapshot to disk.
	if err := s.pendingSnapshot.save(); err != nil {
		return err
	}

	// Swap the current and last snapshots.
	tmp := s.snapshot
	s.snapshot = s.pendingSnapshot

	// Delete the previous snapshot if there is any change
	if tmp != nil && !(tmp.LastIndex == s.snapshot.LastIndex && tmp.LastTerm == s.snapshot.LastTerm) {
		tmp.remove()
	}
	s.pendingSnapshot = nil

	return nil
}

// Retrieves the log path for the server.
func (s *server) SnapshotPath(lastIndex uint64, lastTerm uint64) string {
	return path.Join(s.path, "snapshot", fmt.Sprintf("%v_%v.ss", lastTerm, lastIndex))
}

func (s *server) RequestSnapshot(req *SnapshotRequest) *SnapshotResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotResponse)
	return resp
}

func (s *server) processSnapshotRequest(req *SnapshotRequest) *SnapshotResponse {
	// If the follower’s log contains an entry at the snapshot’s last index with a term
	// that matches the snapshot’s last term, then the follower already has all the
	// information found in the snapshot and can reply false.
	entry := s.log.getEntry(req.LastIndex)

	if entry != nil && entry.Term() == req.LastTerm {
		return newSnapshotResponse(false)
	}

	// Update state.
	s.setState(Snapshotting)

	return newSnapshotResponse(true)
}

func (s *server) SnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	ret, _ := s.send(req)
	resp, _ := ret.(*SnapshotRecoveryResponse)
	return resp
}

func (s *server) processSnapshotRecoveryRequest(req *SnapshotRecoveryRequest) *SnapshotRecoveryResponse {
	// Recover state sent from request.
	if err := s.stateMachine.Recovery(req.State); err != nil {
		panic("cannot recover from previous state")
	}

	// Recover the cluster configuration.
	s.peers = make(map[string]*Peer)
	for _, peer := range req.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.currentTerm = req.LastTerm
	s.log.updateCommitIndex(req.LastIndex)

	// Create local snapshot.
	s.pendingSnapshot = &Snapshot{req.LastIndex, req.LastTerm, req.Peers, req.State, s.SnapshotPath(req.LastIndex, req.LastTerm)}
	s.saveSnapshot()

	// Clear the previous log entries.
	s.log.compact(req.LastIndex, req.LastTerm)

	return newSnapshotRecoveryResponse(req.LastTerm, true, req.LastIndex)
}

// Load a snapshot at restart
func (s *server) LoadSnapshot() error {
	// Open snapshot/ directory.
	dir, err := os.OpenFile(path.Join(s.path, "snapshot"), os.O_RDONLY, 0)
	if err != nil {
		s.debugln("cannot.open.snapshot: ", err)
		return err
	}

	// Retrieve a list of all snapshots.
	filenames, err := dir.Readdirnames(-1)
	if err != nil {
		dir.Close()
		panic(err)
	}
	dir.Close()

	if len(filenames) == 0 {
		s.debugln("no.snapshot.to.load")
		return nil
	}

	// Grab the latest snapshot.
	sort.Strings(filenames)
	snapshotPath := path.Join(s.path, "snapshot", filenames[len(filenames)-1])

	// Read snapshot data.
	file, err := os.OpenFile(snapshotPath, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer file.Close()

	// Check checksum.
	var checksum uint32
	n, err := fmt.Fscanf(file, "%08x\n", &checksum)
	if err != nil {
		return err
	} else if n != 1 {
		return errors.New("checksum.err: bad.snapshot.file")
	}

	// Load remaining snapshot contents.
	b, err := ioutil.ReadAll(file)
	if err != nil {
		return err
	}

	// Generate checksum.
	byteChecksum := crc32.ChecksumIEEE(b)
	if uint32(checksum) != byteChecksum {
		s.debugln(checksum, " ", byteChecksum)
		return errors.New("bad snapshot file")
	}

	// Decode snapshot.
	if err = json.Unmarshal(b, &s.snapshot); err != nil {
		s.debugln("unmarshal.snapshot.error: ", err)
		return err
	}

	// Recover snapshot into state machine.
	if err = s.stateMachine.Recovery(s.snapshot.State); err != nil {
		s.debugln("recovery.snapshot.error: ", err)
		return err
	}

	// Recover cluster configuration.
	for _, peer := range s.snapshot.Peers {
		s.AddPeer(peer.Name, peer.ConnectionString)
	}

	// Update log state.
	s.log.startTerm = s.snapshot.LastTerm
	s.log.startIndex = s.snapshot.LastIndex
	s.log.updateCommitIndex(s.snapshot.LastIndex)

	return err
}

//--------------------------------------
// Config File
//--------------------------------------

// Flushes commit index to the disk.
// So when the raft server restarts, it will commit upto the flushed commitIndex.
func (s *server) FlushCommitIndex() {
	s.debugln("server.conf.update")
	// Write the configuration to file.
	s.writeConf()
}

func (s *server) writeConf() {

	peers := make([]*Peer, len(s.peers))

	i := 0
	for _, peer := range s.peers {
		peers[i] = peer.clone()
		i++
	}

	r := &Config{
		CommitIndex: s.log.commitIndex,
		Peers:       peers,
	}

	b, _ := json.Marshal(r)

	confPath := path.Join(s.path, "conf")
	tmpConfPath := path.Join(s.path, "conf.tmp")

	err := writeFileSynced(tmpConfPath, b, 0600)

	if err != nil {
		panic(err)
	}

	os.Rename(tmpConfPath, confPath)
}

// Read the configuration for the server.
// 读取配置，将 commitIndex 更新 log
func (s *server) readConf() error {
	// 配置位于 conf 文件
	confPath := path.Join(s.path, "conf")
	s.debugln("readConf.open ", confPath)

	// open conf file
	// 打开配置文件
	b, err := ioutil.ReadFile(confPath)

	if err != nil {
		return nil
	}

	conf := &Config{}

	// 反序列化配置
	if err = json.Unmarshal(b, conf); err != nil {
		return err
	}

	// 使用配置的 commitIndex 来刷入日志
	s.log.updateCommitIndex(conf.CommitIndex)

	return nil
}

//--------------------------------------
// Debugging
//--------------------------------------

func (s *server) debugln(v ...interface{}) {
	if logLevel > Debug {
		debugf("[%s Term:%d] %s", s.name, s.Term(), fmt.Sprintln(v...))
	}
}

func (s *server) traceln(v ...interface{}) {
	if logLevel > Trace {
		tracef("[%s] %s", s.name, fmt.Sprintln(v...))
	}
}
