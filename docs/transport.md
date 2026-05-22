# Transport

由 Server 对其他成员发送节点.

## SendVoteRequest

当 Server 从 follower 因为心跳超时,变为 candidate 时,发送投票选举新 Leader 发送的请求

接收端由 `http_transporter#requestVoteHandler` 响应处理,
底层由 `server#RequestVote` 进行处理.
封装事件转发给server的对应状态进行处理.
- `server#followerLoop`
- `server#candidateLoop`
- `server#leaderLoop`


server 除了位于 `stoped`/`initialized`/`snapshotting` 状态外,都可以进行投票
投票的基准:
    1.candidate 的任期必须要 >= 当前服务器的任期,因为任期小于当前,代表 candidate 已经落后了,不能成为 leader
    2.当任期相同时,该服务器如果已经给其他投过了,就不能再给该服务器投票,否则就会造成重复投票,导致多个 candidate 成为 leader,会导致脑裂
    3.当任期相同,如果 candidate 的日志中最新的索引和任期比该服务器要低,则不能成为 leader

```
VoteRequest {
    Term server 的任期
    LastLogIndex 最新的日志信息
    LastLogTerm 最新的日志任期
    CandidateName 候选者名称
}
```

```
VoteResponse {
    Term 当前服务器的任期
    VoteGranted 是否投票
}
```