# Server

`server.go`

Raft 的核心,其状态在 `follower`,`Candidate` 和 `Leader` 之间切换.

## New

state = `Stopped`

## Start

    ↓ (Init)
state = `Initialized`
    ↓ (Start)
state = `Follower`
    ↓ (followerLoop#timeout)                ↓ (followerLoop#SnapshotRequest)
state = `Candidate`                     state = `Snapshoting`
    ↓ (candidateLoop#doVote)                ↓ (snapshotLoop#doVote)
    ↓ (votesGranted == count/2+1)       
state = `Leader`    
    ↓ (leaderLoop#)

## Stop

state = `Stopped`

对于 follower 状态来说,直接停止,不执行任何操作.
对于 candidate 状态来说,直接停止,不执行任何操作.
对于 leader 状态来说,停止前发送取消各个 peer 的定时心跳操作.
对于 snapshotting 状态来说,直接停止,不执行任何操作.