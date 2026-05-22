# Setup

## Server

设计核心,整个生命周期内在 `follower`/`candidate`/`leader` 状态中轮流切换.

newServer 时 state = `Stopped`,

## State Machine

仅提供了 `Recovery` 和 `Save` 来操作字节数组.