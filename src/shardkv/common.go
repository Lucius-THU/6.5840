package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrUnprepared  = "ErrUnprepared"
)

const (
	Paused  = 0
	Stored  = 1
	Serving = 2
	Waiting = 3
	Sending = 4
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	ClientId  int64
	RequestId int
	Shard     int
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key       string
	ClientId  int64
	RequestId int
	Shard     int
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ShardMoveArgs struct {
	Shard     int
	Table     map[string]string
	AnswerMap map[int64]Answer
}

type ShardMoveReply struct {
	Err Err
}
