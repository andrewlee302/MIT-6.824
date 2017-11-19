package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrNoShard      = "ErrNoShard"
	ErrUnaccessible = "ErrUnaccessible"
	ErrPending      = "ErrPending"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type GetReply struct {
	Err   Err
	Value string
}

type MoveShardArgs struct {
	FromGID   int64 // the shard sender
	ToGID     int64 // the shard receiver
	Shard     int   // shard id
	ConfigNum int   // the config number of the target shard
}

type MoveShardReply struct {
	Err Err
	Sd  ShardData
}

type AckMovementArgs MoveShardArgs

type AckMovementReply struct {
	Err Err
}
