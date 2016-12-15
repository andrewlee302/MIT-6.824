package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

const (
	Put    = "Put"
	Append = "Append"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.

	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op     string
	Sender string
	Seq    int64 // dedup the same client request
}

type PutAppendReply struct {
	Err Err
}

type ForwardArgs struct {
	Key     string
	Value   string
	Op      string
	Sender  string
	Seq     int64
	Version int64 // primary version
}

type ForwardReply struct {
	Err     Err
	Version int64 // backup version
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type ReplicateArgs struct {
	Dataset         map[string]string
	LastDataset     map[string]SeqValue
	ProcessedSeqSet map[int64]bool
	Versions        map[string]int64
	Sender          string
}

type ReplicateReply struct {
	Err Err
}

// Your RPC definitions here.
