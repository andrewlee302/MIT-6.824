package kvpaxos

import "hash/fnv"
import "strconv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

func shash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	x := h.Sum32()
  return strconv.Itoa(int(x))
}
