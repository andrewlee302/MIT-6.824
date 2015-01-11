package pbservice

import "hash/fnv"
import "strconv"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
)

type Err string

type PutArgs struct {
	Key     string
	Value   string
	PutType int // For PutX
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


// Your RPC definitions here.

func shash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	x := h.Sum32()
  return strconv.Itoa(int(x))
}
