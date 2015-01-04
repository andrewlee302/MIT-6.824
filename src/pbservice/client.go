package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need these:
// import "time"
import "crypto/rand"
import "math/big"



type Clerk struct {
  vs *viewservice.Clerk
  // Your declarations here
}


func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  // Your ck.* initializations here

  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  // Your code here.

  return "???"
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, puttype int) string {

  // Your code here.
  return "???"
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, PutUpdate)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, PutHash)
  return v
}
func (ck *Clerk) PutAppend(key string, value string) string {
  v := ck.PutExt(key, value, PutAppend)
  return v
}
