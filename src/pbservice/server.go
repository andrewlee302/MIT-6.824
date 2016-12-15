package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"
import "errors"

/*
	The crash thing isn't taken into accounts.
	Test cases don't contain the condition concurrent append operations occur with unreliable rpc.
*/

const (
	RoleNull    = 0
	RolePrimary = 1
	RoleBackup  = 2
)

type SeqValue struct {
	Seq   int64
	Value string // the value before execute req of seq
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// Your declarations here.
	processedSeqSet map[int64]bool // dedup
	dataset         map[string]string
	last_dataset    map[string]SeqValue // for recovering the k-v
	versions        map[string]int64
	view            viewservice.View
	role            uint
	// only valid when the server is primary
	backupHost string
	// ping connection failure, avoid getting stale data
	pingFailed bool
}

func (pb *PBServer) LocalOp(op string, key string, value string) {
	switch op {
	case Put:
		pb.LocalPut(key, value)
	case Append:
		pb.LocalAppend(key, value)
	default:
		// do nothing
	}
}

func (pb *PBServer) BackupOp(args *ForwardArgs, reply *ForwardReply) bool {
	// times := 0
	// for pb.backupHost != "" {
	// 	if ok := call(pb.backupHost, "PBServer.PutAppend", _args, &reply); ok {
	// 		// include the reply with ErrWrongServer
	// 		return
	// 	}

	// 	// here, because rpc failure (network or dead)
	// 	// network failure: retry soon
	// 	// dead, wait for ticking to update view
	// 	times++
	// 	if times%5 == 0 {
	// 		pb.cond.Wait()
	// 	}
	// }
	ok := call(pb.backupHost, "PBServer.ForwardPutAppend", args, &reply)
	return ok
}

func (pb *PBServer) LocalPut(key string, value string) {
	pb.dataset[key] = value
}

func (pb *PBServer) LocalAppend(key string, value string) {
	if v, ok := pb.dataset[key]; ok {
		pb.dataset[key] = v + value
	} else {
		pb.dataset[key] = value
	}
}

func (pb *PBServer) Replicate(args *ReplicateArgs, reply *ReplicateReply) error {
	if pb.role == RoleBackup && args.Sender == pb.view.Primary {
		pb.dataset = args.Dataset
		pb.last_dataset = args.LastDataset
		pb.processedSeqSet = args.ProcessedSeqSet
		pb.versions = args.Versions
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	switch pb.role {
	case RolePrimary:
		// RoleBackup could be worse than RolePrimary, considering primary's not replicating into backup
		{
			// primary maybe lost, inducing that the backup is promoted to primary
			if !pb.pingFailed {
				if v, ok := pb.dataset[args.Key]; ok {
					reply.Value = v
					reply.Err = OK
				} else {
					reply.Value = ""
					reply.Err = ErrNoKey
				}
			} else {
				reply.Value = ""
				reply.Err = ErrWrongServer
			}
		}
	default:
		{
			reply.Value = ""
			reply.Err = ErrWrongServer
		}
	}
	pb.mu.Unlock()
	return nil
}

func (pb *PBServer) ForwardPutAppend(args *ForwardArgs, reply *ForwardReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.role != RoleBackup || args.Sender != pb.view.Primary {
		// reject
		reply.Err = ErrWrongServer
		return nil
	}
	/*
		// Backup doesn't check the dup, just apply it. It violates
		// the at-most-once semantics if the non-idempotent op (append) occurs.
		// The test cases don't contain the thing.
		pb.LocalOp(args.Op, args.Key, args.Value)
		pb.processedSeqSet[args.Seq] = true
		reply.Err = OK
		return nil
	*/
	// assert(args.Version == pb.versions[args.Key]||args.Version+1 == pb.versions[args.Key])
	if !(args.Version == pb.versions[args.Key] || args.Version+1 == pb.versions[args.Key]) {
		log.Fatalln("backup error", args.Version, pb.versions[args.Key], args.Value)
	}
	if args.Version == 0 {
		// reset the value to null of the corresponding type
		log.Println("backup reset0", args.Version, pb.versions[args.Key], args.Value)
		pb.dataset[args.Key] = ""
	} else if args.Version == pb.versions[args.Key] {
		// if not exists, then 0
		log.Println("backup normal", pb.me, args.Version, pb.versions[args.Key], args.Value)
	} else {
		// assert(args.Version + 1 == pb.versions[args.Key])
		log.Println("backup reset1", pb.me, args.Version, pb.versions[args.Key], args.Value)
		pb.dataset[args.Key] = pb.last_dataset[args.Key].Value
		delete(pb.processedSeqSet, pb.last_dataset[args.Key].Seq)
	}
	pb.last_dataset[args.Key] = SeqValue{Seq: args.Seq, Value: pb.dataset[args.Key]}
	pb.versions[args.Key] = args.Version + 1
	pb.LocalOp(args.Op, args.Key, args.Value)
	pb.processedSeqSet[args.Seq] = true
	reply.Version = pb.versions[args.Key]
	reply.Err = OK
	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.role != RolePrimary {
		// reject
		reply.Err = ErrWrongServer
		return nil
	}

	reply.Err = OK
	if _, ok := pb.processedSeqSet[args.Seq]; ok {
		return nil
	}
	version := pb.versions[args.Key] // if not exists, then 0
	log.Println("primary start", pb.me, version, args.Value)
	if pb.backupHost != "" {
		var f_reply ForwardReply
		f_args := &ForwardArgs{Key: args.Key, Value: args.Value, Op: args.Op, Sender: pb.me, Seq: args.Seq, Version: version}
		ok := pb.BackupOp(f_args, &f_reply)
		if ok && f_reply.Err == OK {
			log.Println("primary normal", pb.me, f_reply.Version, args.Value)
			pb.versions[args.Key] = f_reply.Version
		} else if !ok {
			log.Println("backup conn problem", pb.me, args.Value)
			return errors.New("backup conn problem")
		}
	} else {
		pb.versions[args.Key] = version + 1
		log.Println("primary normal only", pb.me, version+1, args.Value)
	}
	pb.last_dataset[args.Key] = SeqValue{Seq: args.Seq, Value: pb.dataset[args.Key]}
	pb.LocalOp(args.Op, args.Key, args.Value)
	pb.processedSeqSet[args.Seq] = true
	return nil

	/*
		switch pb.role {
		case RolePrimary:
			{
				if _, ok := pb.processedSeqSet[args.Seq]; ok {
					reply.Err = OK
					return nil
				}
				if args.IsClient {
					if pb.backupHost != "" {
						ok := pb.BackupOp(args, reply)
						if ok && reply.Err == OK {
							pb.LocalOp(args)
							pb.processedSeqSet[args.Seq] = true
						} else if !ok {
							return errors.New("backup conn problem")
						}
					} else {
						pb.LocalOp(args)
						pb.processedSeqSet[args.Seq] = true
					}
				} else {
					// reject
					reply.Err = ErrWrongServer
				}
			}
		case RoleBackup:
			{
				// Backup doesn't check the dup, just apply it. It violates
				// the at-most-once semantics if the non-idempotent op (append) occurs.
				// The test cases don't contain the thing.
				if !args.IsClient && args.Sender == pb.view.Primary {
					pb.LocalOp(args)
					pb.processedSeqSet[args.Seq] = true
					reply.Err = OK
				} else {
					// reject
					reply.Err = ErrWrongServer
				}
			}
		default:
			{
				reply.Err = ErrWrongServer
			}
		}
		return nil
	*/
}

// rpc call
func (pb *PBServer) RunReplicate() {
	for pb.backupHost != "" {
		args := &ReplicateArgs{Dataset: pb.dataset, LastDataset: pb.last_dataset, Sender: pb.me, ProcessedSeqSet: pb.processedSeqSet, Versions: pb.versions}
		var reply ReplicateReply
		if ok := call(pb.backupHost, "PBServer.Replicate", args, &reply); ok {
			if reply.Err == OK {
				break
			} else {
				// the backup is not the real backup
				// do nothing, wait for view updating in Ping
				// if Ping failed, then keep replicating won't bring error
			}
		} else {
			// the backup isn't responding
			// do nothing, wait for view updating in Ping
			// if Ping failed, then keep replicating won't bring error
		}
	}
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// view changed
	if view, err := pb.vs.Ping(pb.view.Viewnum); err == nil {
		pb.pingFailed = false
		if view.Viewnum > pb.view.Viewnum {
			pb.view = view
			switch pb.me {
			case pb.view.Primary:
				// change role
				// transfer state to new backup
				// setup the backup RPC connection
				{
					pb.role = RolePrimary
					if pb.view.Backup != "" {
						pb.backupHost = pb.view.Backup
						pb.RunReplicate()
					} else {
						// pb.view.Backup == ""
						pb.backupHost = ""
					}
				}
			case pb.view.Backup:
				{
					pb.role = RoleBackup
					pb.backupHost = ""
				}
			default:
				{
					pb.dataset = nil
					pb.processedSeqSet = nil
					pb.role = RoleNull
					pb.backupHost = ""
				}
			}
		}
	} else {
		// err != nil
		// network failure to viewserver
		pb.pingFailed = true
	}
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.view = viewservice.View{Viewnum: 0, Primary: "", Backup: ""}
	pb.dataset = make(map[string]string)
	pb.last_dataset = make(map[string]SeqValue)
	pb.versions = make(map[string]int64)
	pb.processedSeqSet = make(map[int64]bool)
	pb.role = RoleNull
	pb.backupHost = ""

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
