package shardkv

import "net"
import "strings"
import "strconv"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	OpName string
	Key    string
	Value  string
	Id     int64
	Shard  int

	// reconfig
	// "<oldCfgNum>-<newCfgNum>", step by step or 0-newCfgNum
	Tag        string
	NextShards [shardmaster.NShards]int64 // update the `control state`
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	shardMus [shardmaster.NShards]sync.Mutex // mutexs of currShardsData

	// -------- RSM control state ------------
	// will be updated only at RSM construction
	exeDOPHistory map[int64]bool             // record executed ops in the RSM
	applySeq      int                        // seq of op which will be tried to apply
	RSMSeq        int                        // seq for next paxos req
	RSMShards     [shardmaster.NShards]int64 // affect RSM consumption (applying)
	// -----------------------------------

	// -------- RSM data state ------------
	// will be updated only at applying operations
	currShardsData [shardmaster.NShards]ShardData // shard -> ShardData
	currCfg        shardmaster.Config
	// -----------------------------------

	invalidShardCnt int
}

type ShardData struct {
	Shard        int
	Content      map[string]string
	ApplyHistory map[int64]bool // Record the successful ops in the shard.
	Flag         int32          // Valid, Moving, Moved
	ConfigNum    int            // Belong which config
}

const (
	OP_PUT      = "put"
	OP_GET      = "get"
	OP_APPEND   = "append"
	OP_RECONFIG = "reconfig"

	FLAG_VALID   = iota
	FLAG_MOVING  // shard will be moved or is moving
	FLAG_MOVED   // shard is moved if necessary
	FLAG_INVALID // no information
)
const (
	INVALID_SHARD_CNT_THRESHOLD = 5
)

func splitCfgNum(op *Op) (int, int) {
	cfgNums := strings.Split(op.Tag, "-")
	lastCfgNum, _ := strconv.Atoi(cfgNums[0])
	nextCfgNum, _ := strconv.Atoi(cfgNums[1])
	return lastCfgNum, nextCfgNum
}

// Try to decide the op in one of the paxos instance
// increase the seq until decide the op that we want,
// and apply the chosen value to the kv store.
func (kv *ShardKV) TryDecide(op Op) (Err, string) {
	// TODO concurrency optimization

	if op.OpName == OP_RECONFIG {
		// filter unnecessary reconfig op, which is jsut a optimizaiton.
		// when we try to apply it, we should check it.
		// if the currCfg number is not 0, then return.
		lastCfgNum, nextCfgNum := splitCfgNum(&op)
		if lastCfgNum == 0 && kv.currCfg.Num != 0 ||
			lastCfgNum != 0 && nextCfgNum < kv.currCfg.Num {
			return OK, ""
		}

	} else {
		// check whether op.Id is in the executed data-op history of the group.
		// if true, return OK and necessary value.
		// otherwise, try to add it to the RSM.
		if _, ok := kv.exeDOPHistory[op.Id]; ok {
			// TODO below
			if op.OpName == OP_GET {
				// regardless of validation of the corresponding shard.
				return OK, kv.currShardsData[op.Shard].Content[op.Key]
			} else {
				return OK, ""
			}
		}
	}

	for {
		timeout := 0 * time.Millisecond
		sleep_interval := 10 * time.Millisecond
		kv.px.Start(kv.RSMSeq, op)
		fate, v := kv.px.Status(kv.RSMSeq)
		for fate != paxos.Decided {
			fate, v = kv.px.Status(kv.RSMSeq)
			if fate == paxos.Pending {
				if timeout > 10*time.Second {
					return ErrPending, ""
				} else {
					time.Sleep(sleep_interval)
					timeout += sleep_interval
					sleep_interval *= 2
				}
			}
		}

		_op := v.(Op)
		kv.RSMSeq++

		if _op.OpName == OP_RECONFIG {
			kv.RSMShards = _op.NextShards
		}

		if _op.Id == op.Id {
			if op.OpName == OP_RECONFIG {
				kv.applyPastOPs()
				kv.applyReconfig(&op)
				kv.exeDOPHistory[op.Id] = true
				return OK, ""
			} else {
				// avoid invalid ops recording
				if kv.RSMShards[op.Shard] != kv.gid {
					// not applicable
					return ErrWrongGroup, ""
				} else {
					kv.exeDOPHistory[op.Id] = true
					if op.OpName == OP_GET {
						kv.applyPastOPs()
						return OK, kv.currShardsData[op.Shard].Content[op.Key]
					} else {
						return OK, ""
					}
				}
			}
		}
	}
}

// apply the past ops.
func (kv *ShardKV) applyPastOPs() {
	for ; kv.applySeq < kv.RSMSeq-1; kv.applySeq++ {
		_, v := kv.px.Status(kv.applySeq)
		// assert fate == paxos.Decided
		// assert kv.currShardsData[op.Shard].Flag = FLAG_VALID
		op := v.(Op)
		kv.applyOp(&op)
		kv.px.Done(kv.applySeq)
	}
}

func (kv *ShardKV) applyOp(op *Op) {
	if op.OpName == OP_RECONFIG {
		kv.applyReconfig(op)
	} else if op.OpName != OP_GET {
		// only execute it when the shard is valid and
		// apply history doesn't contain it.
		if kv.currShardsData[op.Shard].Flag == FLAG_VALID && kv.currShardsData[op.Shard].ApplyHistory[op.Id] == false {
			switch op.OpName {
			case OP_PUT:
				kv.currShardsData[op.Shard].Content[op.Key] = op.Value
			case OP_APPEND:
				kv.currShardsData[op.Shard].Content[op.Key] += op.Value
			default:
			}
			kv.currShardsData[op.Shard].ApplyHistory[op.Id] = true
		}
	} else {
		// nothing
	}
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	op := Op{OpName: OP_GET, Key: args.Key, Value: "", Id: args.Id, Shard: shard}
	reply.Err, reply.Value = kv.TryDecide(op)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	shard := key2shard(args.Key)
	op := Op{OpName: args.Op, Key: args.Key, Value: args.Value, Id: args.Id, Shard: shard}
	reply.Err, _ = kv.TryDecide(op)
	return nil
}

// try update the config to the lastest,
// then transfer the shards data.
func (kv *ShardKV) tryUpdateConfig() {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	latestCfg := kv.sm.Query(-1)
	if latestCfg.Num > kv.currCfg.Num {
		// submit the reconfig op
		lastCfg := kv.currCfg
		// progress step by step. Not skip the intermediate configs.
		for i := kv.currCfg.Num + 1; i <= latestCfg.Num; i++ {
			nextCfg := kv.sm.Query(i)
			op := Op{OpName: OP_RECONFIG, Tag: fmt.Sprintf("%d-%d", lastCfg.Num, nextCfg.Num), NextShards: nextCfg.Shards}
			kv.TryDecide(op)
			lastCfg = nextCfg
		}
	}
}

// move out of move in the shard, mutual exclusively
// with the data operations.
// precondition: oldcfg.Num == newCfg.Num - 1
// TODO optimize, reduce the overhead of thrash.
// apply reconfig op.
func (kv *ShardKV) applyReconfig(op *Op) {
	oldCfgNum, nextCfgNum := splitCfgNum(op)
	if nextCfgNum <= kv.currCfg.Num {
		return
	}
	oldCfg := kv.sm.Query(oldCfgNum)
	nextCfg := kv.sm.Query(nextCfgNum)
	if nextCfg.Num == 1 {
		// init config is zero config, nothing exists in the group
		for s, g := range nextCfg.Shards {
			if g == kv.gid {
				kv.shardMus[s].Lock()
				kv.currShardsData[s] = ShardData{Shard: s, Content: make(map[string]string), ApplyHistory: make(map[int64]bool), Flag: FLAG_VALID, ConfigNum: nextCfg.Num}
				kv.shardMus[s].Unlock()
			}
		}
	} else {
		for s, g := range oldCfg.Shards {
			if g == kv.gid {
				kv.shardMus[s].Lock()
				if nextCfg.Shards[s] != kv.gid {
					// assert kv.currShardsData[s].Flag == FLAG_VALID
					// move out, set the moving flag to the shard
					kv.currShardsData[s].Flag = FLAG_MOVING
				} else {
					// the shard in the new config keep the same with the old one
					// set the new config number of the shard
					kv.currShardsData[s].ConfigNum = nextCfg.Num
				}
				kv.shardMus[s].Unlock()
			}
		}

		for s, g := range nextCfg.Shards {
			if g == kv.gid && oldCfg.Shards[s] != kv.gid {
				// assert oldCfg.Shards[s] != 0

				// move in from the corresponding group
				args := &MoveShardArgs{FromGID: oldCfg.Shards[s], ToGID: kv.gid, Shard: s, ConfigNum: oldCfg.Num}
				var reply MoveShardReply
				// we must use the oldCfg.Groups
				servers := oldCfg.Groups[oldCfg.Shards[s]]
			Outer:
				for {
					for _, srv := range servers {
						ok := call(srv, "ShardKV.MoveShard", args, &reply)
						if ok && reply.Err == OK {
							// arrange the shard
							// assert reply.sd.ConfigNum == oldcfg.Num
							// assert reply.sd.s == s
							reply.Sd.Flag = FLAG_VALID
							reply.Sd.ConfigNum = nextCfg.Num
							kv.shardMus[s].Lock()
							kv.currShardsData[s] = reply.Sd
							kv.shardMus[s].Unlock()
							break Outer
						}
					}
					time.Sleep(250 * time.Millisecond)
				}
			}
		}
	}
	kv.currCfg = nextCfg

}

// rpc call
func (kv *ShardKV) MoveShard(args *MoveShardArgs, reply *MoveShardReply) error {
	kv.shardMus[args.Shard].Lock()
	defer kv.shardMus[args.Shard].Unlock()
	if args.FromGID != kv.gid {
		reply.Err = ErrWrongGroup
		return nil
	}
	sd := kv.currShardsData[args.Shard]
	switch sd.Flag {
	case FLAG_INVALID, FLAG_MOVED:
		reply.Err = ErrNoShard
	case FLAG_VALID:
		reply.Err = ErrUnaccessible
	case FLAG_MOVING:
		{
			if args.ConfigNum != sd.ConfigNum {
				reply.Err = ErrUnaccessible
			} else {
				reply.Err, reply.Sd = OK, sd
			}
		}
	default:
		reply.Err, reply.Sd = "Unknown", sd
	}
	return nil
}

// rpc call
// clear the memory occupation of unused shards
func (kv *ShardKV) AckMovement(args *AckMovementArgs, reply *AckMovementReply) error {
	// TODO
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.tryUpdateConfig()
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	{
		// init the config to the zero Config
		kv.currCfg.Num = 0
		kv.currCfg.Groups = map[int64][]string{}
		for i := 0; i < shardmaster.NShards; i++ {
			kv.RSMShards[i] = 0
			kv.currCfg.Shards[i] = 0
			kv.currShardsData[i] = ShardData{Shard: i, Flag: FLAG_INVALID, ConfigNum: kv.currCfg.Num}
		}
	}
	kv.exeDOPHistory = make(map[int64]bool)
	kv.applySeq = 0
	kv.RSMSeq = 0
	kv.invalidShardCnt = 0

	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
