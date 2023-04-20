package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	State     int
	PreState  int
	SeqNum    int
	ShardCnt  int
	ClientId  int64
	RequestId int
	Shard     int
	Table     map[string]string
	AnswerMap map[int64]Answer
	Group     []string
	Failed    bool
}

type Answer struct {
	RequestId int
	Value     string
	Err       Err
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	maxraftstate int // snapshot if log grows this big
	persister    *raft.Persister
	mck          *shardctrler.Clerk

	// Your definitions here.
	table            map[int]map[string]string
	chanPool         []chan Answer
	chanMap          map[int]chan Answer
	answerMap        map[int]map[int64]Answer
	appliedId        int
	shards           [shardctrler.NShards]int
	seqNum           [shardctrler.NShards]int
	shardCnt         [shardctrler.NShards]int
	waitingForPaused [shardctrler.NShards]bool
}

const Debug = true

func (kv *ShardKV) readSnapShot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int]map[string]string
	var answerMap map[int]map[int64]Answer
	var appliedId int
	var shards, seqNum, shardCnt [shardctrler.NShards]int
	var waitingForPaused [shardctrler.NShards]bool
	if d.Decode(&table) != nil || d.Decode(&answerMap) != nil || d.Decode(&appliedId) != nil ||
		d.Decode(&shards) != nil || d.Decode(&seqNum) != nil || d.Decode(&shardCnt) != nil || d.Decode(&waitingForPaused) != nil {
		log.Fatal("Decode error")
	} else {
		kv.table = table
		kv.answerMap = answerMap
		kv.appliedId = appliedId
		kv.shards = shards
		kv.seqNum = seqNum
		kv.shardCnt = shardCnt
		kv.waitingForPaused = waitingForPaused
	}
}

func (kv *ShardKV) updateClientRequest(op Op, answer Answer) {
	if _, ok := kv.answerMap[op.Shard]; !ok {
		kv.answerMap[op.Shard] = make(map[int64]Answer)
	}
	kv.answerMap[op.Shard][op.ClientId] = answer
	for k, v := range kv.answerMap {
		if k != op.Shard {
			delete(v, op.ClientId)
		}
	}
}

func (kv *ShardKV) getChannel(index int) chan Answer {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, ok := kv.chanMap[index]; ok {
		return ch
	}
	for len(kv.chanPool) == 0 {
		kv.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		kv.mu.Lock()
	}
	ch := kv.chanPool[0]
	kv.chanMap[index] = ch
	kv.chanPool = kv.chanPool[1:]
	return ch
}

func (kv *ShardKV) releaseChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := kv.chanMap[index]
	kv.chanPool = append(kv.chanPool, ch)
	delete(kv.chanMap, index)
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.shards[args.Shard] != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if answer, ok := kv.answerMap[args.Shard][args.ClientId]; ok && answer.RequestId == args.RequestId {
		reply.Value = answer.Value
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	index, _, ok := kv.rf.Start(Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId, Shard: args.Shard})
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(index)
	select {
	case op := <-ch:
		if op.RequestId == args.RequestId && op.Err != ErrWrongGroup {
			reply.Value = op.Value
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.releaseChannel(index)
	}()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.shards[args.Shard] != Serving {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if answer, ok := kv.answerMap[args.Shard][args.ClientId]; ok && answer.RequestId == args.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	index, _, ok := kv.rf.Start(Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId, Shard: args.Shard})
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(index)
	select {
	case op := <-ch:
		if op.RequestId == args.RequestId && op.Err != ErrWrongGroup {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.releaseChannel(index)
	}()
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	if _, isLeader := kv.rf.GetState(); isLeader && Debug {
		fmt.Println("kill", kv.gid)
	}
	kv.rf.Kill()
	// Your code here, if desired.
}

func deepCopy(t map[string]string, a map[int64]Answer) (map[string]string, map[int64]Answer) {
	table := make(map[string]string)
	for k, v := range t {
		table[k] = v
	}
	answerMap := make(map[int64]Answer)
	for k, v := range a {
		answerMap[k] = v
	}
	return table, answerMap
}

func (kv *ShardKV) sendShard(shard int, group []string) {
	kv.mu.Lock()
	table, answerMap := deepCopy(kv.table[shard], kv.answerMap[shard])
	seqNum := kv.seqNum[shard]
	me := kv.me
	if Debug {
		fmt.Println("send shard", shard, "gid", kv.gid)
	}
	kv.mu.Unlock()
	args := ShardMoveArgs{Shard: shard, Table: table, AnswerMap: answerMap}
	for _, server := range group {
		srv := kv.make_end(server)
		var reply ShardMoveReply
		ok := srv.Call("ShardKV.ShardMove", &args, &reply)
		if ok && reply.Err != ErrWrongLeader {
			if Debug {
				fmt.Println("-send shard", shard, "gid", kv.gid, me)
			}
			kv.rf.Start(Op{Type: "State", Shard: shard, State: Paused, SeqNum: seqNum, PreState: Sending})
			return
		}
	}
	kv.rf.Start(Op{Type: "State", Shard: shard, State: Paused, SeqNum: seqNum, Failed: true, PreState: Sending})
}

func (kv *ShardKV) ShardMove(args *ShardMoveArgs, reply *ShardMoveReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.shards[args.Shard] == Sending {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if kv.shards[args.Shard] == Serving || kv.shards[args.Shard] == Stored {
		reply.Err = OK
		if Debug {
			fmt.Println("ShardMove", kv.gid, args.Shard, kv.shards[args.Shard])
		}
		kv.mu.Unlock()
		return
	}
	index, _, ok := kv.rf.Start(Op{Type: "ShardMove", Shard: args.Shard, Table: args.Table, AnswerMap: args.AnswerMap})
	kv.mu.Unlock()
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(index)
	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.releaseChannel(index)
	}()
}

func (kv *ShardKV) configer() {
	isWorking := false
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(-1)

			flag := false
			kv.mu.Lock()
			if !isWorking {
				kv.mu.Unlock()
				flag = kv.mck.Work()
				kv.mu.Lock()
			}
			isWorking = false
			if Debug {
				fmt.Println("configer", kv.gid, kv.me, "config", config.Num, "shards", config.Shards, kv.shards)
			}
			for i := 0; i < shardctrler.NShards; i++ {
				//fmt.Println("configer", kv.gid, "shard", i, "state", kv.shards[i], "cnt", kv.shardCnt[i], "seqNum", kv.seqNum[i])
				preState := kv.shards[i]
				if kv.shardCnt[i] == 0 {
					if kv.shards[i] == Serving && config.Shards[i] != kv.gid {
						if config.Shards[i] != 0 && config.Groups[config.Shards[i]] != nil {
							kv.rf.Start(Op{Type: "ShardCnt", Shard: i, ShardCnt: 1, SeqNum: kv.seqNum[i]})
							kv.rf.Start(Op{Type: "State", Shard: i, State: Sending, SeqNum: kv.seqNum[i], Group: config.Groups[config.Shards[i]], PreState: preState})
						} else {
							kv.rf.Start(Op{Type: "State", Shard: i, State: Stored, SeqNum: kv.seqNum[i], PreState: preState})
						}
					} else if config.Shards[i] == kv.gid && (flag || kv.shards[i] == Stored) {
						kv.rf.Start(Op{Type: "State", Shard: i, State: Serving, SeqNum: kv.seqNum[i], PreState: preState})
					} else if kv.shards[i] == Stored || flag {
						if config.Shards[i] != 0 && config.Groups[config.Shards[i]] != nil {
							kv.rf.Start(Op{Type: "ShardCnt", Shard: i, ShardCnt: 1, SeqNum: kv.seqNum[i]})
							kv.rf.Start(Op{Type: "State", Shard: i, State: Sending, SeqNum: kv.seqNum[i], Group: config.Groups[config.Shards[i]], PreState: preState})
						} else if flag {
							kv.rf.Start(Op{Type: "State", Shard: i, State: Stored, SeqNum: kv.seqNum[i], PreState: preState})
						}
					} else if kv.gid == config.Shards[i] && kv.shards[i] == Paused {
						kv.rf.Start(Op{Type: "State", Shard: i, State: Waiting, SeqNum: kv.seqNum[i], PreState: preState})
					} else if kv.gid != config.Shards[i] && kv.shards[i] == Waiting {
						kv.rf.Start(Op{Type: "State", Shard: i, State: Paused, SeqNum: kv.seqNum[i], PreState: preState})
					}
				}
				if config.Shards[i] == kv.gid {
					isWorking = true
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) applier() {
	time.Sleep(300 * time.Millisecond) // wait for leader election
	for m := range kv.applyCh {
		kv.mu.Lock()
		if m.SnapshotValid && kv.appliedId < m.SnapshotIndex {
			kv.readSnapShot(m.Snapshot)
		} else if m.CommandValid && kv.appliedId < m.CommandIndex {
			op := m.Command.(Op)
			kv.appliedId = m.CommandIndex
			if op.Type == "State" && op.PreState != kv.shards[op.Shard] {
				if op.State == Paused && kv.waitingForPaused[op.Shard] {
					kv.waitingForPaused[op.Shard] = false
					kv.seqNum[op.Shard]++
				}
				kv.mu.Unlock()
				continue
			}
			if op.Type == "State" || op.Type == "ShardCnt" {
				if kv.waitingForPaused[op.Shard] {
					if op.Type == "State" && op.State == Paused {
						kv.waitingForPaused[op.Shard] = false
					} else {
						kv.mu.Unlock()
						continue
					}
				}
				/*
					if _, isLeader := kv.rf.GetState(); isLeader {
						fmt.Println("seqNum", kv.gid, op.SeqNum, kv.seqNum[op.Shard], op.Shard, op.Type, op.ShardCnt)
					}
				*/
				if op.SeqNum != kv.seqNum[op.Shard] {
					kv.mu.Unlock()
					continue
				}
				if op.Type == "ShardCnt" {
					kv.shardCnt[op.Shard] = op.ShardCnt
					kv.mu.Unlock()
					continue
				} else if kv.shardCnt[op.Shard] == 0 {
					kv.seqNum[op.Shard]++
				} else {
					kv.shardCnt[op.Shard]--
					kv.waitingForPaused[op.Shard] = true
				}
			}
			if op.Type == "State" {
				if _, isLeader := kv.rf.GetState(); isLeader && Debug {
					fmt.Println("State", kv.gid, kv.me, op.Shard, op.State, kv.shards[op.Shard])
				}
				if op.State == Waiting && kv.shards[op.Shard] == Stored {
					kv.shards[op.Shard] = Serving
				} else {
					kv.shards[op.Shard] = op.State
				}
				if op.State == Paused {
					if op.Failed {
						kv.shards[op.Shard] = Stored
					} else {
						delete(kv.table, op.Shard)
						delete(kv.answerMap, op.Shard)
					}
				} else if op.State == Sending {
					if term, isLeader := kv.rf.GetState(); term == m.CommandTerm && op.Group != nil {
						if isLeader {
							go kv.sendShard(op.Shard, op.Group)
						}
					} else {
						kv.shards[op.Shard] = Stored
						kv.waitingForPaused[op.Shard] = false
						kv.seqNum[op.Shard]++
					}
				}
			} else if op.Type == "ShardMove" {
				if kv.shards[op.Shard] != Serving && kv.shards[op.Shard] != Stored {
					kv.table[op.Shard], kv.answerMap[op.Shard] = deepCopy(op.Table, op.AnswerMap)
				}
				if kv.shards[op.Shard] == Waiting {
					kv.shards[op.Shard] = Serving
				} else {
					kv.shards[op.Shard] = Stored
				}
				if _, isLeader := kv.rf.GetState(); isLeader && Debug {
					fmt.Println("ShardMove", kv.gid, kv.me, op.Shard, kv.shards[op.Shard])
				}
				if term, isLeader := kv.rf.GetState(); isLeader && term == m.CommandTerm {
					kv.mu.Unlock()
					ch := kv.getChannel(m.CommandIndex)
					ch <- Answer{}
					kv.mu.Lock()
				}
			} else {
				flag := false
				var value string
				var err Err
				err = OK
				if answer, ok := kv.answerMap[op.Shard][op.ClientId]; ok && answer.RequestId == op.RequestId {
					value = answer.Value
					err = answer.Err
					flag = true
				} else if kv.shards[op.Shard] != Serving {
					err = ErrWrongGroup
				} else if op.Type != "Get" {
					kv.updateClientRequest(op, Answer{RequestId: op.RequestId})
					if _, ok := kv.table[op.Shard]; !ok {
						kv.table[op.Shard] = make(map[string]string)
					}
					if op.Type == "Put" {
						kv.table[op.Shard][op.Key] = op.Value
					} else {
						kv.table[op.Shard][op.Key] += op.Value
					}
				} else {
					kv.updateClientRequest(op, Answer{RequestId: op.RequestId, Value: kv.table[op.Shard][op.Key]})
				}
				if term, isLeader := kv.rf.GetState(); isLeader && term == m.CommandTerm {
					answer := Answer{RequestId: op.RequestId, Value: value, Err: err}
					if !flag && op.Type == "Get" {
						answer.Value = kv.table[op.Shard][op.Key]
					}
					kv.mu.Unlock()
					ch := kv.getChannel(m.CommandIndex)
					ch <- answer
					kv.mu.Lock()
				}
			}
			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.table)
				e.Encode(kv.answerMap)
				e.Encode(kv.appliedId)
				e.Encode(kv.shards)
				e.Encode(kv.seqNum)
				e.Encode(kv.shardCnt)
				e.Encode(kv.waitingForPaused)
				snapshot := w.Bytes()
				kv.rf.Snapshot(kv.appliedId, snapshot)
			}
		}
		kv.mu.Unlock()
	}
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &ShardKV{
		me:           me,
		maxraftstate: maxraftstate,
		make_end:     make_end,
		gid:          gid,
		applyCh:      make(chan raft.ApplyMsg),
		persister:    persister,
		mck:          shardctrler.MakeClerk(ctrlers),
		chanPool:     make([]chan Answer, 1000),
		chanMap:      make(map[int]chan Answer),
	}

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	snapshot := kv.persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.readSnapShot(snapshot)
	} else {
		kv.table = make(map[int]map[string]string)
		kv.answerMap = make(map[int]map[int64]Answer)
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < 1000; i++ {
		kv.chanPool[i] = make(chan Answer)
	}
	go kv.applier()
	go kv.configer()

	return kv
}
