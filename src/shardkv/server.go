package shardkv

import (
	"bytes"
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
	SeqNum    int
	ClientId  int64
	RequestId int
	Shard     int
	Table     map[string]string
	AnswerMap map[int64]Answer
	Failed    int
	Group     []string
	Version   int
}

type Shard struct {
	State   int
	Version int
	SeqNum  int
	Group   []string
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
	table     map[int]map[string]string
	chanMap   map[int]chan Answer
	answerMap map[int]map[int64]Answer
	appliedId int
	shards    [shardctrler.NShards]Shard
}

func (kv *ShardKV) readSnapShot(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var table map[int]map[string]string
	var answerMap map[int]map[int64]Answer
	var appliedId int
	var shards [shardctrler.NShards]Shard
	if d.Decode(&table) != nil || d.Decode(&answerMap) != nil || d.Decode(&appliedId) != nil || d.Decode(&shards) != nil {
		log.Fatal("Decode error")
	} else {
		kv.table = table
		kv.answerMap = answerMap
		kv.appliedId = appliedId
		kv.shards = shards
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
	if ch, ok := kv.chanMap[index]; ok {
		return ch
	}
	kv.chanMap[index] = make(chan Answer, 1)
	return kv.chanMap[index]
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.shards[args.Shard].State != Serving {
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
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getChannel(index)
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op.RequestId == args.RequestId && op.Err != ErrWrongGroup {
			reply.Value = op.Value
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.shards[args.Shard].State != Serving {
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
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getChannel(index)
	kv.mu.Unlock()
	select {
	case op := <-ch:
		if op.RequestId == args.RequestId && op.Err != ErrWrongGroup {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
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

func (kv *ShardKV) saveSnapShot() {
	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.table)
		e.Encode(kv.answerMap)
		e.Encode(kv.appliedId)
		e.Encode(kv.shards)
		snapshot := w.Bytes()
		kv.rf.Snapshot(kv.appliedId, snapshot)
	}
}

func (kv *ShardKV) sendShard(shard int, group []string) {
	kv.mu.Lock()
	table, answerMap := deepCopy(kv.table[shard], kv.answerMap[shard])
	seqNum := kv.shards[shard].SeqNum
	version := kv.shards[shard].Version
	kv.mu.Unlock()
	args := ShardMoveArgs{Shard: shard, Version: version, Table: table, AnswerMap: answerMap}
	for _, server := range group {
		srv := kv.make_end(server)
		var reply ShardMoveReply
		ok := srv.Call("ShardKV.ShardMove", &args, &reply)
		if ok {
			if reply.Err == ErrWrongGroup {
				kv.rf.Start(Op{Type: "State", Shard: shard, State: Paused, SeqNum: seqNum, Failed: 1})
				return
			} else if reply.Err != ErrWrongLeader {
				kv.rf.Start(Op{Type: "State", Shard: shard, State: Paused, SeqNum: seqNum})
				return
			}
		}
	}
	kv.rf.Start(Op{Type: "State", Shard: shard, State: Paused, SeqNum: seqNum, Failed: 2})
}

func (kv *ShardKV) ShardMove(args *ShardMoveArgs, reply *ShardMoveReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if kv.shards[args.Shard].Version > args.Version {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	index, _, ok := kv.rf.Start(Op{Type: "ShardMove", Shard: args.Shard, Version: args.Version, Table: args.Table, AnswerMap: args.AnswerMap, SeqNum: kv.shards[args.Shard].SeqNum})
	if !ok {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	ch := kv.getChannel(index)
	kv.mu.Unlock()
	select {
	case ans := <-ch:
		reply.Err = ans.Err
	case <-time.After(200 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
}

func (kv *ShardKV) applier() {
	for m := range kv.applyCh {
		kv.mu.Lock()
		if m.SnapshotValid && kv.appliedId < m.SnapshotIndex {
			kv.readSnapShot(m.Snapshot)
		} else if m.CommandValid && kv.appliedId < m.CommandIndex {
			op := m.Command.(Op)
			kv.appliedId = m.CommandIndex
			if op.Type == "ShardMove" || op.Type == "State" {
				if op.SeqNum != kv.shards[op.Shard].SeqNum {
					kv.saveSnapShot()
					kv.mu.Unlock()
					continue
				}
				kv.shards[op.Shard].SeqNum++
			}
			if op.Type == "State" {
				kv.shards[op.Shard].State = op.State
				if op.State == Stored {
					kv.shards[op.Shard].Group = append(kv.shards[op.Shard].Group, op.Group...)
				} else if op.State == Paused {
					if op.Failed == 2 {
						kv.shards[op.Shard].State = Stored
					} else if op.Failed == 0 {
						delete(kv.table, op.Shard)
						delete(kv.answerMap, op.Shard)
						kv.shards[op.Shard].Group = nil
					} else {
						kv.shards[op.Shard].State = Stored
						kv.shards[op.Shard].Group = nil
					}
				}
			} else if op.Type == "ShardMove" {
				var err Err = ErrWrongLeader
				if kv.shards[op.Shard].Version > op.Version {
					err = OK
				} else if kv.shards[op.Shard].State == Waiting {
					kv.table[op.Shard], kv.answerMap[op.Shard] = deepCopy(op.Table, op.AnswerMap)
					kv.shards[op.Shard].State = Serving
					kv.shards[op.Shard].Version = op.Version + 1
					err = OK
				} else {
					err = ErrWrongGroup
				}
				if term, isLeader := kv.rf.GetState(); isLeader && term == m.CommandTerm {
					if ch, ok := kv.chanMap[m.CommandIndex]; ok {
						kv.mu.Unlock()
						ch <- Answer{Err: err}
						kv.mu.Lock()
						delete(kv.chanMap, m.CommandIndex)
					}
				}
			} else {
				flag := false
				var value string
				var err Err
				err = OK
				if answer, ok := kv.answerMap[op.Shard][op.ClientId]; ok && answer.RequestId == op.RequestId {
					value = answer.Value
					flag = true
				} else if kv.shards[op.Shard].State != Serving {
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
					if ch, ok := kv.chanMap[m.CommandIndex]; ok {
						kv.mu.Unlock()
						ch <- answer
						kv.mu.Lock()
						delete(kv.chanMap, m.CommandIndex)
					}
				}
			}
		}
		kv.saveSnapShot()
		kv.mu.Unlock()
	}
}

func (kv *ShardKV) configer() {
	kv.mu.Lock()
	gid := kv.gid
	flag := kv.mck.Work(gid) == gid
	kv.mu.Unlock()
	for {
		if _, isLeader := kv.rf.GetState(); isLeader {
			config := kv.mck.Query(-1)

			kv.mu.Lock()
			for i := 0; i < shardctrler.NShards; i++ {
				f := flag && kv.shards[i].SeqNum == 0
				if kv.shards[i].State == Serving && config.Shards[i] != kv.gid {
					var group []string
					if config.Shards[i] != 0 {
						group = append(group, config.Groups[config.Shards[i]]...)
					}
					kv.rf.Start(Op{Type: "State", Shard: i, State: Stored, Group: group, SeqNum: kv.shards[i].SeqNum})
				} else if config.Shards[i] == kv.gid && (f || (kv.shards[i].State == Stored && kv.shards[i].Group == nil)) {
					kv.rf.Start(Op{Type: "State", Shard: i, State: Serving, SeqNum: kv.shards[i].SeqNum})
				} else if f {
					var group []string
					if config.Shards[i] != 0 {
						group = append(group, config.Groups[config.Shards[i]]...)
					}
					kv.rf.Start(Op{Type: "State", Shard: i, State: Stored, Group: group, SeqNum: kv.shards[i].SeqNum})
				} else if kv.gid == config.Shards[i] && kv.shards[i].State == Paused {
					kv.rf.Start(Op{Type: "State", Shard: i, State: Waiting, SeqNum: kv.shards[i].SeqNum})
				} else if kv.gid != config.Shards[i] && kv.shards[i].State == Waiting {
					kv.rf.Start(Op{Type: "State", Shard: i, State: Paused, SeqNum: kv.shards[i].SeqNum})
				} else if kv.shards[i].State == Stored {
					if kv.shards[i].Group != nil {
						go kv.sendShard(i, kv.shards[i].Group)
					} else {
						var group []string
						if config.Shards[i] != 0 && config.Shards[i] != kv.gid {
							group = append(group, config.Groups[config.Shards[i]]...)
						}
						kv.rf.Start(Op{Type: "State", Shard: i, State: Stored, Group: group, SeqNum: kv.shards[i].SeqNum})
					}
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(60 * time.Millisecond)
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
	go kv.applier()
	go kv.configer()

	return kv
}
