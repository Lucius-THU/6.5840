package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type      string
	Key       string
	Value     string
	ClientId  int64
	RequestId int
}

type Answer struct {
	requestId int
	value     string
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	persister *raft.Persister
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	table     map[string]string
	chanPool  []chan Op
	chanMap   map[int]chan Op
	answerMap map[int64]Answer
}

func (kv *KVServer) getChannel(index int) chan Op {
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

func (kv *KVServer) releaseChannel(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	ch := kv.chanMap[index]
	kv.chanPool = append(kv.chanPool, ch)
	delete(kv.chanMap, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if answer, ok := kv.answerMap[args.ClientId]; ok && answer.requestId == args.RequestId {
		reply.Value = answer.value
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, ok := kv.rf.Start(Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(index)
	select {
	case op := <-ch:
		if op.Type == "Get" && op.Key == args.Key {
			reply.Value = op.Value
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.releaseChannel(index)
	}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	if answer, ok := kv.answerMap[args.ClientId]; ok && answer.requestId == args.RequestId {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	index, _, ok := kv.rf.Start(Op{Type: args.Op, Key: args.Key, Value: args.Value, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.Err = ErrWrongLeader
		return
	}
	ch := kv.getChannel(index)
	select {
	case op := <-ch:
		if op.Type == args.Op && op.Key == args.Key && op.Value == args.Value {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(300 * time.Millisecond):
		reply.Err = ErrWrongLeader
	}
	go func() {
		kv.releaseChannel(index)
	}()
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for m := range kv.applyCh {
		if kv.killed() {
			return
		}
		if m.CommandValid {
			kv.mu.Lock()
			op := m.Command.(Op)
			flag := false
			if answer, ok := kv.answerMap[op.ClientId]; ok && answer.requestId == op.RequestId {
				op.Value = answer.value
				flag = true
			} else if op.Type != "Get" {
				kv.answerMap[op.ClientId] = Answer{requestId: op.RequestId}
				if op.Type == "Put" {
					kv.table[op.Key] = op.Value
				} else {
					kv.table[op.Key] += op.Value
				}
			} else {
				kv.answerMap[op.ClientId] = Answer{requestId: op.RequestId, value: kv.table[op.Key]}
			}
			if term, isLeader := kv.rf.GetState(); isLeader && term == m.CommandTerm {
				if !flag && op.Type == "Get" {
					op.Value = kv.table[op.Key]
				}
				kv.mu.Unlock()
				ch := kv.getChannel(m.CommandIndex)
				ch <- op
			} else {
				kv.mu.Unlock()
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := &KVServer{
		me:           me,
		persister:    persister,
		applyCh:      make(chan raft.ApplyMsg),
		maxraftstate: maxraftstate,
		table:        make(map[string]string),
		chanPool:     make([]chan Op, 1000),
		chanMap:      make(map[int]chan Op),
		answerMap:    make(map[int64]Answer),
	}
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	for i := 0; i < 1000; i++ {
		kv.chanPool[i] = make(chan Op)
	}
	go kv.applier()
	// You may need initialization code here.

	return kv
}
