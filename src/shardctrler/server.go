package shardctrler

import (
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	chanPool  []chan Op
	chanMap   map[int]chan Op
	answerMap map[int64]Answer

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Type      string
	Num       int
	GID       int
	Shard     int
	GIDs      []int
	Servers   map[int][]string
	Config    Config
	ClientId  int64
	RequestId int
}

type Answer struct {
	RequestId int
	Config    Config
}

func (sc *ShardCtrler) getChannel(index int) chan Op {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if ch, ok := sc.chanMap[index]; ok {
		return ch
	}
	for len(sc.chanPool) == 0 {
		sc.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		sc.mu.Lock()
	}
	ch := sc.chanPool[0]
	sc.chanMap[index] = ch
	sc.chanPool = sc.chanPool[1:]
	return ch
}

func (sc *ShardCtrler) releaseChannel(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch := sc.chanMap[index]
	sc.chanPool = append(sc.chanPool, ch)
	delete(sc.chanMap, index)
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	if answer, ok := sc.answerMap[args.ClientId]; ok && answer.RequestId == args.RequestId {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, ok := sc.rf.Start(Op{Type: Join, Servers: args.Servers, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.WrongLeader = true
		return
	}
	ch := sc.getChannel(index)
	select {
	case op := <-ch:
		if op.Type != Join {
			reply.WrongLeader = true
		}
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	}
	go func() {
		sc.releaseChannel(index)
	}()
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	if answer, ok := sc.answerMap[args.ClientId]; ok && answer.RequestId == args.RequestId {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, ok := sc.rf.Start(Op{Type: Leave, GIDs: args.GIDs, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.WrongLeader = true
		return
	}
	ch := sc.getChannel(index)
	select {
	case op := <-ch:
		if op.Type != Leave {
			reply.WrongLeader = true
		}
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	}
	go func() {
		sc.releaseChannel(index)
	}()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	if answer, ok := sc.answerMap[args.ClientId]; ok && answer.RequestId == args.RequestId {
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, ok := sc.rf.Start(Op{Type: Move, GID: args.GID, Shard: args.Shard, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.WrongLeader = true
		return
	}
	ch := sc.getChannel(index)
	select {
	case op := <-ch:
		if op.Type != Move && op.GID != args.GID && op.Shard != args.Shard {
			reply.WrongLeader = true
		}
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	}
	go func() {
		sc.releaseChannel(index)
	}()
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if answer, ok := sc.answerMap[args.ClientId]; ok && answer.RequestId == args.RequestId {
		reply.Config = answer.Config
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, ok := sc.rf.Start(Op{Type: Query, Num: args.Num, ClientId: args.ClientId, RequestId: args.RequestId})
	if !ok {
		reply.WrongLeader = true
		return
	}
	ch := sc.getChannel(index)
	select {
	case op := <-ch:
		if op.Type != Query {
			reply.WrongLeader = true
		} else {
			reply.Config = op.Config
		}
	case <-time.After(300 * time.Millisecond):
		reply.WrongLeader = true
	}
	go func() {
		sc.releaseChannel(index)
	}()
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) rebalance() {
	config := &sc.configs[len(sc.configs)-1]
	// sort to get deterministic order
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
		return
	}
	var groups []int
	cnt := make(map[int]int)
	for k := range config.Groups {
		groups = append(groups, k)
		cnt[k] = 0
	}
	sort.Ints(groups)
	sz := len(groups)
	n, rst := NShards/sz, NShards%sz
	var rearrange []int
	for i := 0; i < NShards; i++ {
		if _, find := cnt[config.Shards[i]]; !find || cnt[config.Shards[i]] == n+1 || (cnt[config.Shards[i]] == n && rst == 0) {
			rearrange = append(rearrange, i)
		} else {
			cnt[config.Shards[i]]++
			if cnt[config.Shards[i]] > n {
				rst--
			}
		}
	}
	p := 0
	for _, i := range rearrange {
		for cnt[groups[p]] == n+1 || (cnt[groups[p]] == n && rst == 0) {
			p++
		}
		config.Shards[i] = groups[p]
		cnt[groups[p]]++
		if cnt[groups[p]] > n {
			rst--
		}
	}
}

func (sc *ShardCtrler) applier() {
	for m := range sc.applyCh {
		if m.CommandValid {
			sc.mu.Lock()
			op := m.Command.(Op)
			flag := false
			if answer, ok := sc.answerMap[op.ClientId]; ok && answer.RequestId == op.RequestId {
				op.Config = answer.Config
				flag = true
			} else if op.Type != Query {
				sc.answerMap[op.ClientId] = Answer{RequestId: op.RequestId}
				if op.Type == Join {
					config := Config{Num: len(sc.configs), Groups: make(map[int][]string), Shards: sc.configs[len(sc.configs)-1].Shards}
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[k] = v
					}
					for k, v := range op.Servers {
						config.Groups[k] = v
					}
					sc.configs = append(sc.configs, config)
					sc.rebalance()
				} else if op.Type == Leave {
					config := Config{Num: len(sc.configs), Groups: make(map[int][]string), Shards: sc.configs[len(sc.configs)-1].Shards}
					for k, v := range sc.configs[len(sc.configs)-1].Groups {
						config.Groups[k] = v
					}
					for _, v := range op.GIDs {
						delete(config.Groups, v)
					}
					sc.configs = append(sc.configs, config)
					sc.rebalance()
				} else {
					config := sc.configs[len(sc.configs)-1]
					config.Num++
					config.Shards[op.Shard] = op.GID
					sc.configs = append(sc.configs, config)
				}
			} else {
				num := len(sc.configs) - 1
				if op.Num != -1 && op.Num < num {
					num = op.Num
				}
				sc.answerMap[op.ClientId] = Answer{RequestId: op.RequestId, Config: sc.configs[num]}
			}
			if term, isLeader := sc.rf.GetState(); isLeader && term == m.CommandTerm {
				if !flag && op.Type == Query {
					op.Config = sc.answerMap[op.ClientId].Config
				}
				sc.mu.Unlock()
				ch := sc.getChannel(m.CommandIndex)
				ch <- op
				sc.mu.Lock()
			}
			sc.mu.Unlock()
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := &ShardCtrler{
		me:        me,
		configs:   []Config{{}},
		applyCh:   make(chan raft.ApplyMsg),
		chanPool:  make([]chan Op, 1000),
		chanMap:   make(map[int]chan Op),
		answerMap: make(map[int64]Answer),
	}

	labgob.Register(Op{})
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	for i := 0; i < 1000; i++ {
		sc.chanPool[i] = make(chan Op)
	}

	// Your code here.
	go sc.applier()

	return sc
}
