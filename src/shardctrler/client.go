package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int
	id        int64
	requestId int
	cnt       int
	// Your data here.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers: servers,
		id:      nrand(),
		cnt:     len(servers),
	}
	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num, ClientId: ck.id, RequestId: ck.requestId}
	// Your code here.
	ck.requestId++
	reply := QueryReply{}
	ok := ck.servers[ck.leaderId].Call("ShardCtrler.Query", args, &reply)
	if ok && !reply.WrongLeader {
		return reply.Config
	}
	for {
		// try each known server.
		for i := 0; i < ck.cnt; i++ {
			reply := QueryReply{}
			ok := ck.servers[i].Call("ShardCtrler.Query", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return reply.Config
			}
		}
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClientId: ck.id, RequestId: ck.requestId}
	// Your code here.
	ck.requestId++
	reply := JoinReply{}
	ok := ck.servers[ck.leaderId].Call("ShardCtrler.Join", args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	for {
		// try each known server.
		for i := 0; i < ck.cnt; i++ {
			reply := JoinReply{}
			ok := ck.servers[i].Call("ShardCtrler.Join", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return
			}
		}
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClientId: ck.id, RequestId: ck.requestId}
	// Your code here.
	ck.requestId++
	reply := LeaveReply{}
	ok := ck.servers[ck.leaderId].Call("ShardCtrler.Leave", args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	for {
		// try each known server.
		for i := 0; i < ck.cnt; i++ {
			reply := JoinReply{}
			ok := ck.servers[i].Call("ShardCtrler.Leave", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return
			}
		}
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClientId: ck.id, RequestId: ck.requestId}
	// Your code here.
	ck.requestId++
	reply := MoveReply{}
	ok := ck.servers[ck.leaderId].Call("ShardCtrler.Move", args, &reply)
	if ok && !reply.WrongLeader {
		return
	}
	for {
		// try each known server.
		for i := 0; i < ck.cnt; i++ {
			reply := MoveReply{}
			ok := ck.servers[i].Call("ShardCtrler.Move", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return
			}
		}
	}
}

func (ck *Clerk) Work(gid int) int {
	args := &WorkArgs{ClientId: ck.id, RequestId: ck.requestId, Gid: gid}
	// Your code here.
	ck.requestId++
	reply := WorkReply{}
	ok := ck.servers[ck.leaderId].Call("ShardCtrler.Work", args, &reply)
	if ok && !reply.WrongLeader {
		return reply.Gid
	}
	for {
		// try each known server.
		for i := 0; i < ck.cnt; i++ {
			reply := WorkReply{}
			ok := ck.servers[i].Call("ShardCtrler.Work", args, &reply)
			if ok && !reply.WrongLeader {
				ck.leaderId = i
				return reply.Gid
			}
		}
	}
}
