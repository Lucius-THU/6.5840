package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"6.5840/labrpc"
)

const Null = -1

type Clerk struct {
	servers   []*labrpc.ClientEnd
	leaderId  int
	id        int64
	requestId int
	mu        sync.Mutex
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := &Clerk{
		servers:   servers,
		leaderId:  Null,
		id:        nrand(),
		requestId: 0,
	}
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	value := ""
	ck.mu.Lock()
	args := GetArgs{Key: key, ClientId: ck.id, RequestId: ck.requestId}
	ck.requestId++
	defer ck.mu.Unlock()
	if ck.leaderId != Null {
		reply := GetReply{}
		ck.mu.Unlock()
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		ck.mu.Lock()
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = Null
		} else if reply.Err == OK {
			value = reply.Value
		}
	}
	if ck.leaderId == Null {
		loop := true
		n := len(ck.servers)
		for loop {
			for i := 0; i < n; i++ {
				reply := GetReply{}
				ck.mu.Unlock()
				ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
				ck.mu.Lock()
				if ok && reply.Err != ErrWrongLeader {
					if reply.Err == OK {
						value = reply.Value
					}
					ck.leaderId = i
					loop = false
					break
				}
			}
		}
	}
	return value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.id, RequestId: ck.requestId}
	ck.requestId++
	defer ck.mu.Unlock()
	if ck.leaderId != Null {
		reply := PutAppendReply{}
		ck.mu.Unlock()
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		ck.mu.Lock()
		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = Null
		}
	}
	if ck.leaderId == Null {
		loop := true
		n := len(ck.servers)
		for loop {
			for i := 0; i < n; i++ {
				reply := PutAppendReply{}
				ck.mu.Unlock()
				ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
				ck.mu.Lock()
				if ok && reply.Err != ErrWrongLeader {
					ck.leaderId = i
					loop = false
					break
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
