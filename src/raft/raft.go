package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const Null, Follower, Candidate, Leader = -1, 0, 1, 2

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state
	currentTerm int
	votedFor    int
	log         []LogEntry
	snapshot    []byte
	logOffset   int
	lastTerm    int

	// Volatile state
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int
	isCommiting bool

	// Auxiliary variables
	role        int
	heartbeated bool
	peerCnt     int
	halfCnt     int
	voteCnt     int
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isCommiting {
		return
	}
	rf.isCommiting = true
	if rf.lastApplied+1 < rf.logOffset {
		rf.lastApplied = rf.logOffset - 1
	}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		msg := ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied-rf.logOffset].Command, CommandIndex: rf.lastApplied}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
	}
	rf.isCommiting = false
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.logOffset)
	e.Encode(rf.lastTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, logOffset, lastTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil || d.Decode(&logOffset) != nil || d.Decode(&lastTerm) != nil {
		return
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.logOffset = logOffset
		if logOffset > 0 {
			rf.lastApplied = logOffset - 1
			rf.commitIndex = logOffset - 1
		}
		rf.lastTerm = lastTerm
		rf.snapshot = rf.persister.ReadSnapshot()
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index >= rf.logOffset {
		if len(rf.log) > index-rf.logOffset {
			rf.lastTerm = rf.log[index-rf.logOffset].Term
		}
		rf.log = rf.log[index+1-rf.logOffset:]
		rf.logOffset = index + 1
		rf.snapshot = snapshot
		go rf.persist()
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntryArgs struct {
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
	XTerm   int
	XIndex  int
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		reply.Term = args.Term
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = Null
			go rf.persist()
		}
		lastLogIndex := len(rf.log) - 1
		lastLogTerm := rf.lastTerm
		if lastLogIndex >= 0 {
			lastLogTerm = rf.log[lastLogIndex].Term
		}
		lastLogIndex += rf.logOffset
		if (rf.votedFor == Null || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
			rf.votedFor = args.CandidateId
			rf.heartbeated = true
			reply.VoteGranted = true
			go rf.persist()
		} else {
			reply.VoteGranted = false
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
	} else {
		reply.Term = args.Term
		rf.heartbeated = true
		if args.Term > rf.currentTerm || rf.role == Candidate {
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = Null
			go rf.persist()
		}
		if len(rf.log)+rf.logOffset <= args.PrevLogIndex {
			reply.XTerm = -1
			reply.XIndex = len(rf.log) + rf.logOffset
		} else if args.PrevLogIndex >= rf.logOffset && rf.log[args.PrevLogIndex-rf.logOffset].Term != args.PrevLogTerm {
			reply.XTerm = rf.log[args.PrevLogIndex-rf.logOffset].Term
			l, r := 0, args.PrevLogIndex-rf.logOffset
			for l < r {
				mid := (l + r) >> 1
				if rf.log[mid].Term < reply.XTerm {
					l = mid + 1
				} else {
					r = mid
				}
			}
			reply.XIndex = r + rf.logOffset
		} else {
			pos, length := args.PrevLogIndex+1-rf.logOffset, len(rf.log)
			for i, v := range args.Entries {
				if pos+i >= length {
					rf.log = append(rf.log, args.Entries[i:]...)
					go rf.persist()
					break
				} else if pos+i >= 0 && rf.log[pos+i].Term != v.Term {
					rf.log = append(rf.log[:pos+i], args.Entries[i:]...)
					go rf.persist()
					break
				}
			}
			if args.LeaderCommit > rf.commitIndex {
				rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1+rf.logOffset)
			}
			if rf.commitIndex > rf.lastApplied {
				go rf.applyLog()
			}
			reply.Success = true
		}
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
	} else {
		reply.Term = args.Term
		rf.heartbeated = true
		if args.Term > rf.currentTerm || rf.role == Candidate {
			rf.currentTerm = args.Term
			rf.role = Follower
			rf.votedFor = Null
			go rf.persist()
		}
		if args.LastIncludedIndex >= rf.logOffset {
			rf.lastTerm = args.LastIncludedTerm
			rf.snapshot = args.Data
			if args.LastIncludedIndex > rf.lastApplied {
				msg := ApplyMsg{CommandValid: false, SnapshotValid: true, Snapshot: rf.snapshot, SnapshotIndex: args.LastIncludedIndex, SnapshotTerm: rf.lastTerm}
				if args.LastIncludedIndex > rf.commitIndex {
					rf.commitIndex = args.LastIncludedIndex
				}
				rf.lastApplied = args.LastIncludedIndex
				if len(rf.log)+rf.logOffset > args.LastIncludedIndex+1 {
					rf.log = rf.log[args.LastIncludedIndex+1-rf.logOffset:]
				} else {
					rf.log = make([]LogEntry, 0)
				}
				rf.logOffset = args.LastIncludedIndex + 1
				rf.mu.Unlock()
				rf.applyCh <- msg
			} else {
				rf.mu.Unlock()
			}
			rf.persist()
		} else {
			rf.mu.Unlock()
		}
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	index := len(rf.log) + rf.logOffset
	term := rf.currentTerm
	isLeader := rf.role == Leader

	// Your code here (2B).
	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: term, Command: command})
		go rf.persist()
		rf.nextIndex[rf.me]++
		rf.matchIndex[rf.me]++
		for i := 0; i < rf.peerCnt; i++ {
			if i != rf.me {
				go rf.sendSingleHeartbeat(i)
			}
		}
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 300 and 450
		// milliseconds.
		ms := 200 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Follower && !rf.heartbeated {
			rf.role = Candidate
		}
		if rf.role == Candidate {
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCnt = 1
			go rf.persist()
			lastLogIndex := len(rf.log) - 1
			lastLogTerm := rf.lastTerm
			if lastLogIndex >= 0 {
				lastLogTerm = rf.log[lastLogIndex].Term
			}
			lastLogIndex += rf.logOffset
			args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogTerm: lastLogTerm, LastLogIndex: lastLogIndex}
			for i := 0; i < rf.peerCnt; i++ {
				if i != rf.me {
					go rf.askVote(i, &args)
				}
			}
		}
		rf.heartbeated = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) askVote(peer int, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	if rf.sendRequestVote(peer, args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = Null
			go rf.persist()
		}
		if rf.role == Candidate && reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.voteCnt++
			if rf.voteCnt >= rf.halfCnt {
				rf.role = Leader
				nextIndex := len(rf.log) + rf.logOffset
				for i := 0; i < rf.peerCnt; i++ {
					rf.nextIndex[i] = nextIndex
					rf.matchIndex[i] = 0
				}
				rf.matchIndex[rf.me] = nextIndex - 1
				go rf.sendHeartbeat()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendInstallSnapshot(peer int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.logOffset - 1,
		LastIncludedTerm:  rf.lastTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if rf.peers[peer].Call("Raft.InstallSnapshot", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = Follower
			rf.votedFor = Null
			go rf.persist()
		} else {
			rf.nextIndex[peer] = args.LastIncludedIndex + 1
			rf.matchIndex[peer] = args.LastIncludedIndex
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) sendSingleHeartbeat(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[peer] < rf.logOffset {
		rf.mu.Unlock()
		rf.sendInstallSnapshot(peer)
		return
	}
	args := AppendEntryArgs{
		Term:         rf.currentTerm,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		LeaderCommit: rf.commitIndex,
	}
	if args.PrevLogIndex < rf.logOffset {
		args.PrevLogTerm = rf.lastTerm
	} else {
		args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.logOffset].Term
	}
	args.Entries = rf.log[rf.nextIndex[peer]-rf.logOffset:]
	rf.mu.Unlock()
	reply := AppendEntryReply{}
	if rf.peers[peer].Call("Raft.AppendEntries", &args, &reply) {
		rf.mu.Lock()
		if reply.Term > args.Term {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = Null
				go rf.persist()
			}
		} else if reply.Success {
			nextIndex := args.PrevLogIndex + 1 + len(args.Entries)
			rf.nextIndex[peer] = nextIndex
			rf.matchIndex[peer] = nextIndex - 1
			go rf.updateCommitIdx()
		} else if reply.XTerm == -1 {
			rf.nextIndex[peer] = reply.XIndex
		} else if args.PrevLogIndex < rf.logOffset {
			rf.nextIndex[peer] = rf.logOffset - 1
		} else {
			l, r := 0, args.PrevLogIndex-rf.logOffset
			for l < r {
				mid := (l + r) >> 1
				if rf.log[mid].Term <= reply.XTerm {
					l = mid + 1
				} else {
					r = mid
				}
			}
			if (r == 0 && rf.lastTerm == reply.Term) || (r > 0 && rf.log[r-1].Term == reply.Term) {
				rf.nextIndex[peer] = r + rf.logOffset
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCommitIdx() {
	rf.mu.Lock()
	if rf.role == Leader {
		l, r := len(rf.log), len(rf.log)-1
		for l+rf.logOffset > rf.commitIndex && l > 0 && rf.log[l-1].Term == rf.currentTerm {
			l--
		}
		ans := rf.commitIndex
		for l <= r {
			mid := (l + r) >> 1
			cnt := 0
			for i := 0; i < rf.peerCnt; i++ {
				if rf.matchIndex[i] >= mid+rf.logOffset {
					cnt++
				}
			}
			if cnt >= rf.halfCnt {
				ans = mid + rf.logOffset
				l = mid + 1
			} else {
				r = mid - 1
			}
		}
		rf.commitIndex = ans
		if rf.lastApplied < rf.commitIndex {
			go rf.applyLog()
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.role == Leader {
			for i := 0; i < rf.peerCnt; i++ {
				if i != rf.me {
					go rf.sendSingleHeartbeat(i)
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			break
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		applyCh:    applyCh,
		votedFor:   Null,
		log:        make([]LogEntry, 0),
		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		peerCnt:    len(peers),
		halfCnt:    (len(peers) + 1) >> 1,
	}

	// Your initialization code here (2A, 2B, 2C).
	rf.log = append(rf.log, LogEntry{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
