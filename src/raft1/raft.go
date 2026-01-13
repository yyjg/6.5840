package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type Identity string

const (
	Leader    Identity = "Leader"
	Candidate Identity = "Candidate"
	Follower  Identity = "Follower"
)

type Log struct {
	Term    int
	Command string
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	voteFor     int

	log           []Log
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
	identity      Identity
	lastHeartBeat time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here (3A).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.identity == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		rf.voteFor = -1
		rf.currentTerm = args.Term
		rf.identity = Follower
	}
	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) && rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastHeartBeat = time.Now()
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.identity = Follower
	}

	rf.lastHeartBeat = time.Now()

	reply.Term = rf.currentTerm
	reply.Success = true
}

func (rf *Raft) isLogUpToDate(candidateLastIdx, candidateLastTerm int) bool {
	if len(rf.log) == 0 {
		return true
	}
	if candidateLastTerm == 0 && candidateLastIdx == 0 {
		return false
	}
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	if candidateLastTerm > lastTerm {
		return true
	}
	if candidateLastTerm == lastTerm && candidateLastIdx >= lastIdx {
		return true
	}
	return false
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
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	// 非领导者直接返回
	if rf.identity != Leader {
		return -1, rf.currentTerm, false
	}
	// 3A 阶段暂不处理日志，返回占位值
	return -1, rf.currentTerm, true
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
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		electionTimeout := time.Duration(300+rand.Int63()%200) * time.Millisecond
		time.Sleep(electionTimeout)

		rf.mu.Lock()

		if rf.identity != Leader && time.Since(rf.lastHeartBeat) > electionTimeout {
			rf.currentTerm += 1
			rf.voteFor = rf.me
			rf.identity = Candidate
			rf.lastHeartBeat = time.Now()
			go rf.startElection()
		}

		rf.mu.Unlock()
	}
}
func (rf *Raft) leaderHeartBeatLoop() {
	for rf.killed() == false {
		time.Sleep(100 * time.Millisecond)

		rf.mu.Lock()
		if rf.identity != Leader {
			rf.mu.Unlock()
			return
		}
		currentTerm := rf.currentTerm
		rf.mu.Unlock()

		for server := 0; server < len(rf.peers); server++ {
			if server == rf.me {
				continue
			}
			args := AppendEntriesArgs{
				Term:         currentTerm,
				LeaderID:     rf.me,
				PrevLogIndex: rf.lastLogIndex(),
				PrevLogTerm:  rf.lastLogTerm(),
				Entries:      []Log{},
				LeaderCommit: rf.commitIndex,
			}
			reply := AppendEntriesReply{}
			go func(server int, args AppendEntriesArgs, reply AppendEntriesReply) {

				rf.sendAppendEntries(server, &args, &reply)
			}(server, args, reply)
		}
	}
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	// 保存选举时的任期（防止任期变更后误统计）
	electionTerm := rf.currentTerm
	peerCount := len(rf.peers)
	rf.mu.Unlock()

	voteCnt := 1
	var voteMu sync.Mutex
	for server := 0; server < peerCount; server++ {
		if server == rf.me {
			continue
		}
		rf.mu.Lock()
		args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogIndex(), LastLogTerm: rf.lastLogTerm()}
		rf.mu.Unlock()
		reply := RequestVoteReply{}
		go func(server int, args RequestVoteArgs, reply RequestVoteReply) {
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.currentTerm != electionTerm || rf.identity != Candidate {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.voteFor = -1
					rf.identity = Follower
					return
				}
				if reply.VoteGranted {
					voteMu.Lock()
					voteCnt++
					defer voteMu.Unlock()
					if voteCnt > peerCount/2 {
						rf.identity = Leader
						// 防御性初始化 nextIndex/matchIndex
						if len(rf.nextIndex) != peerCount {
							rf.nextIndex = make([]int, peerCount)
							rf.matchIndex = make([]int, peerCount)
						}
						for i := range rf.nextIndex {
							rf.nextIndex[i] = rf.lastLogIndex() + 1
							rf.matchIndex[i] = 0
						}
						go rf.leaderHeartBeatLoop()
					}

				}
			}
		}(server, args, reply)
	}
}

// 获取日志最后一条的索引
func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return 0 // 空日志时返回0
	}
	return len(rf.log) - 1
}

// 获取日志最后一条的任期
func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return 0 // 空日志时任期为0（Raft协议中任期从1开始，0表示无日志）
	}
	return rf.log[len(rf.log)-1].Term
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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.currentTerm = 0
	rf.voteFor = -1
	rf.identity = Follower
	rf.lastHeartBeat = time.Now()
	rf.log = make([]Log, 0)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
