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

import "sync"
import (
	"labrpc"
	"time"
	"math/rand"
)

// import "bytes"
// import "encoding/gob"

type State int

const (
	STATE_LEADER State = iota
	STATE_CANDIDATE
	STATE_FLLOWER

	HBINTERVAL = 50 * time.Millisecond // 50ms
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type LogEntry struct {
	Index int
	Term int
	Cmd interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state State

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log []LogEntry

	//Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int

	//Channels
	chanHeartbeat chan bool
	chanLeader chan bool
	//chanGrantVote chan bool

	voteCount int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here.

	return rf.currentTerm, rf.state == STATE_LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here.
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here.
	Term int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	if(args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		return
	}

	if(args.Term > rf.currentTerm) {
		rf.currentTerm = args.Term
		rf.state = STATE_FLLOWER
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm

	var lastIndex = rf.log[len(rf.log) - 1].Index
	var lastTerm = rf.log[len(rf.log) - 1].Term

	var logIsLatest = args.LastLogTerm >= lastTerm || ((lastTerm == args.LastLogTerm) && (lastIndex <= args.LastLogIndex))
	var isFirstVote = rf.votedFor == -1 || rf.votedFor == args.CandidateId

	if(logIsLatest && isFirstVote) {
		//rf.chanGrantVote <- true
		rf.state = STATE_FLLOWER
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	}

	//println(rf.me, " vote for ", rf.votedFor)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if(args.Term < rf.currentTerm) {
		reply.Term = rf.currentTerm
		return
	}

	rf.chanHeartbeat <- true

	if(args.Term > rf.currentTerm) {
		rf.currentTerm = args.Term
		rf.state = STATE_FLLOWER
		rf.votedFor = -1
	}

	reply.Term = args.Term

	return
}

//
// example code to send a AppendEntries RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if(ok) {
		if(rf.state != STATE_CANDIDATE) {
			return ok
		}


		if(args.Term != rf.currentTerm) {
			return ok
		}

		if (reply.Term > rf.currentTerm) {
			rf.state = STATE_FLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
		}

		if (reply.VoteGranted) {
			rf.voteCount++
			if (rf.state == STATE_CANDIDATE && rf.voteCount > len(rf.peers)/2) {
				rf.state = STATE_FLLOWER
				rf.chanLeader <- true
			}
		}
	}

	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (ok) {
		if (rf.state != STATE_LEADER) {
			return ok
		}
		if (args.Term != rf.currentTerm) {
			return ok
		}

		if (reply.Term > rf.currentTerm) {
			rf.currentTerm = reply.Term
			rf.state = STATE_FLLOWER
			rf.votedFor = -1
			return ok
		}

	}

	return ok
}

func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var args AppendEntriesArgs
	args.LeaderId = rf.me
	args.Term = rf.currentTerm

	for index := range rf.peers {
		if index != rf.me && rf.state == STATE_LEADER {
			go func(i int) {
				var reply AppendEntriesReply
				rf.sendAppendEntries(i, args, &reply)
			}(index)
		}
	}
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// pwdtester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.state = STATE_FLLOWER
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term:0})
	rf.commitIndex = 0
	rf.lastApplied = 0

	//Channels
	rf.chanHeartbeat = make(chan bool,100)
	rf.chanLeader = make (chan bool,100)
	//rf.chanGrantVote = make(chan bool,100)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case STATE_FLLOWER:
				//println("follower:", rf.me, rf.currentTerm)
				select {
				case <- rf.chanHeartbeat:
				//case <-rf.chanGrantVote:
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
					rf.mu.Lock()
					//println("Time out", rf.me, rf.currentTerm)
					rf.state = STATE_CANDIDATE
					rf.mu.Unlock()

				}
			case STATE_CANDIDATE:
				//println("candidate:", rf.me, rf.currentTerm)
				rf.mu.Lock()
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.voteCount = 1
				rf.mu.Unlock()

				var args RequestVoteArgs
				rf.mu.Lock()
				args.CandidateId = rf.me
				args.Term = rf.currentTerm
				args.LastLogTerm = rf.log[len(rf.log) - 1].Term
				args.LastLogIndex = rf.log[len(rf.log)-1].Index
				rf.mu.Unlock()

				for index := range rf.peers {
					if index != rf.me && rf.state == STATE_CANDIDATE {
						go func(i int) {
							var reply RequestVoteReply

							rf.sendRequestVote(i, args, &reply)
						}(index)
					}
				}
				select {
				case <- rf.chanHeartbeat:
					rf.mu.Lock()
					rf.state = STATE_FLLOWER
					rf.mu.Unlock()
				case <- rf.chanLeader:
					rf.mu.Lock()
					rf.state = STATE_LEADER
					rf.nextIndex = make([]int,len(rf.peers))
					rf.matchIndex = make([]int,len(rf.peers))
					for i := range rf.peers {
						rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
				case <- time.After(time.Duration(rand.Int63() % 333 + 550) * time.Millisecond):
				}
			case STATE_LEADER:
				//println("leader:", rf.me, rf.currentTerm)
				rf.sendHeartbeat()
				time.Sleep(HBINTERVAL)
			}
		}

	}()

	return rf
}
