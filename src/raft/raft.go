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
	"encoding/gob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

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

// state of each raft endpoint
type RaftState int

const (
	Follower RaftState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

func (state RaftState) String() string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	default:
		return "Undefined"
	}
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

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers:
	state       RaftState
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Channel for judging whether receive RPC or not
	requestVoteCh   chan bool
	appendEntriesCh chan bool
	winElectionCh   chan bool
	applyCh         chan ApplyMsg
	isKilled        bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	return term, isleader
}

/*
	Set channel when receives RPC
*/
func (rf *Raft) receiveRPC(ch chan bool) {
	select {
	case <-ch:
	default:
	}
	ch <- true
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.convertTo(Follower)
		}

		reply.VoteGranted = false
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (args.LastLogTerm > rf.getLastLogTerm() ||
			args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex()) {
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.state = Follower
			rf.receiveRPC(rf.requestVoteCh)
			DPrintf("server[%v]@%p votes for server[%v] at term[%v]", rf.me, rf, args.CandidateId, rf.currentTerm)
		}
		reply.Term = rf.currentTerm
	}
}

// Candidate starts RequestVote
func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	if rf.state != Candidate {
		DPrintf("server[%v]@%p is not Candidate, can not send RequestVote RPC", rf.me, rf)
		rf.mu.Unlock()
	} else {
		DPrintf("server[%v]@%p starts to send RequestVote RPC", rf.me, rf)
		args := RequestVoteArgs{
			rf.currentTerm,
			rf.me,
			rf.getLastLogIndex(),
			rf.getLastLogTerm(),
		}
		rf.mu.Unlock()

		type VotedNum struct {
			cnt int
			mu  sync.Mutex
		}
		votedNum := VotedNum{1, sync.Mutex{}}

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(id int, args RequestVoteArgs, votedNum *VotedNum) {
				reply := &RequestVoteReply{}
				ok := rf.sendRequestVote(id, args, reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					switch {
					case reply.Term > rf.currentTerm:
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						return
					case rf.state != Candidate:
						return
					case reply.VoteGranted:
						votedNum.mu.Lock()
						votedNum.cnt++
						votedNum.mu.Unlock()
					}

					if votedNum.cnt > int(len(rf.peers)/2) {
						rf.convertTo(Leader)
						rf.receiveRPC(rf.winElectionCh)
					}
				}
			}(i, args, &votedNum)

		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		DPrintf("server[%v]@%p receives AppendEntries RPC from server[%v]", rf.me, rf, args.LeaderId)
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.convertTo(Follower)
		}
		rf.receiveRPC(rf.appendEntriesCh)
		reply.Term = rf.currentTerm
		reply.Success = true
	}
}

func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	if rf.state != Leader {
		DPrintf("server[%v]@%p is not Leader, can not send AppendEntries RPC", rf.me, rf)
		rf.mu.Unlock()
	} else {
		DPrintf("server[%v]@%p starts to send AppendEntries RPC", rf.me, rf)
		rf.mu.Unlock()
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				Entries:      nil,
				LeaderCommit: rf.commitIndex,
			}
			rf.mu.Unlock()

			go func(id int, args AppendEntriesArgs) {
				reply := &AppendEntriesReply{}
				ok := rf.sendAppendEntries(id, args, reply)
				if ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					switch {
					case reply.Term > rf.currentTerm:
						rf.currentTerm = reply.Term
						rf.convertTo(Follower)
						return
					case rf.state != Leader:
						return
					}
				}
			}(i, args)
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
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
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) convertTo(state RaftState) {
	//defer rf.persist()
	switch state {
	case Follower:
		DPrintf("server[%v]@%p convert from %v to Follower", rf.me, rf, rf.state.String())
		rf.state = Follower
		rf.votedFor = -1
	case Candidate:
		DPrintf("server[%v]@%p convert from %v to Candidate", rf.me, rf, rf.state.String())
		rf.state = Candidate
		rf.currentTerm += 1
		rf.votedFor = rf.me
	case Leader:
		if rf.state != Candidate {
			DPrintf("server[%v]@%p cannot convert from %v to Leader", rf.me, rf, rf.state.String())
		} else {
			DPrintf("server[%v]@%p convert from %v to Leader", rf.me, rf, rf.state.String())
			//defer rf.persist()
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for i, _ := range rf.nextIndex {
				rf.nextIndex[i] = rf.getLastLogIndex() + 1
			}
		}
	}

}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	term := -1
	if rf.getLastLogIndex() > 0 {
		term = rf.log[rf.getLastLogIndex()].Term
	}
	return term
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
	index := -1
	term := -1
	isLeader := true

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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isKilled = true
	DPrintf("Kill Server[%v]@%p", rf.me, rf)
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
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
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.requestVoteCh = make(chan bool, 1)
	rf.appendEntriesCh = make(chan bool, 1)
	rf.winElectionCh = make(chan bool, 1)
	rf.applyCh = applyCh
	rf.isKilled = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("make server[%v]@%p", rf.me, rf)

	go func() {
		for !rf.isKilled {
			electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
			heartbeatInterval := time.Duration(50) * time.Millisecond
			rf.mu.Lock()
			state := rf.state
			rf.mu.Unlock()
			//DPrintf("Server[%v]@%p state:%v, electionTimeout:%v", rf.me, &rf, state, electionTimeout)

			switch state {
			case Follower:
				select {
				case <-rf.appendEntriesCh:
				case <-rf.requestVoteCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertTo(Candidate)
					rf.mu.Unlock()
				}
			case Candidate:
				go rf.startRequestVote()
				//if rf.state == Leader {
				//	break
				//}
				select {
				case <-rf.appendEntriesCh:
					rf.mu.Lock()
					DPrintf("Candidate server[%v]@%p receives AppendEntries RPC", rf.me, rf)
					rf.convertTo(Follower)
					rf.mu.Unlock()
				case <-rf.requestVoteCh:
				case <-rf.winElectionCh:
				case <-time.After(electionTimeout):
					rf.mu.Lock()
					rf.convertTo(Candidate)
					rf.mu.Unlock()
					//DPrintf("Server(%d) state:%v, convert success", rf.me, state)
				}
			case Leader:
				rf.startAppendEntries()
				time.Sleep(heartbeatInterval)
			}
		}
	}()

	return rf
}
