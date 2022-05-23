package raft

import (
	"distributed-systems/rpcutil"
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Raft struct {
	mu    sync.Mutex
	peers []*rpcutil.ClientEnd
	me    int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	logs        []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	state       State
	voteCount   int
	stepDownCh  chan bool
	grantVoteCh chan bool
	winElectCh  chan bool
	heartbeatCh chan bool
	applyCh     chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type ApplyMsg struct {
	Command        interface{}
	CommandIndex   int
	IsCommandValid bool
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[rf.getLastLogIndex()].Term
}

func (rf *Raft) isLogUpToDate(cLastLogIndex int, cLastLogTerm int) bool {
	lastLogIndex, lastLogTerm := rf.getLastLogIndex(), rf.getLastLogTerm()
	if cLastLogTerm == lastLogTerm {
		return cLastLogIndex >= lastLogIndex
	}
	return cLastLogTerm > lastLogTerm
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) && (rf.votedFor < 0 || rf.votedFor == args.CandidateId)
	if reply.VoteGranted {
		rf.votedFor = args.CandidateId
		rf.grantVoteCh <- true
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs) {
	reply := &RequestVoteReply{}
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	if rf.state == Candidate && args.Term == rf.currentTerm && reply.Term == rf.currentTerm && reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount == len(rf.peers)/2+1 {
			rf.winElectCh <- true
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	for server := range rf.peers {
		if server != rf.me {
			go rf.sendRequestVote(server, &args)
		}
	}
}

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied; i <= rf.commitIndex; i++ {
		rf.applyCh <- ApplyMsg{
			Command:        rf.logs[i].Command,
			CommandIndex:   i,
			IsCommandValid: true,
		}
		rf.lastApplied = i
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	reply.Success = args.Term == rf.currentTerm && args.PrevLogIndex < len(rf.logs) && args.PrevLogTerm == rf.logs[args.PrevLogIndex].Term
	if reply.Success {
		rf.heartbeatCh <- true
		for i := 0; i < len(args.Entries); i++ {
			if i+args.PrevLogIndex+1 < len(rf.logs) {
				rf.logs[i+args.PrevLogIndex+1] = args.Entries[i]
			} else {
				rf.logs = append(rf.logs, args.Entries[i])
			}
		}
		if args.LeaderCommit > rf.commitIndex {
			if args.LeaderCommit > rf.getLastLogIndex() {
				rf.commitIndex = rf.getLastLogIndex()
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			go rf.applyLogs()
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader && args.Term == rf.currentTerm && reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
	}
	if reply.Success {
		rf.nextIndex[server] += len(args.Entries)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	} else {
		rf.nextIndex[server] >>= 1
	}
	for i := rf.getLastLogIndex(); i >= rf.commitIndex && rf.logs[i].Term == rf.currentTerm; i-- {
		count := 1
		for j := 0; j < len(rf.peers); j++ {
			if rf.matchIndex[j] >= i {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = i
			go rf.applyLogs()
			break
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	for server := range rf.peers {
		if server != rf.me {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[server] - 1,
				PrevLogTerm:  rf.logs[rf.nextIndex[server]-1].Term,
				Entries:      rf.logs[rf.nextIndex[server]:],
				LeaderCommit: rf.commitIndex,
			}
			go rf.sendAppendEntries(server, &args)
		}
	}
}

func (rf *Raft) convertToFollower(term int) {
	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	if state != Follower {
		rf.stepDownCh <- true
	}
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for server := range rf.peers {
		rf.nextIndex[server] = rf.getLastLogIndex() + 1
	}
	rf.broadcastAppendEntries()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.broadcastRequestVote()
}

func getElectionTimeout() time.Duration {
	return time.Duration(360+rand.Intn(240)) * time.Millisecond
}

func (rf *Raft) runServer() {
	for {
		switch rf.state {
		case Follower:
			select {
			case <-rf.grantVoteCh:
			case <-rf.heartbeatCh:
			case <-time.After(getElectionTimeout()):
				rf.convertToCandidate()
			}
		case Candidate:
			select {
			case <-rf.stepDownCh:
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(getElectionTimeout()):
				rf.convertToCandidate()
			}
		case Leader:
			select {
			case <-rf.stepDownCh:
			case <-time.After(60 * time.Millisecond):
				rf.broadcastAppendEntries()
			}
		}
	}
}

func Make(peers []*rpcutil.ClientEnd, me int, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.voteCount = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = []LogEntry{{Term: 0}}
	rf.stepDownCh = make(chan bool)
	rf.grantVoteCh = make(chan bool)
	rf.winElectCh = make(chan bool)
	rf.heartbeatCh = make(chan bool)
	rf.applyCh = applyCh
	go rf.runServer()
	return rf
}
