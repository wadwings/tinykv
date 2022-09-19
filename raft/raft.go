// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionBaseline int

	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	if raftLog == nil {
		return nil
	}
	raftLog.applied = c.Applied
	prs := make(map[uint64]*Progress)
	for _, v := range c.peers {
		prs[v] = &Progress{
			Match: 0,                       // current been committed by 'v' replicant
			Next:  raftLog.LastIndex() + 1, // replicant be expected to receive entry index
		}
	}
	hardState, _, err := c.Storage.InitialState()
	if err != nil {
		return nil
	}
	return &Raft{
		id:               c.ID,
		State:            StateFollower,
		electionBaseline: c.ElectionTick,
		electionTimeout:  randomizedTimeout(c.ElectionTick),
		heartbeatTimeout: c.HeartbeatTick,
		Term:             hardState.Term,
		Prs:              prs,
		votes:            make(map[uint64]bool),
		Vote:             hardState.Vote,
		RaftLog:          raftLog,
	}
}

func randomizedTimeout(tz int) int {
	return int(float32(tz) * (rand.Float32() + 1))
}

// sendAppend sends an ap1pend RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	if r.State == StateLeader {
		var entries []*pb.Entry
		logTerm, _ := r.RaftLog.Term(r.Prs[to].Match)
		index := r.Prs[to].Match
		for i := r.Prs[to].Match + 1; i < r.Prs[to].Next; i++ {
			entries = append(entries, r.RaftLog.at(i))
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgAppend,
			To:                   to,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              logTerm,
			Index:                index,
			Entries:              entries,
			Commit:               r.RaftLog.committed,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		return true
	}
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgHeartbeat,
			To:                   to,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              0,
			Index:                0,
			Entries:              nil,
			Commit:               r.RaftLog.committed,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
	}
}

func (r *Raft) sendVote(to uint64) {
	if r.State == StateCandidate {
		logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		r.msgs = append(r.msgs, pb.Message{
			MsgType:              pb.MessageType_MsgRequestVote,
			To:                   to,
			From:                 r.id,
			Term:                 r.Term,
			LogTerm:              logTerm,
			Index:                r.RaftLog.LastIndex(),
			Entries:              nil,
			Commit:               0,
			Snapshot:             nil,
			Reject:               false,
			XXX_NoUnkeyedLiteral: struct{}{},
			XXX_unrecognized:     nil,
			XXX_sizecache:        0,
		})
		//log.Infof("%+v", r.msgs)
	}
}

func (r *Raft) bcastMsg(msgType pb.MessageType) {
	//log.Infof("%v broadcast Msg, %+v", r.id, msgType)
	for key := range r.Prs {
		if key == r.id {
			continue
		}
		switch msgType {
		case pb.MessageType_MsgHeartbeat:
			r.sendHeartbeat(key)
		case pb.MessageType_MsgRequestVote:
			//log.Infof("send to %v", key)
			r.sendVote(key)
		case pb.MessageType_MsgAppend:
			r.sendAppend(key)
		}
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	if r.State == StateLeader {
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.bcastMsg(pb.MessageType_MsgHeartbeat)
			r.heartbeatElapsed = 0
		}
	} else {
		r.electionElapsed++
		if r.electionElapsed == r.electionTimeout {
			r.becomeCandidate()
			r.electionElapsed = 0
			r.electionTimeout = randomizedTimeout(r.electionBaseline)
			r.bcastMsg(pb.MessageType_MsgRequestVote)
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	//log.Infof("%v now is Follower!", r.id)
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	//log.Infof("%v now is Candidate!", r.id)

	r.State = StateCandidate
	r.votes = make(map[uint64]bool)
	r.votes[r.id] = true
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	//log.Infof("%v now is Leader!", r.id)

	if r.State == StateCandidate {
		r.State = StateLeader
	}
	for _, peer := range r.Prs {
		peer.Match = r.RaftLog.LastIndex()
		peer.Next = r.RaftLog.LastIndex() + 1
	}
	r.Lead = 0
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      r.id,
		From:    r.id,
		Term:    r.Term,
		LogTerm: r.Term,
		Entries: []*pb.Entry{{Data: nil}},
	})

	// broadcast Append Msg
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		{
			switch m.GetMsgType() {
			case pb.MessageType_MsgAppend:
				r.handleAppendEntries(m)
			case pb.MessageType_MsgHeartbeat:
				r.handleHeartbeat(m)
			case pb.MessageType_MsgRequestVote:
				r.handleRequestVote(m)
			case pb.MessageType_MsgHup:
				r.handleHup(m)
			}
		}
	case StateCandidate:
		{
			switch m.GetMsgType() {
			case pb.MessageType_MsgAppend:
				r.handleAppendEntries(m)
			case pb.MessageType_MsgHeartbeat:
				r.handleHeartbeat(m)
			case pb.MessageType_MsgHup:
				r.handleHup(m)
			case pb.MessageType_MsgRequestVote:
				r.handleRequestVote(m)
			case pb.MessageType_MsgRequestVoteResponse:
				r.handleRequestVoteRes(m)
			}
		}
	case StateLeader:
		{
			switch m.GetMsgType() {
			case pb.MessageType_MsgPropose:
				r.handlePropose(m)
			case pb.MessageType_MsgAppend:
				r.handleAppendEntries(m)
			case pb.MessageType_MsgRequestVote:
				r.handleRequestVote(m)
			case pb.MessageType_MsgBeat:
				r.handleBeat()
			case pb.MessageType_MsgAppendResponse:
				r.handleAppendResponse(m)
			}
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	term := r.Term
	reject := false
	//check for the leadership transfer

	//TODO
	if m.Term < r.Term {
		//follower have a higher term
		reject = true
	}
	if m.Term == r.Term && m.From == r.Lead {
		r.electionElapsed = 0
	}
	if m.Term > r.Term || m.Term == r.Term && r.Lead != m.From {
		//leadership transfer happen
		r.becomeFollower(m.Term, m.From)
		term = m.Term
	}
	if r.RaftLog.LastIndex() < m.Index {
		//follower have a lower last index, imply there is a gap between older entries and newer entries
		reject = true
	}
	if term, _ := r.RaftLog.Term(m.Index); term != m.LogTerm {
		//follower last match entry's term don't meet with leader, imply error in follower raftlog and need to be fixed
		reject = true
	}
	if term, _ := r.RaftLog.Term(r.RaftLog.LastIndex()); len(m.Entries) != 0 && term > m.Entries[len(m.Entries)-1].Term {
		//TODO I doubt it
		//follower Raftlog is newer than current appendRequest
		//kept refusing append will result in entirely rewritten
		reject = true
	}
	if !reject {
		//latest match entry
		latestMatchIndex := m.Index

		if len(m.Entries) != 0 {
			//If an existing entry conflicts with a new one (same index but different terms),
			//delete the existing entry and all that follow it; append any new entries not already in the log.
			r.RaftLog.entries = r.RaftLog.entries[0 : m.Index-r.RaftLog.offset+1]
			r.RaftLog.stabled = min(r.RaftLog.stabled, r.RaftLog.LastIndex())
			r.RaftLog.Append(m.Entries)
			// Now we match newer entry
			latestMatchIndex = r.RaftLog.LastIndex()

		}
		if min(m.Commit, latestMatchIndex) > r.RaftLog.committed {
			//3.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
			r.RaftLog.committed = min(m.Commit, latestMatchIndex)
		}
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      m.From,
		From:    m.To,
		Term:    term,
		Reject:  reject,
		Index:   r.RaftLog.LastIndex(),
	})
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	// m.index = r.raftlog.commit + uint64(len(entries))
	if m.Reject {
		if m.Term > r.Term || m.Index == r.RaftLog.LastIndex() {
			//follower have a higher term
			return
		} else {
			//follower have a lower last index
			if r.Prs[m.From].Match != 0 {
				// debug of double append, it's test-direction
				r.Prs[m.From].Match = r.Prs[m.From].Match - 1
			}
			r.sendAppend(m.From)
		}
	} else if term, _ := r.RaftLog.Term(m.Index); term == r.Term {
		if r.Prs[m.From].Match != m.Index {
			r.Prs[m.From].Match = m.Index
		}
		r.checkCommit()
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if m.Term >= r.Term {
		r.becomeFollower(m.Term, m.From)
		r.RaftLog.committed = m.Commit
	}
	r.electionElapsed = 0
}

func (r *Raft) handlePropose(m pb.Message) {
	var ents []*pb.Entry
	for _, entry := range m.Entries {
		ents = append(ents, &pb.Entry{Data: entry.Data, Term: r.Term})
	}
	r.RaftLog.Append(ents)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	for _, value := range r.Prs {
		value.Next = r.RaftLog.LastIndex() + 1
	}
	r.bcastMsg(pb.MessageType_MsgAppend)
	r.checkCommit()
}

func (r *Raft) handleBeat() {
	r.bcastMsg(pb.MessageType_MsgHeartbeat)
}

func (r *Raft) handleRequestVote(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	reject := logTerm > m.LogTerm ||
		logTerm == m.LogTerm && lastIndex > m.Index ||
		r.Term == m.Term && (r.Vote != None && r.Vote != m.From)

	if m.Term > r.Term {
		r.becomeFollower(m.Term, None)
		if !reject {
			r.Vote = m.From
		}
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType:              pb.MessageType_MsgRequestVoteResponse,
		To:                   m.From,
		From:                 r.id,
		Term:                 m.Term,
		LogTerm:              0,
		Index:                0,
		Entries:              nil,
		Commit:               0,
		Snapshot:             nil,
		Reject:               reject,
		XXX_NoUnkeyedLiteral: struct{}{},
		XXX_unrecognized:     nil,
		XXX_sizecache:        0,
	})
}

func (r *Raft) handleRequestVoteRes(m pb.Message) {
	if !m.Reject {
		r.votes[m.From] = true
	} else {
		r.votes[m.From] = false
	}
	if r.State == StateCandidate {
		r.checkVoteResult()
	}
}

func (r *Raft) handleHup(m pb.Message) {
	r.becomeCandidate()
	r.bcastMsg(pb.MessageType_MsgRequestVote)
	r.checkVoteResult()
}

func (r *Raft) checkVoteResult() {
	approved := 0
	for _, v := range r.votes {
		if v {
			approved++
		}
	}
	if approved > (len(r.Prs) / 2) {
		r.becomeLeader()
	} else if len(r.votes) == len(r.Prs) {
		r.becomeFollower(r.Term, 0)
	}
}

func (r *Raft) checkCommit() {
	if len(r.Prs) == 0 {
		// if there is only one leader and no follower
		r.RaftLog.committed = r.RaftLog.LastIndex()
		return
	}
	var commitArr []uint64
	for id, v := range r.Prs {
		if id == r.id {
			// the leader shouldn't be counted in
			continue
		}
		commitArr = append(commitArr, v.Match)
	}
	tempCommit := r.RaftLog.committed + 1
	for ; tempCommit <= r.RaftLog.LastIndex(); tempCommit++ {
		approved := 0
		rejected := 0
		for _, v := range commitArr {
			if tempCommit <= uint64(v) {
				approved++
			} else {
				rejected++
			}
		}
		if approved < rejected {
			break
		}
	}

	if term, _ := r.RaftLog.Term(tempCommit - 1); term == r.Term && r.RaftLog.committed != tempCommit-1 {
		r.RaftLog.committed = tempCommit - 1
		r.bcastMsg(pb.MessageType_MsgAppend)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) softState() *SoftState {
	return &SoftState{
		r.Lead,
		r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
