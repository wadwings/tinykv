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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"time"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// ]
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot
	// Your Data Here (2A).

	//firstIndex
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, _ := storage.LastIndex()
	firstIndex, _ := storage.FirstIndex()
	entries, _ := storage.Entries(firstIndex, lastIndex+1)
	//entries range : entries[firstIndex - offset, lastIndex + 1 - offset)
	hardState, _, _ := storage.InitialState()
	return &RaftLog{
		storage:   storage,
		applied:   lastIndex,
		stabled:   lastIndex,
		entries:   entries,
		committed: hardState.Commit,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	offset, _ := l.storage.FirstIndex()
	if len(l.entries) != 0 && offset > l.entries[0].Index+1 {
		term := l.entries[offset-1-l.entries[0].Index].Term
		l.truncateEntries(term, offset-1)
	}
}

func (l *RaftLog) at(index uint64) *pb.Entry {
	if len(l.entries) == 0 || index > l.LastIndex() {
		return &pb.Entry{}
	}
	entry := l.entries[index-l.GetOffset()]
	return &entry
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.Entries(l.stabled-l.GetOffset()+1, l.LastIndex()-l.GetOffset()+1)
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.Entries(l.applied-l.GetOffset()+1, l.committed-l.GetOffset()+1)
}

func (l *RaftLog) Entries(lo, hi uint64) []pb.Entry {
	res := make([]pb.Entry, hi-lo)
	copy(res, l.entries[lo:hi])
	return res
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return l.GetOffset() - 1
	}
	return l.entries[len(l.entries)-1].Index
}

func (l *RaftLog) NextIndex() uint64 {
	if len(l.entries) == 0 {
		return l.GetOffset()
	} else {
		return l.LastIndex() + 1
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i < l.GetOffset() || i > l.LastIndex() {
		// maybe a break change
		return l.storage.Term(i)
	}
	return l.at(i).Term, nil
}

func (l *RaftLog) Append(entries []*pb.Entry) {
	for _, entry := range entries {
		entry.Index = l.NextIndex()
		l.entries = append(l.entries, *entry)
	}
}

func (l *RaftLog) Commit(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.committed = entries[len(entries)-1].Index
	}
	l.IndexCheck()
	return l.committed
}

func (l *RaftLog) Stable(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.stabled = entries[len(entries)-1].Index
	}
	l.IndexCheck()
	return l.stabled
}

func (l *RaftLog) Apply(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.applied = entries[len(entries)-1].Index
	}
	l.IndexCheck()
	return l.applied
}

func (l *RaftLog) IndexCheck() {
	if l.applied > l.committed {
		l.printRaftlog()
		panic("")
	}
	if l.committed > l.LastIndex() {
		l.printRaftlog()
		panic("")
	}
	if l.stabled > l.LastIndex() {
		l.printRaftlog()
		panic("")
	}
}

func (l *RaftLog) printRaftlog() {
	log.Infof("%+v", l)
	log.Infof("%+v", l.entries)
}

func (l *RaftLog) GetSnapshot() (*pb.Snapshot, error) {
	var snapshot pb.Snapshot
	var err error
	for tryCount := 1; tryCount != 300; tryCount++ {
		snapshot, err = l.storage.Snapshot()
		if err != nil {
			time.Sleep(20 * time.Millisecond)
		} else {
			break
		}
	}
	return &snapshot, err
}

func (l *RaftLog) ApplySnapshot(snapshot *pb.Snapshot) {
	snapIndex := snapshot.Metadata.Index
	snapTerm := snapshot.Metadata.Term
	l.truncateEntries(snapTerm, snapIndex)
	l.pendingSnapshot = snapshot
	l.IndexCheck()
}

func (l *RaftLog) GetOffset() uint64 {
	offset, err := l.storage.FirstIndex()
	if err != nil {
		return 0
	}
	//l.entries[0] is snapshot entry, so offset need oversize than l.entries[0].index + 1 then we shall
	//there is a snapshot needed to be handled
	if len(l.entries) != 0 {
		l.maybeCompact()
		offset = l.entries[0].Index
	}
	return offset
}

func (l *RaftLog) truncateEntries(term uint64, index uint64) {
	if index < l.LastIndex() && len(l.entries) != 0 && index+1 > l.entries[0].Index {
		l.entries = append([]pb.Entry{{Term: term, Index: index}}, l.entries[index-l.entries[0].Index+1:]...)
	} else {
		l.entries = []pb.Entry{{Term: term, Index: index}}
	}
	l.stabled = max(l.stabled, index)
	l.applied = max(l.applied, index)
	l.committed = max(l.committed, index)
}

func (l *RaftLog) HasPendingSnapshot() bool {
	return l.pendingSnapshot != nil
}
