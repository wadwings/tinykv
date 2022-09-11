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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//]
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

	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		return nil
	}
	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		return nil
	}
	firstIndex--
	offset := uint64(1)
	hardState, _, _ := storage.InitialState()
	if len(entries) != 0 {
		offset = entries[0].Index
	}
	return &RaftLog{
		storage:   storage,
		applied:   firstIndex,
		stabled:   lastIndex,
		entries:   entries,
		offset:    offset,
		committed: hardState.Commit,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

func (l *RaftLog) at(index uint64) *pb.Entry {
	if len(l.entries) == 0 || index-l.offset > uint64(len(l.entries)-1) {
		return &pb.Entry{Index: 0, Term: 0, Data: nil}
	}
	return &l.entries[index-l.offset]
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.stabled+1-l.offset : l.LastIndex()+1-l.offset]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	return l.entries[l.applied+1-l.offset : l.committed+1-l.offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	if i > l.LastIndex() {
		return 0, errors.New("read uninitialized address buffer")
	}
	return l.entries[i-l.offset].Term, nil
}

func (l *RaftLog) Commit(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.committed = entries[len(entries)-1].Index
	}
	return l.committed
}

func (l *RaftLog) Stable(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.stabled = entries[len(entries)-1].Index
	}
	return l.stabled
}

func (l *RaftLog) Apply(entries []pb.Entry) uint64 {
	if len(entries) != 0 {
		l.applied = entries[len(entries)-1].Index
	}
	return l.applied
}
