package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	value, err := reader.GetCF(req.Cf, req.Key)
	var res = &kvrpcpb.RawGetResponse{}
	if value != nil {
		res = &kvrpcpb.RawGetResponse{Value: value}
	} else {
		res = &kvrpcpb.RawGetResponse{NotFound: true}
	}
	return res, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	modify := []storage.Modify{{
		Data: storage.Put{Cf: req.Cf, Value: req.Value, Key: req.Key},
	}}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	modify := []storage.Modify{{
		Data: storage.Put{Cf: req.Cf, Key: req.Key},
	}}
	err := server.storage.Write(req.Context, modify)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}

	iter := reader.IterCF(req.Cf)
	defer iter.Close()
	iter.Seek(req.StartKey)

	var kvPairs []*kvrpcpb.KvPair
	count := uint32(0)
	for iter.Valid() && count < req.Limit {
		key := iter.Item().Key()
		value, _ := iter.Item().Value()
		if len(value) != 0 {
			kvPairs = append(kvPairs, &kvrpcpb.KvPair{Key: key, Value: value})
			count++
		}
		iter.Next()
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvPairs}, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	getResp := new(kvrpcpb.GetResponse)
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.Version)
	preLock, _ := txn.GetLock(req.Key)
	if preLock.IsLockedFor(req.Key, req.Version, getResp) {
		return getResp, nil
	}
	val, err := txn.GetValue(req.Key)
	if err != nil {
		return nil, err
	}
	return &kvrpcpb.GetResponse{
		Value:    val,
		NotFound: val == nil,
	}, nil
	// Your Code Here (4B).
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	keys := make([][]byte, 0)
	for _, mutation := range req.Mutations {
		keys = append(keys, mutation.Key)
	}
	server.Latches.WaitForLatches(keys)
	defer server.Latches.ReleaseLatches(keys)
	prewriteResp := new(kvrpcpb.PrewriteResponse)
	t := new(kvrpcpb.GetResponse)
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, mutation := range req.Mutations {
		preLock, _ := txn.GetLock(mutation.Key)
		if preLock.IsLockedFor(mutation.Key, req.StartVersion, t) {
			prewriteResp.Errors = append(prewriteResp.Errors, t.Error)
			continue
		}
		if preLock != nil {
			txn.DeleteLock(mutation.Key)
		}
		writeInProgress, commitTS, _ := txn.MostRecentWrite(mutation.Key)
		if writeInProgress != nil && writeInProgress.StartTS < req.StartVersion && commitTS > req.StartVersion {
			return &kvrpcpb.PrewriteResponse{Errors: []*kvrpcpb.KeyError{{Conflict: &kvrpcpb.WriteConflict{}}}}, nil
		}
		lock := mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    mvcc.WriteKindFromProto(mutation.Op),
		}
		txn.PutLock(mutation.Key, &lock)
		if mvcc.WriteKindFromProto(mutation.Op) == mvcc.WriteKindPut {
			txn.PutValue(mutation.Key, mutation.Value)
		} else {
			txn.DeleteValue(mutation.Key)
		}
	}
	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}
	// Your Code Here (4B).
	return prewriteResp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	commitResp := new(kvrpcpb.CommitResponse)
	server.Latches.WaitForLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, key := range req.Keys {
		lock, _ := txn.GetLock(key)
		if lock == nil {
			continue
		}
		if lock.Ts != req.StartVersion {
			commitResp.Error = &kvrpcpb.KeyError{
				Retryable: "",
			}
			continue
		}
		txn.PutWrite(key, req.CommitVersion, &mvcc.Write{StartTS: req.StartVersion, Kind: lock.Kind})
		txn.DeleteLock(key)
	}
	if err := server.storage.Write(req.Context, txn.Writes()); err != nil {
		return nil, err
	}
	// Your Code Here (4B).
	return commitResp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	scanResp := &kvrpcpb.ScanResponse{}
	txn := mvcc.NewMvccTxn(storageReader, req.Version)
	scanner := mvcc.NewScanner(req.StartKey, txn)
	for i := 0; i < int(req.Limit); i++ {
		key, value, err := scanner.Next()
		if err != nil {
			return nil, err
		}
		if key == nil {
			break
		}
		scanResp.Pairs = append(scanResp.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return scanResp, nil
	// Your Code Here (4C).
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	checkResp := new(kvrpcpb.CheckTxnStatusResponse)
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.LockTs)
	lock, _ := txn.GetLock(req.PrimaryKey)
	write, commitVersion, _ := txn.CurrentWrite(req.PrimaryKey)
	if write != nil {
		checkResp.Action = kvrpcpb.Action_NoAction
		if write.Kind != mvcc.WriteKindRollback {
			checkResp.CommitVersion = commitVersion
		}
		return checkResp, nil
	}
	if lock == nil {
		checkResp.Action = kvrpcpb.Action_LockNotExistRollback
		server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.LockTs,
			Keys:         [][]byte{req.PrimaryKey},
		})
	} else {
		checkResp.LockTtl = lock.Ttl
		checkResp.CommitVersion = lock.Ts + lock.Ttl
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl < mvcc.PhysicalTime(req.CurrentTs) {
			checkResp.Action = kvrpcpb.Action_TTLExpireRollback
			server.KvResolveLock(nil, &kvrpcpb.ResolveLockRequest{
				Context:       req.Context,
				StartVersion:  req.LockTs,
				CommitVersion: 0,
			})
		} else {
			checkResp.Action = kvrpcpb.Action_NoAction
		}
	}
	return checkResp, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	for _, key := range req.Keys {
		T := mvcc.NewMvccTxn(storageReader, req.StartVersion)
		if write, _, _ := T.CurrentWrite(key); write != nil {
			if write.Kind != mvcc.WriteKindRollback {
				return &kvrpcpb.BatchRollbackResponse{Error: &kvrpcpb.KeyError{
					Abort: "",
				}}, nil
			} else {
				continue
			}
		}
		lock, _ := txn.GetLock(key)
		if lock == nil || lock.Ts != req.StartVersion {
			txn.PutWrite(key, req.StartVersion, &mvcc.Write{
				StartTS: req.StartVersion,
				Kind:    mvcc.WriteKindRollback,
			})
			continue
		}

		txn.DeleteValue(key)
		txn.DeleteLock(key)
		txn.PutWrite(key, req.StartVersion, &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    mvcc.WriteKindRollback,
		})
	}
	server.storage.Write(req.Context, txn.Writes())
	// Your Code Here (4C).
	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	commitVersion := req.CommitVersion
	storageReader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	txn := mvcc.NewMvccTxn(storageReader, req.StartVersion)
	var keys [][]byte
	iter := txn.Reader.IterCF(engine_util.CfLock)
	for ; iter.Valid(); iter.Next() {
		val, _ := iter.Item().Value()
		lock, _ := mvcc.ParseLock(val)
		if lock.Ts == req.StartVersion {
			keys = append(keys, iter.Item().Key())
			write, _, _ := txn.CurrentWrite(iter.Item().Key())
			if write != nil && write.Kind != mvcc.WriteKindRollback {
				commitVersion = 0
			}
		}
	}
	if commitVersion == 0 {
		resp, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		return &kvrpcpb.ResolveLockResponse{
			RegionError: resp.RegionError,
			Error:       resp.Error,
		}, err
	} else {
		resp, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: commitVersion,
		})
		return &kvrpcpb.ResolveLockResponse{
			RegionError: resp.RegionError,
			Error:       resp.Error,
		}, err
	}
	// Your Code Here (4C).
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
