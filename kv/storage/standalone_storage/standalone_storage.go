package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	KPath string
	VPath string
	db *badger.DB
	Iter *engine_util.BadgerIterator
	txn *badger.Txn
	// Your Data Here (1).
}

type StorageReader struct {
	GetCF func(cf string, key []byte) ([]byte, error)
	IterCF func(cf string) engine_util.DBIterator
	Close func()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	example := StandAloneStorage{KPath: conf.DBPath}
	example.VPath = conf.DBPath
	return &example
}

func (s *StandAloneStorage) Start() error {
	opts := badger.DefaultOptions
	opts.Dir = s.KPath
	opts.ValueDir = s.VPath
	db, err := badger.Open(opts)
	s.db = db
	return err
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()
	return err
}

func (s *StandAloneStorage) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(s.db, cf, key)
}

func (s *StandAloneStorage) IterCF(cf string) engine_util.DBIterator{
	txn := &badger.Txn{}
	s.txn = txn
	s.Iter = engine_util.NewCFIterator(cf, txn)
	return s.Iter
}

func (s *StandAloneStorage) Close() {
	s.txn.Discard()
	s.Iter.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return s, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, v := range batch {
		err := engine_util.PutCF(s.db, v.Cf(), v.Key(), v.Value())
		if err != nil{
			return err
		}
	}
	return nil
}
