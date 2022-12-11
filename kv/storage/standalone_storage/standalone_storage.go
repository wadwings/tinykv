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
	db    *badger.DB
	// Your Data Here (1).
}

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	txn     *badger.Txn
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

func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	return engine_util.GetCF(reader.storage.db, cf, key)
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := reader.storage.db.NewTransaction(true)
	reader.txn = txn
	return engine_util.NewCFIterator(cf, txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &StandAloneStorageReader{storage: s}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, v := range batch {
		if v.Value() != nil {
			err := engine_util.PutCF(s.db, v.Cf(), v.Key(), v.Value())
			if err != nil {
				return err
			}
		} else {
			err := engine_util.DeleteCF(s.db, v.Cf(), v.Key())
			if err != nil {
				return err
			}
			err = engine_util.PutCF(s.db, v.Cf(), v.Key(), v.Value())
			if err != nil {
				return err
			}
		}
	}
	return nil
}
