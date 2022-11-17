package mvcc

import (
	"bytes"
	"encoding/binary"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/tsoutil"
)

// KeyError is a wrapper type so we can implement the `error` interface.
type KeyError struct {
	kvrpcpb.KeyError
}

func (ke *KeyError) Error() string {
	return ke.String()
}

// MvccTxn groups together writes as part of a single transaction. It also provides an abstraction over low-level
// storage, lowering the concepts of timestamps, writes, and locks into plain keys and values.
type MvccTxn struct {
	StartTS uint64
	Reader  storage.StorageReader
	writes  []storage.Modify
}

func NewMvccTxn(reader storage.StorageReader, startTs uint64) *MvccTxn {
	return &MvccTxn{
		Reader:  reader,
		StartTS: startTs,
	}
}

// Writes returns all changes added to this transaction.
func (txn *MvccTxn) Writes() []storage.Modify {
	return txn.writes
}

func (txn *MvccTxn) Put(key []byte, value []byte, cf string) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Put{
			Key:   key,
			Value: value,
			Cf:    cf,
		},
	})
}
func (txn *MvccTxn) Delete(key []byte, cf string) {
	txn.writes = append(txn.writes, storage.Modify{
		Data: storage.Delete{
			Key: key,
			Cf:  cf,
		},
	})
}

// PutWrite records a write at key and ts.
func (txn *MvccTxn) PutWrite(key []byte, ts uint64, write *Write) {
	write.StartTS = txn.StartTS
	txn.Put(EncodeKey(key, ts), write.ToBytes(), engine_util.CfWrite)
	// Your Code Here (4A).
}

// GetLock returns a lock if key is locked. It will return (nil, nil) if there is no lock on key, and (nil, err)
// if an error occurs during lookup.
func (txn *MvccTxn) GetLock(key []byte) (*Lock, error) {
	// Your Code Here (4A).
	val, err := txn.Reader.GetCF(engine_util.CfLock, key)
	if err != nil {
		return nil, err
	}
	lock, err := ParseLock(val)
	if err != nil {
		return nil, err
	}
	return lock, nil
}

// PutLock adds a key/lock to this transaction.
func (txn *MvccTxn) PutLock(key []byte, lock *Lock) {
	txn.Put(key, lock.ToBytes(), engine_util.CfLock)
	// Your Code Here (4A).
}

// DeleteLock adds a delete lock to this transaction.
func (txn *MvccTxn) DeleteLock(key []byte) {
	txn.Delete(key, engine_util.CfLock)
	// Your Code Here (4A).
}

// GetValue finds the value for key, valid at the start timestamp of this transaction.
// I.e., the most recent value committed before the start of this transaction.
func (txn *MvccTxn) GetValue(key []byte) ([]byte, error) {
	// TODO Value roll back
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	for iter.Seek(key); iter.Valid() && bytes.Compare(DecodeUserKey(iter.Item().Key()), key) == 0; iter.Next() {
		commitTS := DecodeTimestamp(iter.Item().Key())
		if commitTS > txn.StartTS {
			continue
		}
		val, err := iter.Item().Value()
		if err != nil {
			continue
		}
		write, err := ParseWrite(val)
		if err != nil {
			continue
		}
		if write != nil {
			if write.Kind == WriteKindPut {
				return txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
			} else if write.Kind == WriteKindDelete {
				return nil, nil
			}
		}
	}
	return nil, nil
	// Your Code Here (4A).
}

// PutValue adds a key/value write to this transaction.
func (txn *MvccTxn) PutValue(key []byte, value []byte) {
	txn.Put(EncodeKey(key, txn.StartTS), value, engine_util.CfDefault)
	// Your Code Here (4A).
}

// DeleteValue removes a key/value pair in this transaction.
func (txn *MvccTxn) DeleteValue(key []byte) {
	txn.Delete(EncodeKey(key, txn.StartTS), engine_util.CfDefault)
	// Your Code Here (4A).
}

// CurrentWrite searches for a write with this transaction's start timestamp. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) CurrentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)

	for iter.Seek(key); iter.Valid() && bytes.Compare(DecodeUserKey(iter.Item().Key()), key) == 0; iter.Next() {
		val, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		if write != nil && write.StartTS == txn.StartTS {
			return write, DecodeTimestamp(iter.Item().Key()), nil
		}
	}
	// Your Code Here (4A).
	return nil, 0, nil
}

// MostRecentWrite finds the most recent write with the given key. It returns a Write from the DB and that
// write's commit timestamp, or an error.
func (txn *MvccTxn) MostRecentWrite(key []byte) (*Write, uint64, error) {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	for iter.Seek(key); iter.Valid() && bytes.Compare(DecodeUserKey(iter.Item().Key()), key) == 0; iter.Next() {
		commitTS := DecodeTimestamp(iter.Item().Key())
		val, err := iter.Item().Value()
		if err != nil {
			return nil, 0, err
		}
		write, err := ParseWrite(val)
		if err != nil {
			return nil, 0, err
		}
		if write != nil {
			return write, commitTS, nil
		}
	}
	// Your Code Here (4A).
	return nil, 0, nil
}

// EncodeKey encodes a user key and appends an encoded timestamp to a key. Keys and timestamps are encoded so that
// timestamped keys are sorted first by key (ascending), then by timestamp (descending). The encoding is based on
// https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format.
func EncodeKey(key []byte, ts uint64) []byte {
	encodedKey := codec.EncodeBytes(key)
	newKey := append(encodedKey, make([]byte, 8)...)
	binary.BigEndian.PutUint64(newKey[len(encodedKey):], ^ts)
	return newKey
}

// DecodeUserKey takes a key + timestamp and returns the key part.
func DecodeUserKey(key []byte) []byte {
	_, userKey, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return userKey
}

// DecodeTimestamp takes a key + timestamp and returns the timestamp part.
func DecodeTimestamp(key []byte) uint64 {
	left, _, err := codec.DecodeBytes(key)
	if err != nil {
		panic(err)
	}
	return ^binary.BigEndian.Uint64(left)
}

// PhysicalTime returns the physical time part of the timestamp.
func PhysicalTime(ts uint64) uint64 {
	return ts >> tsoutil.PhysicalShiftBits
}
