package mvcc

import (
	"bytes"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

// Scanner is used for reading multiple sequential key/value pairs from the storage layer. It is aware of the implementation
// of the storage layer and returns results suitable for users.
// Invariant: either the scanner is finished and cannot be used, or it is ready to return a value immediately.
type Scanner struct {
	Iter   engine_util.DBIterator
	Txn    *MvccTxn
	curKey []byte
	// Your Data Here (4C).
}

// NewScanner creates a new scanner ready to read from the snapshot in txn.
func NewScanner(startKey []byte, txn *MvccTxn) *Scanner {
	iter := txn.Reader.IterCF(engine_util.CfWrite)
	iter.Seek(startKey)
	return &Scanner{
		Iter: iter,
		Txn:  txn,
	}
	// Your Code Here (4C).
}

func (scan *Scanner) Close() {
	scan.Iter.Close()
	// Your Code Here (4C).
}

// Next returns the next key/value pair from the scanner. If the scanner is exhausted, then it will return `nil, nil, nil`.
func (scan *Scanner) Next() ([]byte, []byte, error) {
	invalidKey := scan.curKey
	for ; scan.Iter.Valid(); scan.Iter.Next() {
		commitTS := DecodeTimestamp(scan.Iter.Item().Key())
		if commitTS > scan.Txn.StartTS {
			continue
		}
		key := DecodeUserKey(scan.Iter.Item().Key())
		if bytes.Compare(key, invalidKey) == 0 {
			// the key is deleted or tried, so we skip it to the next key
			continue
		}
		val, err := scan.Iter.Item().Value()
		if err != nil {
			continue
		}
		write, err := ParseWrite(val)
		if err != nil {
			continue
		}
		if write != nil {
			if write.Kind == WriteKindPut {
				scan.curKey = key
				val, err := scan.Txn.Reader.GetCF(engine_util.CfDefault, EncodeKey(key, write.StartTS))
				return key, val, err
			} else if write.Kind == WriteKindDelete {
				invalidKey = DecodeUserKey(scan.Iter.Item().Key())
			}
		}
	}
	// Your Code Here (4C).
	return nil, nil, nil
}
