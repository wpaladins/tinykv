package standalone_storage

import (
	"io/ioutil"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	dir, err := ioutil.TempDir("", "standalone_storage")
	if err != nil {
		log.Fatalf("NewStandAloneStorage ioutil.TempDir error %v", err)
	}
	db := engine_util.CreateDB(dir, false)
	s.engine = engine_util.NewEngines(db, nil, dir, "")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.engine.Destroy()
	return nil
}

type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(db *badger.DB) (reader *StandAloneStorageReader) {
	txn := db.NewTransaction(false)
	return &StandAloneStorageReader{txn}
}

// return []byte(nil) if not found
func (reader *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		value = []byte(nil)
	}
	return value, nil
}

func (reader *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneStorageReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	return NewStandAloneStorageReader(s.engine.Kv), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Delete:
			d := m.Data.(storage.Delete)
			writeBatch.DeleteCF(d.Cf, d.Key)
			log.Debugf("StandAloneStorage Write Delete Key %v", m.Data.(storage.Delete).Key)
		case storage.Put:
			p := m.Data.(storage.Put)
			log.Debugf("StandAloneStorage Write Put Key %v", p.Key)
			writeBatch.SetCF(p.Cf, p.Key, p.Value)
		}
	}
	s.engine.WriteKV(writeBatch)
	return nil
}
