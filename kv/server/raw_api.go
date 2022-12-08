package server

import (
	"bytes"
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer reader.Close()

	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		log.Fatal(err)
		return nil, err
	} else if bytes.Compare(value, []byte(nil)) == 0 {
		return &kvrpcpb.RawGetResponse{NotFound: true}, err
	}
	log.Debugf("RawGet value %v\n", value)
	return &kvrpcpb.RawGetResponse{Value: value}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	m := storage.Modify{Data: storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}}
	err := server.storage.Write(req.Context, append([]storage.Modify{}, m))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &kvrpcpb.RawPutResponse{}, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	m := storage.Modify{Data: storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}}
	err := server.storage.Write(req.Context, append([]storage.Modify{}, m))
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &kvrpcpb.RawDeleteResponse{}, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer reader.Close()

	iterCF := reader.IterCF(req.GetCf())
	defer iterCF.Close()

	readNum := 0
	kvs := make([]*kvrpcpb.KvPair, 0)
	for iterCF.Seek(req.StartKey); iterCF.Valid(); iterCF.Next() {
		item := iterCF.Item()
		value, err := item.ValueCopy(nil)
		if err != nil {
			log.Fatal(err)
			continue
		}

		kvs = append(kvs, &kvrpcpb.KvPair{
			Key:   item.KeyCopy(nil),
			Value: value,
		})

		readNum++
		if readNum >= int(req.Limit) {
			break
		}
	}
	return &kvrpcpb.RawScanResponse{Kvs: kvs}, nil
}
