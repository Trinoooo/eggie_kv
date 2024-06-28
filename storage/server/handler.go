package server

import (
	"encoding/binary"
	"github.com/Trinoooo/eggie_kv/utils"
	"log"
	"time"
)

func HandleGet(req *KvRequest) (*KvResponse, error) {
	resp := &KvResponse{
		Data: make([]byte, 8),
	}
	log.Printf(utils.WrapInfo("HandleGet kvRequest: %#v", req))
	v := bizLogic(binary.BigEndian.Uint64(req.Value))
	binary.BigEndian.PutUint64(resp.Data, v)
	log.Printf(utils.WrapInfo("HandleGet kvResponse: %#v", resp))
	return resp, nil
}

func HandleSet(req *KvRequest) (*KvResponse, error) {
	resp := &KvResponse{}
	log.Printf(utils.WrapInfo("HandleGet kvRequest: %#v", req))
	bizLogic(binary.BigEndian.Uint64(req.Value))
	log.Printf(utils.WrapInfo("HandleGet kvResponse: %#v", resp))
	return resp, nil
}

func bizLogic(v uint64) uint64 {
	// 模拟耗时cpu密集性计算
	for i := 0; i < 1000000; i++ {
		v += uint64(i)
	}
	// 模拟耗时io密集性操作
	time.Sleep(1 * time.Second)
	return v
}
