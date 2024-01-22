package handle

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	// "time"

	"github.com/Trinoooo/eggie_kv/consts"
)

func Get(url string, args []string) {
	if len(args) <= 0 {
		log.Println("error occur when marshal get command")
		return
	}
	kvReq := &consts.KvRequest{
		OperationType: consts.OperatorTypeGet,
		Key:           []byte(args[0]),
	}
	kvResp, ok := cmdPost(url, kvReq)
	if !ok {
		return
	}
	log.Printf("# %s\n", string(kvResp.Data))
}

func Set(url string, args []string) {
	if len(args) <= 0 {
		log.Println("error occur when marshal set command")
		return
	}
	kvReq := &consts.KvRequest{
		OperationType: consts.OperatorTypeSet,
		Key:           []byte(args[0]),
		Value:         []byte(args[1]),
	}

	kvResp, ok := cmdPost(url, kvReq)
	if !ok {
		return
	}
	log.Printf("# %s\n", string(kvResp.Data))
}

func cmdPost(url string, kvReq *consts.KvRequest) (*consts.KvResponse, bool) {
	reqBytes, err := json.Marshal(kvReq)
	if err != nil {
		log.Println("error occur when marshal req, err: ", err)
		return nil, false
	}

	// 服务器http无响应时，readline无法响应程序中断
	client := http.Client{
		// Timeout: 5 * time.Second,
	}

	resp, err := client.Post(url, "application/json", bytes.NewBuffer(reqBytes))
	if err != nil {
		log.Println("error occur when http post, err: ", err)
		return nil, false
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Println("error occur when read resp body bytes, err: ", err)
		return nil, false
	}

	kvResp := &consts.KvResponse{}
	err = json.Unmarshal(bodyBytes, kvResp)
	if err != nil {
		log.Println("error occur when unmarshal resp, err: ", err)
		return nil, false
	}

	return kvResp, true
}
