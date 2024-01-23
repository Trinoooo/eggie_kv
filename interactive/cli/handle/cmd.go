package handle

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/Trinoooo/eggie_kv/consts"
)

type ClientWrapper struct {
	Url  *url.URL
	Http *http.Client
	Ctx  *context.Context
}

func (c *ClientWrapper) HandleInput(input string) {
	inputs := strings.Fields(input)
	if len(inputs) <= 0 {
		log.Println("error occur when get command")
		return
	}

	cmd := inputs[0]
	args := inputs[1:]
	switch strings.ToLower(cmd) {
	case "get":
		c.Get(args)
	case "set":
		c.Set(args)
	default:
		log.Println("error occur when parse form input, err: Unspported command type ", cmd)
		return
	}
}

func (c *ClientWrapper) Get(args []string) {
	if len(args) <= 0 {
		log.Println("error occur when marshal get command")
		return
	}
	kvReq := &consts.KvRequest{
		OperationType: consts.OperatorTypeGet,
		Key:           []byte(args[0]),
	}
	kvResp, ok := c.cmdPost(kvReq)
	if !ok {
		return
	}
	log.Printf("# %s\n", string(kvResp.Data))
}

func (c *ClientWrapper) Set(args []string) {
	if len(args) <= 0 {
		log.Println("error occur when marshal set command")
		return
	}
	kvReq := &consts.KvRequest{
		OperationType: consts.OperatorTypeSet,
		Key:           []byte(args[0]),
		Value:         []byte(args[1]),
	}

	kvResp, ok := c.cmdPost(kvReq)
	if !ok {
		return
	}
	log.Printf("# %s\n", string(kvResp.Data))
}

func (c *ClientWrapper) cmdPost(kvReq *consts.KvRequest) (*consts.KvResponse, bool) {
	reqBytes, err := json.Marshal(kvReq)
	if err != nil {
		log.Println("error occur when marshal req, err: ", err)
		return nil, false
	}

	req, err := http.NewRequestWithContext(*c.Ctx, http.MethodPost, c.Url.String(), bytes.NewBuffer(reqBytes))
	if err != nil {
		log.Println("error occur when build http post, err", err)
		return nil, false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Http.Do(req)
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
