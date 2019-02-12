package main

import (
	rpcclient "github.com/tendermint/tendermint/rpc/client"
	"github.com/tsuna/gohbase"
	"fmt"
	"encoding/json"
	"github.com/tsuna/gohbase/hrpc"
	"context"
)

var (
	bClient     = newClient("37.189.50.35:26657")
	hClient     = gohbase.NewClient("localhost")
	//ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
)

func main() {
	status, err := bClient.Status()
	if err != nil {
		panic("get block status error")
	}
	height := status.SyncInfo.LatestBlockHeight
	now := int64(1)
	for ; now <= height; now++ {
		block, err := bClient.Block(&now)
		if err != nil {
			panic(err)
		}
		jstr, _ := json.Marshal(block)
		values := map[string]map[string][]byte{"info": map[string][]byte{"data": jstr}}
		putRequest, err := hrpc.NewPutStr(context.Background(), "block", string(block.Block.Height), values)
		_, err = hClient.Put(putRequest)
		if err != nil {
			panic(err)
		}
	}
}

type Client struct {
	rpcclient.Client
	Id string
}

func newClient(addr string) *Client {
	return &Client{
		Client: rpcclient.NewHTTP(addr, "/websocket"),
		Id:     generateId(addr),
	}
}

func generateId(address string) string {
	return fmt.Sprintf("peer[%s]", address)
}
