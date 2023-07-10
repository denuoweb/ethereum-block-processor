package eth

import (
	"context"
	"strconv"

	"github.com/denuoweb/ethereum-block-processor/jsonrpc"
	"github.com/sirupsen/logrus"
)

func GetLatestBlock(ctx context.Context, logger *logrus.Entry, url string) (latestBlock int64, err error) {
	rpcClient := jsonrpc.NewClient(url, 0)
	rpcResponse, err := rpcClient.Call(ctx, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		logger.Error("Invalid endpoint: ", err)
		return
	}
	if rpcResponse.Error != nil {
		logger.Error("rpc response error: ", rpcResponse.Error)
		return
	}
	var htmlcoinBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &htmlcoinBlock)
	if err != nil {
		logger.Error("could not convert result to htmlcoin.GetBlockByNumberResponse", err)
		return
	}
	latestBlock, _ = strconv.ParseInt(htmlcoinBlock.Number, 0, 64)
	logger.Debug("LatestBlock: ", latestBlock)
	return
}
