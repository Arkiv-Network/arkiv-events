package rpciterator

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

var ArkivProcessorAddress = common.HexToAddress("0x00000000000000000000000000000061726B6976")

type RawReceipt struct {
	Status hexutil.Uint64 `json:"status"`
	Logs   []types.Log    `json:"logs"`
}

func (r RawReceipt) IsSuccessful() bool {
	return r.Status == 1
}

func (r RawReceipt) CreatedEntities() []common.Hash {
	entities := []common.Hash{}
	for _, log := range r.Logs {
		if log.Topics[0] == ArkivEntityCreated {
			entityKey := log.Topics[1]
			entities = append(entities, entityKey)
		}
	}
	return entities
}

type RawTransaction struct {
	To   common.Address `json:"to"`
	From common.Address `json:"from"`
	Data hexutil.Bytes  `json:"input"`
}

type RawBlock struct {
	Number       hexutil.Uint64   `json:"number"`
	Transactions []RawTransaction `json:"transactions"`
}
