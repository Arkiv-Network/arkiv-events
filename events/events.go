package events

import (
	"github.com/ethereum/go-ethereum/common"
)

type OPExpire common.Hash

type OPDelete common.Hash

type OPCreate struct {
	Key               common.Hash       `json:"key"`
	ContentType       string            `json:"content_type"`
	BTL               uint64            `json:"btl"`
	Owner             common.Address    `json:"owner"`
	Content           []byte            `json:"content"`
	StringAttributes  map[string]string `json:"string_attributes"`
	NumericAttributes map[string]uint64 `json:"numeric_attributes"`
}

type OPContext struct {
	BlockNumber uint64
	TxIndex     uint64
	OpIndex     uint64
}

type OPUpdate struct {
	Key               common.Hash       `json:"key"`
	ContentType       string            `json:"content_type"`
	BTL               uint64            `json:"btl"`
	Owner             common.Address    `json:"owner"`
	Content           []byte            `json:"content"`
	StringAttributes  map[string]string `json:"string_attributes"`
	NumericAttributes map[string]uint64 `json:"numeric_attributes"`
}

type OPExtendBTL struct {
	Key common.Hash `json:"key"`
	BTL uint64      `json:"btl"`
}

type OPChangeOwner struct {
	Key   common.Hash    `json:"key"`
	Owner common.Address `json:"owner"`
}

type Operation struct {
	TxIndex     uint64         `json:"tx_index"`
	OpIndex     uint64         `json:"op_index"`
	Delete      *OPDelete      `json:"delete,omitempty"`
	Expire      *OPExpire      `json:"expire,omitempty"`
	Create      *OPCreate      `json:"create,omitempty"`
	Update      *OPUpdate      `json:"update,omitempty"`
	ExtendBTL   *OPExtendBTL   `json:"extend_btl,omitempty"`
	ChangeOwner *OPChangeOwner `json:"change_owner,omitempty"`
}

type Block struct {
	Number     uint64      `json:"number"`
	Operations []Operation `json:"operations"`
}

type BlockBatch struct {
	Blocks []Block `json:"blocks"`
}
