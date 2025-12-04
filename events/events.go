package events

import (
	"github.com/ethereum/go-ethereum/common"
)

type OPDelete common.Hash

type OPCreate struct {
	Key               common.Hash
	ContentType       string
	BTL               uint64
	Owner             common.Address
	Content           []byte
	StringAttributes  map[string]string
	NumericAttributes map[string]uint64
}

type OPContext struct {
	BlockNumber uint64
	TxIndex     uint64
	OpIndex     uint64
}

type OPUpdate struct {
	Key               common.Hash
	ContentType       string
	BTL               uint64
	Owner             common.Address
	Content           []byte
	StringAttributes  map[string]string
	NumericAttributes map[string]uint64
}

type OPExtendBTL struct {
	Key common.Hash
	BTL uint64
}

type OPChangeOwner struct {
	Key   common.Hash
	Owner common.Address
}

type Operation struct {
	TxIndex     uint64
	OpIndex     uint64
	Delete      *OPDelete
	Create      *OPCreate
	Update      *OPUpdate
	ExtendBTL   *OPExtendBTL
	ChangeOwner *OPChangeOwner
}

type Block struct {
	Number     uint64
	Operations []Operation
}

type BlockBatch struct {
	Blocks []Block
}
