package arkivtx

import (
	"bytes"
	"fmt"
	"io"

	"github.com/andybalholm/brotli"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// ArkivTransaction represents a transaction that can be applied to the storage layer.
// It contains a list of Create operations, a list of Update operations and a list of Delete operations.
//
// Semantics of the transaction operations are as follows:
//   - Create: adds new entities to the storage layer. Each entity has a BTL (number of blocks), a payload and a list of annotations. The Key of the entity is derived from the payload content, the transaction hash where the entity was created and the index of the create operation in the transaction.
//   - Update: updates existing entities. Each entity has a key, a BTL (number of blocks), a payload and a list of annotations. If the entity does not exist, the operation fails, failing the whole transaction.
//   - Delete: removes entities from the storage layer. If the entity does not exist, the operation fails, failing back the whole transaction.
//
// The transaction is atomic, meaning that all operations are applied or none are.
//
// Annotations are key-value pairs where the key is a string and the value is either a string or a number.
// The key-value pairs are used to build indexes and to query the storage layer.
// Same key can have both string and numeric annotation, but not multiple values of the same type.
type ArkivTransaction struct {
	Create      []ArkivCreate      `json:"create"`
	Update      []ArkivUpdate      `json:"update"`
	Delete      []common.Hash      `json:"delete"`
	Extend      []ExtendBTL        `json:"extend"`
	ChangeOwner []ArkivChangeOwner `json:"changeOwner"`
}

type ExtendBTL struct {
	EntityKey      common.Hash `json:"entityKey"`
	NumberOfBlocks uint64      `json:"numberOfBlocks"`
}

type ArkivCreate struct {
	BTL               uint64            `json:"btl"`
	ContentType       string            `json:"contentType"`
	Payload           []byte            `json:"payload"`
	StringAttributes  StringAttributes  `json:"stringAttributes"`
	NumericAttributes NumericAttributes `json:"numericAttributes"`
}

type ArkivUpdate struct {
	EntityKey         common.Hash       `json:"entityKey"`
	ContentType       string            `json:"contentType"`
	BTL               uint64            `json:"btl"`
	Payload           []byte            `json:"payload"`
	StringAttributes  StringAttributes  `json:"stringAttributes"`
	NumericAttributes NumericAttributes `json:"numericAttributes"`
}

type StringAttribute struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type StringAttributes []StringAttribute

func (s StringAttributes) ToMap() map[string]string {
	m := make(map[string]string)
	for _, attribute := range s {
		m[attribute.Key] = attribute.Value
	}
	return m
}

type NumericAttribute struct {
	Key   string `json:"key"`
	Value uint64 `json:"value"`
}

type NumericAttributes []NumericAttribute

func (n NumericAttributes) ToMap() map[string]uint64 {
	m := make(map[string]uint64)
	for _, attribute := range n {
		m[attribute.Key] = attribute.Value
	}
	return m
}

type ArkivChangeOwner struct {
	EntityKey common.Hash    `json:"entityKey"`
	NewOwner  common.Address `json:"newOwner"`
}

const maxCompressedSize = 1024 * 1024 * 20 // 20MB

func UnpackArkivTransaction(compressed []byte) (*ArkivTransaction, error) {
	reader := brotli.NewReader(bytes.NewReader(compressed))
	lr := io.LimitReader(reader, maxCompressedSize)

	d, err := io.ReadAll(lr)
	if err != nil {
		return nil, fmt.Errorf("failed to read compressed storage transaction: %w", err)
	}

	tx := &ArkivTransaction{}
	err = rlp.DecodeBytes(d, tx)
	if err != nil {
		return nil, fmt.Errorf("failed to decode storage transaction: %w", err)
	}

	return tx, nil
}
