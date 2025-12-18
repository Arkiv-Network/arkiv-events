package tariterator

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/Arkiv-Network/arkiv-events/events"
	"github.com/ethereum/go-ethereum/common"
	"github.com/google/go-cmp/cmp"
	"github.com/klauspost/compress/zstd"
)

func TestIterateTar(t *testing.T) {
	// Create test blocks with operations
	testBlocks := []events.Block{
		{
			Number: 100,
			Operations: []events.Operation{
				{TxIndex: 0, OpIndex: 0, Create: &events.OPCreate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               100,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
				{TxIndex: 0, OpIndex: 1, Create: &events.OPCreate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               102,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
			},
		},
		{
			Number: 101,
			Operations: []events.Operation{
				{TxIndex: 0, OpIndex: 0, Create: &events.OPCreate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               103,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
				{TxIndex: 0, OpIndex: 1, Create: &events.OPCreate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               104,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
			},
		},
		{
			Number: 102,
			Operations: []events.Operation{
				{TxIndex: 0, OpIndex: 0, Update: &events.OPUpdate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               105,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
				{TxIndex: 1, OpIndex: 0, Update: &events.OPUpdate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               106,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
				{TxIndex: 1, OpIndex: 1, Update: &events.OPUpdate{
					Key:               common.HexToHash("0x1234567890123456789012345678901234567890"),
					ContentType:       "text/plain",
					BTL:               107,
					Owner:             common.HexToAddress("0x1234567890123456789012345678901234567890"),
					Content:           []byte("Hello, world!"),
					StringAttributes:  map[string]string{"key": "value"},
					NumericAttributes: map[string]uint64{"key": 100},
				}},
			},
		},
	}

	// Build tar file in memory
	var tarBuffer bytes.Buffer
	tarWriter := tar.NewWriter(&tarBuffer)

	for _, block := range testBlocks {
		// Create zstd-compressed content
		var zstdBuffer bytes.Buffer
		zstdWriter, err := zstd.NewWriter(&zstdBuffer)
		if err != nil {
			t.Fatalf("failed to create zstd writer: %v", err)
		}

		encoder := json.NewEncoder(zstdWriter)

		// Write operations as separate JSON objects
		for _, op := range block.Operations {
			err := encoder.Encode(op)
			if err != nil {
				t.Fatalf("failed to encode operation: %v", err)
			}
		}

		err = zstdWriter.Close()
		if err != nil {
			t.Fatalf("failed to close zstd writer: %v", err)
		}

		// Write tar entry
		header := &tar.Header{
			Name: fmt.Sprintf("block-%020d.tar.zst", block.Number),
			Size: int64(zstdBuffer.Len()),
			Mode: 0644,
		}

		err = tarWriter.WriteHeader(header)

		if err != nil {
			t.Fatalf("failed to write tar header: %v", err)
		}

		_, err = tarWriter.Write(zstdBuffer.Bytes())
		if err != nil {
			t.Fatalf("failed to write tar content: %v", err)
		}
	}

	err := tarWriter.Close()
	if err != nil {
		t.Fatalf("failed to close tar writer: %v", err)
	}

	// Iterate using IterateTar
	iterator := IterateTar(3, &tarBuffer)

	var resultBlocks []events.Block
	for item := range iterator {
		if item.Error != nil {
			t.Fatalf("unexpected error during iteration: %v", item.Error)
		}
		resultBlocks = append(resultBlocks, item.Batch.Blocks...)
	}

	// Verify results
	if len(resultBlocks) != len(testBlocks) {
		t.Fatalf("expected %d blocks, got %d", len(testBlocks), len(resultBlocks))
	}

	if !cmp.Equal(resultBlocks, testBlocks) {
		t.Fatalf("expected %v, got %v", testBlocks, resultBlocks)
	}

}
