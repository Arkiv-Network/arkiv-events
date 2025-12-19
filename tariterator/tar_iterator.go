package tariterator

import (
	"archive/tar"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strconv"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
	"github.com/klauspost/compress/zstd"
)

var blockNumberRegex = regexp.MustCompile(`^block-(\d+).json.zst$`)

func IterateTar(batchSize int, tarFileReader io.Reader) arkivevents.BatchIterator {

	return func(yield func(arkivevents.BatchOrError) bool) {
		tarReader := tar.NewReader(tarFileReader)

		batch := arkivevents.BatchOrError{
			Batch: events.BlockBatch{
				Blocks: []events.Block{},
			},
			Error: nil,
		}

		for {
			header, err := tarReader.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to read tar header: %w", err)})
				return
			}

			blockNumber := blockNumberRegex.FindStringSubmatch(header.Name)
			if len(blockNumber) == 0 {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to find block number in filename %s of tar header: %w", header.Name, err)})
				return
			}

			blockNumberInt, err := strconv.ParseUint(blockNumber[1], 10, 64)
			if err != nil {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to parse block number %s: %w", blockNumber[1], err)})
				return
			}

			eventsReder, err := zstd.NewReader(tarReader)
			if err != nil {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to create zstd reader: %w", err)})
				return
			}
			defer eventsReder.Close()

			decoder := json.NewDecoder(eventsReder)
			decoder.DisallowUnknownFields()

			block := events.Block{
				Number:     blockNumberInt,
				Operations: []events.Operation{},
			}

			for {
				operation := events.Operation{}
				err = decoder.Decode(&operation)
				if err == io.EOF {
					break
				}
				if err != nil {
					yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to decode operation: %w", err)})
					return
				}
				block.Operations = append(block.Operations, operation)
			}
			batch.Batch.Blocks = append(batch.Batch.Blocks, block)

			if len(batch.Batch.Blocks) >= batchSize {
				if !yield(arkivevents.BatchOrError{Batch: batch.Batch}) {
					return
				}
				batch = arkivevents.BatchOrError{
					Batch: events.BlockBatch{
						Blocks: []events.Block{},
					},
				}
			}
		}

		if len(batch.Batch.Blocks) > 0 {
			if !yield(arkivevents.BatchOrError{Batch: batch.Batch}) {
				return
			}
		}

	}
}
