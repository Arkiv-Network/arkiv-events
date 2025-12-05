package rpciterator

import (
	"context"
	"fmt"
	"iter"
	"log/slog"
	"time"

	arkivevents "github.com/Arkiv-Network/arkiv-events"
	"github.com/Arkiv-Network/arkiv-events/events"
	"github.com/Arkiv-Network/arkiv-events/rpciterator/arkivtx"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/sync/errgroup"
)

const maxBlocksPerBatch = 50

func IterateBlocks(
	ctx context.Context,
	log *slog.Logger,
	rpcClient *rpc.Client,
	lastBlockNumber uint64,
) iter.Seq[arkivevents.BatchOrError] {

	fetchBlocksInBatch := func(ctx context.Context, startBlock uint64, lastBlockNumber uint64) ([]RawBlock, error) {

		batchSize := min(maxBlocksPerBatch, lastBlockNumber-startBlock+1)
		batch := make([]rpc.BatchElem, batchSize)
		blocks := make([]RawBlock, batchSize)
		for i := range uint64(batchSize) {
			batch[i] = rpc.BatchElem{
				Method: "eth_getBlockByNumber",
				Args:   []any{hexutil.Uint64(startBlock + i), true},
				Result: &blocks[i],
			}
		}
		err := rpcClient.BatchCallContext(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("failed to batch call: %w", err)
		}
		for i, b := range batch {
			if b.Error != nil {
				return nil, fmt.Errorf("fetching block %d: %w", startBlock+uint64(i), b.Error)
			}
		}
		return blocks, nil
	}

	fetchBlockReceiptsInBatch := func(ctx context.Context, startBlock uint64, lastBlockNumber uint64) ([][]RawReceipt, error) {

		batchSize := min(maxBlocksPerBatch, lastBlockNumber-startBlock+1)

		batch := make([]rpc.BatchElem, batchSize)
		receipts := make([][]RawReceipt, batchSize)
		for i := range uint64(batchSize) {
			batch[i] = rpc.BatchElem{
				Method: "eth_getBlockReceipts",
				Args:   []any{hexutil.Uint64(startBlock + i)},
				Result: &receipts[i],
			}
		}
		err := rpcClient.BatchCallContext(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("failed to batch call: %w", err)
		}
		for i, b := range batch {
			if b.Error != nil {
				return nil, fmt.Errorf("fetching receipts for block %d: %w", startBlock+uint64(i), b.Error)
			}
		}
		return receipts, nil
	}

	ec := ethclient.NewClient(rpcClient)

	return func(yield func(arkivevents.BatchOrError) bool) {

		for {

			blockNumber, err := ec.BlockNumber(ctx)
			if err != nil {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to get block number: %w", err)})
				return
			}

			if lastBlockNumber >= blockNumber {
				// log.Info("Last block number is greater than current block number", "lastBlockNumber", lastBlockNumber, "currentBlockNumber", blockNumber)
				log.Info("waiting for new blocks", "lastBlockNumber", lastBlockNumber, "currentBlockNumber", blockNumber)
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
				}
				continue
			}

			eg, egCtx := errgroup.WithContext(ctx)

			var rawBlocks []RawBlock

			eg.Go(func() (err error) {
				rawBlocks, err = fetchBlocksInBatch(egCtx, lastBlockNumber+1, blockNumber)
				if err != nil {
					return fmt.Errorf("failed to fetch blocks: %w", err)
				}
				return nil
			})

			var receipts [][]RawReceipt

			eg.Go(func() (err error) {
				receipts, err = fetchBlockReceiptsInBatch(egCtx, lastBlockNumber+1, blockNumber)
				if err != nil {
					return fmt.Errorf("failed to fetch block receipts: %w", err)
				}
				return nil
			})

			err = eg.Wait()
			if err != nil {
				yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to fetch blocks or block receipts: %w", err)})
				return
			}

			blocks := []events.Block{}

			for i, rawBlock := range rawBlocks {
				rawReceipts := receipts[i]

				block := events.Block{
					Number:     uint64(rawBlock.Number),
					Operations: []events.Operation{},
				}

				lastBlockNumber = uint64(rawBlock.Number)

				if len(rawReceipts) == 0 {
					continue
				}

				firstReceipt := rawReceipts[0]

				opIndex := uint64(0)

				for _, log := range firstReceipt.Logs {
					if log.Topics[0] == ArkivEntityExpired && len(log.Data) >= 32 {
						entityKey := common.BytesToHash(log.Data[:32])
						expire := events.OPExpire(entityKey.Bytes())
						block.Operations = append(block.Operations, events.Operation{
							TxIndex: 0,
							OpIndex: opIndex,
							Expire:  &expire,
						})
					}
				}

				for i, transaction := range rawBlock.Transactions {
					if transaction.To != ArkivProcessorAddress {
						continue
					}

					receipt := rawReceipts[i]

					if !receipt.IsSuccessful() {
						continue
					}

					atx, err := arkivtx.UnpackArkivTransaction(transaction.Data)
					if err != nil {
						yield(arkivevents.BatchOrError{Error: fmt.Errorf("failed to unpack arkiv transaction: %w", err)})
						return
					}

					createdEntities := receipt.CreatedEntities()

					for opIndex, create := range atx.Create {
						createdEntityKey := createdEntities[0]
						createdEntities = createdEntities[1:]

						block.Operations = append(block.Operations, events.Operation{
							TxIndex: uint64(i),
							OpIndex: uint64(opIndex),
							Create: &events.OPCreate{
								Key:               createdEntityKey,
								ContentType:       create.ContentType,
								BTL:               create.BTL,
								Owner:             transaction.From,
								Content:           create.Payload,
								StringAttributes:  create.StringAttributes.ToMap(),
								NumericAttributes: create.NumericAttributes.ToMap(),
							},
						})
					}

					for opIndex, update := range atx.Update {

						block.Operations = append(block.Operations, events.Operation{
							TxIndex: uint64(i),
							OpIndex: uint64(opIndex),
							Update: &events.OPUpdate{
								Key:               update.EntityKey,
								ContentType:       update.ContentType,
								BTL:               update.BTL,
								Owner:             transaction.From,
								Content:           update.Payload,
								StringAttributes:  update.StringAttributes.ToMap(),
								NumericAttributes: update.NumericAttributes.ToMap(),
							},
						})
					}

					for opIndex, extendBTL := range atx.Extend {

						block.Operations = append(block.Operations, events.Operation{
							TxIndex: uint64(i),
							OpIndex: uint64(opIndex),
							ExtendBTL: &events.OPExtendBTL{
								Key: extendBTL.EntityKey,
								BTL: extendBTL.NumberOfBlocks,
							},
						})

					}
					for opIndex, changeOwner := range atx.ChangeOwner {

						block.Operations = append(block.Operations, events.Operation{
							TxIndex: uint64(i),
							OpIndex: uint64(opIndex),
							ChangeOwner: &events.OPChangeOwner{
								Key:   changeOwner.EntityKey,
								Owner: changeOwner.NewOwner,
							},
						})

					}

				}

				blocks = append(blocks, block)

			}

			if !yield(arkivevents.BatchOrError{Batch: events.BlockBatch{Blocks: blocks}}) {
				return
			}

		}

	}

}
