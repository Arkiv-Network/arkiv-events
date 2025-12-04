package arkivevents

import (
	"iter"

	"github.com/Arkiv-Network/arkiv-events/events"
)

type BatchOrError struct {
	Batch events.BlockBatch
	Error error
}

type BatchIterator iter.Seq[BatchOrError]
