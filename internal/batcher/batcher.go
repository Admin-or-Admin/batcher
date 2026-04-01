package batcher

import (
	"context"
	"encoding/json"
	"time"

	"github.com/aurora/batcher/internal/consumer"
	"github.com/aurora/batcher/internal/producer"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Engine accumulates events from the consumer and flushes them to the producer
// based on two independent triggers:
//  1. The batch has reached MaxSize events (size trigger)
//  2. MaxWait time has elapsed since the last flush (timer trigger)
//
// On graceful shutdown, any in-flight partial batch is flushed before returning.
type Engine struct {
	events   <-chan consumer.Event
	producer *producer.Producer
	maxSize  int
	maxWait  time.Duration
	log      *zap.Logger

	// Counters – exported for introspection from main if needed
	TotalReceived int64
	TotalFlushed  int64
	TotalBatches  int64
}

// New creates a new batcher Engine.
func New(
	events <-chan consumer.Event,
	prod *producer.Producer,
	maxSize int,
	maxWait time.Duration,
	log *zap.Logger,
) *Engine {
	return &Engine{
		events:   events,
		producer: prod,
		maxSize:  maxSize,
		maxWait:  maxWait,
		log:      log,
	}
}

// Run is the main accumulation loop. It blocks until ctx is cancelled.
func (e *Engine) Run(ctx context.Context) {
	ticker := time.NewTicker(e.maxWait)
	defer ticker.Stop()

	batch := make([]json.RawMessage, 0, e.maxSize)
	sourceTopic := "" // track most recent source topic for the envelope
	lastFlushTime := time.Now()

	flush := func(reason string) {
		if len(batch) == 0 {
			return
		}

		envelope := &producer.BatchEnvelope{
			BatchID:     uuid.New().String(),
			BatchSize:   len(batch),
			SourceTopic: sourceTopic,
			FlushedAt:   time.Now().UTC().Format(time.RFC3339Nano),
			FlushReason: reason,
			Events:      batch,
		}

		if err := e.producer.Publish(ctx, envelope); err != nil {
			e.log.Error("failed to publish batch",
				zap.String("batch_id", envelope.BatchID),
				zap.Int("size", envelope.BatchSize),
				zap.Error(err),
			)
		} else {
			e.TotalFlushed += int64(len(batch))
			e.TotalBatches++
			e.log.Info("batch flushed",
				zap.String("batch_id", envelope.BatchID),
				zap.Int("size", envelope.BatchSize),
				zap.String("reason", reason),
				zap.Duration("since_last", time.Since(lastFlushTime)),
			)
		}

		// Reset accumulator
		batch = make([]json.RawMessage, 0, e.maxSize)
		lastFlushTime = time.Now()
	}

	e.log.Info("batcher engine started",
		zap.Int("max_size", e.maxSize),
		zap.Duration("max_wait", e.maxWait),
	)

	for {
		select {
		case evt, ok := <-e.events:
			if !ok {
				// Channel closed – flush remainder and exit
				e.log.Info("event channel closed, flushing final batch")
				flush("shutdown")
				return
			}

			batch = append(batch, evt.RawPayload)
			sourceTopic = evt.SourceTopic
			e.TotalReceived++

			// Size trigger: flush immediately when max reached
			if len(batch) >= e.maxSize {
				flush("size")
				ticker.Reset(e.maxWait) // reset timer after size-triggered flush
			}

		case <-ticker.C:
			// Timer trigger: flush whatever has accumulated
			flush("timeout")

		case <-ctx.Done():
			// Context cancelled (SIGTERM/SIGINT) – drain remaining events then flush
			e.log.Info("context cancelled, draining remaining events")
		drain:
			for {
				select {
				case evt, ok := <-e.events:
					if !ok {
						break drain
					}
					batch = append(batch, evt.RawPayload)
					sourceTopic = evt.SourceTopic
					e.TotalReceived++
					if len(batch) >= e.maxSize {
						flush("shutdown")
					}
				default:
					break drain
				}
			}
			flush("shutdown")
			e.log.Info("batcher engine stopped",
				zap.Int64("total_received", e.TotalReceived),
				zap.Int64("total_flushed", e.TotalFlushed),
				zap.Int64("total_batches", e.TotalBatches),
			)
			return
		}
	}
}
