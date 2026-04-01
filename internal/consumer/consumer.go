package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Event represents a single raw event read from Kafka, preserving the original JSON.
type Event struct {
	SourceTopic string
	RawPayload  json.RawMessage
}

// Consumer wraps a franz-go poll-loop and sends Events onto an output channel.
type Consumer struct {
	client *kgo.Client
	events chan<- Event
	log    *zap.Logger
}

// New creates a Consumer that subscribes to the provided topics.
func New(brokers, topics []string, groupID string, events chan<- Event, log *zap.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),                    // we commit only after the batcher has accepted the event
		kgo.FetchMaxWait(500*time.Millisecond),     // ms; keeps the poll loop responsive
		kgo.FetchMaxBytes(52428800),                // 50 MB max fetch size
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	return &Consumer{
		client: client,
		events: events,
		log:    log,
	}, nil
}

// Run starts the poll loop. It blocks until ctx is cancelled.
// For every Kafka record it sends an Event and then commits the offset.
func (c *Consumer) Run(ctx context.Context) {
	c.log.Info("consumer poll loop started")
	for {
		fetches := c.client.PollFetches(ctx)

		// Propagate context cancellation
		if ctx.Err() != nil {
			c.log.Info("consumer context cancelled, stopping poll loop")
			return
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				c.log.Error("fetch error",
					zap.String("topic", fe.Topic),
					zap.Int32("partition", fe.Partition),
					zap.Error(fe.Err),
				)
			}
			continue
		}

		fetches.EachRecord(func(r *kgo.Record) {
			// Validate that the value is JSON; skip malformed records
			if !json.Valid(r.Value) {
				c.log.Warn("skipping non-JSON record",
					zap.String("topic", r.Topic),
					zap.Int64("offset", r.Offset),
				)
				return
			}

			evt := Event{
				SourceTopic: r.Topic,
				RawPayload:  json.RawMessage(r.Value),
			}

			select {
			case c.events <- evt:
			case <-ctx.Done():
				return
			}
		})

		// Commit all offsets for this fetch batch
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
			c.log.Warn("offset commit failed", zap.Error(err))
		}
	}
}

// Close gracefully shuts down the consumer.
func (c *Consumer) Close() {
	c.client.Close()
}
