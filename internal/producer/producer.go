package producer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Producer wraps a franz-go client and publishes batch envelopes to Kafka.
type Producer struct {
	client      *kgo.Client
	outputTopic string
	log         *zap.Logger
}

// New creates a new Producer and ensures the output topic exists.
func New(brokers []string, outputTopic string, log *zap.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProducerLinger(0),           // flush immediately, batching is our job
		kgo.RecordRetries(5),            // retry transient errors
		kgo.ProduceRequestTimeout(30*time.Second),
		kgo.RetryTimeout(60*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}

	// Ensure output topic exists (auto-create with safe defaults)
	if err := ensureTopic(client, outputTopic, log); err != nil {
		// Non-fatal: topic may be auto-created by broker or already exist
		log.Warn("could not ensure output topic exists (may already exist)", zap.String("topic", outputTopic), zap.Error(err))
	}

	return &Producer{
		client:      client,
		outputTopic: outputTopic,
		log:         log,
	}, nil
}

// BatchEnvelope is the JSON message published to the output Kafka topic.
type BatchEnvelope struct {
	BatchID     string            `json:"batch_id"`
	BatchSize   int               `json:"batch_size"`
	SourceTopic string            `json:"source_topic"`
	FlushedAt   string            `json:"flushed_at"`
	FlushReason string            `json:"flush_reason"` // "size" | "timeout" | "shutdown"
	Events      []json.RawMessage `json:"events"`
}

// Publish serialises the envelope and sends it synchronously to the output topic.
// It blocks until Kafka acknowledges the record or returns an error.
func (p *Producer) Publish(ctx context.Context, envelope *BatchEnvelope) error {
	payload, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal batch envelope: %w", err)
	}

	record := &kgo.Record{
		Topic: p.outputTopic,
		Key:   []byte(envelope.BatchID),
		Value: payload,
	}

	// ProduceSync blocks, which gives us a strong durability guarantee
	results := p.client.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}

	p.log.Debug("batch published",
		zap.String("batch_id", envelope.BatchID),
		zap.Int("size", envelope.BatchSize),
		zap.String("reason", envelope.FlushReason),
		zap.String("topic", p.outputTopic),
	)
	return nil
}

// Close flushes any buffered records and closes the underlying client.
func (p *Producer) Close() {
	p.client.Flush(context.Background())
	p.client.Close()
}

// ensureTopic creates the topic if it does not already exist.
func ensureTopic(client *kgo.Client, topic string, log *zap.Logger) error {
	adm := kadm.NewClient(client)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, 1, 1, nil, topic)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			// ErrTopicAlreadyExists is fine
			log.Debug("create topic response", zap.String("topic", r.Topic), zap.Error(r.Err))
		} else {
			log.Info("output topic created", zap.String("topic", r.Topic))
		}
	}
	return nil
}
