package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all runtime configuration sourced from environment variables.
type Config struct {
	// Kafka
	KafkaBrokers     []string
	KafkaInputTopics []string
	KafkaOutputTopic string
	KafkaGroupID     string

	// Batching
	MaxBatchSize int
	MaxWaitTime  time.Duration

	// Operational
	LogLevel string
}

// Load reads all configuration from environment variables with production-safe defaults.
func Load() *Config {
	brokers := getEnv("KAFKA_BROKERS", "localhost:29092")
	inputTopics := getEnv("KAFKA_INPUT_TOPICS", "logs.unfiltered")
	outputTopic := getEnv("KAFKA_OUTPUT_TOPIC", "logs.batched")
	groupID := getEnv("KAFKA_GROUP_ID", "batcher-group")
	logLevel := getEnv("LOG_LEVEL", "info")

	maxSize := getEnvInt("BATCHER_MAX_SIZE", 100)
	maxWaitMs := getEnvInt("BATCHER_MAX_WAIT_MS", 500)

	return &Config{
		KafkaBrokers:     splitTrimmed(brokers, ","),
		KafkaInputTopics: splitTrimmed(inputTopics, ","),
		KafkaOutputTopic: outputTopic,
		KafkaGroupID:     groupID,
		MaxBatchSize:     maxSize,
		MaxWaitTime:      time.Duration(maxWaitMs) * time.Millisecond,
		LogLevel:         logLevel,
	}
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func splitTrimmed(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}
