package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/aurora/batcher/internal/batcher"
	"github.com/aurora/batcher/internal/config"
	"github.com/aurora/batcher/internal/consumer"
	"github.com/aurora/batcher/internal/producer"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const version = "1.0.0"

func main() {
	cfg := config.Load()

	log := buildLogger(cfg.LogLevel)
	defer log.Sync() //nolint:errcheck

	log.Info("Aurora Batcher Service starting",
		zap.String("version", version),
		zap.Strings("input_topics", cfg.KafkaInputTopics),
		zap.String("output_topic", cfg.KafkaOutputTopic),
		zap.String("group_id", cfg.KafkaGroupID),
		zap.Strings("brokers", cfg.KafkaBrokers),
		zap.Int("max_batch_size", cfg.MaxBatchSize),
		zap.Duration("max_wait", cfg.MaxWaitTime),
	)

	// Health-check shortcut: verify config and exit 0 (used by Docker HEALTHCHECK)
	if len(os.Args) > 1 && os.Args[1] == "--health" {
		if len(cfg.KafkaBrokers) == 0 || cfg.KafkaOutputTopic == "" {
			fmt.Fprintln(os.Stderr, "health: invalid configuration")
			os.Exit(1)
		}
		fmt.Println("ok")
		os.Exit(0)
	}

	// Root context – cancelled on SIGINT or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── Kafka Producer ───────────────────────────────────────────────────────
	prod, err := producer.New(cfg.KafkaBrokers, cfg.KafkaOutputTopic, log)
	if err != nil {
		log.Fatal("failed to create Kafka producer", zap.Error(err))
	}
	defer prod.Close()

	// ── Event Channel ────────────────────────────────────────────────────────
	// Buffer = 2× max batch size so the consumer never blocks while the engine
	// is flushing.
	eventCh := make(chan consumer.Event, cfg.MaxBatchSize*2)

	// ── Batcher Engine ───────────────────────────────────────────────────────
	engine := batcher.New(eventCh, prod, cfg.MaxBatchSize, cfg.MaxWaitTime, log)

	// ── Kafka Consumer ───────────────────────────────────────────────────────
	cons, err := consumer.New(cfg.KafkaBrokers, cfg.KafkaInputTopics, cfg.KafkaGroupID, eventCh, log)
	if err != nil {
		log.Fatal("failed to create Kafka consumer", zap.Error(err))
	}

	// ── Start goroutines ─────────────────────────────────────────────────────

	// Batcher engine runs in its own goroutine; closes eventCh when done so the
	// consumer's select can also unblock cleanly.
	engineDone := make(chan struct{})
	go func() {
		engine.Run(ctx)
		close(engineDone)
	}()

	// Consumer poll loop runs in its own goroutine.
	go func() {
		cons.Run(ctx)
		// Once the consumer stops (ctx cancelled), close the event channel so
		// the engine can drain and exit.
		close(eventCh)
		cons.Close()
	}()

	// ── Wait for shutdown ────────────────────────────────────────────────────
	<-ctx.Done()
	log.Info("shutdown signal received, waiting for batcher to drain...")
	<-engineDone

	log.Info("Aurora Batcher Service stopped cleanly",
		zap.Int64("total_received", engine.TotalReceived),
		zap.Int64("total_flushed", engine.TotalFlushed),
		zap.Int64("total_batches", engine.TotalBatches),
	)
}

// buildLogger constructs a production zap logger at the specified level.
func buildLogger(level string) *zap.Logger {
	lvl := zapcore.InfoLevel
	switch strings.ToLower(level) {
	case "debug":
		lvl = zapcore.DebugLevel
	case "warn":
		lvl = zapcore.WarnLevel
	case "error":
		lvl = zapcore.ErrorLevel
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.TimeKey = "ts"
	encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	zapCfg := zap.Config{
		Level:            zap.NewAtomicLevelAt(lvl),
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    encoderCfg,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}

	log, err := zapCfg.Build()
	if err != nil {
		panic("failed to build logger: " + err.Error())
	}
	return log
}
