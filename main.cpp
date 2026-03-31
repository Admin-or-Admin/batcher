#include <iostream>
#include <csignal>
#include "batcher.h"
#include "kafka_consumer.h"

// Global flag so Ctrl+C shuts down cleanly
static bool global_running = true;

void signal_handler(int) {
    std::cout << "\nShutting down..." << std::endl;
    global_running = false;
}

int main() {
    signal(SIGINT, signal_handler);   // handle Ctrl+C
    signal(SIGTERM, signal_handler);  // handle docker stop

    // Read config from environment variables
    // with sensible defaults for local dev
    const char* brokers = std::getenv("KAFKA_BROKERS");
    const char* topic   = std::getenv("KAFKA_INPUT_TOPIC");

    std::string kafka_brokers = brokers ? brokers : "localhost:29092";
    std::string input_topic   = topic   ? topic   : "raw-logs";

    Batcher batcher(50, 5);  // 50 logs max, flush every 5s

    KafkaConsumer consumer(kafka_brokers, "batcher-group", input_topic);

    std::cout << "Consuming from: " << input_topic << std::endl;
    std::cout << "Press Ctrl+C to stop." << std::endl;

    // consume() blocks and calls this lambda for every message
    consumer.consume([&batcher](LogEntry entry) {
        batcher.add(entry);
    });

    return 0;
}