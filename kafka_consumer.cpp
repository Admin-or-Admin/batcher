#include "kafka_consumer.h"
#include <librdkafka/rdkafka.h>
#include <iostream>
#include <stdexcept>

// Helper — parse a raw Kafka message string into a LogEntry
// For now we do minimal parsing — just put the whole message in "message"
// Later we will parse proper JSON from the ingestor
static LogEntry parse_message(const std::string& raw) {
    LogEntry entry;
    entry.timestamp    = "unknown";
    entry.message      = raw;
    entry.source_ip    = "unknown";
    entry.service_name = "unknown";
    return entry;
}

KafkaConsumer::KafkaConsumer(const std::string& brokers,
                             const std::string& group_id,
                             const std::string& topic) {
    running = true;
    char errstr[512];

    // Create Kafka config
    rd_kafka_conf_t* conf = rd_kafka_conf_new();
    rd_kafka_conf_set(conf, "bootstrap.servers", brokers.c_str(),
                      errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "group.id", group_id.c_str(),
                      errstr, sizeof(errstr));
    rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
                      errstr, sizeof(errstr));

    // Create consumer
    rd_kafka_t* rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
                                   errstr, sizeof(errstr));
    if (!rk) {
        throw std::runtime_error(std::string("Failed to create consumer: ")
                                 + errstr);
    }

    // Subscribe to topic
    rd_kafka_topic_partition_list_t* subscription =
        rd_kafka_topic_partition_list_new(1);
    rd_kafka_topic_partition_list_add(subscription, topic.c_str(),
                                      RD_KAFKA_PARTITION_UA);
    rd_kafka_subscribe(rk, subscription);

    consumer_handle = rk;
    topic_partition = subscription;

    std::cout << "Kafka consumer connected to " << brokers
              << " topic: " << topic << std::endl;
}

KafkaConsumer::~KafkaConsumer() {
    rd_kafka_consumer_close((rd_kafka_t*)consumer_handle);
    rd_kafka_topic_partition_list_destroy(
        (rd_kafka_topic_partition_list_t*)topic_partition);
    rd_kafka_destroy((rd_kafka_t*)consumer_handle);
}

void KafkaConsumer::stop() {
    running = false;
}

void KafkaConsumer::consume(std::function<void(LogEntry)> callback) {
    rd_kafka_t* rk = (rd_kafka_t*)consumer_handle;

    while (running) {
        // Poll for a message — wait up to 1000ms
        rd_kafka_message_t* msg = rd_kafka_consumer_poll(rk, 1000);

        if (!msg) continue;  // timeout — no message, try again

        if (msg->err) {
            if (msg->err != RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                std::cerr << "Kafka error: "
                          << rd_kafka_message_errstr(msg) << std::endl;
            }
        } else {
            // Got a real message — parse it and call the callback
            std::string raw((char*)msg->payload, msg->len);
            LogEntry entry = parse_message(raw);
            callback(entry);
        }

        rd_kafka_message_destroy(msg);
    }
}