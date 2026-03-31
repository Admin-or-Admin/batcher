#pragma once
#include <string>
#include <functional>
#include "log_entry.h"

class KafkaConsumer {
public:
    KafkaConsumer(const std::string& brokers,
                  const std::string& group_id,
                  const std::string& topic);
    ~KafkaConsumer();

    // Calls the callback for each message received
    void consume(std::function<void(LogEntry)> callback);
    void stop();

private:
    void* consumer_handle;  // rd_kafka_t pointer hidden from header
    void* topic_partition;  // rd_kafka_topic_partition_list_t
    bool running;
};