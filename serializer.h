#pragma once
#include <string>
#include <vector>
#include <sstream>
#include "log_entry.h"

std::string serialize_log(const LogEntry& entry) {
    std::ostringstream oss;
    oss << "{"
        << "\"timestamp\":\""    << entry.timestamp    << "\","
        << "\"message\":\""      << entry.message      << "\","
        << "\"source_ip\":\""    << entry.source_ip    << "\","
        << "\"service_name\":\"" << entry.service_name << "\""
        << "}";
    return oss.str();
}

std::string serialize_batch(const std::vector<LogEntry>& batch) {
    std::ostringstream oss;
    oss << "{"
        << "\"batch_size\":" << batch.size() << ","
        << "\"logs\":[";

    for (size_t i = 0; i < batch.size(); i++) {
        oss << serialize_log(batch[i]);
        if (i < batch.size() - 1) oss << ",";
    }

    oss << "]}";
    return oss.str();
}