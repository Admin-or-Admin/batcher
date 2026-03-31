#pragma once
#include <string>

struct LogEntry {
    std::string timestamp;
    std::string message;
    std::string source_ip;
    std::string service_name;
};