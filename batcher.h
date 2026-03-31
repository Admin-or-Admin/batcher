#pragma once
#include <vector>
#include <thread>
#include <mutex>
#include <atomic>
#include <iostream>
#include "log_entry.h"
#include "serializer.h"

class Batcher {
private:
    std::vector<LogEntry> buffer;
    size_t max_batch_size;
    int flush_interval_seconds;
    std::mutex buffer_mutex;
    std::atomic<bool> running;
    std::thread timer_thread;

    void flush_internal() {
        if (buffer.empty()) return;
        std::string json = serialize_batch(buffer);
        std::cout << "=== FLUSH " << buffer.size() << " logs ===" << std::endl;
        std::cout << json << std::endl;
        buffer.clear();
    }

    void timer_loop() {
        while (running) {
            std::this_thread::sleep_for(
                std::chrono::seconds(flush_interval_seconds));
            std::lock_guard<std::mutex> lock(buffer_mutex);
            std::cout << "[TIMER] Flushing..." << std::endl;
            flush_internal();
        }
    }

public:
    Batcher(size_t max_size, int interval_seconds)
        : max_batch_size(max_size),
          flush_interval_seconds(interval_seconds),
          running(true) {
        timer_thread = std::thread([this]() { timer_loop(); });
        std::cout << "Batcher started. Max: " << max_batch_size
                  << " logs, every " << flush_interval_seconds
                  << "s" << std::endl;
    }

    ~Batcher() {
        running = false;
        timer_thread.join();
    }

    void add(const LogEntry& entry) {
        std::lock_guard<std::mutex> lock(buffer_mutex);
        buffer.push_back(entry);
        std::cout << "Buffered: " << buffer.size()
                  << "/" << max_batch_size << std::endl;
        if (buffer.size() >= max_batch_size) {
            std::cout << "[SIZE] Flushing..." << std::endl;
            flush_internal();
        }
    }
};