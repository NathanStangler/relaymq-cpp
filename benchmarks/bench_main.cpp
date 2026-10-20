#include "relaymq/client.hpp"
#include "relaymq/endpoint.hpp"

#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <chrono>
#include <numeric>
#include <algorithm>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <cstdint>

struct BenchConfig {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;
    size_t throughput_count = 50000;
    size_t latency_count = 5000;
    size_t payload_size = 64;
    std::string auth_token;
    bool run_throughput = true;
    bool run_latency = true;
};

struct LatencyStats {
    double min_us = 0.0;
    double mean_us = 0.0;
    double p50_us = 0.0;
    double p90_us = 0.0;
    double p99_us = 0.0;
    double p99_9_us = 0.0;
    double max_us = 0.0;
};

static LatencyStats calculate_stats(std::vector<int64_t>& samples_ns) {
    LatencyStats stats{};
    if (samples_ns.empty()) {
        return stats;
    }

    std::sort(samples_ns.begin(), samples_ns.end());
    size_t n = samples_ns.size();

    int64_t sum_ns = 0;
    for (size_t i = 0; i < n; i++) {
        sum_ns += samples_ns[i];
    }

    stats.min_us = static_cast<double>(samples_ns.front()) / 1000.0;
    stats.max_us = static_cast<double>(samples_ns.back()) / 1000.0;
    stats.mean_us = (static_cast<double>(sum_ns) / static_cast<double>(n)) / 1000.0;
    stats.p50_us = static_cast<double>(samples_ns[n * 50 / 100]) / 1000.0;
    stats.p90_us = static_cast<double>(samples_ns[n * 90 / 100]) / 1000.0;
    stats.p99_us = static_cast<double>(samples_ns[n * 99 / 100]) / 1000.0;
    stats.p99_9_us = static_cast<double>(samples_ns[n * 999 / 1000]) / 1000.0;

    return stats;
}

static void print_latency_table(const std::string& title, const LatencyStats& stats, size_t count) {
    std::cout << "\n--- " << title << " (" << count << " iterations) ---\n";
    std::cout << std::fixed << std::setprecision(2);
    std::cout << std::left << std::setw(12) << "Metric" << std::right << std::setw(16) << "Latency (us)" << std::setw(16) << "Latency (ms)" << "\n";
    std::cout << std::string(44, '-') << "\n";
    std::cout << std::left << std::setw(12) << "Min" << std::right << std::setw(16) << stats.min_us << std::setw(16) << (stats.min_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "Mean" << std::right << std::setw(16) << stats.mean_us << std::setw(16) << (stats.mean_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p50" << std::right << std::setw(16) << stats.p50_us << std::setw(16) << (stats.p50_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p90" << std::right << std::setw(16) << stats.p90_us << std::setw(16) << (stats.p90_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p99" << std::right << std::setw(16) << stats.p99_us << std::setw(16) << (stats.p99_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "p99.9" << std::right << std::setw(16) << stats.p99_9_us << std::setw(16) << (stats.p99_9_us / 1000.0) << "\n";
    std::cout << std::left << std::setw(12) << "Max" << std::right << std::setw(16) << stats.max_us << std::setw(16) << (stats.max_us / 1000.0) << "\n";
}

static void run_throughput_benchmark(const std::string& host, uint16_t port, size_t count, size_t payload_size, const std::string& auth_token = "") {
    std::cout << "Throughput Benchmark\n\n";
    std::cout << "Target: " << host << ":" << port << "\n";
    std::cout << "Messages: " << count << "\n";
    std::cout << "Payload size: " << payload_size << " bytes\n";

    std::string payload(payload_size, 'X');
    const std::string subject = "service.bench.throughput";

    relaymq::Client sub_client;
    if (!sub_client.connect(host, port, auth_token)) {
        std::cerr << "Throughput benchmark: failed to connect subscriber (" << sub_client.last_error() << ").\n";
        return;
    }

    std::mutex cv_mutex;
    std::condition_variable cv;
    std::atomic<size_t> received_count{0};
    bool done = false;

    std::string sid = sub_client.subscribe(subject, [&](const relaymq::Message&) {
        size_t r = received_count.fetch_add(1, std::memory_order_relaxed) + 1;
        if (r >= count) {
            std::unique_lock<std::mutex> lock(cv_mutex);
            done = true;
            cv.notify_one();
        }
    });

    if (sid.empty()) {
        std::cerr << "Throughput benchmark: failed to subscribe.\n";
        sub_client.disconnect();
        return;
    }

    sub_client.ping();

    relaymq::Client pub_client;
    if (!pub_client.connect(host, port, auth_token)) {
        std::cerr << "Throughput benchmark: failed to connect publisher (" << pub_client.last_error() << ").\n";
        sub_client.disconnect();
        return;
    }

    std::cout << "Publishing " << count << " messages...\n";

    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < count; i++) {
        if (!pub_client.publish(subject, payload)) {
            std::cerr << "Publish failed at iteration " << i << "\n";
            break;
        }
    }

    std::unique_lock<std::mutex> lock(cv_mutex);
    bool completed = cv.wait_for(lock, std::chrono::seconds(15), [&]() {
        return done;
    });
    std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
    lock.unlock();

    if (!completed) {
        std::cerr << "Throughput benchmark timed out. Received "
                  << received_count.load() << " / " << count << " messages.\n";
        pub_client.disconnect();
        sub_client.disconnect();
        return;
    }

    int64_t elapsed_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    double elapsed_sec = static_cast<double>(elapsed_ns) / 1e9;
    double msgs_per_sec = static_cast<double>(count) / elapsed_sec;
    double mb_per_sec = (msgs_per_sec * static_cast<double>(payload_size)) / (1024.0 * 1024.0);

    std::cout << std::fixed << std::setprecision(2);
    std::cout << "Completed in: " << elapsed_sec << " s\n";
    std::cout << "Throughput: " << msgs_per_sec << " msg/sec (" << mb_per_sec << " MB/sec)\n";

    pub_client.disconnect();
    sub_client.disconnect();
}

static void run_ping_latency_benchmark(const std::string& host, uint16_t port, size_t count, const std::string& auth_token = "") {
    relaymq::Client client;
    if (!client.connect(host, port, auth_token)) {
        std::cerr << "Ping latency benchmark: failed to connect (" << client.last_error() << ").\n";
        return;
    }

    for (size_t i = 0; i < 50; i++) {
        client.ping();
    }

    std::vector<int64_t> samples_ns;
    samples_ns.reserve(count);

    for (size_t i = 0; i < count; i++) {
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        int64_t response_time = client.ping();
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();

        if (response_time >= 0) {
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            samples_ns.push_back(ns);
        }
    }

    LatencyStats stats = calculate_stats(samples_ns);
    print_latency_table("Ping RTT", stats, samples_ns.size());

    client.disconnect();
}

static void run_pubsub_latency_benchmark(const std::string& host, uint16_t port, size_t count, size_t payload_size, const std::string& auth_token = "") {
    const std::string req_subject = "service.bench.latency.req";
    const std::string res_subject = "service.bench.latency.res";
    std::string payload(payload_size, 'A');

    relaymq::Client echo_client;
    if (!echo_client.connect(host, port, auth_token)) {
        std::cerr << "Pub/Sub latency benchmark: failed to connect echo client (" << echo_client.last_error() << ").\n";
        return;
    }

    echo_client.subscribe(req_subject, [&](const relaymq::Message& msg) {
        echo_client.publish(res_subject, msg.payload);
    });
    echo_client.ping();

    relaymq::Client req_client;
    if (!req_client.connect(host, port, auth_token)) {
        std::cerr << "Pub/Sub latency benchmark: failed to connect requester client (" << req_client.last_error() << ").\n";
        echo_client.disconnect();
        return;
    }

    std::mutex mtx;
    std::condition_variable cv;
    bool reply_received = false;

    req_client.subscribe(res_subject, [&](const relaymq::Message&) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = true;
        lock.unlock();
        cv.notify_one();
    });
    req_client.ping();

    for (size_t i = 0; i < 50; i++) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = false;
        req_client.publish(req_subject, "warmup");
        cv.wait_for(lock, std::chrono::milliseconds(100), [&]() {
            return reply_received;
        });
    }

    std::vector<int64_t> samples_ns;
    samples_ns.reserve(count);

    for (size_t i = 0; i < count; i++) {
        std::unique_lock<std::mutex> lock(mtx);
        reply_received = false;

        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        req_client.publish(req_subject, payload);

        bool ok = cv.wait_for(lock, std::chrono::milliseconds(500), [&]() {
            return reply_received;
        });
        std::chrono::steady_clock::time_point end = std::chrono::steady_clock::now();
        lock.unlock();

        if (ok) {
            int64_t ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
            samples_ns.push_back(ns);
        }
    }

    LatencyStats stats = calculate_stats(samples_ns);
    print_latency_table("Pub/Sub RTT", stats, samples_ns.size());

    req_client.disconnect();
    echo_client.disconnect();
}

static void print_usage() {
    std::cout << "RelayMQ Benchmarking\n\n"
              << "Usage: relay-bench [options]\n\n"
              << "Options:\n"
              << "  --server <host:port>     RelayMQ broker (default: 127.0.0.1:9000)\n"
              << "  --throughput-count <N>   Number of messages for throughput test (default: 50000)\n"
              << "  --latency-count <N>      Number of iterations for latency test (default: 5000)\n"
              << "  --payload <bytes>        Message payload size in bytes (default: 64)\n"
              << "  --token <token>          Authentication bearer token\n"
              << "  --throughput-only        Run only the throughput benchmark\n"
              << "  --latency-only           Run only the latency benchmark\n"
              << "  --help                   Display this help message\n";
}

int main(int argc, char* argv[]) {
    BenchConfig config;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        if (arg == "--help") {
            print_usage();
            return 0;
        } else if (arg == "--server" && i + 1 < argc) {
            i++;
            relaymq::Endpoint endpoint;
            if (relaymq::Endpoint::parse(argv[i], endpoint)) {
                config.host = endpoint.host;
                config.port = endpoint.port;
            }
        } else if (arg == "--throughput-count" && i + 1 < argc) {
            i++;
            config.throughput_count = static_cast<size_t>(std::stoul(argv[i]));
        } else if (arg == "--latency-count" && i + 1 < argc) {
            i++;
            config.latency_count = static_cast<size_t>(std::stoul(argv[i]));
        } else if (arg == "--payload" && i + 1 < argc) {
            i++;
            config.payload_size = static_cast<size_t>(std::stoul(argv[i]));
        } else if (arg == "--token" && i + 1 < argc) {
            i++;
            config.auth_token = argv[i];
        } else if (arg == "--throughput-only") {
            config.run_throughput = true;
            config.run_latency = false;
        } else if (arg == "--latency-only") {
            config.run_throughput = false;
            config.run_latency = true;
        }
    }

    std::cout << "[RelayMQ Benchmark] Broker: " << config.host << ":" << config.port << "\n\n";

    if (config.run_throughput) {
        run_throughput_benchmark(config.host, config.port, config.throughput_count, config.payload_size, config.auth_token);
    }

    if (config.run_latency) {
        run_ping_latency_benchmark(config.host, config.port, config.latency_count, config.auth_token);
        run_pubsub_latency_benchmark(config.host, config.port, config.latency_count, config.payload_size, config.auth_token);
    }

    std::cout << "\n[RelayMQ Benchmark] Complete.\n";
    return 0;
}
