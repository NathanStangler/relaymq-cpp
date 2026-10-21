#include "relaymq/client.hpp"
#include "relaymq/endpoint.hpp"

#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <chrono>
#include <future>

namespace relaymq {

static int tcp_connect(const std::string& host, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    int opt = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (inet_pton(AF_INET, host.c_str(), &addr.sin_addr) <= 0) {
        close(fd);
        return -1;
    }

    if (connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    return fd;
}

static ssize_t tcp_send_all(int fd, std::string_view data) {
    if (fd < 0) {
        return -1;
    }

    size_t total_sent = 0;
    while (total_sent < data.size()) {
        ssize_t sent = send(fd, data.data() + total_sent, data.size() - total_sent, MSG_NOSIGNAL);
        if (sent <= 0) {
            return -1;
        }
        total_sent += static_cast<size_t>(sent);
    }
    return static_cast<ssize_t>(total_sent);
}

void Client::set_last_error(std::string err) {
    std::lock_guard<std::mutex> lock(error_mutex_);
    last_error_ = std::move(err);
}

std::string Client::last_error() {
    std::lock_guard<std::mutex> lock(error_mutex_);
    return last_error_;
}

Client::Client() = default;

Client::~Client() {
    disconnect();
}

bool Client::connect(const std::string& host, uint16_t port, const std::string& auth_token) {
    ClientOptions options;
    options.host = host;
    options.port = port;
    options.auth_token = auth_token;
    return connect(options);
}

bool Client::connect(const std::string& address, const std::string& auth_token) {
    Endpoint endpoint;
    if (Endpoint::parse(address, endpoint)) {
        return connect(endpoint.host, endpoint.port, auth_token);
    }
    return connect(address, 9000, auth_token);
}

bool Client::connect(const ClientOptions& options) {
    if (is_connected_) {
        return true;
    }

    disconnect();

    parser_.clear();
    set_last_error("");

    int fd = tcp_connect(options.host, options.port);
    if (fd < 0) {
        set_last_error("Failed to establish TCP connection");
        return false;
    }
    socket_fd_.store(fd);

    is_connected_ = true;
    receive_thread_ = std::thread(&Client::receive_loop, this);

    if (!options.auth_token.empty()) {
        std::unique_lock<std::mutex> lock(auth_mutex_);
        awaiting_auth_ = true;
        auth_success_ = false;

        std::string auth_msg = ProtocolParser::format_auth(options.auth_token);
        if (!send_raw(auth_msg)) {
            awaiting_auth_ = false;
            disconnect();
            set_last_error("Failed to send AUTH frame");
            return false;
        }

        bool received = auth_cv_.wait_for(lock, std::chrono::milliseconds(options.timeout_ms), [this]() {
            return !awaiting_auth_.load() || !is_connected_.load();
        });

        if (!received || !is_connected_ || !auth_success_.load()) {
            awaiting_auth_ = false;
            disconnect();
            if (last_error().empty()) {
                set_last_error("Authentication timed out or rejected");
            }
            return false;
        }
    }

    return true;
}

void Client::disconnect() {
    is_connected_ = false;

    int fd = socket_fd_.load();
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
    }

    if (receive_thread_.joinable()) {
        if (std::this_thread::get_id() != receive_thread_.get_id()) {
            receive_thread_.join();
        } else {
            receive_thread_.detach();
        }
    }

    int to_close = socket_fd_.exchange(-1);
    if (to_close >= 0) {
        close(to_close);
    }

    std::unique_lock<std::mutex> lock(sub_mutex_);
    subscriptions_.clear();
    lock.unlock();

    pong_cv_.notify_all();
    auth_cv_.notify_all();
}

bool Client::is_connected() const {
    return is_connected_;
}

void Client::set_disconnect_callback(DisconnectCallback callback) {
    std::unique_lock<std::mutex> lock(sub_mutex_);
    disconnect_callback_ = std::move(callback);
}

void Client::set_error_callback(ErrorCallback callback) {
    std::lock_guard<std::mutex> lock(error_mutex_);
    error_callback_ = std::move(callback);
}

bool Client::send_raw(std::string_view data) {
    std::lock_guard<std::mutex> lock(send_mutex_);
    int fd = socket_fd_.load();
    if (!is_connected_ || fd < 0) {
        return false;
    }

    ssize_t sent = tcp_send_all(fd, data);
    if (sent < 0 || static_cast<size_t>(sent) < data.size()) {
        is_connected_ = false;
        return false;
    }
    return true;
}

bool Client::publish(const std::string& subject, const std::string& payload, const std::string& reply_to) {
    if (subject.empty()) {
        set_last_error("Subject cannot be empty");
        return false;
    }
    std::string msg = ProtocolParser::format_pub(subject, payload, reply_to);
    return send_raw(msg);
}

std::string Client::subscribe(const std::string& subject, MessageCallback callback) {
    if (subject.empty() || !callback) {
        set_last_error("Subject and callback cannot be empty");
        return "";
    }

    uint64_t sid_num = next_sid_++;
    std::string sid = std::to_string(sid_num);

    std::unique_lock<std::mutex> lock(sub_mutex_);
    subscriptions_[sid] = std::move(callback);
    lock.unlock();

    std::string msg = ProtocolParser::format_sub(subject, sid);
    if (!send_raw(msg)) {
        lock.lock();
        subscriptions_.erase(sid);
        lock.unlock();
        return "";
    }

    return sid;
}

bool Client::unsubscribe(const std::string& sid) {
    std::unique_lock<std::mutex> lock(sub_mutex_);
    subscriptions_.erase(sid);
    lock.unlock();

    std::string msg = ProtocolParser::format_unsub(sid);
    return send_raw(msg);
}

std::optional<Message> Client::request(const std::string& subject, const std::string& payload, int timeout_ms) {
    if (!is_connected_) {
        set_last_error("Not connected");
        return std::nullopt;
    }
    if (subject.empty()) {
        set_last_error("Subject cannot be empty");
        return std::nullopt;
    }

    uint64_t req_id = next_request_id_++;
    std::string reply_subject = "reply." + std::to_string(req_id);

    std::shared_ptr<std::promise<Message>> promise = std::make_shared<std::promise<Message>>();
    std::future<Message> future = promise->get_future();
    std::shared_ptr<std::atomic<bool>> delivered = std::make_shared<std::atomic<bool>>(false);

    std::string sid = subscribe(reply_subject, [promise, delivered](const Message& msg) {
        bool expected = false;
        if (delivered->compare_exchange_strong(expected, true)) {
            promise->set_value(msg);
        }
    });

    if (sid.empty()) {
        return std::nullopt;
    }

    if (!publish(subject, payload, reply_subject)) {
        unsubscribe(sid);
        return std::nullopt;
    }

    int interval_ms = 20;
    int waited_ms = 0;
    while (waited_ms < timeout_ms) {
        int step = std::min(interval_ms, timeout_ms - waited_ms);
        if (future.wait_for(std::chrono::milliseconds(step)) == std::future_status::ready) {
            unsubscribe(sid);
            return future.get();
        }
        if (!is_connected_) {
            unsubscribe(sid);
            return std::nullopt;
        }
        waited_ms += step;
    }

    unsubscribe(sid);
    return std::nullopt;
}

int64_t Client::ping(int timeout_ms) {
    std::unique_lock<std::mutex> lock(pong_mutex_);
    if (!is_connected_ || socket_fd_ < 0) {
        return -1;
    }

    awaiting_pong_ = true;
    std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();

    std::string ping_msg = ProtocolParser::format_ping();
    if (!send_raw(ping_msg)) {
        awaiting_pong_ = false;
        return -1;
    }

    bool received = pong_cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [this]() {
        return !awaiting_pong_.load() || !is_connected_.load();
    });

    if (!received || !is_connected_ || awaiting_pong_.load()) {
        awaiting_pong_ = false;
        return -1;
    }

    std::chrono::steady_clock::time_point end_time = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
}

void Client::receive_loop() {
    char buffer[16384];

    while (is_connected_) {
        int fd = socket_fd_.load();
        if (fd < 0) {
            break;
        }
        ssize_t bytes_read = recv(fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            is_connected_ = false;
            DisconnectCallback cb;
            std::unique_lock<std::mutex> lock(sub_mutex_);
            cb = disconnect_callback_;
            lock.unlock();

            if (cb) {
                cb();
            }
            break;
        }

        parser_.append(buffer, static_cast<size_t>(bytes_read));

        Command cmd;
        while (parser_.next_command(cmd)) {
            if (cmd.type == CommandType::MSG) {
                MessageCallback cb;
                std::unique_lock<std::mutex> lock(sub_mutex_);
                auto it = subscriptions_.find(cmd.sid);
                if (it != subscriptions_.end()) {
                    cb = it->second;
                }
                lock.unlock();

                if (cb) {
                    cb(Message{cmd.subject, cmd.sid, cmd.reply_to, cmd.payload});
                }
            } else if (cmd.type == CommandType::PONG) {
                std::unique_lock<std::mutex> lock(pong_mutex_);
                awaiting_pong_ = false;
                lock.unlock();

                pong_cv_.notify_all();
            } else if (cmd.type == CommandType::OK) {
                if (awaiting_auth_.load()) {
                    std::unique_lock<std::mutex> lock(auth_mutex_);
                    auth_success_ = true;
                    awaiting_auth_ = false;
                    lock.unlock();

                    auth_cv_.notify_all();
                }
            } else if (cmd.type == CommandType::ERR) {
                set_last_error(cmd.error_message);

                if (awaiting_auth_.load()) {
                    std::unique_lock<std::mutex> lock(auth_mutex_);
                    auth_success_ = false;
                    awaiting_auth_ = false;
                    lock.unlock();

                    auth_cv_.notify_all();
                }

                ErrorCallback err_cb;
                std::unique_lock<std::mutex> lock(error_mutex_);
                err_cb = error_callback_;
                lock.unlock();

                if (err_cb) {
                    err_cb(cmd.error_message);
                }
            }
        }
    }

    pong_cv_.notify_all();
    auth_cv_.notify_all();
}

}
