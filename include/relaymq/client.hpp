#pragma once

#include "relaymq/protocol.hpp"
#include <string>
#include <string_view>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <thread>
#include <optional>
#include <cstdint>

namespace relaymq {

struct Message {
    std::string subject;
    std::string sid;
    std::string reply_to;
    std::string payload;
};

struct ClientOptions {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;
    std::string auth_token = "";
    int timeout_ms = 2000;
};

using MessageCallback = std::function<void(const Message&)>;
using DisconnectCallback = std::function<void()>;
using ErrorCallback = std::function<void(const std::string&)>;

class Client {
public:
    Client();
    ~Client();

    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;

    bool connect(const std::string& host = "127.0.0.1", uint16_t port = 9000, const std::string& auth_token = "");
    bool connect(const std::string& address, const std::string& auth_token = "");
    bool connect(const ClientOptions& options);
    void disconnect();
    bool is_connected() const;
    const std::string& last_error() const;
    void set_disconnect_callback(DisconnectCallback callback);
    void set_error_callback(ErrorCallback callback);
    bool publish(const std::string& subject, const std::string& payload, const std::string& reply_to = "");
    std::string subscribe(const std::string& subject, MessageCallback callback);
    bool unsubscribe(const std::string& sid);
    std::optional<Message> request(const std::string& subject, const std::string& payload, int timeout_ms = 2000);
    int64_t ping(int timeout_ms = 2000);

private:
    int socket_fd_ = -1;
    std::atomic<bool> is_connected_ = false;
    ProtocolParser parser_;
    std::thread receive_thread_;
    std::mutex send_mutex_;
    std::mutex sub_mutex_;
    std::atomic<uint64_t> next_sid_ = 1;
    std::atomic<uint64_t> next_request_id_ = 1;
    std::unordered_map<std::string, MessageCallback> subscriptions_;
    DisconnectCallback disconnect_callback_;
    ErrorCallback error_callback_;
    std::string last_error_;
    std::mutex pong_mutex_;
    std::condition_variable pong_cv_;
    std::atomic<bool> awaiting_pong_ = false;
    std::mutex auth_mutex_;
    std::condition_variable auth_cv_;
    std::atomic<bool> awaiting_auth_ = false;
    std::atomic<bool> auth_success_ = false;

    bool send_raw(std::string_view data);
    void receive_loop();
};

}
