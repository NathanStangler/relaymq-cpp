#include "relaymq/client.hpp"
#include <gtest/gtest.h>
#include <thread>
#include <chrono>
#include <future>
#include <atomic>
#include <string_view>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

static void close_fd(int fd) {
    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }
}

static void send_all(int fd, std::string_view data) {
    if (fd < 0) {
        return;
    }
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t n = send(fd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            break;
        }
        sent += static_cast<size_t>(n);
    }
}

class MockServer {
public:
    MockServer() : listen_fd_(-1), port_(0), is_running_(false), client_fd_(-1) {}

    ~MockServer() {
        stop();
    }

    bool start() {
        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return false;
        }

        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = 0;

        if (bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
            close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        if (listen(listen_fd_, 5) < 0) {
            close(listen_fd_);
            listen_fd_ = -1;
            return false;
        }

        socklen_t len = sizeof(addr);
        if (getsockname(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), &len) == 0) {
            port_ = ntohs(addr.sin_port);
        }

        is_running_ = true;
        server_thread_ = std::thread(&MockServer::run, this);
        return true;
    }

    void stop() {
        is_running_ = false;
        int cfd = client_fd_.exchange(-1);
        if (cfd >= 0) {
            close_fd(cfd);
        }
        if (listen_fd_ >= 0) {
            close_fd(listen_fd_);
            listen_fd_ = -1;
        }
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    uint16_t port() const {
        return port_;
    }

private:
    int listen_fd_;
    uint16_t port_;
    std::atomic<bool> is_running_;
    std::atomic<int> client_fd_;
    std::thread server_thread_;

    void run() {
        while (is_running_) {
            sockaddr_in client_addr{};
            socklen_t len = sizeof(client_addr);
            int fd = accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&client_addr), &len);
            if (fd < 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(5));
                continue;
            }
            client_fd_ = fd;

            relaymq::ProtocolParser parser;
            char buf[4096];

            while (is_running_) {
                ssize_t bytes = recv(fd, buf, sizeof(buf), 0);
                if (bytes <= 0) {
                    break;
                }
                parser.append(buf, static_cast<size_t>(bytes));

                relaymq::Command cmd;
                while (parser.next_command(cmd)) {
                    if (cmd.type == relaymq::CommandType::PING) {
                        send_all(fd, relaymq::ProtocolParser::format_pong());
                    } else if (cmd.type == relaymq::CommandType::AUTH) {
                        if (cmd.auth_token == "valid_token") {
                            send_all(fd, relaymq::ProtocolParser::format_ok());
                        } else {
                            send_all(fd, relaymq::ProtocolParser::format_err("Invalid Credentials"));
                        }
                    } else if (cmd.type == relaymq::CommandType::SUB) {
                        send_all(fd, relaymq::ProtocolParser::format_ok());
                    } else if (cmd.type == relaymq::CommandType::PUB) {
                        if (!cmd.reply_to.empty()) {
                            std::string reply = relaymq::ProtocolParser::format_msg(cmd.reply_to, "1", "reply:" + cmd.payload);
                            send_all(fd, reply);
                        } else {
                            std::string msg = relaymq::ProtocolParser::format_msg(cmd.subject, "1", cmd.payload);
                            send_all(fd, msg);
                        }
                    }
                }
            }

            int cfd = client_fd_.exchange(-1);
            if (cfd >= 0) {
                close_fd(cfd);
            }
        }
    }
};

TEST(ClientTest, ConnectAndDisconnect) {
    MockServer server;
    ASSERT_TRUE(server.start());

    relaymq::Client client;
    EXPECT_FALSE(client.is_connected());

    ASSERT_TRUE(client.connect("127.0.0.1", server.port()));
    EXPECT_TRUE(client.is_connected());

    client.disconnect();
    EXPECT_FALSE(client.is_connected());
}

TEST(ClientTest, PingLatency) {
    MockServer server;
    ASSERT_TRUE(server.start());

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server.port()));

    int64_t latency = client.ping();
    EXPECT_GE(latency, 0);

    client.disconnect();
}

TEST(ClientTest, AuthSuccessAndFailure) {
    MockServer server;
    ASSERT_TRUE(server.start());

    relaymq::Client bad_client;
    EXPECT_FALSE(bad_client.connect("127.0.0.1", server.port(), "invalid_token"));

    relaymq::Client good_client;
    EXPECT_TRUE(good_client.connect("127.0.0.1", server.port(), "valid_token"));
    good_client.disconnect();
}

TEST(ClientTest, SubscribeAndReceive) {
    MockServer server;
    ASSERT_TRUE(server.start());

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server.port()));

    std::promise<relaymq::Message> promise;
    std::future<relaymq::Message> future = promise.get_future();

    std::string sid = client.subscribe("service.events.data", [&](const relaymq::Message& msg) {
        promise.set_value(msg);
    });
    EXPECT_FALSE(sid.empty());

    EXPECT_TRUE(client.publish("service.events.data", "payload"));

    ASSERT_EQ(future.wait_for(std::chrono::seconds(2)), std::future_status::ready);
    relaymq::Message received = future.get();
    EXPECT_EQ(received.subject, "service.events.data");
    EXPECT_EQ(received.payload, "payload");

    client.disconnect();
}

TEST(ClientTest, RequestReply) {
    MockServer server;
    ASSERT_TRUE(server.start());

    relaymq::Client client;
    ASSERT_TRUE(client.connect("127.0.0.1", server.port()));

    std::optional<relaymq::Message> reply = client.request("service.echo", "hello", 1000);
    ASSERT_TRUE(reply.has_value());
    EXPECT_EQ(reply->payload, "reply:hello");

    client.disconnect();
}
