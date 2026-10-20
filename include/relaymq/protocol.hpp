#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace relaymq {

enum class CommandType {
    UNKNOWN = 0,
    PUB,
    SUB,
    UNSUB,
    MSG,
    PING,
    PONG,
    OK,
    ERR,
    AUTH
};

std::string to_string(CommandType type);

struct Command {
    CommandType type = CommandType::UNKNOWN;
    std::string subject;
    std::string sid;
    std::string reply_to;
    std::string payload;
    std::string error_message;
    std::string auth_token;
};

class ProtocolFormatter {
public:
    static std::string format_pub(std::string_view subject, std::string_view payload, std::string_view reply_to = "");
    static std::string format_sub(std::string_view subject, std::string_view sid);
    static std::string format_unsub(std::string_view sid);
    static std::string format_msg(std::string_view subject, std::string_view sid, std::string_view payload, std::string_view reply_to = "");
    static std::string format_ping();
    static std::string format_pong();
    static std::string format_ok();
    static std::string format_err(std::string_view reason);
    static std::string format_auth(std::string_view token);
};

class ProtocolParser : public ProtocolFormatter {
public:
    ProtocolParser() = default;

    void append(const std::string& data);
    void append(const char* data, size_t length);
    bool next_command(Command& cmd);
    void clear();
    size_t buffered_bytes() const;

private:
    std::string buffer_;
};

}
