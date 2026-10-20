#include "relaymq/protocol.hpp"
#include <charconv>
#include <format>
#include <vector>

namespace relaymq {

std::string to_string(CommandType type) {
    switch (type) {
        case CommandType::PUB: return "PUB";
        case CommandType::SUB: return "SUB";
        case CommandType::UNSUB: return "UNSUB";
        case CommandType::MSG: return "MSG";
        case CommandType::PING: return "PING";
        case CommandType::PONG: return "PONG";
        case CommandType::OK: return "OK";
        case CommandType::ERR: return "ERR";
        case CommandType::AUTH: return "AUTH";
        case CommandType::UNKNOWN:
        default: return "UNKNOWN";
    }
}

std::string ProtocolFormatter::format_pub(std::string_view subject, std::string_view payload, std::string_view reply_to) {
    if (reply_to.empty()) {
        return std::format("PUB {} {}\r\n{}\r\n", subject, payload.size(), payload);
    }
    return std::format("PUB {} {} {}\r\n{}\r\n", subject, reply_to, payload.size(), payload);
}

std::string ProtocolFormatter::format_sub(std::string_view subject, std::string_view sid) {
    return std::format("SUB {} {}\r\n", subject, sid);
}

std::string ProtocolFormatter::format_unsub(std::string_view sid) {
    return std::format("UNSUB {}\r\n", sid);
}

std::string ProtocolFormatter::format_msg(std::string_view subject, std::string_view sid, std::string_view payload, std::string_view reply_to) {
    if (reply_to.empty()) {
        return std::format("MSG {} {} {}\r\n{}\r\n", subject, sid, payload.size(), payload);
    }
    return std::format("MSG {} {} {} {}\r\n{}\r\n", subject, sid, reply_to, payload.size(), payload);
}

std::string ProtocolFormatter::format_ping() {
    return "PING\r\n";
}

std::string ProtocolFormatter::format_pong() {
    return "PONG\r\n";
}

std::string ProtocolFormatter::format_ok() {
    return "OK\r\n";
}

std::string ProtocolFormatter::format_err(std::string_view reason) {
    return std::format("ERR {}\r\n", reason);
}

std::string ProtocolFormatter::format_auth(std::string_view token) {
    return std::format("AUTH {}\r\n", token);
}

void ProtocolParser::append(const std::string& data) {
    buffer_.append(data);
}

void ProtocolParser::append(const char* data, size_t length) {
    buffer_.append(data, length);
}

void ProtocolParser::clear() {
    buffer_.clear();
}

size_t ProtocolParser::buffered_bytes() const {
    return buffer_.size();
}

static std::vector<std::string_view> split_tokens(std::string_view line) {
    std::vector<std::string_view> tokens;
    size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) {
            i++;
        }
        if (i >= line.size()) {
            break;
        }
        size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') {
            i++;
        }
        tokens.push_back(line.substr(start, i - start));
    }
    return tokens;
}

bool ProtocolParser::next_command(Command& cmd) {
    size_t line_end = buffer_.find("\r\n");
    if (line_end == std::string::npos) {
        return false;
    }

    std::string_view line(buffer_.data(), line_end);
    std::vector<std::string_view> tokens = split_tokens(line);
    if (tokens.empty()) {
        buffer_.erase(0, line_end + 2);
        return false;
    }

    std::string_view action_name = tokens[0];

    if (action_name == "PING") {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::PING;
        return true;
    }
    if (action_name == "PONG") {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::PONG;
        return true;
    }
    if (action_name == "OK") {
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::OK;
        return true;
    }
    if (action_name == "ERR") {
        std::string reason;
        if (line.size() > 4) {
            reason = std::string(line.substr(4));
        }
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::ERR;
        cmd.error_message = std::move(reason);
        return true;
    }
    if (action_name == "AUTH") {
        if (tokens.size() < 2) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid AUTH syntax: expected AUTH <token>";
            return true;
        }
        std::string token(tokens[1]);
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::AUTH;
        cmd.auth_token = std::move(token);
        return true;
    }
    if (action_name == "SUB") {
        if (tokens.size() < 3) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid SUB syntax: expected SUB <subject> <sid>";
            return true;
        }
        std::string sub_subject(tokens[1]);
        std::string sub_sid(tokens[2]);
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::SUB;
        cmd.subject = std::move(sub_subject);
        cmd.sid = std::move(sub_sid);
        return true;
    }
    if (action_name == "UNSUB") {
        if (tokens.size() < 2) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid UNSUB syntax: expected UNSUB <sid>";
            return true;
        }
        std::string unsub_sid(tokens[1]);
        buffer_.erase(0, line_end + 2);
        cmd = Command{};
        cmd.type = CommandType::UNSUB;
        cmd.sid = std::move(unsub_sid);
        return true;
    }
    if (action_name == "PUB") {
        if (tokens.size() < 3) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid PUB syntax: expected PUB <subject> [reply-to] <bytes>";
            return true;
        }
        std::string_view subject = tokens[1];
        std::string_view reply_to;
        size_t payload_len = 0;

        if (tokens.size() == 3) {
            std::from_chars_result result = std::from_chars(tokens[2].data(), tokens[2].data() + tokens[2].size(), payload_len);
            if (result.ec != std::errc() || result.ptr != tokens[2].data() + tokens[2].size()) {
                buffer_.erase(0, line_end + 2);
                cmd = Command{};
                cmd.type = CommandType::ERR;
                cmd.error_message = "Invalid byte length in PUB command";
                return true;
            }
        } else {
            reply_to = tokens[2];
            std::from_chars_result result = std::from_chars(tokens[3].data(), tokens[3].data() + tokens[3].size(), payload_len);
            if (result.ec != std::errc() || result.ptr != tokens[3].data() + tokens[3].size()) {
                buffer_.erase(0, line_end + 2);
                cmd = Command{};
                cmd.type = CommandType::ERR;
                cmd.error_message = "Invalid byte length in PUB command";
                return true;
            }
        }

        size_t total_required = line_end + payload_len + 4;
        if (buffer_.size() < total_required) {
            return false;
        }

        std::string s_subject(subject);
        std::string s_reply_to(reply_to);
        std::string payload = buffer_.substr(line_end + 2, payload_len);
        buffer_.erase(0, total_required);

        cmd = Command{};
        cmd.type = CommandType::PUB;
        cmd.subject = std::move(s_subject);
        cmd.reply_to = std::move(s_reply_to);
        cmd.payload = std::move(payload);
        return true;
    }
    if (action_name == "MSG") {
        if (tokens.size() < 4) {
            buffer_.erase(0, line_end + 2);
            cmd = Command{};
            cmd.type = CommandType::ERR;
            cmd.error_message = "Invalid MSG syntax: expected MSG <subject> <sid> [reply-to] <bytes>";
            return true;
        }
        std::string_view subject = tokens[1];
        std::string_view sid = tokens[2];
        std::string_view reply_to;
        size_t payload_len = 0;

        if (tokens.size() == 4) {
            std::from_chars_result result = std::from_chars(tokens[3].data(), tokens[3].data() + tokens[3].size(), payload_len);
            if (result.ec != std::errc() || result.ptr != tokens[3].data() + tokens[3].size()) {
                buffer_.erase(0, line_end + 2);
                cmd = Command{};
                cmd.type = CommandType::ERR;
                cmd.error_message = "Invalid byte length in MSG command";
                return true;
            }
        } else {
            reply_to = tokens[3];
            std::from_chars_result result = std::from_chars(tokens[4].data(), tokens[4].data() + tokens[4].size(), payload_len);
            if (result.ec != std::errc() || result.ptr != tokens[4].data() + tokens[4].size()) {
                buffer_.erase(0, line_end + 2);
                cmd = Command{};
                cmd.type = CommandType::ERR;
                cmd.error_message = "Invalid byte length in MSG command";
                return true;
            }
        }

        size_t total_required = line_end + payload_len + 4;
        if (buffer_.size() < total_required) {
            return false;
        }

        std::string s_subject(subject);
        std::string s_sid(sid);
        std::string s_reply_to(reply_to);
        std::string payload = buffer_.substr(line_end + 2, payload_len);
        buffer_.erase(0, total_required);

        cmd = Command{};
        cmd.type = CommandType::MSG;
        cmd.subject = std::move(s_subject);
        cmd.sid = std::move(s_sid);
        cmd.reply_to = std::move(s_reply_to);
        cmd.payload = std::move(payload);
        return true;
    }

    buffer_.erase(0, line_end + 2);
    cmd = Command{};
    cmd.type = CommandType::UNKNOWN;
    return true;
}

}
