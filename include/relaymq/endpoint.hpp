#pragma once

#include <string>
#include <string_view>
#include <charconv>
#include <cstdint>
#include <cctype>

namespace relaymq {

struct Endpoint {
    std::string host = "127.0.0.1";
    uint16_t port = 9000;

    Endpoint() = default;
    Endpoint(std::string h, uint16_t p) : host(std::move(h)), port(p) {}

    std::string to_string() const {
        return host + ":" + std::to_string(port);
    }

    static bool parse(std::string_view str, Endpoint& endpoint) {
        if (str.empty()) {
            return false;
        }

        size_t colon_pos = str.find(':');
        if (colon_pos != std::string_view::npos) {
            std::string_view host_part = str.substr(0, colon_pos);
            std::string_view port_part = str.substr(colon_pos + 1);

            if (port_part.empty()) {
                return false;
            }

            uint32_t parsed_port = 0;
            std::from_chars_result result = std::from_chars(port_part.data(), port_part.data() + port_part.size(), parsed_port);
            if (result.ec != std::errc() || result.ptr != port_part.data() + port_part.size()) {
                return false;
            }
            if (parsed_port == 0 || parsed_port > 65535) {
                return false;
            }

            endpoint.host = host_part.empty() ? "127.0.0.1" : std::string(host_part);
            endpoint.port = static_cast<uint16_t>(parsed_port);
            return true;
        }

        bool all_digits = true;
        for (size_t i = 0; i < str.size(); i++) {
            if (!std::isdigit(static_cast<unsigned char>(str[i]))) {
                all_digits = false;
                break;
            }
        }

        if (all_digits) {
            uint32_t parsed_port = 0;
            std::from_chars_result result = std::from_chars(str.data(), str.data() + str.size(), parsed_port);
            if (result.ec != std::errc() || result.ptr != str.data() + str.size()) {
                return false;
            }
            if (parsed_port == 0 || parsed_port > 65535) {
                return false;
            }

            endpoint.host = "127.0.0.1";
            endpoint.port = static_cast<uint16_t>(parsed_port);
            return true;
        }

        endpoint.host = std::string(str);
        endpoint.port = 9000;
        return true;
    }
};

}
