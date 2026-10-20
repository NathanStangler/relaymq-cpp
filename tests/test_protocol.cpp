#include "relaymq/protocol.hpp"
#include <gtest/gtest.h>

TEST(ProtocolParserTest, ControlCommands) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("PING\r\nPONG\r\nOK\r\nERR invalid subject\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PING);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PONG);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::OK);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::ERR);
    EXPECT_EQ(cmd.error_message, "invalid subject");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, SubscribeAndUnsubscribe) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("SUB service.message.value 101\r\nUNSUB 101\r\n");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::SUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.sid, "101");

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::UNSUB);
    EXPECT_EQ(cmd.sid, "101");

    EXPECT_FALSE(parser.next_command(cmd));
}

TEST(ProtocolParserTest, PublishPayload) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string wire = relaymq::ProtocolParser::format_pub("service.message.value", "amount=50");
    parser.append(wire);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.payload, "amount=50");
    EXPECT_TRUE(cmd.reply_to.empty());
}

TEST(ProtocolParserTest, PublishWithReplyTo) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string pub = relaymq::ProtocolParser::format_pub("service.message.value", "amount=50", "box.user_1");
    parser.append(pub);

    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.value");
    EXPECT_EQ(cmd.reply_to, "box.user_1");
    EXPECT_EQ(cmd.payload, "amount=50");
}

TEST(ProtocolParserTest, StreamFragmentation) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    parser.append("PUB service.message.new 1");
    EXPECT_FALSE(parser.next_command(cmd));

    parser.append("4\r\n{\"value\":\"");
    EXPECT_FALSE(parser.next_command(cmd));

    parser.append("50\"}\r\n");
    ASSERT_TRUE(parser.next_command(cmd));

    EXPECT_EQ(cmd.type, relaymq::CommandType::PUB);
    EXPECT_EQ(cmd.subject, "service.message.new");
    EXPECT_EQ(cmd.payload, "{\"value\":\"50\"}");

    EXPECT_EQ(parser.buffered_bytes(), 0);
}

TEST(ProtocolParserTest, Serializers) {
    EXPECT_EQ(relaymq::ProtocolParser::format_ping(), "PING\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_pong(), "PONG\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_ok(), "OK\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_err("unknown topic"), "ERR unknown topic\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_sub("service.counts.*", "1"), "SUB service.counts.* 1\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_unsub("1"), "UNSUB 1\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_pub("service.message", "abc"), "PUB service.message 3\r\nabc\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_msg("service.message", "1", "abc"), "MSG service.message 1 3\r\nabc\r\n");
    EXPECT_EQ(relaymq::ProtocolParser::format_auth("token_123"), "AUTH token_123\r\n");
}

TEST(ProtocolParserTest, AuthCommands) {
    relaymq::ProtocolParser parser;
    relaymq::Command cmd;

    std::string msg = relaymq::ProtocolParser::format_auth("secret_token_123");
    EXPECT_EQ(msg, "AUTH secret_token_123\r\n");

    parser.append(msg);
    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::AUTH);
    EXPECT_EQ(cmd.auth_token, "secret_token_123");

    parser.append("AUTH\r\n");
    ASSERT_TRUE(parser.next_command(cmd));
    EXPECT_EQ(cmd.type, relaymq::CommandType::ERR);
    EXPECT_EQ(cmd.error_message, "Invalid AUTH syntax: expected AUTH <token>");
}
