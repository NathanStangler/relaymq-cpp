#include "relaymq/endpoint.hpp"
#include <gtest/gtest.h>

TEST(EndpointTest, ParseHostPort) {
    relaymq::Endpoint endpoint;
    EXPECT_TRUE(relaymq::Endpoint::parse("127.0.0.1:9000", endpoint));
    EXPECT_EQ(endpoint.host, "127.0.0.1");
    EXPECT_EQ(endpoint.port, 9000);
    EXPECT_EQ(endpoint.to_string(), "127.0.0.1:9000");

    EXPECT_TRUE(relaymq::Endpoint::parse("service.internal:9001", endpoint));
    EXPECT_EQ(endpoint.host, "service.internal");
    EXPECT_EQ(endpoint.port, 9001);
}

TEST(EndpointTest, ParsePort) {
    relaymq::Endpoint endpoint;
    EXPECT_TRUE(relaymq::Endpoint::parse("9000", endpoint));
    EXPECT_EQ(endpoint.host, "127.0.0.1");
    EXPECT_EQ(endpoint.port, 9000);

    EXPECT_TRUE(relaymq::Endpoint::parse(":9001", endpoint));
    EXPECT_EQ(endpoint.host, "127.0.0.1");
    EXPECT_EQ(endpoint.port, 9001);
}

TEST(EndpointTest, ParseHost) {
    relaymq::Endpoint endpoint;
    EXPECT_TRUE(relaymq::Endpoint::parse("127.0.0.1", endpoint));
    EXPECT_EQ(endpoint.host, "127.0.0.1");
    EXPECT_EQ(endpoint.port, 9000);
}

TEST(EndpointTest, ParseInvalid) {
    relaymq::Endpoint endpoint;
    EXPECT_FALSE(relaymq::Endpoint::parse("", endpoint));
    EXPECT_FALSE(relaymq::Endpoint::parse("127.0.0.1:0", endpoint));
    EXPECT_FALSE(relaymq::Endpoint::parse("127.0.0.1:70000", endpoint));
    EXPECT_FALSE(relaymq::Endpoint::parse("127.0.0.1:abc", endpoint));
    EXPECT_FALSE(relaymq::Endpoint::parse("127.0.0.1:", endpoint));
}
