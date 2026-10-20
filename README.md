# relaymq-cpp

`relaymq-cpp` is the C++ client SDK for RelayMQ.

## Build

Requires GCC 14+ and CMake 3.25+:

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j
```

## Run Tests

```bash
ctest --test-dir build --output-on-failure
```

## Usage

```cpp
#include "relaymq/client.hpp"
#include <iostream>

int main() {
    relaymq::Client client;
    if (!client.connect("127.0.0.1", 9000)) {
        std::cerr << "Failed to connect: " << client.last_error() << "\n";
        return 1;
    }

    client.subscribe("service.events.>", [](const relaymq::Message& msg) {
        std::cout << "[" << msg.subject << "] " << msg.payload << "\n";
    });

    client.publish("service.events.created", "status=active");

    client.disconnect();
    return 0;
}
```
