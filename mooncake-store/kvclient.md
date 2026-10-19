# Native KvClient access from Client

`mooncake::Client` can own a UCM `kv::KvClient` for direct access to ASU.
It uses the SDK's existing key, buffer, asynchronous task and result types.
There is no second transport abstraction or implicit string-key hashing.

## Build and initialize

Build the UCM KvClient SDK separately, then configure Mooncake:

```sh
cmake -S . -B build -DUSE_KVCLIENT=ON \
  -DKVCLIENT_INCLUDE_DIR=/path/to/ucm/kv_semantics/include \
  -DKVCLIENT_LIBRARY=/path/to/libkv_client.so
```

The matching `libkv_transport`, runtime and metrics libraries must also be
available to the loader. KvClient provider and hardware requirements belong
to the SDK build, not the default Mooncake build.

Given an existing `mooncake::Client`:

```cpp
#include <kv_client.h>
#include "client_service.h"

// client is an already created mooncake::Client.
auto initialized = client.InitKvClient("/path/to/kvclient.conf");
if (!initialized) {
    // Handle the Mooncake initialization error; the SDK diagnostic is logged.
    return;
}
auto sdk = client.GetKvClient();
kv::TaskId task = kv::kInvalidTaskId;
kv::CacheKey key{};  // Use a key allocated by the application's key scheme.
key[0] = std::byte{1};
auto submitted = sdk->QueryAsync({key}, task);
if (submitted.ok()) {
    kv::TaskResult result;
    auto waited = sdk->Wait(task, 1000, result);
    // Inspect waited, result.status, result.entryStatus and queryResult.
}
```

Alternatively, initialize the SDK in the application and attach it with
`client.SetKvClient(sdk)`. This injection path needs no `USE_KVCLIENT` build:
the application includes and links the SDK; Mooncake only stores a shared
pointer to its forward-declared type. The build option enables the convenience
factory `InitKvClient`; it is off by default and returns `NOT_SUPPORTED` then.

`GetKvClient()` returns an owning snapshot. Retain it while using the SDK,
including queued/in-flight tasks. Detaching with `SetKvClient(nullptr)` or
replacing the attachment does not explicitly shut down the old instance.
Coordinate Shutdown with all users, drain operations, and unregister their
memory first. A Wait timeout does not prove the IO has stopped and must not
trigger early buffer release. SDK registrations are separate from TE ones.

## Scope

This PR provides native SDK access, not automatic dual writes. Ordinary
Put/Get, Master metadata, eviction and object deletion retain their existing
behavior. ASU data is addressed explicitly through the SDK. Mapping Mooncake
string/tenant/object-generation keys to 8-byte SDK keys, buffer preparation,
dual-write success policy and ASU read fallback need a separate agreed design.
No persistence or durability guarantee is added by attaching a KvClient.

## Tests

With `BUILD_UNIT_TESTS=ON`, run:

```sh
ctest --test-dir build -R '^client_kvclient_test$' --output-on-failure
```

The disabled build checks default behavior and `NOT_SUPPORTED`. The enabled
build additionally uses real SDK factory instances to check native status
propagation, snapshot ownership and failed initialization. These tests require
no ASU connection; they do not validate hardware IO or a successful SDK Init.
