# KvClient-backed ASU replicas

Build Mooncake with `USE_KVCLIENT=ON` and an independently built UCM SDK:

```sh
cmake -S . -B build -DUSE_KVCLIENT=ON \
  -DKVCLIENT_INCLUDE_DIR=/path/to/ucm/kv_semantics/include \
  -DKVCLIENT_LIBRARY=/path/to/libkv_client.so
export MOONCAKE_KVCLIENT_CONFIG=/path/to/kvclient.conf
```

Set this environment variable in each Store client process, including remote
workers. All clients accessing an ASU pool must use the same SDK namespace and
compatible routing configuration. The SDK transport/runtime/metrics libraries
must be available to the loader. `Client::Create`, used by Python
`MooncakeDistributedStore` and RealClient, calls `InitKvClient` automatically.
An explicit configuration fails setup if the SDK is unavailable or cannot init.
No vLLM dual-write logic is required.

An application may instead initialize the SDK and call `SetKvClient` before
serving requests. The attachment now owns the SDK's operating lifecycle; do not
share the same instance across independently managed Client attachments, change
attachments while serving, or call SDK Shutdown/Wait on the integration's tasks.
`GetKvClient` is retained for diagnostics; its pointer lifetime is not a license
to shut down an attachment in use.

## Write/read behavior

With ASU enabled, ordinary Put and BatchPut request one ASU replica in addition
to the configured memory replicas. Master allocates a fresh 128-bit generation
and records the logical size. Client stages all object slices (including device
slices) into one aligned host frame and submits the ASU operation before waiting
for the memory writes. Ordinary and same-node batch paths use the same ASU path.

Both memory and ASU must finish successfully before `PutEnd(ALL)` publishes the
object. Any data-transfer failure revokes all pending replicas. A lost finalize
RPC response remains an uncertain outcome, as with the original Store protocol;
this is not a distributed transaction across Master and ASU.

Reads prefer memory. A completed ASU descriptor remains readable after the
memory segment or writer goes away. Any client configured with the same SDK pool
can read it. Master continues to grant read leases and control visibility; ASU
existence does not bypass Master metadata or make revoked objects visible.

The physical 8-byte SDK key is derived from the logical key and Master-issued
generation. The stored frame contains the full key, generation, logical length
and payload CRC, which are checked before copying into the destination. No
unverified truncated-key cache hit is accepted. Removing and recreating a logical
key produces a different generation, so old cleanup cannot target the new object.
A 64-bit physical-key collision is detected by the stored identity; it can cause
a cache IO failure rather than returning another object's bytes.

An individual framed object must fit the SDK's 32-bit IO length (slightly
less than 4 GiB after metadata and padding). Each frame and IO length is
512-byte aligned; padding is not returned as user
data. TE and SDK registrations are independent. Owned buffers are retained until
the task is terminal. Transport-error buffers remain registered/quarantined until
SDK shutdown; failed shutdown retains unsafe buffers instead of freeing memory
that may still be referenced by DMA.

`Remove` atomically obtains the removed ASU generations with metadata removal,
then deletes precisely those physical objects. BatchRemove preserves per-key
results. A storage delete failure is reported after logical removal; unreachable
bytes can remain on ASU if the backend is down or the client crashes during
cleanup. The SDK provides no enumeration/automatic eviction facility, so durable
ASU garbage collection and capacity policy are not introduced by this PR.

## Compatibility boundaries

- Disabled by default; an ordinary build has no SDK dependency.
- Master does not include or link the SDK. It understands the new replica
  descriptor, so **all Store clients and Masters must run this version**;
  mixed-version rolling upgrades are not supported by this feature.
- Immutable whole-object Put/Get and their batch forms are supported.
- Upsert, mutable range writes, read/write sessions, RemoveAll and regex removal
  are rejected when ASU is enabled. They must not silently operate on one tier.
- ASU cannot be combined with NoF or DFS replicas in the same write policy.
- An existing memory-only key is not silently upgraded by an idempotent Put;
  remove/recreate it to obtain the ASU generation. A failed upgrade is reported.

## Validation

```sh
ctest --test-dir build -R '^(client_kvclient_test|kvclient_dual_write_test|serializer_test)$' --output-on-failure
```

The dual-write suite uses a real in-process Master and TCP memory transfer path
with a fake implementing the actual public `kv::KvClient` contract. It checks
write submission, multi-slice layout, both batch paths, failure revocation,
cross-client ASU reads after memory removal, immutable duplicates and generation
changes. The scoped CI workflow also creates clients from an SDK config file and
roundtrips data through the real SDK's CPU FAKE Provider after the writer exits
and memory is unmounted. It then rebuilds with SDK support disabled and runs the
default-mode and snapshot tests. Real ASU hardware validation is a separate
requirement.
