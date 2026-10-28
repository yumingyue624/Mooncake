# KVClient backend

Build with `STORE_USE_KVCLIENT=ON`, `KVCLIENT_INCLUDE_DIR`, and
`KVCLIENT_LIBRARY`. Use the existing Store setup configuration:

```json
{
  "external_store_backend": "kvclient",
  "external_store_options.config_path": "/path/to/kvclient.json"
}
```

The backend is initialized before device memory is registered. Put sources must
be registered device memory accessible in the RealClient process, including
imported IPC mappings. Host buffers and paths that stage through Mooncake's
internal host buffer (such as `RealClient::put`) are unsupported by this backend;
use FileStorage for that workload. External submission errors are logged and do
not prevent the native path from running.

Put and Upsert submit external work before the native write, then wait and merge
results per input key: either side succeeding is sufficient. Native replica
finalization and revoke behavior are unchanged. Remove and BatchRemove submit
external deletion regardless of `force`, run native deletion with `force`, and
collect the external task. Their return values remain the native results.
RemoveAll, RemoveByRegex, replica eviction and session writes remain native-only.

## Integration status

- Read/query/write/delete share XXH64(seed=0) over
  `TenantId::MakeScopedKey(key)`, encoded with `XXH64_canonicalFromHash`.
  Keys written by the earlier little-endian prototype are incompatible.
- Store object offsets are temporarily zero for every Slice. Multiple Slices
  targeting the same object therefore overlap; concatenation and TP/layer
  placement are not implemented.
- Wait uses the SDK timeout (`0` selects the SDK default). KVClient must stop all
  buffer access before completion resources or caller buffers are released.
  Timeout/drain behavior, IPC registration, and device/transport combinations
  require SDK and hardware validation; a timeout status alone is not proof of
  transport completion.
