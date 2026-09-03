# Porting feat/0.7.0 → feat/0.7.1 — Doc 04: Object-Store Registry & S3 Client Tuning

> Part of the `docs/dev/port-v0.7.0/` inventory. Covers the `sail-object-store` deltas on
> `feat/0.7.0` vs base `f0b137d6` (commit `c07ad0c8` "temp WIP" plus the big-bang): the
> dynamic object-store registry and the S3 store builder now honor Sail's
> `ObjectStoreConfig`. Config keys are documented in `01-session-runtime-and-config.md`
> §2.2; wiring into the runtime env is in doc 01 §4.3.
>
> Ground truth: `feat/0.7.0` tip `c07ad0c8`.

---

## 1. Files

| File | Change |
|---|---|
| `sail-object-store/src/registry.rs` | `DynamicObjectStoreRegistry` now carries an `ObjectStoreConfig`; `new_with_config()` ctor; free `get_dynamic_object_store(url)` became a method `get_dynamic_object_store(&self, url)` that threads the S3 config into every lazy S3 initializer |
| `sail-object-store/src/s3.rs` | `get_s3_object_store(url, &ObjectStoreConfig)`; new `client_options_from_config`; region resolution honors connect/request timeouts |

---

## 2. `registry.rs`

### 2.1 Constructors

```rust
impl DynamicObjectStoreRegistry {
    /// Legacy ctor for tests / call sites without a config — object_store crate defaults.
    pub fn new(runtime: RuntimeHandle) -> Self;                       // → new_with_config(.., default)

    /// Idiomatic ctor threading Sail's ObjectStoreConfig (S3 only) explicitly.
    pub fn new_with_config(runtime: RuntimeHandle, object_store_config: ObjectStoreConfig) -> Self;
}
```

The struct gains `object_store_config: ObjectStoreConfig`. Both constructors keep seeding the
registry with a `LoggingObjectStore`-wrapped `LocalFileSystem` for the local scheme key. The
only production caller switch is `RuntimeEnvFactory` (doc 01 §4.3) →
`new_with_config(runtime, config.object_store.clone())`.

### 2.2 Lazy store creation becomes a method

`get_dynamic_object_store(url)` (free fn) → `impl DynamicObjectStoreRegistry {
fn get_dynamic_object_store(&self, url) -> object_store::Result<Arc<dyn ObjectStore>> }`. The
dispatch tree is unchanged in structure — scheme switch plus Aliyun-OSS URL detection — but the
**S3** initializers now close over a clone of the config:

- `"oss"` (Aliyun) — `LazyObjectStore::new(move || { async move { get_s3_object_store(&url, &cfg).await } })`
- `is_aliyun_oss_url(url)` branch — same
- `ObjectStoreScheme::AmazonS3` — same

Every store is still wrapped in `LoggingObjectStore` before returning, and scheme parse
failures produce `object_store::Error::Generic { store: "unknown", source: … }` as before.
`get_object_store`'s `or_try_insert_with` now calls `self.get_dynamic_object_store(url)` inside
the `RuntimeAwareObjectStore` initializer (needs `self`, hence the method conversion).

---

## 3. `s3.rs`

### 3.1 `client_options_from_config(cfg: &ObjectStoreConfig) -> ClientOptions` (pub(crate))

```rust
let mut opts = ClientOptions::default()
    .with_connect_timeout(Duration::from_secs(cfg.connect_timeout_secs))      // default 5
    .with_timeout(Duration::from_secs(cfg.request_timeout_secs))              // default 30
    .with_pool_idle_timeout(Duration::from_secs(cfg.pool_idle_timeout_secs))  // default 90
    .with_pool_max_idle_per_host(cfg.pool_max_idle_per_host);                 // 0 = unlimited
if cfg.http2_keep_alive_interval_secs > 0 {
    opts = opts
        .with_http2_keep_alive_interval(Duration::from_secs(cfg.http2_keep_alive_interval_secs))
        .with_http2_keep_alive_timeout(Duration::from_secs(cfg.http2_keep_alive_timeout_secs))
        .with_http2_keep_alive_while_idle();
}
```

`0` sentinels mirror the `object_store` crate defaults (5s/30s/90s/unlimited/disabled).

### 3.2 `get_s3_object_store(url, config)`

- Signature gains `config: &ObjectStoreConfig`.
- `AmazonS3Builder::from_env().with_client_options(client_options_from_config(config))`.
- The credential-provider path (AWS config → `S3CredentialProvider`) is unchanged except the
  local var rename `config` → `aws_config` (avoid clash).
- **Region resolution** when the builder region is empty: previously
  `resolve_bucket_region(bucket, &ClientOptions::default())`; now uses a region-resolution
  client built from the same connect/request timeouts:
  `ClientOptions::default().with_connect_timeout(connect).with_timeout(request)` (only those
  two knobs apply to region lookup).

---

## 4. Port notes / risks

1. Small, low-risk, pure-addition change. Depends only on the `ObjectStoreConfig` type and
   `AppConfig.object_store` field (doc 01 §2.2) — port those first.
2. If 0.7.1's `s3.rs` region-resolution or builder code has drifted (e.g. custom
   endpoint/credential handling merged upstream), merge `client_options_from_config` onto the
   current builder construction; the config contract stays the same.
3. `pool_max_idle_per_host` (0 = unlimited) and the `0 = disabled` HTTP/2 keepalive sentinel
   semantics must be preserved verbatim, since `application.yaml` documents them.
4. The hdfs feature branch, HuggingFace authority check, and Azure/GCS/HTTP lazy stores are
   untouched by this delta — do not port changes there.
