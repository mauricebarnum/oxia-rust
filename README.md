# What

This contains an incomplete implementation of an [Oxia](https://github.com/oxia-db/oxia) client for Rust.  The project currently exists as a way for me to experiment with Rust as well as explore Oxia.  The crates are not published and use awkward "private" names.  Everything is subject to change, there is minimal testing.  The only reason this is public at this point is to avoid any GitHub ecosystem limitations regarding private repositories.

Grpc support is based upon [tonic](https://github.com/hyperium/tonic).

Support for all of the Oxia service is incomplete.

# Implementation notes

## Layout

* oxia-bin-util - testing support
  * internal crate
  * build an oxia server for running tests
  * provide a compile-time path to the built binary
  * Go sources are vendored from upstream
* common - low-level client
  * crate: mauricebarnum-oxia-common
  * provide grpc binding to the [OxiaClient](crates/common/proto/client.proto) service, using tonic
* client - higher-level client
  * crate: mauricebarnum-oxia-client
  * implements sharding, dispatch, etc. intended to be more ergonomic than the raw grpc binding
  * minimal exposure to tonic api
  * async rt not exposed
* cmd
  * crate: mauricebarnum-oxia-cmd
  * oxia client cli: oxia-cmd
  * implemented commands: get, put, delete,
list (keys), range scan, range delete, notifications
  * not yet: batch write, write secondary index
 
## Trait object versus concrete interface

`Client` is a concrete type, not a trait object, so API calls are direct.  They are also generic:

```rust
    pub async fn put(
        &self,
        key: impl Into<String>,
        value: impl Into<Bytes>,
    ) -> Result<PutResponse> {
        self.put_with_options(key, value, PutOptions::default())
            .await
    }
```

This allows the caller more flexibility on the types used for "string" and "bytes" paramters, and permits the implementation freedom with resource management.  In the example above, both the `key` and `value` content will need to be owned so it can be moved into a GRPC request.

Direct calls are more efficient, but given that the client is primarily a GRPC façade, the overhead is minor.

The `x / x_with_options` API pattern is (or can be) zero cost abstractions as direct calls where using a trait object would add one, maybe two, extra function calls.

Downsides:
   * No ABI:  any changes to `Client` will require recompiling callers.  In the Rust ecosystem with poor support for ABI contracts, this doesn't seem to be a major concern outside of compile time.
   * "Plugin" model is the user's problem:  in the case where an application would like to switch implementations, such as a mock client or one with custom instrumentation, the burden is on the caller to implement the necessary wrappers.

Switching to a trait interface should be doable with minimal changes to both the implementation and the callers.  In the "this is an experiment" nature of the current code base, it made more sense to explore the generic decision space ("impl Into<T>"? "impl AsRef<T>"? concrete param? etc).  I'm considering putting in a test that will validate at compile time that the `Client` interface remains close to object safe.  I haven't done it yet because I didn't want to deal with the maintenance overhead of the test, but the API is stable enough now that it shouldn't be a problem going forward.  Maybe an LLM can generate it for me without making too much of a mess.


  
# TODO

1. Add robustness for transient errors
   1. don't fail assignment processing if unable to connect to a shard
   1. reconnect client when remote fails
1. Command-client client
    1. pretty output, configurable formats: JSON, raw, tabular
    1. additional commands:
        1. `shell`: add tab completion
        1. create, stop, list
        1. `put`: support secondary index
1. Notifications: implement optional reconnect to shards when streams close
1. instrument client library
1. collect metrics
1. add more tracing
1. Fix flaky test:

```
        Sep  9 17:11:08.874154 INF Starting Oxia standalone config={"AuthOptions":{"ProviderName":"","ProviderParams":""},"DataDir":"/var/folders/c_/ffflkqfj25d590trtpmzss0w0000gn/T/.tmpyXa5uj/db","DbBlockCacheMB":100,"InternalServerTLS":null,"InternalServiceAddr":"","MetricsServiceAddr":"127.0.0.1:57803","NotificationsEnabled":true,"NotificationsRetentionTime":3600000000000,"NumShards":1,"PeerTLS":null,"PublicServiceAddr":"127.0.0.1:57802","ServerTLS":null,"WalDir":"/var/folders/c_/ffflkqfj25d590trtpmzss0w0000gn/T/.tmpyXa5uj/wal","WalRetentionTime":3600000000000,"WalSyncData":true}
        Sep  9 17:11:09.372626 INF Created leader controller component=leader-controller namespace=default shard=0 term=-1
        Sep  9 17:11:09.441553 INF Leader successfully initialized in new term component=leader-controller last-entry={"offset":"-1","term":"-1"} namespace=default shard=0 term=0
        Sep  9 17:11:09.441688 INF Applying all pending entries to database commit-offset=-1 component=leader-controller head-offset=-1 namespace=default shard=0 term=0
        Sep  9 17:11:09.441773 INF All sessions component=session-manager count=0 namespace=default shard=0 term=0
        Sep  9 17:11:09.441806 INF Started leading the shard component=leader-controller head-offset=-1 namespace=default shard=0 term=0
        Sep  9 17:11:09.442084 INF Started Grpc server bindAddress=127.0.0.1:57802 grpc-server=public
        Sep  9 17:11:09.442274 INF Update shares assignments. component=shard-assignment-dispatcher current={"namespaces":{"default":{"assignments":[{"int32HashRange":{"maxHashInclusive":4294967295,"minHashInclusive":0},"leader":"","shard":"0"}],"shardKeyRouter":"XXHASH3"}}} previous={"namespaces":{}}
        Sep  9 17:11:09.442350 INF Serving Prometheus metrics at http://localhost:57803/metrics
        2025-09-10T00:11:09.487131Z  WARN mauricebarnum_oxia_client::pool: ChannelPool::get() send completion failed err=SendError(Ok(Channel))
        2025-09-10T00:11:09.487622Z  INFO mauricebarnum_oxia_client::shard: processing assignments a=ShardAssignment { shard: 0, leader: "127.0.0.1:57802", shard_boundaries: Some(Int32HashRange(Int32HashRange { min_hash_inclusive: 0, max_hash_inclusive: 4294967295 })) }
        2025-09-10T00:11:09.497491Z  INFO basic: restarting test server
        Sep  9 17:11:09.511981 INF Starting Oxia standalone config={"AuthOptions":{"ProviderName":"","ProviderParams":""},"DataDir":"/var/folders/c_/ffflkqfj25d590trtpmzss0w0000gn/T/.tmpyXa5uj/db","DbBlockCacheMB":100,"InternalServerTLS":null,"InternalServiceAddr":"","MetricsServiceAddr":"127.0.0.1:57803","NotificationsEnabled":true,"NotificationsRetentionTime":3600000000000,"NumShards":1,"PeerTLS":null,"PublicServiceAddr":"127.0.0.1:57802","ServerTLS":null,"WalDir":"/var/folders/c_/ffflkqfj25d590trtpmzss0w0000gn/T/.tmpyXa5uj/wal","WalRetentionTime":3600000000000,"WalSyncData":true}
        Sep  9 17:11:09.515032 INF [JOB 1] WAL file /var/folders/c_/ffflkqfj25d590trtpmzss0w0000gn/T/.tmpyXa5uj/db/default/shard-0/000002.log with log number 000002 stopped reading at offset: 0; replayed 0 keys in 0 batches component=pebble shard=0
        Sep  9 17:11:09.599957 INF Created leader controller component=leader-controller namespace=default shard=0 term=0
        Sep  9 17:11:09.647657 INF Leader successfully initialized in new term component=leader-controller last-entry={"offset":"0","term":"0"} namespace=default shard=0 term=1
        Sep  9 17:11:09.647760 INF Applying all pending entries to database commit-offset=-1 component=leader-controller head-offset=0 namespace=default shard=0 term=1
        Sep  9 17:11:09.648026 INF All sessions component=session-manager count=0 namespace=default shard=0 term=1
        Sep  9 17:11:09.648041 INF Started leading the shard component=leader-controller head-offset=0 namespace=default shard=0 term=1
        Sep  9 17:11:09.648351 INF Started Grpc server bindAddress=127.0.0.1:57802 grpc-server=public
        Sep  9 17:11:09.648492 INF Update shares assignments. component=shard-assignment-dispatcher current={"namespaces":{"default":{"assignments":[{"int32HashRange":{"maxHashInclusive":4294967295,"minHashInclusive":0},"leader":"","shard":"0"}],"shardKeyRouter":"XXHASH3"}}} previous={"namespaces":{}}
        Sep  9 17:11:09.648575 INF Serving Prometheus metrics at http://localhost:57803/metrics
        2025-09-10T00:11:09.800253Z ERROR basic::common: Operation failed target="basic" file="client/tests/basic.rs" line=110 e=RequestTimeout { source: Elapsed(()) }
        test test_disconnect ... FAILED

        failures:

        failures:
            test_disconnect

        test result: FAILED. 0 passed; 1 failed; 0 ignored; 0 measured; 3 filtered out; finished in 1.04s

    stderr ───
        Error: Request time out

        Caused by:
            deadline has elapsed

```

# Copying

See (LICENSE)  
