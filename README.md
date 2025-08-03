# What

This contains an incomplete implementation of an [Oxia](https://github.com/oxia-db/oxia) client for Rust.  The project currently exists as a way for me to experiment with Rust as well as explore Oxia.  The crates are not published and use awkward "private" names.  Everything is subject to change, there is minimal testing.  The only reason this is public at this point is to avoid any GitHub ecosystem limitations regarding private repositories.

Grpc support is based upon [tonic](https://github.com/hyperium/tonic).

Support for all of the Oxia service is incomplete,


# Layout

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
    * oxia client cli
    * unimplemented, intended as a test-bed/example for the client crate

# TODO

Create a TODO list

# Copying

See (LICENSE)  

