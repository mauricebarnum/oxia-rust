#![allow(clippy::derive_partial_eq_without_eq)]

pub mod proto {
    tonic::include_proto!("io.streamnative.oxia.proto");
    pub use oxia_client_client::OxiaClientClient;
}



