use mauricebarnum_oxia_client::*;
use mauricebarnum_oxia_common::proto as oxia_proto;

fn grpc_to_client(ggr: oxia_proto::GetResponse) -> GetResponse {
    if ggr.status != 0 {
        panic!("oops");
    }
    ggr.into()
}

fn import_read_response(grr: oxia_proto::ReadResponse) -> Vec<Option<GetResponse>> {
    grr.gets
        .into_iter()
        .map(|gr| match gr.status {
            0 => Some(gr.into()),
            _ => None,
        })
        .collect()
}

fn main() {
    let gr = oxia_proto::GetResponse {
        status: 0,
        value: Some("abcd".into()),
        version: None,
        key: None,
        secondary_index_key: None,
    };
    println!("{:#?}", grpc_to_client(gr));
    let read_response = oxia_proto::ReadResponse {
        gets: vec![oxia_proto::GetResponse {
            status: 0,
            value: Some("abcd".into()),
            version: Some(oxia_proto::Version {
                version_id: 13,
                client_identity: Some("localhost".into()),
                ..Default::default()
            }),
            key: None,
            secondary_index_key: None,
        }],
    };
    println!("{read_response:#?}");
    println!("{:#?}", import_read_response(read_response));
}
