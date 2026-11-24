// Copyright 2026 Maurice S. Barnum
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use clap::ValueEnum;
use clap::builder::EnumValueParser;
use clap::builder::TypedValueParser;
#[cfg(feature = "_compression_enabled")]
use tracing::warn;

#[cfg(feature = "_compression_enabled")]
use crate::client_lib::config::CompressionCodec;
#[cfg(feature = "_compression_enabled")]
use crate::client_lib::config::CompressionCodecSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum ArgValue {
    None,
    Auto,
    Deflate,
    Gzip,
    Zstd,
}

impl ArgValue {
    #[inline]
    pub const fn supported(self) -> bool {
        match self {
            Self::Auto | Self::None => true,
            Self::Deflate => cfg!(feature = "deflate"),
            Self::Gzip => cfg!(feature = "gzip"),
            Self::Zstd => cfg!(feature = "zstd"),
        }
    }

    #[cfg(feature = "_compression_enabled")]
    #[inline]
    pub const fn to_codec(self) -> Option<CompressionCodec> {
        match self {
            Self::None | Self::Auto => None,
            Self::Deflate => {
                cfg_select! {
                    feature = "deflate" => Some(CompressionCodec::Deflate),
                    _ => None,
                }
            }
            Self::Gzip => {
                cfg_select! {
                    feature = "gzip" => Some(CompressionCodec::Gzip),
                    _ => None,
                }
            }
            Self::Zstd => {
                cfg_select! {
                    feature = "zstd" => Some(CompressionCodec::Zstd),
                    _ => None,
                }
            }
        }
    }

    #[cfg(feature = "_compression_enabled")]
    pub fn to_codec_set(values: &[Self]) -> CompressionCodecSet {
        let mut result = CompressionCodecSet::EMPTY;
        for &v in values {
            match v {
                Self::None => result = CompressionCodecSet::EMPTY,
                Self::Auto => result = CompressionCodecSet::ALL,
                other => {
                    if let Some(c) = other.to_codec() {
                        result.insert(c);
                    } else {
                        warn!(?other, "compression feature not enabled");
                    }
                }
            }
        }
        result
    }
}

pub fn argvalue_parser() -> impl TypedValueParser<Value = ArgValue> {
    EnumValueParser::<ArgValue>::new().try_map(|v| {
        if v.supported() {
            Ok(v)
        } else {
            Err("not implemented in this build")
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn supported_matches_enabled_features() {
        assert!(ArgValue::None.supported());
        assert!(ArgValue::Auto.supported());
        assert_eq!(ArgValue::Deflate.supported(), cfg!(feature = "deflate"));
        assert_eq!(ArgValue::Gzip.supported(), cfg!(feature = "gzip"));
        assert_eq!(ArgValue::Zstd.supported(), cfg!(feature = "zstd"));
    }

    #[cfg(feature = "_compression_enabled")]
    #[test]
    fn to_codec_maps_enabled_variants() {
        assert_eq!(ArgValue::None.to_codec(), None);
        assert_eq!(ArgValue::Auto.to_codec(), None);

        assert_eq!(
            ArgValue::Deflate.to_codec().is_some(),
            cfg!(feature = "deflate")
        );
        assert_eq!(ArgValue::Gzip.to_codec().is_some(), cfg!(feature = "gzip"));
        assert_eq!(ArgValue::Zstd.to_codec().is_some(), cfg!(feature = "zstd"));

        #[cfg(feature = "deflate")]
        assert_eq!(
            ArgValue::Deflate.to_codec(),
            Some(CompressionCodec::Deflate)
        );
        #[cfg(feature = "gzip")]
        assert_eq!(ArgValue::Gzip.to_codec(), Some(CompressionCodec::Gzip));
        #[cfg(feature = "zstd")]
        assert_eq!(ArgValue::Zstd.to_codec(), Some(CompressionCodec::Zstd));
    }

    #[cfg(feature = "_compression_enabled")]
    #[test]
    fn to_codec_set_handles_auto_none_and_explicit_codecs() {
        assert_eq!(ArgValue::to_codec_set(&[]), CompressionCodecSet::EMPTY);
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::Auto]),
            CompressionCodecSet::ALL
        );
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::None]),
            CompressionCodecSet::EMPTY
        );
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::Deflate, ArgValue::Gzip, ArgValue::Zstd]),
            CompressionCodecSet::ALL
        );
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::Auto, ArgValue::None]),
            CompressionCodecSet::EMPTY
        );
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::None, ArgValue::Auto]),
            CompressionCodecSet::ALL
        );

        #[cfg(feature = "gzip")]
        {
            assert_eq!(
                ArgValue::to_codec_set(&[ArgValue::Gzip, ArgValue::None]),
                CompressionCodecSet::EMPTY
            );
            assert_eq!(
                ArgValue::to_codec_set(&[ArgValue::None, ArgValue::Gzip]),
                CompressionCodecSet::from(CompressionCodec::Gzip)
            );
            assert_eq!(
                ArgValue::to_codec_set(&[ArgValue::Gzip, ArgValue::Auto]),
                CompressionCodecSet::ALL
            );
            assert_eq!(
                ArgValue::to_codec_set(&[ArgValue::Auto, ArgValue::Gzip]),
                CompressionCodecSet::ALL
            );
        }

        #[cfg(all(feature = "deflate", feature = "zstd"))]
        assert_eq!(
            ArgValue::to_codec_set(&[ArgValue::Deflate, ArgValue::Zstd]),
            CompressionCodecSet::from([CompressionCodec::Deflate, CompressionCodec::Zstd])
        );

        for disabled in [ArgValue::Deflate, ArgValue::Gzip, ArgValue::Zstd]
            .into_iter()
            .filter(|v| !v.supported())
        {
            assert_eq!(
                ArgValue::to_codec_set(&[disabled]),
                CompressionCodecSet::EMPTY
            );
            #[cfg(feature = "gzip")]
            assert_eq!(
                ArgValue::to_codec_set(&[disabled, ArgValue::Gzip]),
                CompressionCodecSet::from(CompressionCodec::Gzip)
            );
        }
    }
}
