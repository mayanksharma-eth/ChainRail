//! Postgres codecs for ChainRail's domain newtypes.
//!
//! These live here (behind the `sqlx` feature) rather than in the `database`
//! crate because the orphan rule would otherwise force every query to convert
//! `BigDecimal <-> Amount` and `String <-> Address` by hand. Encoding the
//! mapping once removes that boilerplate from ~every repository function.
//!
//! `Amount` maps to `NUMERIC`; the decode path *rejects* values that do not fit
//! `i128` rather than truncating, so a corrupted or hand-edited row surfaces as
//! an error instead of a wrong balance.

use bigdecimal::BigDecimal;
use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
use sqlx::{Decode, Encode, Postgres, Type};
use std::str::FromStr;

use crate::chain::{Address, ChainId, Hash32};
use crate::money::{Amount, Direction};

impl Type<Postgres> for Amount {
    fn type_info() -> PgTypeInfo {
        <BigDecimal as Type<Postgres>>::type_info()
    }
    fn compatible(ty: &PgTypeInfo) -> bool {
        <BigDecimal as Type<Postgres>>::compatible(ty)
    }
}

impl Encode<'_, Postgres> for Amount {
    fn encode_by_ref(
        &self,
        buf: &mut PgArgumentBuffer,
    ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
        <BigDecimal as Encode<Postgres>>::encode(self.to_bigdecimal(), buf)
    }
}

impl<'r> Decode<'r, Postgres> for Amount {
    fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
        let raw = <BigDecimal as Decode<Postgres>>::decode(value)?;
        Amount::from_bigdecimal(&raw).map_err(|e| Box::new(e) as sqlx::error::BoxDynError)
    }
}

/// Generates a `TEXT`-backed codec for a validated string newtype.
macro_rules! text_codec {
    ($ty:ty, $decode:expr) => {
        impl Type<Postgres> for $ty {
            fn type_info() -> PgTypeInfo {
                <String as Type<Postgres>>::type_info()
            }
            fn compatible(ty: &PgTypeInfo) -> bool {
                <String as Type<Postgres>>::compatible(ty)
            }
        }

        impl Encode<'_, Postgres> for $ty {
            fn encode_by_ref(
                &self,
                buf: &mut PgArgumentBuffer,
            ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
                <&str as Encode<Postgres>>::encode(self.as_str(), buf)
            }
        }

        impl<'r> Decode<'r, Postgres> for $ty {
            fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
                let s = <&str as Decode<Postgres>>::decode(value)?;
                #[allow(clippy::redundant_closure_call)]
                ($decode)(s)
            }
        }
    };
}

// Addresses and hashes are stored already-normalized, so decoding trusts the
// database rather than re-running validation on every row read.
text_codec!(Address, |s: &str| Ok(Address::from_storage(s)));
text_codec!(Hash32, |s: &str| Ok(Hash32::from_storage(s)));
text_codec!(ChainId, |s: &str| ChainId::new(s)
    .map_err(|e| Box::new(e) as sqlx::error::BoxDynError));
text_codec!(Direction, |s: &str| Direction::from_str(s)
    .map_err(|e| Box::new(e) as sqlx::error::BoxDynError));
