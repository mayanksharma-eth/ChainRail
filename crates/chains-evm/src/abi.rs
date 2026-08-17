//! ERC-20 encoding and decoding.
//!
//! Pure functions over bytes and hex strings -- no network, no config -- so
//! every parsing edge case is unit-testable. This is a trust boundary: log data
//! arrives from an RPC provider we do not control, so malformed or hostile input
//! must be rejected rather than misinterpreted.

use chainrail_common::{Address, Amount, Error, Result};

/// `keccak256("Transfer(address,address,uint256)")`
pub const TRANSFER_TOPIC: &str =
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// `keccak256("transfer(address,uint256)")[0..4]`
pub const TRANSFER_SELECTOR: [u8; 4] = [0xa9, 0x05, 0x9c, 0xbb];

/// A decoded ERC-20 `Transfer` event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Erc20Transfer {
    pub from: Address,
    pub to: Address,
    pub value: Amount,
}

/// Decode a `Transfer` event from its topics and data.
///
/// Returns `Ok(None)` when the log is simply not a `Transfer` (the common case
/// while scanning), and `Err` when it *claims* to be one but is malformed --
/// that distinction matters, because silently skipping a malformed transfer
/// would lose a deposit.
pub fn decode_transfer_log(topics: &[String], data: &str) -> Result<Option<Erc20Transfer>> {
    let Some(topic0) = topics.first() else {
        return Ok(None);
    };
    if !topic0.eq_ignore_ascii_case(TRANSFER_TOPIC) {
        return Ok(None);
    }
    // Canonical ERC-20 indexes both address arguments, giving 3 topics. A
    // non-standard token that indexes `value` too, or fewer arguments, is not
    // something we will guess at.
    if topics.len() != 3 {
        return Err(Error::Validation(format!(
            "Transfer log has {} topics, expected 3",
            topics.len()
        )));
    }

    let from = address_from_topic(&topics[1])?;
    let to = address_from_topic(&topics[2])?;
    let value = uint256_from_data(data)?;
    Ok(Some(Erc20Transfer { from, to, value }))
}

/// Extract an address from a 32-byte topic (left-padded with zeros).
fn address_from_topic(topic: &str) -> Result<Address> {
    let body = topic.trim().trim_start_matches("0x");
    if body.len() != 64 || !body.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Validation(
            "address topic must be 32 bytes of hex".into(),
        ));
    }
    // The top 12 bytes must be zero; anything else is not an address topic and
    // treating it as one would silently truncate.
    if body[..24].chars().any(|c| c != '0') {
        return Err(Error::Validation(
            "address topic has non-zero padding".into(),
        ));
    }
    Ok(Address::from_storage(format!("0x{}", &body[24..])))
}

/// Decode the single `uint256` in a `Transfer` log's data field.
fn uint256_from_data(data: &str) -> Result<Amount> {
    let body = data.trim().trim_start_matches("0x");
    if body.is_empty() {
        return Err(Error::Validation("Transfer log has empty data".into()));
    }
    if body.len() != 64 {
        return Err(Error::Validation(format!(
            "Transfer data is {} hex chars, expected 64",
            body.len()
        )));
    }
    if !body.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(Error::Validation("Transfer data is not hex".into()));
    }
    // Rejects values above i128::MAX rather than truncating: see
    // chainrail_common::money for the reasoning.
    Amount::from_hex_quantity(body)
}

/// ABI-encode `transfer(address to, uint256 amount)`.
pub fn encode_transfer_call(to: &Address, amount: Amount) -> Result<Vec<u8>> {
    if !amount.is_positive() {
        return Err(Error::InvalidAmount(
            "ERC-20 transfer amount must be positive".into(),
        ));
    }
    let to_body = to.storage_key();
    let to_body = to_body.trim_start_matches("0x");
    if to_body.len() != 40 {
        return Err(Error::InvalidAddress {
            chain: "evm".into(),
            reason: "destination must be 20 bytes".into(),
        });
    }

    let mut out = Vec::with_capacity(4 + 32 + 32);
    out.extend_from_slice(&TRANSFER_SELECTOR);
    // address: left-padded to 32 bytes
    out.extend_from_slice(&[0u8; 12]);
    out.extend_from_slice(
        &hex::decode(to_body)
            .map_err(|e| Error::Validation(format!("destination is not hex: {e}")))?,
    );
    // uint256: big-endian, left-padded to 32 bytes
    let value = amount.raw() as u128;
    out.extend_from_slice(&[0u8; 16]);
    out.extend_from_slice(&value.to_be_bytes());
    Ok(out)
}

/// Decode a hex quantity (`0x1a`) into a `u64`. Used for block numbers, gas and
/// nonces, all of which are quantities rather than money.
pub fn hex_to_u64(s: &str) -> Result<u64> {
    let body = s.trim().trim_start_matches("0x").trim_start_matches("0X");
    if body.is_empty() {
        return Ok(0);
    }
    let trimmed = body.trim_start_matches('0');
    if trimmed.is_empty() {
        return Ok(0);
    }
    if trimmed.len() > 16 {
        return Err(Error::Validation(format!(
            "hex quantity too wide for u64: {}",
            chainrail_common::chain::truncate_for_log(s)
        )));
    }
    u64::from_str_radix(trimmed, 16).map_err(|_| {
        Error::Validation(format!(
            "malformed hex quantity: {}",
            &body[..body.len().min(20)]
        ))
    })
}

pub fn u64_to_hex(v: u64) -> String {
    format!("0x{v:x}")
}

/// Verify the ERC-20 `transfer` selector at the start of calldata. Used when
/// reconciling a broadcast transaction against what we intended to send.
pub fn is_transfer_calldata(data: &[u8]) -> bool {
    data.len() == 68 && data[..4] == TRANSFER_SELECTOR
}

#[cfg(test)]
mod tests {
    use super::*;
    use chainrail_common::chain::keccak256;
    use chainrail_common::ChainKind;

    fn topic_addr(hex40: &str) -> String {
        format!("0x{}{}", "0".repeat(24), hex40)
    }

    fn data_u256(v: u128) -> String {
        format!("0x{v:064x}")
    }

    #[test]
    fn transfer_topic_matches_the_keccak_of_the_signature() {
        let computed = format!(
            "0x{}",
            hex::encode(keccak256(b"Transfer(address,address,uint256)"))
        );
        assert_eq!(computed, TRANSFER_TOPIC);
    }

    #[test]
    fn transfer_selector_matches_the_keccak_of_the_signature() {
        let full = keccak256(b"transfer(address,uint256)");
        assert_eq!(&full[..4], &TRANSFER_SELECTOR);
    }

    #[test]
    fn decodes_a_well_formed_transfer() {
        let topics = vec![
            TRANSFER_TOPIC.to_string(),
            topic_addr("1111111111111111111111111111111111111111"),
            topic_addr("2222222222222222222222222222222222222222"),
        ];
        let t = decode_transfer_log(&topics, &data_u256(1_000_000))
            .unwrap()
            .expect("should decode");
        assert_eq!(
            t.from.storage_key(),
            "0x1111111111111111111111111111111111111111"
        );
        assert_eq!(
            t.to.storage_key(),
            "0x2222222222222222222222222222222222222222"
        );
        assert_eq!(t.value, Amount::new(1_000_000));
    }

    #[test]
    fn topic_matching_is_case_insensitive() {
        let topics = vec![
            TRANSFER_TOPIC.to_uppercase().replace("0X", "0x"),
            topic_addr("1111111111111111111111111111111111111111"),
            topic_addr("2222222222222222222222222222222222222222"),
        ];
        assert!(decode_transfer_log(&topics, &data_u256(5))
            .unwrap()
            .is_some());
    }

    #[test]
    fn non_transfer_logs_are_skipped_not_errors() {
        // Approval event topic -- a different event entirely.
        let topics = vec![format!(
            "0x{}",
            hex::encode(keccak256(b"Approval(address,address,uint256)"))
        )];
        assert_eq!(decode_transfer_log(&topics, &data_u256(1)).unwrap(), None);
        // An anonymous log with no topics at all.
        assert_eq!(decode_transfer_log(&[], "0x").unwrap(), None);
    }

    #[test]
    fn malformed_transfer_logs_are_errors_not_silent_skips() {
        // A Transfer log we cannot decode must never be quietly dropped: that
        // would lose a user's deposit.
        let two_topics = vec![
            TRANSFER_TOPIC.to_string(),
            topic_addr("1111111111111111111111111111111111111111"),
        ];
        assert!(decode_transfer_log(&two_topics, &data_u256(1)).is_err());

        let good_topics = vec![
            TRANSFER_TOPIC.to_string(),
            topic_addr("1111111111111111111111111111111111111111"),
            topic_addr("2222222222222222222222222222222222222222"),
        ];
        for bad_data in ["0x", "0x01", &format!("0x{}", "f".repeat(63)), "0xzz"] {
            assert!(
                decode_transfer_log(&good_topics, bad_data).is_err(),
                "accepted data {bad_data}"
            );
        }
    }

    #[test]
    fn transfer_values_above_i128_are_rejected() {
        let topics = vec![
            TRANSFER_TOPIC.to_string(),
            topic_addr("1111111111111111111111111111111111111111"),
            topic_addr("2222222222222222222222222222222222222222"),
        ];
        // uint256 max: a hostile or buggy token could emit this.
        let huge = format!("0x{}", "f".repeat(64));
        assert!(decode_transfer_log(&topics, &huge).is_err());
    }

    #[test]
    fn address_topics_with_dirty_padding_are_rejected() {
        let topics = vec![
            TRANSFER_TOPIC.to_string(),
            // Non-zero byte in the padding region: not a valid address topic.
            format!(
                "0x01{}{}",
                "0".repeat(22),
                "1111111111111111111111111111111111111111"
            ),
            topic_addr("2222222222222222222222222222222222222222"),
        ];
        assert!(decode_transfer_log(&topics, &data_u256(1)).is_err());
    }

    #[test]
    fn encodes_transfer_calldata_to_the_canonical_layout() {
        let to =
            Address::parse(ChainKind::Evm, "0x5aAeb6053F3E94C9b9A09f33669435E7Ef1BeAed").unwrap();
        let data = encode_transfer_call(&to, Amount::new(25_000_000)).unwrap();
        assert_eq!(data.len(), 68, "4-byte selector + two 32-byte words");
        assert_eq!(&data[..4], &TRANSFER_SELECTOR);
        assert!(is_transfer_calldata(&data));
        let expected = format!(
            "a9059cbb{}5aaeb6053f3e94c9b9a09f33669435e7ef1beaed{:064x}",
            "0".repeat(24),
            25_000_000u128
        );
        assert_eq!(hex::encode(&data), expected);
    }

    #[test]
    fn calldata_round_trips_through_the_log_decoder() {
        // Encoding then decoding the same amount must be lossless -- this is the
        // property that makes withdrawal/deposit amounts agree.
        let to = Address::from_storage("0x2222222222222222222222222222222222222222");
        for amount in [1i128, 1_000_000, i64::MAX as i128] {
            let data = encode_transfer_call(&to, Amount::new(amount)).unwrap();
            let value_word = format!("0x{}", hex::encode(&data[36..68]));
            let topics = vec![
                TRANSFER_TOPIC.to_string(),
                topic_addr("1111111111111111111111111111111111111111"),
                topic_addr("2222222222222222222222222222222222222222"),
            ];
            let decoded = decode_transfer_log(&topics, &value_word).unwrap().unwrap();
            assert_eq!(decoded.value, Amount::new(amount));
            assert_eq!(decoded.to, to);
        }
    }

    #[test]
    fn non_positive_transfer_amounts_are_refused() {
        let to = Address::from_storage("0x2222222222222222222222222222222222222222");
        assert!(encode_transfer_call(&to, Amount::ZERO).is_err());
        assert!(encode_transfer_call(&to, Amount::new(-1)).is_err());
    }

    #[test]
    fn hex_quantities_parse_and_render() {
        assert_eq!(hex_to_u64("0x0").unwrap(), 0);
        assert_eq!(hex_to_u64("0x").unwrap(), 0);
        assert_eq!(hex_to_u64("0x10").unwrap(), 16);
        assert_eq!(hex_to_u64("0x1a2b3c").unwrap(), 0x1a2b3c);
        assert_eq!(hex_to_u64("1a").unwrap(), 26, "bare hex accepted");
        assert_eq!(hex_to_u64(&format!("0x{}", "0".repeat(40))).unwrap(), 0);
        assert_eq!(hex_to_u64("0xffffffffffffffff").unwrap(), u64::MAX);
        assert_eq!(u64_to_hex(84532), "0x14a34");
        assert_eq!(hex_to_u64(&u64_to_hex(12345)).unwrap(), 12345);
    }

    #[test]
    fn oversized_or_malformed_quantities_are_rejected() {
        assert!(hex_to_u64(&format!("0x{}", "f".repeat(17))).is_err());
        assert!(hex_to_u64("0xnope").is_err());
    }

    #[test]
    fn transfer_calldata_detection_requires_the_exact_length() {
        assert!(!is_transfer_calldata(&TRANSFER_SELECTOR));
        assert!(!is_transfer_calldata(&[]));
        let mut short = TRANSFER_SELECTOR.to_vec();
        short.extend_from_slice(&[0u8; 63]);
        assert!(!is_transfer_calldata(&short));
    }
}
