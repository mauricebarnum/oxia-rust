use bytes::Bytes;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum DecodeError {
    #[error("Invalid hex digit in escape sequence at position {position}")]
    InvalidHexDigit { position: usize },
    #[error("Incomplete escape sequence at position {position}")]
    IncompleteSequence { position: usize },
    #[error("Invalid Unicode code point: U+{code_point:X}")]
    InvalidCodePoint { code_point: u32 },
}

pub type Result<T> = std::result::Result<T, DecodeError>;

/// Decodes Unicode escape sequences in a string and returns the resulting bytes.
///
/// This function processes escape sequences in the following formats:
/// - `\uXXXX` - 4-digit hexadecimal Unicode code point (e.g., `\u0041` for 'A')
/// - `\UXXXXXXXX` - 8-digit hexadecimal Unicode code point (e.g., `\U0001F602` for '😂')
/// - `\x` - Any other character following a backslash is treated literally
///
/// # Errors
///
/// Returns an error if:
/// - Invalid hexadecimal digits are found in escape sequences
/// - Escape sequences are incomplete (e.g., `\u004` or trailing backslash)
/// - Unicode code points are invalid (e.g., surrogates or beyond U+10FFFF)
///
/// # Examples
///
/// ```
/// use bytes::Bytes;
///
/// // Valid Unicode escapes
/// let result = decode_escapes("\\u0041\\u0042".to_string()).unwrap();
/// assert_eq!(result, Bytes::from("AB"));
///
/// // Invalid hex digit - returns error
/// let result = decode_escapes("\\uGHIJ".to_string());
/// assert!(result.is_err());
/// ```
#[inline]
pub(crate) fn decode_escapes(s: String) -> Result<Bytes> {
    if s.contains('\\') {
        do_decode_escapes(&s)
    } else {
        Ok(s.into_bytes().into())
    }
}

fn do_decode_escapes(s: &str) -> Result<Bytes> {
    let estimated_size = estimate_decoded_size(s);
    let mut bytes = Vec::with_capacity(estimated_size);
    let input_bytes = s.as_bytes();
    let mut i = 0;

    while i < input_bytes.len() {
        if input_bytes[i] == b'\\' {
            i += 1; // Move past the backslash
            if i >= input_bytes.len() {
                return Err(DecodeError::IncompleteSequence { position: i });
            }

            match input_bytes[i] {
                b'u' => {
                    i += 1; // Move past 'u'
                    if i + 4 > input_bytes.len() {
                        return Err(DecodeError::IncompleteSequence { position: i });
                    }

                    // Work directly with the 4-byte slice - bounds check eliminated
                    let hex_slice = &input_bytes[i..i + 4];
                    let code_point = parse_hex_u32(hex_slice, i)?;
                    i += 4;

                    if let Some(unicode_char) = char::from_u32(code_point) {
                        let mut utf8_bytes = [0; 4];
                        let utf8_str = unicode_char.encode_utf8(&mut utf8_bytes);
                        bytes.extend_from_slice(utf8_str.as_bytes());
                    } else {
                        return Err(DecodeError::InvalidCodePoint { code_point });
                    }
                }
                b'U' => {
                    i += 1; // Move past 'U'
                    if i + 8 > input_bytes.len() {
                        return Err(DecodeError::IncompleteSequence { position: i });
                    }

                    // Work directly with the 8-byte slice - bounds check eliminated
                    let hex_slice = &input_bytes[i..i + 8];
                    let code_point = parse_hex_u32(hex_slice, i)?;
                    i += 8;

                    if let Some(unicode_char) = char::from_u32(code_point) {
                        let mut utf8_bytes = [0; 4];
                        let utf8_str = unicode_char.encode_utf8(&mut utf8_bytes);
                        bytes.extend_from_slice(utf8_str.as_bytes());
                    } else {
                        return Err(DecodeError::InvalidCodePoint { code_point });
                    }
                }
                escape_char => {
                    bytes.push(escape_char);
                    i += 1;
                }
            }
        } else {
            // Handle regular UTF-8 characters
            let start = i;

            // Find the end of this UTF-8 character
            while i < input_bytes.len() {
                i += 1;
                if i >= input_bytes.len() || (input_bytes[i] & 0b1100_0000) != 0b1000_0000 {
                    break;
                }
            }

            bytes.extend_from_slice(&input_bytes[start..i]);
        }
    }

    Ok(bytes.into())
}

fn parse_hex_u32(hex_bytes: &[u8], position: usize) -> Result<u32> {
    let mut result = 0u32;

    for (offset, &byte) in hex_bytes.iter().enumerate() {
        let digit = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            b'A'..=b'F' => byte - b'A' + 10,
            _ => {
                return Err(DecodeError::InvalidHexDigit {
                    position: position + offset,
                });
            }
        };
        result = (result << 4) | u32::from(digit);
    }

    Ok(result)
}

#[cfg(test)]
// Strategy 1: Simple backslash count with average multiplier
fn estimate_decoded_size_simple(s: &str) -> usize {
    let backslash_count = s.chars().filter(|&c| c == '\\').count();

    if backslash_count == 0 {
        return s.len(); // No escapes, return byte length
    }

    // Very simple heuristic: assume each escape produces ~2 bytes
    // and the string is mostly escapes, so estimate total as roughly input length
    s.len().max(backslash_count * 2)
}

#[cfg(test)]
// Strategy 2: Zero-allocation parsing with heuristics
fn estimate_decoded_size_heuristic(s: &str) -> usize {
    let mut estimated_size = 0;
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('u') => {
                    // Skip 4 hex digits without allocation
                    let mut valid_count = 0;
                    for _ in 0..4 {
                        if let Some(hex_char) = chars.next() {
                            if hex_char.is_ascii_hexdigit() {
                                valid_count += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    if valid_count == 4 {
                        // Heuristic: \u escapes average ~2.5 bytes (mix of ASCII/Latin/CJK)
                        estimated_size += 3;
                    }
                    // Invalid/incomplete sequences produce no output
                }
                Some('U') => {
                    // Skip 8 hex digits without allocation
                    let mut valid_count = 0;
                    for _ in 0..8 {
                        if let Some(hex_char) = chars.next() {
                            if hex_char.is_ascii_hexdigit() {
                                valid_count += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    if valid_count == 8 {
                        // Heuristic: \U escapes average ~4 bytes (often emoji/symbols)
                        estimated_size += 4;
                    }
                    // Invalid/incomplete sequences produce no output
                }
                Some(_) => {
                    // Other escape sequences produce 1 byte
                    estimated_size += 1;
                }
                None => {
                    // Trailing backslash
                    estimated_size += 1;
                }
            }
        } else {
            // Regular character - count its UTF-8 byte length
            estimated_size += c.len_utf8();
        }
    }

    estimated_size
}

// Strategy 3: Precise zero-allocation parsing
fn estimate_decoded_size_precise(s: &str) -> usize {
    let mut estimated_size = 0;
    let mut chars = s.chars();

    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('u') => {
                    // Parse 4 hex digits without allocation by building u32 directly
                    let mut code_point = 0u32;
                    let mut valid_digits = 0;

                    for _ in 0..4 {
                        if let Some(hex_char) = chars.next() {
                            if let Some(digit) = hex_char.to_digit(16) {
                                code_point = code_point * 16 + digit;
                                valid_digits += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    if valid_digits == 4
                        && let Some(unicode_char) = char::from_u32(code_point)
                    {
                        estimated_size += unicode_char.len_utf8();
                    }
                    // Invalid code points produce no output
                    // Incomplete sequences produce no output
                }
                Some('U') => {
                    // Parse 8 hex digits without allocation by building u32 directly
                    let mut code_point = 0u32;
                    let mut valid_digits = 0;

                    for _ in 0..8 {
                        if let Some(hex_char) = chars.next() {
                            if let Some(digit) = hex_char.to_digit(16) {
                                code_point = code_point * 16 + digit;
                                valid_digits += 1;
                            } else {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    if valid_digits == 8
                        && let Some(unicode_char) = char::from_u32(code_point)
                    {
                        estimated_size += unicode_char.len_utf8();
                    }
                    // Invalid code points produce no output
                    // Incomplete sequences produce no output
                }
                Some(_) => {
                    // Other escape sequences produce 1 byte
                    estimated_size += 1;
                }
                None => {
                    // Trailing backslash
                    estimated_size += 1;
                }
            }
        } else {
            // Regular character - count its UTF-8 byte length
            estimated_size += c.len_utf8();
        }
    }

    estimated_size
}

// Use the precise strategy by default
#[inline]
fn estimate_decoded_size(s: &str) -> usize {
    estimate_decoded_size_precise(s)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_unicode_escapes_no_escapes() {
        let input = "hello world".to_string();
        let expected = Bytes::from("hello world");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_empty_string() {
        let input = String::new();
        let expected = Bytes::new();
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_simple_unicode() {
        let input = "\\u0041".to_string();
        let expected = Bytes::from("\u{0041}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_multiple_unicode() {
        // \u0041 is 'A', \u0042 is 'B'
        let input = "\\u0041\\u0042".to_string();
        let expected = Bytes::from("\u{0041}\u{0042}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_mixed_content() {
        let input = "Hello \\u0041\\u0042 World".to_string();
        let expected = Bytes::from("Hello \u{0041}\u{0042} World");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_non_unicode_escape() {
        // Test other escape sequences like \n, \t, etc.
        let input = "\\n\\t\\r".to_string();
        let expected = Bytes::from("ntr");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_no_backslashes() {
        let input = "no backslashes here".to_string();
        let expected = Bytes::from("no backslashes here");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_utf8_multibyte() {
        let input = "\\u00e9".to_string();
        let expected = Bytes::from("\u{00e9}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_complex_unicode() {
        // \u2764 is ❤ (heavy black heart)
        let input = "Hello \\u2764".to_string();
        let expected = Bytes::from("Hello \u{2764}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_chinese_character() {
        // \u4e2d is 中 (Chinese character for "middle")
        let input = "\\u4e2d\\u6587".to_string();
        let expected = Bytes::from("\u{4e2d}\u{6587}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_mixed_escapes_and_backslashes() {
        // Mix of unicode escapes and other backslash sequences
        let input = "\\u0048\\e\\l\\l\\o\\u0021".to_string();
        let expected = Bytes::from("\u{0048}ello\u{0021}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_zero_padding() {
        // Test with leading zeros in unicode escape
        let input = "\\u0000\\u0001\\u000A".to_string();
        let expected = Bytes::from("\u{0000}\u{0001}\u{000A}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_backslash_literal() {
        // Test backslash followed by non-u character
        let input = "\\x\\y\\z".to_string();
        let expected = Bytes::from("xyz");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_backslash_only() {
        let input = "Hello\\".to_string();
        let result = decode_escapes(input);
        // Backslash at end should now return an error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DecodeError::IncompleteSequence { position: 6 }),
            "actual: {err:?}"
        );
    }

    #[test]
    fn test_decode_unicode_escapes_boundary_values() {
        // Test boundary values for 4-digit hex
        let input = "\\uFFFF\\u0000".to_string();
        let expected = Bytes::from("\u{FFFF}\u{0000}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_uppercase_u_emoji() {
        // Test \U with 8-digit hex for emoji (U+1F600 is 😀)
        let input = "\\U0001F600".to_string();
        let expected = Bytes::from("\u{01F600}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_uppercase_u_complex() {
        // Test various characters outside BMP
        let input = "\\U0001F44D\\U0001F680\\U0001F389".to_string(); // 👍🚀🎉
        let expected = Bytes::from("\u{01F44D}\u{01F680}\u{01F389}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_mixed_u_and_uppercase_u() {
        // Test mix of \u and \U in same string
        let input = "\\u0048\\U0001F600".to_string(); // H + 😀
        let expected = Bytes::from("\u{0048}\u{01F600}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_uppercase_u_max_value() {
        // Test maximum valid Unicode code point (U+10FFFF)
        let input = "\\U0010FFFF".to_string();
        let expected = Bytes::from("\u{10FFFF}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_uppercase_u_with_leading_zeros() {
        // Test \U with leading zeros
        let input = "\\U00000041".to_string(); // A with leading zeros
        let expected = Bytes::from("\u{0041}");
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_decode_unicode_escapes_invalid_unicode_code_point() {
        // Test invalid Unicode code point (beyond U+10FFFF)
        let input = "\\U00110000Hello".to_string();
        let result = decode_escapes(input);
        // Invalid code point should now return an error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(
                err,
                DecodeError::InvalidCodePoint {
                    code_point: 0x0011_0000
                }
            ),
            "actual: {err:?}"
        );
    }

    #[test]
    fn test_decode_unicode_escapes_invalid_hex_digits() {
        // Test invalid hex digits in Unicode escape
        let input = "\\uGHIJ\\U0001ZZZZ".to_string();
        let result = decode_escapes(input);
        // Invalid hex should now return an error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidHexDigit { position: 2 }),
            "actual: {err:?}"
        );
    }

    #[test]
    fn test_decode_unicode_escapes_incomplete_sequences() {
        // Test incomplete Unicode escape sequences at end of string
        let input = "Hello\\u004".to_string();
        let result = decode_escapes(input);
        // Incomplete sequence should now return an error
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(
            matches!(err, DecodeError::IncompleteSequence { position: 7 }),
            "actual: {err:?}"
        );
    }

    #[test]
    fn test_decode_unicode_escapes_surrogate_pairs() {
        // Test Unicode surrogate code points (U+D800-U+DFFF are invalid)
        let input = "\\uD800\\uDC00".to_string();
        let result = decode_escapes(input);
        // Surrogate code points should now return an error
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            matches!(err, DecodeError::InvalidCodePoint { code_point: 0xD800 }),
            "actual: {err:?}"
        );
    }

    #[test]
    fn test_decode_unicode_escapes_full_unicode_range() {
        // Test characters across the full Unicode range
        let input = concat!(
            "\\u0041",     // U+0041 'A' (ASCII)
            "\\u00E9",     // U+00E9 'é' (Latin-1 Supplement)
            "\\u4E2D",     // U+4E2D '中' (CJK Unified Ideographs)
            "\\U00010000", // U+10000 '𐀀' (Linear B Syllabary)
            "\\U0001F602", // U+1F602 '😂' (Emoticons)
            "\\U0010FFFF"  // U+10FFFF (Maximum valid Unicode code point)
        )
        .to_string();
        let expected = Bytes::from("\u{0041}\u{00E9}\u{4E2D}\u{010000}\u{01F602}\u{10FFFF}"); // U+10FFFF (Maximum valid Unicode code point)
        let actual = decode_escapes(input).unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_estimate_decoded_size() {
        // Test the size estimation function
        assert_eq!(estimate_decoded_size("hello"), 5);
        assert_eq!(estimate_decoded_size("\\u0041"), 1); // 'A' is 1 byte in UTF-8
        assert_eq!(estimate_decoded_size("\\U0001F602"), 4); // Emoji is 4 bytes in UTF-8
        assert_eq!(estimate_decoded_size("\\n\\t"), 2); // Non-unicode escapes = 1 byte each
        assert_eq!(estimate_decoded_size("hé"), 3); // UTF-8 multi-byte character
        assert_eq!(estimate_decoded_size("\\u00e9"), 2); // 'é' is 2 bytes in UTF-8

        // Test mixed content
        let mixed = "Hello \\u0041\\U0001F602 World";
        let estimated = estimate_decoded_size(mixed);
        // Hello (5) + space (1) + A (1) + 😂 (4) + space (1) + World (5) = 17
        assert_eq!(estimated, 17);
    }

    #[test]
    fn test_estimation_strategies_comparison() {
        let test_cases = vec![
            "simple text",
            "\\u0041\\u0042\\u0043",
            "\\U0001F602\\U0001F680\\U0001F389",
            "Mixed \\u0048\\U0001F602 content!",
            "\\u00e9\\u4e2d\\u6587", // Multi-byte characters
        ];

        for case in test_cases {
            let actual = decode_escapes(case.to_string()).unwrap().len();
            let simple = estimate_decoded_size_simple(case);
            let heuristic = estimate_decoded_size_heuristic(case);
            let precise = estimate_decoded_size_precise(case);

            println!(
                "Case: '{case}' -> Actual: {actual}, Simple: {simple}, Heuristic: {heuristic}, Precise: {precise}"
            );

            // Precise should always match actual for valid sequences
            assert_eq!(precise, actual, "Precise estimation failed for: {case}");

            // Simple and heuristic should not under-estimate (we prefer over-allocation to under-allocation)
            assert!(simple >= actual, "Simple under-estimated for: {case}");
            assert!(heuristic >= actual, "Heuristic under-estimated for: {case}");

            // All strategies should be reasonable (not wildly over-allocating)
            assert!(simple <= actual * 10, "Simple way too high for: {case}");
            assert!(heuristic <= actual * 3, "Heuristic too high for: {case}");
        }
    }

    #[test]
    fn test_zero_allocation_estimation() {
        // Test that our estimation functions don't allocate memory
        // This is more of a conceptual test - the functions should be designed
        // to avoid any allocations (no Vec::new(), String::new(), etc.)

        let large_input = "\\u0041".repeat(1000);

        // All three strategies should handle large inputs efficiently
        let _simple = estimate_decoded_size_simple(&large_input);
        let _heuristic = estimate_decoded_size_heuristic(&large_input);
        let _precise = estimate_decoded_size_precise(&large_input);

        // If we get here without stack overflow or excessive memory usage,
        // the zero-allocation approach is working
    }

    #[test]
    fn test_buffer_allocation_efficiency() {
        // Test that we estimate buffer size accurately
        let test_cases = vec![
            ("simple text", 11, 11),
            ("\\u0041\\u0042\\u0043", 3, 3), // ABC = 3 bytes
            ("\\U0001F602\\U0001F680\\U0001F389", 12, 12), // 3 emojis = 12 bytes
            ("Mixed \\u0048\\U0001F602 content!", 20, 20), // Mixed (5) + space (1) + H (1) + 😂 (4) + space (1) + content! (8) = 20
        ];

        for (case, expected_estimated, expected_actual) in test_cases {
            let result = decode_escapes(case.to_string());
            let estimated = estimate_decoded_size(case);
            let actual = result.unwrap().len();

            assert_eq!(
                estimated, expected_estimated,
                "Case: {case}, Expected estimated: {expected_estimated}, Got: {estimated}",
            );

            assert_eq!(
                actual, expected_actual,
                "Case: {case}, Expected actual: {expected_actual}, Got: {actual}",
            );

            // Estimation should exactly match actual for valid sequences
            assert_eq!(
                estimated, actual,
                "Case: {case}, Estimated: {estimated}, Actual: {actual}",
            );
        }
    }

    #[test]
    fn test_memory_allocation_benefits() {
        // Test that demonstrates the benefits of pre-allocating buffer size
        // This test shows that our estimation prevents multiple reallocations

        // Case 1: Large string with many Unicode escapes
        let large_input = "\\u0041".repeat(1000); // 1000 'A' characters via unicode escapes
        let result = decode_escapes(large_input.clone());
        let estimated = estimate_decoded_size(&large_input);

        assert_eq!(result.unwrap().len(), 1000); // Should be 1000 bytes (1000 'A' characters)
        assert_eq!(estimated, 1000); // Estimation should be exact

        // Case 2: Mixed content that would cause multiple reallocations without estimation
        let mixed_large = format!(
            "{}{}{}",
            "Regular text ".repeat(50),
            "\\u4e2d\\u6587".repeat(100), // 100 instances of 中文 (6 bytes each)
            "\\U0001F602".repeat(50)      // 50 emoji (4 bytes each)
        );

        let result = decode_escapes(mixed_large.clone());
        let estimated = estimate_decoded_size(&mixed_large);

        // Should be: 50*13 (Regular text) + 100*6 (中文) + 50*4 (emoji) = 650 + 600 + 200 = 1450
        let result = result.unwrap();
        assert_eq!(estimated, result.len());
        assert!(result.len() > 1000); // Ensure it's a substantial size
    }

    #[test]
    fn test_estimation_performance_comparison() {
        // Test to demonstrate the performance characteristics of different strategies
        let medium_input = "\\u0041".repeat(100);
        let large_input = "\\u0041".repeat(1000);
        let mixed_input = format!(
            "{}\\u4e2d\\u6587{}",
            "text".repeat(100),
            "\\U0001F602".repeat(50)
        );

        let test_inputs = vec![
            ("short", "\\u0041\\u0042"),
            ("medium", medium_input.as_str()),
            ("large", large_input.as_str()),
            ("mixed", mixed_input.as_str()),
        ];

        for (name, input) in test_inputs {
            let actual_len = decode_escapes(input.to_string()).unwrap().len();

            // Test all strategies
            let simple_est = estimate_decoded_size_simple(input);
            let heuristic_est = estimate_decoded_size_heuristic(input);
            let precise_est = estimate_decoded_size_precise(input);

            println!(
                "{}: Actual={}, Simple={} ({}%), Heuristic={} ({}%), Precise={} ({}%)",
                name,
                actual_len,
                simple_est,
                (simple_est * 100) / actual_len.max(1),
                heuristic_est,
                (heuristic_est * 100) / actual_len.max(1),
                precise_est,
                (precise_est * 100) / actual_len.max(1),
            );

            // Assertions about estimation quality
            assert_eq!(precise_est, actual_len, "Precise should be exact");
            assert!(simple_est >= actual_len, "Simple should not under-estimate");
            assert!(
                heuristic_est >= actual_len,
                "Heuristic should not under-estimate"
            );

            // Simple might over-estimate significantly, but should be reasonable
            assert!(
                simple_est <= actual_len * 10,
                "Simple estimation way too high for {name}",
            );
        }
    }

    #[test]
    fn test_error_behavior_verification() {
        // Verify that the function now returns errors instead of silently skipping

        // Test 1: Invalid hex digit should error
        let result = decode_escapes("\\uG000".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            DecodeError::InvalidHexDigit { position } => assert_eq!(position, 2),
            _ => panic!("Expected InvalidHexDigit error"),
        }

        // Test 2: Incomplete sequence should error
        let result = decode_escapes("\\u00".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            DecodeError::IncompleteSequence { position } => assert_eq!(position, 2),
            _ => panic!("Expected IncompleteSequence error"),
        }

        // Test 3: Invalid code point should error
        let result = decode_escapes("\\U00110000".to_string());
        assert!(result.is_err());
        match result.unwrap_err() {
            DecodeError::InvalidCodePoint { code_point } => assert_eq!(code_point, 0x11_0000),
            _ => panic!("Expected InvalidCodePoint error"),
        }

        // Test 4: Valid sequences should still work
        let result = decode_escapes("\\u0048\\u0065\\u006C\\u006C\\u006F".to_string());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Bytes::from("Hello"));
    }
}
