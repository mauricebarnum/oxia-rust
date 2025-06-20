use std::cmp::Ordering;

#[cfg(test)]
pub(crate) fn compare_with_slash(xa: impl AsRef<str>, ya: impl AsRef<str>) -> Ordering {
    let mut x = xa.as_ref();
    let mut y = ya.as_ref();

    while !x.is_empty() && !y.is_empty() {
        let xs = x.find('/');
        let ys = y.find('/');
        match (xs, ys) {
            (None, None) => return x.cmp(y),
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(xi), Some(yi)) => {
                let c = x[..xi].cmp(&y[..yi]);
                if c != Ordering::Equal {
                    return c;
                }
                x = &x[xi + 1..];
                y = &y[xi + 1..];
            }
        };
    }

    x.cmp(y)
}

use crate::{GetResponse, KeyComparisonType};

// Compare the keys of two GetResponse instances, ignoring the value and other metadata.
// TODO: consider at least looking at the record version, but I don't beleive the Go implementation
// does this, and that's our reference. See https://github.com/streamnative/oxia/blob/main/oxia/async_client_impl.go
#[inline]
pub(crate) fn select_response(
    prev: Option<GetResponse>,
    candidate: GetResponse,
    ct: KeyComparisonType,
) -> GetResponse {
    assert_ne!(KeyComparisonType::Equal, ct);

    // For selection, we only look at the key, secondary_index_key, and version
    fn cmp(a: &GetResponse, b: &GetResponse) -> Ordering {
        a.secondary_index_key
            .cmp(&b.secondary_index_key)
            .then_with(|| a.key.cmp(&b.key))
    }

    if let Some(pv) = prev {
        use KeyComparisonType::*;
        let ordering = cmp(&pv, &candidate);
        match ct {
            Equal => panic!("bug: should not get here"),

            // Lower and Floor are treated the same here: the server-side for each shard has
            // already applied the specific logic, and now we're just merging the partial results
            Floor | Lower => match ordering {
                Ordering::Less => candidate,
                _ => pv,
            },

            // Ceiling and Higher are similar to Lower and Floor
            Ceiling | Higher => match ordering {
                Ordering::Greater => candidate,
                _ => pv,
            },
        }
    } else {
        candidate
    }
}

#[cfg(test)]
mod tests {
    use mauricebarnum_oxia_common::proto::GetResponse;

    use super::*;
    use std::cmp::Ordering;
    use itertools::Itertools;

    #[test]
    fn test_compare_with_slash() {
        let data = [
            (Ordering::Equal, "", ""),
            (Ordering::Equal, "aaa", "aaa"),
            (Ordering::Equal, "aaaaa", "aaaaa"),
            (Ordering::Greater, "/", "a"),
            (Ordering::Greater, "/a/b/a/a/a", "/a/b/a/b"),
            (Ordering::Greater, "/aaaa/a/a", "/aaaa/bbbbbbbbbb"),
            (Ordering::Greater, "/aaaa/a/a", "/bbbbbbbbbb"),
            (Ordering::Greater, "aaaaa", ""),
            (Ordering::Greater, "aaaaaaaaaaa", "aaa"),
            (Ordering::Greater, "bbbbb", "aaaaa"),
            (Ordering::Less, "", "aaaaaa"),
            (Ordering::Less, "/aaaa", "/aa/a"),
            (Ordering::Less, "/aaaa", "/bbbbb"),
            (Ordering::Less, "/aaaa/a", "/aaaa/b"),
            (Ordering::Less, "a", "/"),
            (Ordering::Less, "aaa", "zzzzz"),
            (Ordering::Less, "aaaaa", "aaaaaaaaaaa"),
            (Ordering::Less, "aaaaa", "zzzzz"),
        ];
        for (i, &(expected, x, y)) in data.iter().enumerate() {
            println!("{}: '{}' '{}' => {:?}", i, x, y, expected);
            assert_eq!(expected, compare_with_slash(x, y));
        }
    }

    #[test]
    fn test_select_response() {
        use crate::*;

        fn gk(k: &str) -> GetResponse {
            GetResponse {
                key: Some(k.to_string()),
                ..Default::default()
            }
        }

        fn gs(k: &str, s: &str) -> GetResponse {
            GetResponse {
                key: Some(k.to_string()),
                secondary_index_key: Some(s.to_string()),
                ..Default::default()
            }
        }

        struct TestCase {
            expected: GetResponse,
            comparison: crate::KeyComparisonType,
            responses: Vec<GetResponse>,
        }

        let test_cases = [
            TestCase {
                expected: gk("a"),
                comparison: KeyComparisonType::Higher,
                responses: vec![gk("b"), gk("a"), gk("c")],
            },
            TestCase {
                expected: gk("a"),
                comparison: KeyComparisonType::Higher,
                responses: vec![gk("c"), gk("b"), gk("a")],
            },
            TestCase {
                expected: gk("x"),
                comparison: KeyComparisonType::Higher,
                responses: vec![gk("x"), gk("x"), gk("x")],
            },
            TestCase {
                expected: gs("b", "a"),
                comparison: KeyComparisonType::Higher,
                responses: vec![gs("a", "s"), gs("b", "a"), gs("a", "z"), gs("c", "b")],
            },
            TestCase {
                expected: gk("single"),
                comparison: KeyComparisonType::Higher,
                responses: vec![gk("single")],
            },
            TestCase {
                expected: gk("a"),
                comparison: KeyComparisonType::Ceiling,
                responses: vec![gk("b"), gk("a"), gk("c")],
            },
            TestCase {
                expected: gk("c"),
                comparison: KeyComparisonType::Floor,
                responses: vec![gk("b"), gk("c"), gk("a")],
            },
            TestCase {
                expected: gk("c"),
                comparison: KeyComparisonType::Floor,
                responses: vec![gk("a"), gk("b"), gk("c")],
            },
            TestCase {
                expected: gs("b", "z"),
                comparison: KeyComparisonType::Floor,
                responses: vec![gs("b", "z"), gs("a", "t"), gs("c", "s"), gs("a", "s")],
            },
            TestCase {
                expected: gk("c"),
                comparison: KeyComparisonType::Lower,
                responses: vec![gk("c"), gk("b"), gk("a")],
            },
            TestCase {
                expected: gk("c"),
                comparison: KeyComparisonType::Lower,
                responses: vec![gk("a"), gk("b"), gk("c")],
            },
            TestCase {
                expected: gs("b", "z"),
                comparison: KeyComparisonType::Lower,
                responses: vec![gs("b", "z"), gs("a", "t"), gs("c", "s"), gs("a", "s")],
            },
        ];

        fn format_response(r: &GetResponse) -> String {
            format!(
                "{{k: {}, s: {}, v: {}}}",
                r.key.as_deref().unwrap_or("None"),
                r.secondary_index_key.as_deref().unwrap_or("None"),
                r.version.version_id
            )
        }

        for ref tc in test_cases {
            let mut selected = None;
            for r in &tc.responses {
                selected = Some(select_response(selected, r.clone(), tc.comparison));
            }

            println!(
                "--- Comparision: {:?}\n    Responses: [{}]\n    Expected: {}\n    Actual:   {}\n",
                tc.comparison,
                tc.responses.iter().map(format_response).join(", "),
                format_response(&tc.expected),
                format_response(selected.as_ref().unwrap())
            );

            let actual = selected.unwrap();

            assert_eq!(
                tc.expected.key, actual.key,
                "Test case failed: Key mismatch for comparison {:?}",
                tc.comparison
            );

            assert_eq!(
                tc.expected.secondary_index_key, actual.secondary_index_key,
                "Test case failed: Secondary index key mismatch for comparison {:?}",
                tc.comparison
            );

            assert_eq!(
                tc.expected.version, actual.version,
                "Test case failed: Version mismatch for comparison {:?}",
                tc.comparison
            );
        }
    }

    #[test]
    #[should_panic]
    fn test_select_response_panic_on_equal() {
        select_response(
            None,
            crate::GetResponse::default(),
            KeyComparisonType::Equal,
        );
    }
}
