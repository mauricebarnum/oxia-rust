#[cfg(test)]
use std::cmp::Ordering;

#[cfg(test)]
pub fn compare_with_slash(xa: impl AsRef<str>, ya: impl AsRef<str>) -> Ordering {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

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
}
