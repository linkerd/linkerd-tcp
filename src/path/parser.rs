use super::Path;

use std::result::Result as StdResult;

#[derive(Debug)]
pub struct Error {
    pub message: String,
    pub offset: usize,
    pub input: String,
}

pub type Parsed = StdResult<Path, Error>;

/// Parses a slash-delimited path.
pub fn parse_path(txt: &str) -> Parsed {
    let mut elems: Vec<Vec<u8>> = Vec::new();

    let mut is_first = true;
    let mut cur_elem: Vec<u8> = Vec::new();
    for (i, c) in txt.bytes().enumerate() {
        if c == b'/' {
            if is_first {
                is_first = false;
            } else if cur_elem.is_empty() {
                return Err(Error {
                               message: "Empty path element".into(),
                               offset: i,
                               input: txt.into(),
                           });
            } else {
                elems.push(cur_elem);
                cur_elem = Vec::new();
            }
        } else if !is_valid(c) {
            return Err(Error {
                           message: format!("Invalid character: {}", c),
                           offset: i,
                           input: txt.into(),
                       });
        } else {
            cur_elem.push(c);
        }
    }

    if is_first {
        return Err(Error {
                       message: "Empty string".into(),
                       offset: 0,
                       input: txt.into(),
                   });
    }

    // If cur_elem is empty, the path ended in a slash.
    if !cur_elem.is_empty() {
        elems.push(cur_elem);
    }

    Ok(Path::new(elems))
}

fn is_valid(c: u8) -> bool {
    match c {
        b'-' | b'_' | b'\\' | b':' | b'#' | b'%' | b'$' | b'.' => true,
        _ => (b'A' <= c && c <= b'Z') || (b'a' <= c && c <= b'z') || (b'0' <= c && c <= b'9'),
    }
}

#[test]
fn test_parse_path_empty() {
    assert!(parse_path("/").unwrap().is_empty());
}

#[test]
fn test_parse_path_ok() {
    let strs = ["/slim_k/all_finesse", "/slim_k/all_finesse/"];
    for s in &strs {
        let path = parse_path(s).unwrap();
        assert_eq!(path.len(), 2);
        assert_eq!(path[0], b"slim_k");
        assert_eq!(path[1], b"all_finesse");
    }
}

#[test]
fn test_parse_path_whitespace_err() {
    let err = parse_path("/red bone").unwrap_err();
    assert_eq!(err.offset, 4);

    let err = parse_path(" /redbone").unwrap_err();
    assert_eq!(err.offset, 0);

    let err = parse_path("/redbone/ ").unwrap_err();
    assert_eq!(err.offset, 9);
}
