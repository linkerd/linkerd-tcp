use std::fmt;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct Path(String);
impl Path {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_empty(&self) -> bool {
        self.len() == 1
    }
    fn len(&self) -> usize {
        self.0.len()
    }

    pub fn starts_with(&self, other: &Path) -> bool {
        let &Path(other) = other;
        if self.0.len() > other.len() {
            self.0.starts_with(&other) &&
            (self.0.ends_with('/') || other[self.0.len()..].starts_with('/'))
        } else if other.len() == self.0.len() {
            self.0 == other
        } else {
            false
        }
    }
}
impl fmt::Display for Path {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl From<String> for Path {
    fn from(s: String) -> Path {
        assert!(s.starts_with('/'));
        Path(s)
    }
}
impl<'a> From<&'a str> for Path {
    fn from(s: &'a str) -> Path {
        assert!(s.starts_with('/'));
        Path(s.into())
    }
}
