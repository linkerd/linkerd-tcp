use std::fmt;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct Path(String);
impl Path {
    pub fn as_str(&self) -> &str {
        &self.0
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
