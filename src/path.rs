use std::ops::Index;
use std::rc::Rc;

pub type PathElem = Rc<Vec<u8>>;
pub type PathElems = Rc<Vec<PathElem>>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Path(PathElems);
impl Path {
    pub fn empty() -> Path {
        Path(Rc::new(Vec::new()))
    }

    // TODO pub fn read(s: &str) -> Path {}

    pub fn new(elems0: Vec<Vec<u8>>) -> Path {
        let mut elems = Vec::with_capacity(elems0.len());
        for el in elems0 {
            elems.push(Rc::new(el));
        }
        Path(Rc::new(elems))
    }

    pub fn from_strings(strs: Vec<String>) -> Path {
        let mut elems = Vec::with_capacity(strs.len());
        for s in strs {
            elems.push(Rc::new(s.into()));
        }
        Path(Rc::new(elems))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn elems(&self) -> PathElems {
        self.0.clone()
    }

    pub fn concat(&self, other: Path) -> Path {
        if self.is_empty() {
            other.clone()
        } else if other.is_empty() {
            self.clone()
        } else {
            let mut elems = Vec::with_capacity(self.len() + other.len());
            for el in &*self.0 {
                elems.push(el.clone());
            }
            for el in &*other.0 {
                elems.push(el.clone());
            }
            Path(Rc::new(elems))
        }
    }

    pub fn append(&self, el: Vec<u8>) -> Path {
        self.concat(Path::new(vec![el]))
    }

    pub fn append_string(&self, s: String) -> Path {
        self.append(s.as_bytes().to_vec())
    }
}

impl Index<usize> for Path {
    type Output = Vec<u8>;
    fn index(&self, idx: usize) -> &Vec<u8> {
        &*self.0[idx]
    }
}

#[test]
fn test_empty() {
    let path = Path::empty();
    assert!(path.is_empty());
    assert_eq!(path.len(), 0);
}

#[test]
fn test_binary() {
    let elems = vec!["hello".as_bytes().to_vec(), vec![4, 5, 6]];
    let path = Path::new(elems.clone());
    assert!(!path.is_empty());
    assert_eq!(path.len(), 2);
    assert_eq!(path[0], "hello".as_bytes());
    assert_eq!(path[1], vec![4, 5, 6]);
    assert_eq!(path, Path::new(elems));
}

#[test]
fn test_string() {
    let elems = vec!["bowie".into(), "outside".into(), "ramona".into()];
    let path = Path::from_strings(elems.clone());
    assert!(!path.is_empty());
    assert_eq!(path.len(), 3);
    assert_eq!(path[0], "bowie".as_bytes());
    assert_eq!(path[1], "outside".as_bytes());
    assert_eq!(path[2], "ramona".as_bytes());
    assert_eq!(path, Path::from_strings(elems));
}

#[test]
fn test_concat() {
    let pfx = Path::from_strings(vec!["bowie".into()]);
    let sfx = Path::from_strings(vec!["earthling".into()]);
    let path = pfx.concat(sfx);
    assert_eq!(path.len(), 2);
    assert_eq!(path,
               Path::from_strings(vec!["bowie".into(), "earthling".into()]));
}
