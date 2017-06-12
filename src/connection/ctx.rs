use std::io;

/// A connection context
pub trait Ctx {
    fn read(&mut self, sz: usize);
    fn wrote(&mut self, sz: usize);
    fn complete(self, res: &io::Result<()>);
}

#[allow(dead_code)]
pub fn null() -> Null {
    Null()
}
#[allow(dead_code)]
pub struct Null();
impl Ctx for Null {
    fn read(&mut self, _sz: usize) {}
    fn wrote(&mut self, _sz: usize) {}
    fn complete(self, _res: &io::Result<()>) {}
}
