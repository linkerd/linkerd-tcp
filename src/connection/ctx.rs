/// A connection context
pub trait Ctx: Drop {
    fn read(&mut self, sz: usize);
    fn wrote(&mut self, sz: usize);
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
}
impl Drop for Null {
    fn drop(&mut self) {}
}
