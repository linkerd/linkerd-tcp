use std::cell::RefCell;
use std::rc::Rc;

mod receiver;
mod sender;
mod shared;

pub use self::receiver::{Receiver, Recv, RecvLostSender};
pub use self::sender::Sender;

/// Creates a channel capable of receiving values from an .
///
/// It is intended to allow one or more producer tasks to dispatch to an arbitrary number of
/// worker tasks on a single Core.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Rc::new(RefCell::new(shared::new(capacity)));
    let rx = receiver::new(&shared);
    let tx = sender::new(shared);
    (tx, rx)
}
