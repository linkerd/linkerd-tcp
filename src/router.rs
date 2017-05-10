use futures::sync::mpsc::Receiver;

pub struct Router {
    server: Receiver<ServerConnection>,
}

impl Router {
    pub fn new(srv: Receiver<ServerConnection>) -> Router {
        Router {}
    }
}
