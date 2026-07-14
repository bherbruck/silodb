//! Connection actors: rusqlite is sync and `Connection` is `!Sync`, so
//! each connection lives on its own thread eating a queue of jobs. One
//! writer (writes, DDL, maintenance — SQLite is single-writer anyway)
//! plus a small pool of read-only WAL readers.

use rusqlite::Connection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

type Job = Box<dyn FnOnce(&mut Connection) + Send>;

#[derive(Clone)]
pub struct Actor {
    tx: mpsc::Sender<Job>,
}

impl Actor {
    pub fn spawn(mut conn: Connection) -> Actor {
        let (tx, rx) = mpsc::channel::<Job>();
        std::thread::spawn(move || {
            while let Ok(job) = rx.recv() {
                job(&mut conn);
            }
        });
        Actor { tx }
    }

    /// Run a closure on this actor's connection, await the result.
    pub async fn run<R, F>(&self, f: F) -> R
    where
        R: Send + 'static,
        F: FnOnce(&mut Connection) -> R + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Box::new(move |conn| {
                let _ = tx.send(f(conn));
            }))
            .expect("connection thread died");
        rx.await.expect("connection thread dropped the job")
    }
}

pub struct ReaderPool {
    readers: Vec<Actor>,
    next: AtomicUsize,
}

impl ReaderPool {
    pub fn new(readers: Vec<Actor>) -> Self {
        assert!(!readers.is_empty());
        ReaderPool {
            readers,
            next: AtomicUsize::new(0),
        }
    }

    pub fn get(&self) -> &Actor {
        let i = self.next.fetch_add(1, Ordering::Relaxed);
        &self.readers[i % self.readers.len()]
    }
}
