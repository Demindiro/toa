pub use super::Work;

use core::cmp;
#[cfg(feature = "rayon")]
use std::{collections::BinaryHeap, sync::mpsc};

pub trait Workers<R> {
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: 'static + Send + FnOnce() -> R;

    fn poll(&mut self) -> Option<R>;
    fn wait(&mut self) -> Option<R>;
}

#[derive(Default)]
pub struct SingleThread;

#[cfg(test)]
pub(crate) struct ForcedQueue<R>(std::collections::VecDeque<R>);

#[cfg(feature = "rayon")]
pub struct ThreadPool<R> {
    recv: mpsc::Receiver<(u64, R)>,
    send: mpsc::Sender<(u64, R)>,
    queue: BinaryHeap<(cmp::Reverse<u64>, SkipCmp<R>)>,
    ticket_first: u64,
    ticket_last: u64,
    max_pending: u32,
}

#[allow(dead_code)]
struct SkipCmp<T>(T);

impl<R> Workers<R> for SingleThread {
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        Some((f)())
    }

    fn poll(&mut self) -> Option<R> {
        None
    }

    fn wait(&mut self) -> Option<R> {
        None
    }
}

#[cfg(test)]
impl<R> Workers<R> for ForcedQueue<R> {
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        self.0.push_back((f)());
        self.poll()
    }

    fn poll(&mut self) -> Option<R> {
        (self.0.len() > 2).then(|| self.wait()).flatten()
    }

    fn wait(&mut self) -> Option<R> {
        self.0.pop_front()
    }
}

#[cfg(test)]
impl<R> Default for ForcedQueue<R> {
    fn default() -> Self {
        Self(Default::default())
    }
}

#[cfg(feature = "rayon")]
impl<R> ThreadPool<R>
where
    R: Send,
{
    fn next_ticket(&mut self) -> u64 {
        if self.ticket_last - self.ticket_first > u64::from(self.max_pending) {
            self.recv_one();
        }
        let x = self.ticket_last;
        self.ticket_last += 1;
        x
    }

    fn can_pop(&self) -> bool {
        self.queue
            .peek()
            .is_some_and(|x| x.0.0 == self.ticket_first)
    }

    fn pop(&mut self) -> Option<R> {
        if self.can_pop() {
            self.ticket_first += 1;
            self.queue.pop().map(|x| x.1.0)
        } else {
            None
        }
    }

    fn recv_one(&mut self) {
        // avoid a potential deadlock when we can already pop finished work.
        //
        // also defensively check for the case where no work has been enqueued at all.
        if self.can_pop() || self.ticket_first == self.ticket_last {
            return;
        }
        let (ticket, x) = self.recv.recv().expect("we own at least one handle");
        self.queue.push((cmp::Reverse(ticket), SkipCmp(x)));
    }

    fn try_recv_one(&mut self) {
        if let Ok((ticket, x)) = self.recv.try_recv() {
            self.queue.push((cmp::Reverse(ticket), SkipCmp(x)));
        }
    }
}

#[cfg(feature = "rayon")]
impl<R> Default for ThreadPool<R> {
    fn default() -> Self {
        let (send, recv) = std::sync::mpsc::channel();
        Self {
            send,
            recv,
            queue: Default::default(),
            ticket_first: 0,
            ticket_last: 0,
            max_pending: 1024,
        }
    }
}

#[cfg(feature = "rayon")]
impl<R> Workers<R> for ThreadPool<R>
where
    R: 'static + Send,
{
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: 'static + Send + FnOnce() -> R,
    {
        let ticket = self.next_ticket();
        let send = self.send.clone();
        rayon_core::spawn_fifo(move || {
            send.send((ticket, f()))
                .expect("main thread exited before we finished?")
        });
        self.pop()
    }

    fn poll(&mut self) -> Option<R> {
        self.try_recv_one();
        self.pop()
    }

    fn wait(&mut self) -> Option<R> {
        while self.ticket_first != self.ticket_last {
            if let Some(x) = self.pop() {
                return Some(x);
            }
            self.recv_one();
        }
        None
    }
}

impl<T> PartialEq for SkipCmp<T> {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl<T> Eq for SkipCmp<T> {}

impl<T> PartialOrd for SkipCmp<T> {
    fn partial_cmp(&self, _: &Self) -> Option<cmp::Ordering> {
        Some(cmp::Ordering::Equal)
    }
}

impl<T> Ord for SkipCmp<T> {
    fn cmp(&self, _: &Self) -> cmp::Ordering {
        cmp::Ordering::Equal
    }
}
