pub use super::Work;

pub trait Workers<R> {
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce() -> R;

    fn wait(&mut self) -> Option<R>;
}

#[derive(Clone, Default)]
pub struct SingleThread;

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct ForcedQueue<R>(std::collections::VecDeque<R>);

#[cfg(feature = "std")]
#[derive(Clone, Default)]
pub struct ThreadPool {}

impl<R> Workers<R> for SingleThread {
    fn add<F>(&mut self, f: F) -> Option<R>
    where
        F: FnOnce() -> R,
    {
        Some((f)())
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
