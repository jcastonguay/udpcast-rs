use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

pub struct Produconsum {
    size: usize,
    inner: Mutex<Inner>,
    cond: Condvar,
    name: &'static str,
}

struct Inner {
    produced: usize,
    consumed: usize,
    at_end: bool,
    consumer_waiting: bool,
}

impl Produconsum {
    pub fn new(size: usize, name: &'static str) -> Self {
        Self {
            size,
            inner: Mutex::new(Inner {
                produced: 0,
                consumed: 0,
                at_end: false,
                consumer_waiting: false,
            }),
            cond: Condvar::new(),
            name,
        }
    }

    fn get_produced_amount(inner: &Inner, size: usize) -> usize {
        let produced = inner.produced;
        let consumed = inner.consumed;
        if produced < consumed {
            produced + 2 * size - consumed
        } else {
            produced - consumed
        }
    }

    pub fn produce(&self, amount: usize) {
        let mut inner = self.inner.lock().unwrap();
        if amount > self.size {
            crate::util::fatal(
                1,
                &format!(
                    "Buffer overflow in produce {}: {} > {}\n",
                    self.name, amount, self.size
                ),
            );
        }
        let mut produced = inner.produced + amount;
        if produced >= 2 * self.size {
            produced -= 2 * self.size;
        }
        let consumed = inner.consumed;
        if produced > consumed + self.size
            || (produced < consumed && produced > consumed - self.size)
        {
            crate::util::fatal(
                1,
                &format!(
                    "Buffer overflow in produce {}: {} > {} [{}]\n",
                    self.name, produced, consumed, self.size
                ),
            );
        }
        inner.produced = produced;
        if inner.consumer_waiting {
            self.cond.notify_one();
        }
    }

    pub fn produce_end(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.at_end = true;
        if inner.consumer_waiting {
            self.cond.notify_one();
        }
    }

    pub fn get_waiting(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        Self::get_produced_amount(&inner, self.size)
    }

    pub fn get_consumer_position(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.consumed % self.size
    }

    pub fn get_producer_position(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.produced % self.size
    }

    pub fn get_size(&self) -> usize {
        self.size
    }

    pub fn consumed(&self, amount: usize) -> usize {
        let mut inner = self.inner.lock().unwrap();
        let consumed = inner.consumed;
        if consumed >= 2 * self.size - amount {
            inner.consumed = consumed + amount - 2 * self.size;
        } else {
            inner.consumed = consumed + amount;
        }
        amount
    }

    fn consume_any_inner(
        inner_guard: std::sync::MutexGuard<Inner>,
        cond: &Condvar,
        size: usize,
        min_amount: usize,
        deadline: Option<Instant>,
    ) -> usize {
        let mut inner = inner_guard;
        inner.consumer_waiting = true;
        let mut amount = Self::get_produced_amount(&inner, size);
        if amount >= min_amount || inner.at_end {
            inner.consumer_waiting = false;
            return amount;
        }
        loop {
            let result = if let Some(dl) = deadline {
                let now = Instant::now();
                if now >= dl {
                    amount = Self::get_produced_amount(&inner, size);
                    break;
                }
                let timeout = dl - now;
                let (guard, _) = cond.wait_timeout(inner, timeout).unwrap();
                inner = guard;
                if Instant::now() >= dl {
                    amount = Self::get_produced_amount(&inner, size);
                    break;
                }
                amount = Self::get_produced_amount(&inner, size);
                amount
            } else {
                inner = cond.wait(inner).unwrap();
                Self::get_produced_amount(&inner, size)
            };
            if result >= min_amount || inner.at_end {
                amount = result;
                break;
            }
        }
        inner.consumer_waiting = false;
        amount
    }

    pub fn consume_any(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        Self::consume_any_inner(inner, &self.cond, self.size, 1, None)
    }

    pub fn consume_any_with_timeout(&self, timeout: Duration) -> usize {
        let inner = self.inner.lock().unwrap();
        let deadline = Some(Instant::now() + timeout);
        Self::consume_any_inner(inner, &self.cond, self.size, 1, deadline)
    }

    pub fn consume(&self, amount: usize) -> usize {
        let inner = self.inner.lock().unwrap();
        Self::consume_any_inner(inner, &self.cond, self.size, amount, None)
    }

    pub fn consume_contiguous_min_amount(&self, amount: usize) -> usize {
        let mut n = {
            let inner = self.inner.lock().unwrap();
            Self::consume_any_inner(inner, &self.cond, self.size, amount, None)
        };
        let consumed = self.inner.lock().unwrap().consumed;
        let l = self.size - (consumed % self.size);
        if n > l {
            n = l;
        }
        n
    }
}
