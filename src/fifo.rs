//! The shared ring buffer between the local reader / writer threads and
//! the network threads.
//!
//! C keeps the ring in a `static char *` guarded by two
//! produce/consume queues. The Rust port used an `Arc<UnsafeCell<Vec<u8>>>`
//! with manual `unsafe` accessors (and hand-written `Send`/`Sync` impls).
//! The buffer is now a plain `Mutex<Vec<u8>>`: the compiler guarantees
//! `Fifo: Send + Sync`, the lock scopes the accesses, and nothing can be
//! forgotten or double-freed.

use std::sync::Mutex;

use crate::produconsum::Produconsum;

pub struct Fifo {
    pub buffer: Mutex<Vec<u8>>,
    pub data_buf_size: usize,
    pub free_mem_queue: Produconsum,
    pub data: Produconsum,
}

impl Fifo {
    pub fn new(block_size: usize) -> Self {
        let data_buf_size = block_size * 4096;
        let free_mem_queue = Produconsum::new(data_buf_size, "free mem");
        free_mem_queue.produce(data_buf_size);
        let data = Produconsum::new(data_buf_size, "receive");
        Self {
            buffer: Mutex::new(vec![0u8; data_buf_size]),
            data_buf_size,
            free_mem_queue,
            data,
        }
    }

    /// Copy `data` into the ring buffer at `offset` (must be <
    /// data_buf_size), wrapping at most once.
    pub fn write_at(&self, offset: usize, data: &[u8]) {
        let mut buf = self.buffer.lock().unwrap();
        let first = std::cmp::min(data.len(), self.data_buf_size - offset);
        buf[offset..offset + first].copy_from_slice(&data[..first]);
        if first < data.len() {
            buf[..data.len() - first].copy_from_slice(&data[first..]);
        }
    }

    /// Copy `dst.len()` bytes from the ring buffer at `offset` (must be <
    /// data_buf_size), wrapping at most once.
    pub fn read_at(&self, offset: usize, dst: &mut [u8]) {
        let buf = self.buffer.lock().unwrap();
        let first = std::cmp::min(dst.len(), self.data_buf_size - offset);
        dst[..first].copy_from_slice(&buf[offset..offset + first]);
        let rest = dst.len() - first;
        if rest > 0 {
            dst[first..].copy_from_slice(&buf[..rest]);
        }
    }

    /// Run `f` on a shared view of the ring buffer, holding the lock for
    /// the duration of the call.
    pub fn with_buffer<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        let buf = self.buffer.lock().unwrap();
        f(&buf)
    }

    /// Run `f` on a mutable view of the ring buffer, holding the lock for
    /// the duration of the call.
    pub fn with_buffer_mut<R>(&self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        let mut buf = self.buffer.lock().unwrap();
        f(&mut buf)
    }
}
