use std::sync::Arc;
use crate::produconsum::Produconsum;

pub struct Fifo {
    pub data_buffer: Arc<std::cell::UnsafeCell<Vec<u8>>>,
    pub data_buf_size: usize,
    pub free_mem_queue: Arc<Produconsum>,
    pub data: Arc<Produconsum>,
}

unsafe impl Send for Fifo {}
unsafe impl Sync for Fifo {}

impl Fifo {
    pub fn new(block_size: usize) -> Self {
        let data_buf_size = block_size * 4096;
        let data_buffer = Arc::new(std::cell::UnsafeCell::new(vec![0u8; data_buf_size]));
        let free_mem_queue = Arc::new(Produconsum::new(data_buf_size, "free mem"));
        free_mem_queue.produce(data_buf_size);
        let data = Arc::new(Produconsum::new(data_buf_size, "receive"));
        Self {
            data_buffer,
            data_buf_size,
            free_mem_queue,
            data,
        }
    }

    pub fn buffer(&self) -> &Vec<u8> {
        unsafe { &*self.data_buffer.get() }
    }

    pub fn buffer_mut(&self) -> &mut Vec<u8> {
        unsafe { &mut *self.data_buffer.get() }
    }

    /// Copy `data` into the ring buffer at `offset` (must be < data_buf_size),
    /// wrapping at most once.
    pub fn write_at(&self, offset: usize, data: &[u8]) {
        let buf = self.buffer_mut();
        let first = std::cmp::min(data.len(), self.data_buf_size - offset);
        buf[offset..offset + first].copy_from_slice(&data[..first]);
        if first < data.len() {
            buf[..data.len() - first].copy_from_slice(&data[first..]);
        }
    }

    /// Copy `dst.len()` bytes from the ring buffer at `offset` (must be <
    /// data_buf_size), wrapping at most once.
    pub fn read_at(&self, offset: usize, dst: &mut [u8]) {
        let buf = self.buffer();
        let first = std::cmp::min(dst.len(), self.data_buf_size - offset);
        dst[..first].copy_from_slice(&buf[offset..offset + first]);
        let rest = dst.len() - first;
        if rest > 0 {
            dst[first..].copy_from_slice(&buf[..rest]);
        }
    }
}
