use std::{
    alloc::{AllocError, Allocator, Layout},
    ptr::NonNull,
};

pub struct IoBuffer<A: Allocator> {
    alloc: A,
    ptr: NonNull<u8>,
    layout: Layout,
    len: usize,
}

impl<A: Allocator> IoBuffer<A> {
    pub fn new(layout: Layout, alloc: A) -> Result<Self, AllocError> {
        let mut slice = alloc.allocate(layout)?;
        let ptr = unsafe { NonNull::new_unchecked(slice.as_mut().as_mut_ptr()) };
        let len = layout.size();
        let layout = Layout::from_size_align(slice.len(), layout.align()).unwrap();
        Ok(Self {
            alloc,
            ptr,
            layout,
            len,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl<A: Allocator> Drop for IoBuffer<A> {
    fn drop(&mut self) {
        unsafe {
            self.alloc.deallocate(self.ptr, self.layout);
        }
    }
}
