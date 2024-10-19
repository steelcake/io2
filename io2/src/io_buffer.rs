use std::{
    alloc::{AllocError, Allocator, Layout},
    ptr::NonNull,
    rc::Rc,
};

pub struct IoBuffer<A: Allocator> {
    alloc: A,
    ptr: NonNull<u8>,
    layout: Layout,
    size: usize,
}

impl<A: Allocator> IoBuffer<A> {
    pub fn new(layout: Layout, alloc: A) -> Result<Self, AllocError> {
        let mut slice = alloc.allocate_zeroed(layout)?;
        let ptr = unsafe { NonNull::new_unchecked(slice.as_mut().as_mut_ptr()) };
        let size = layout.size();
        let layout = Layout::from_size_align(slice.len(), layout.align()).unwrap();
        Ok(Self {
            alloc,
            ptr,
            layout,
            size,
        })
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    pub fn view(self, offset: usize, len: usize) -> IoBufferView<A> {
        IoBufferView::new(self, offset, len)
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl<A: Allocator> Drop for IoBuffer<A> {
    fn drop(&mut self) {
        unsafe {
            self.alloc.deallocate(self.ptr, self.layout);
        }
    }
}

pub struct IoBufferView<A: Allocator> {
    buf: Rc<IoBuffer<A>>,
    offset: usize,
    len: usize,
}

impl<A: Allocator> IoBufferView<A> {
    pub fn new(buf: IoBuffer<A>, offset: usize, len: usize) -> Self {
        assert!(buf.layout.size() >= offset + len);
        Self {
            buf: Rc::new(buf),
            offset,
            len,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buf.ptr.add(self.offset).as_ptr(), self.len) }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len > 0
    }

    pub fn view(&self, offset: usize, len: usize) -> IoBufferView<A> {
        assert!(self.len >= offset + len);

        IoBufferView {
            buf: self.buf.clone(),
            offset: self.offset + offset,
            len: len,
        }
    }
}
