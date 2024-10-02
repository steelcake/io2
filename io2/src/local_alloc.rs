use std::{
    alloc::{AllocError, Allocator, Layout},
    cell::RefCell,
    marker::PhantomData,
    ptr::NonNull,
};

thread_local! {
    static PAGES: RefCell<Vec<Page>> = RefCell::new(Vec::with_capacity(256));
}

const ALIGN: usize = 2 * 1024 * 1024;

struct Page {
    ptr: *mut u8,
    layout: Layout,
    free_list: Vec<FreeRange>,
}

#[derive(Clone, Copy)]
struct FreeRange {
    start: usize,
    len: usize,
}

#[derive(Clone, Copy)]
pub struct LocalAlloc {
    _marker: PhantomData<*mut u8>,
}

impl LocalAlloc {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

unsafe impl Allocator for LocalAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        PAGES.with_borrow_mut(|pages| {
            for page in pages.iter_mut() {
                let mut alloc = None;
                for (page_idx, range) in page.free_list.iter().enumerate() {
                    let alloc_start =
                        (page.ptr as usize + range.start).next_multiple_of(layout.align());
                    let alloc_start = alloc_start - page.ptr as usize;
                    if alloc_start + layout.size() > range.start + range.len {
                        continue;
                    }

                    alloc = Some((page_idx, alloc_start));
                    break;
                }

                if let Some(alloc) = alloc {
                    let mut range = *page.free_list.get(alloc.0).unwrap();
                    if alloc.1 == range.start {
                        range.start += layout.size();
                        range.len -= layout.size();
                    } else {
                        let new_len = alloc.1 - range.start;
                        page.free_list.push(FreeRange {
                            start: alloc.1 + layout.size(),
                            len: range.len - new_len - layout.size(),
                        });
                        range.len = new_len;
                    }
                    page.free_list[alloc.0] = range;

                    unsafe {
                        return Ok(NonNull::new_unchecked(std::ptr::slice_from_raw_parts_mut(
                            page.ptr.add(alloc.1),
                            layout.size(),
                        )));
                    }
                }
            }

            let align = ALIGN.next_multiple_of(layout.align());
            let size = ALIGN.next_multiple_of(layout.size());
            let page_layout = Layout::from_size_align(size, align).unwrap();
            let ptr = unsafe { std::alloc::alloc(page_layout) };
            let mut free_list = Vec::with_capacity(32);
            free_list.push(FreeRange {
                start: layout.size(),
                len: size - layout.size(),
            });

            pages.push(Page {
                ptr,
                layout: page_layout,
                free_list,
            });

            unsafe {
                Ok(NonNull::new_unchecked(std::ptr::slice_from_raw_parts_mut(
                    ptr,
                    layout.size(),
                )))
            }
        })
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: Layout) {
        let ptr = ptr.as_ptr();
        PAGES.with_borrow_mut(|pages| {
            for page in pages.iter_mut() {
                if page.ptr <= ptr && page.ptr.add(page.layout.size()) >= ptr.add(layout.size()) {
                    let start = ptr.sub(page.ptr as usize) as usize;
                    let end = start + layout.size();
                    let mut found = false;
                    dbg!((start, end, page.layout.size()));
                    for range in page.free_list.iter_mut() {
                        if start == range.start + range.len {
                            range.len += layout.size();
                            found = true;
                        } else if end == range.start {
                            range.start -= layout.size();
                            found = true;
                        }
                    }
                    if !found {
                        page.free_list.push(FreeRange {
                            start: start,
                            len: layout.size(),
                        });
                    }

                    return;
                }
            }

            panic!("bad deallocate");
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send() {
        let mut v = Vec::new_in(LocalAlloc::new());
        v.push(3i32);
        std::thread::spawn(move || {
            v.push(2i32);
        });
    }
}
