use std::{
    alloc::{AllocError, Allocator, Layout},
    cell::RefCell,
    io,
    marker::PhantomData,
    ptr::NonNull,
};

thread_local! {
    static STATE: RefCell<State> = RefCell::new(State::new());
}

struct State {
    alloc: unsafe fn(size: usize) -> io::Result<NonNull<[u8]>>,
    free: unsafe fn(ptr: *mut u8, length: usize) -> io::Result<()>,
    // TODO: do allocation of these vectors with a good strategy instead of using global allocator
    pages: Vec<Page>,
    free_list: Vec<Vec<FreeRange>>,
}

impl State {
    fn new() -> Self {
        let (alloc, free): (unsafe fn(_) -> _, unsafe fn(_, _) -> _) = match std::env::var(
            HUGE_PAGE_SIZE_ENV_VAR_NAME,
        ) {
            Err(e) => {
                log::trace!("failed to read {} from environment: {}\nDefaulting using regular 2MB aligned allocations", HUGE_PAGE_SIZE_ENV_VAR_NAME, e);
                (alloc_2mb, free_wrapper)
            }
            Ok(huge_page_size) => match huge_page_size.as_str() {
                "2MB" => {
                    log::trace!("using explicit 2MB huge pages");
                    (alloc_2mb_explicit, munmap_wrapper)
                }
                "1GB" => {
                    log::trace!("using explicit 1GB huge pages");
                    (alloc_1gb_explicit, munmap_wrapper)
                }
                _ => {
                    log::trace!(
                        "unknown value read from {} in environment: {}. Expected 2MB or 1GB.\nDefaulting using regular 2MB aligned allocations",
                        HUGE_PAGE_SIZE_ENV_VAR_NAME,
                        huge_page_size
                    );
                    (alloc_2mb, free_wrapper)
                }
            },
        };

        Self {
            alloc,
            free,
            pages: Vec::with_capacity(128),
            free_list: Vec::with_capacity(128),
        }
    }
}

#[derive(Clone, Copy)]
struct Page {
    ptr: *mut u8,
    size: usize,
}

#[derive(Clone, Copy)]
struct FreeRange {
    start: *mut u8,
    len: usize,
}

#[derive(Clone, Copy)]
pub struct LocalAlloc {
    _non_send: PhantomData<*mut ()>,
}

impl LocalAlloc {
    #[inline(always)]
    pub fn new() -> Self {
        Self {
            _non_send: PhantomData,
        }
    }
}

unsafe impl Allocator for LocalAlloc {
    fn allocate(&self, layout: Layout) -> Result<NonNull<[u8]>, AllocError> {
        if layout.align() > TWO_MB {
            return Err(AllocError);
        }

        STATE.with_borrow_mut(|state| {
            for free_ranges in state.free_list.iter_mut() {
                let mut found = None;
                for (idx, range) in free_ranges.iter_mut().enumerate() {
                    let start = range.start.align_offset(layout.align());
                    if range.len >= start + layout.size() {
                        if start == 0 && layout.size() == range.len {
                            found = Some((
                                idx,
                                NonNull::slice_from_raw_parts(
                                    NonNull::new(range.start).unwrap(),
                                    layout.size(),
                                ),
                                (None, None),
                            ));
                        } else {
                            let mut new_ranges = (None, None);
                            unsafe {
                                if start == 0 {
                                    new_ranges.0 = Some(FreeRange {
                                        start: range.start.add(layout.size()),
                                        len: range.len - layout.size(),
                                    });
                                } else {
                                    new_ranges.0 = Some(FreeRange {
                                        start: range.start.add(start),
                                        len: start,
                                    });
                                    if start + layout.size() < range.len {
                                        let offset = start + layout.size();
                                        new_ranges.1 = Some(FreeRange {
                                            start: range.start.add(offset),
                                            len: range.len - offset,
                                        });
                                    }
                                }
                            }
                            found = Some((
                                idx,
                                NonNull::slice_from_raw_parts(
                                    unsafe { NonNull::new(range.start.add(start)).unwrap() },
                                    layout.size(),
                                ),
                                new_ranges,
                            ));
                        }

                        break;
                    }
                }
                if let Some((idx, allocated_slice, new_ranges)) = found {
                    free_ranges.swap_remove(idx);
                    if let Some(x) = new_ranges.0 {
                        free_ranges.push(x);
                    }
                    if let Some(x) = new_ranges.1 {
                        free_ranges.push(x);
                    }
                    return Ok(allocated_slice);
                }
            }

            let page = unsafe {
                match (state.alloc)(layout.size()) {
                    Ok(mut page) => page.as_mut(),
                    Err(e) => {
                        log::trace!("failed to allocate a page: {}", e);
                        return Err(AllocError);
                    }
                }
            };
            let page = Page {
                ptr: page.as_mut_ptr(),
                size: page.len(),
            };
            let free_range = FreeRange {
                start: unsafe { page.ptr.add(layout.size()) },
                len: page.size.checked_sub(layout.size()).unwrap(),
            };
            let mut free_ranges = Vec::with_capacity(16);
            free_ranges.push(free_range);

            state.pages.push(page);
            state.free_list.push(free_ranges);

            Ok(NonNull::slice_from_raw_parts(
                NonNull::new(page.ptr).unwrap(),
                layout.size(),
            ))
        })
    }

    unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        let ptr = ptr.as_ptr();
        let size = layout.size();
        let end_ptr = ptr.add(size.checked_add(1).unwrap());

        STATE.with_borrow_mut(|state| {
            let (page_idx, &page) = state
                .pages
                .iter()
                .enumerate()
                .find(|(_, page)| page.ptr <= ptr && page.ptr.add(page.size) >= ptr.add(size))
                .expect("bad deallocate, couldn't find the page that contains this allocation");
            let free_ranges = state.free_list.get_mut(page_idx).unwrap();

            let mut found = false;
            for free in free_ranges.iter_mut() {
                if free.start == end_ptr {
                    free.start = ptr;
                    free.len += size;
                    found = true;
                }

                if free.start.add(free.len.checked_add(1).unwrap()) == ptr {
                    free.len += size;
                    found = true;
                }
            }

            if !found {
                free_ranges.push(FreeRange {
                    start: ptr,
                    len: size,
                });
            }

            if free_ranges.len() == 1 {
                let range = free_ranges.first().unwrap();
                if range.start == page.ptr && range.len == page.size {
                    state.pages.swap_remove(page_idx);
                    state.free_list.swap_remove(page_idx);
                    unsafe { (state.free)(page.ptr, page.size).expect("free a page") };
                }
            }
        })
    }
}

unsafe fn alloc_2mb(size: usize) -> io::Result<NonNull<[u8]>> {
    let size = size.next_multiple_of(TWO_MB);
    let mut ptr = std::ptr::null_mut();
    match libc::posix_memalign(&mut ptr, TWO_MB, size) {
        0 => {
            match libc::madvise(ptr, size, libc::MADV_HUGEPAGE) {
                0 => (),
                -1 => {
                    let errno = *libc::__errno_location();
                    let err = std::io::Error::from_raw_os_error(errno);
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to madvise: {}", err),
                    ));
                }
                x => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "unexpected return value from madvise: {}. Expected 0 or -1",
                            x
                        ),
                    ));
                }
            }
            Ok(NonNull::slice_from_raw_parts(
                NonNull::new(ptr as *mut u8).unwrap(),
                size,
            ))
        }
        err => Err(io::Error::new(
            io::ErrorKind::Other,
            format!("posix_memalign returned error: {}", err),
        )),
    }
}

unsafe fn alloc_2mb_explicit(size: usize) -> io::Result<NonNull<[u8]>> {
    let size = size.next_multiple_of(TWO_MB);
    malloc_wrapper(size, libc::MAP_HUGE_2MB | libc::MAP_HUGETLB)
}

unsafe fn alloc_1gb_explicit(size: usize) -> io::Result<NonNull<[u8]>> {
    let size = size.next_multiple_of(ONE_GB);
    malloc_wrapper(size, libc::MAP_HUGE_1GB | libc::MAP_HUGETLB)
}

unsafe fn malloc_wrapper(len: usize, huge_page_flag: libc::c_int) -> io::Result<NonNull<[u8]>> {
    match libc::mmap(
        std::ptr::null_mut(),
        len,
        libc::PROT_READ | libc::PROT_WRITE,
        libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | huge_page_flag,
        -1,
        0,
    ) {
        libc::MAP_FAILED => {
            let errno = *libc::__errno_location();
            let err = std::io::Error::from_raw_os_error(errno);
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("mmap returned error: {}", err),
            ))
        }
        ptr => match NonNull::new(ptr as *mut u8) {
            Some(ptr) => Ok(NonNull::slice_from_raw_parts(ptr, len)),
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "mmap returned null pointer",
            )),
        },
    }
}

unsafe fn munmap_wrapper(ptr: *mut u8, length: usize) -> io::Result<()> {
    match libc::munmap(ptr as *mut libc::c_void, length) {
        0 => Ok(()),
        -1 => {
            let errno = *libc::__errno_location();
            let err = std::io::Error::from_raw_os_error(errno);
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("failed to free memory: {}", err),
            ))
        }
        x => Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "unexpected return value from munmap: {}. Expected 0 or -1",
                x
            ),
        )),
    }
}

unsafe fn free_wrapper(ptr: *mut u8, _length: usize) -> io::Result<()> {
    libc::free(ptr as *mut libc::c_void);
    Ok(())
}

const ONE_GB: usize = 1024 * 1024 * 1024;
const TWO_MB: usize = 2 * 1024 * 1024;
const HUGE_PAGE_SIZE_ENV_VAR_NAME: &str = "LOCAL_ALLOC_HUGE_PAGE_SIZE";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn check_thp_allocation() {
        // Run this and execute `grep -i huge /proc/meminfo` in a terminal to see if it is really allocating anon(transparent) huge pages
        // Kernel doesn't really allocate all of this memory or it doesn't make all of it hugepages which is normal. It should at least allocate some.
        let mut ptrs = Vec::new();
        for _ in 0..100 {
            ptrs.push(unsafe { alloc_2mb(16 * 1024 * 1024).unwrap() });
        }
        // touch some memory so it allocates more
        for p in ptrs.iter_mut() {
            unsafe {
                *p.as_mut().as_mut_ptr().add(1024 * 1024 * 16 - 1) = 31;
            };
            unsafe {
                *p.as_mut().as_mut_ptr().add(1024 * 1024 * 12 - 1) = 31;
            };
        }
        loop {
            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    }
}
