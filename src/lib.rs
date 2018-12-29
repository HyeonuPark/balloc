
use std::alloc::{GlobalAlloc, Layout, System};
use std::mem::{self, ManuallyDrop};
use std::ptr::{self, NonNull};
use std::cell::Cell;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::ops::Deref;

use memmap::MmapMut;

const USIZE_MAX: usize = !0;
const U16_MAX: u16 = !0;

const PAGE_SIZE: usize = 4096;
const PAGE_MASK: usize = !(PAGE_SIZE - 1);

const HEAD_SIZE: usize = mem::size_of::<Head>();
const HEAD_OWNED_SIZE: usize = mem::size_of::<HeadOwned>();
const PAGE_BUF_SIZE: usize = PAGE_SIZE - HEAD_SIZE - HEAD_OWNED_SIZE;

const HEAD_OFFSET: isize = (PAGE_SIZE - HEAD_SIZE) as isize;
const HEAD_OWNED_OFFSET: isize = (PAGE_SIZE - HEAD_SIZE - HEAD_OWNED_SIZE) as isize;

const UNIT_SIZE: usize = 8;
const UNIT_PER_PAGE: usize = PAGE_BUF_SIZE / UNIT_SIZE;

const MAX_SMALL_SLOT: usize = 64;
const MAX_ALLOC_SIZE: usize = MAX_SMALL_SLOT * UNIT_SIZE;

pub struct Balloc {
    fallback: System,
}

macro_rules! array_64 {
    ($elem:expr) => ([
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
        $elem, $elem, $elem, $elem, $elem, $elem, $elem, $elem,
    ]);
}

thread_local! {
    static LOCAL: [Cell<Option<Page>>; MAX_SMALL_SLOT] = array_64!(Cell::new(None));
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PageRef {
    start: NonNull<u8>,
}

#[derive(Debug)]
struct Page {
    page: PageRef,
}

#[derive(Debug)]
struct Head {
    slot_size: u8,
    is_owned: AtomicBool,
    next_free: AtomicUsize,
}

#[derive(Debug)]
struct HeadOwned {
    length: u16,
    next_free: u16,
    handle: ManuallyDrop<MmapMut>,
}

impl Deref for Page {
    type Target = PageRef;

    fn deref(&self) -> &PageRef {
        &self.page
    }
}

impl PageRef {
    fn of(ptr: *mut u8) -> Self {
        let start = ptr as usize & PAGE_MASK;
        let start = unsafe { NonNull::new_unchecked(start as *mut u8) };

        PageRef {
            start,
        }
    }

    fn head(&self) -> &Head {
        unsafe {
            let head = self.start.as_ptr().offset(HEAD_OFFSET) as *const Head;
            &*head
        }
    }

    fn max_len(&self) -> u16 {
        (PAGE_BUF_SIZE / self.head().slot_size as usize) as u16
    }
}

impl Page {
    fn new(slot_size: u8) -> Self {
        assert_eq!(mem::align_of::<usize>(), mem::align_of::<Head>());
        assert_eq!(mem::align_of::<usize>(), mem::align_of::<HeadOwned>());

        let mut handle = MmapMut::map_anon(PAGE_SIZE).expect("OOM");
        let start = handle.as_mut_ptr();

        let head = Head {
            slot_size,
            is_owned: AtomicBool::new(true),
            next_free: AtomicUsize::new(USIZE_MAX),
        };
        let head_owned = HeadOwned {
            length: 0,
            next_free: U16_MAX,
            handle: ManuallyDrop::new(handle),
        };

        unsafe {
            ptr::write(start.offset(HEAD_OFFSET) as *mut Head, head);
            ptr::write(start.offset(HEAD_OWNED_OFFSET) as *mut HeadOwned, head_owned);
        }

        let page = PageRef { start: unsafe { NonNull::new_unchecked(start) } };
        Page { page }
    }

    fn head_owned(&mut self) -> &mut HeadOwned {
        unsafe {
            let head = self.page.start.as_ptr().offset(HEAD_OWNED_OFFSET) as *mut HeadOwned;
            &mut *head
        }
    }

    fn release(self) {
        self.head().is_owned.store(false, Ordering::Release);
    }

    fn unmap(mut self) {
        unsafe {
            let _: MmapMut = ptr::read_volatile(&mut *self.head_owned().handle);
        }
    }
}

unsafe impl GlobalAlloc for Balloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() > MAX_ALLOC_SIZE {
            return self.fallback.alloc(layout)
        }

        let slot_size = get_slot_size(layout.size());
        let page = LOCAL.with(|local| local[slot_size].take())
            .unwrap_or_else(|| Page::new(slot_size as u8));

        ptr::null_mut()
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        self.fallback.dealloc(ptr, layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if layout.size() > MAX_ALLOC_SIZE {
            self.fallback.alloc_zeroed(layout)
        } else {
            let result = self.alloc(layout);
            if result.is_null() { return result }
            ptr::write_bytes(result, 0, layout.size());
            result
        }
    }

    unsafe fn realloc(&self, prev: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if layout.size() > MAX_ALLOC_SIZE {
            self.fallback.realloc(prev, layout, new_size)
        } else if get_slot_size(layout.size()) == get_slot_size(new_size) {
            prev
        } else {
            self.dealloc(prev, layout);
            self.alloc(Layout::from_size_align_unchecked(new_size, layout.align()))
        }
    }
}

fn get_slot_size(size: usize) -> usize {
    size / UNIT_SIZE + if size % UNIT_SIZE == 0 {0} else {1}
}
