// Copyright (c) 2026, Decisym, LLC
// Licensed under the BSD 3-Clause License (see LICENSE file in the project root).

//! A counting allocator for the memory contracts.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// The system allocator, counting the bytes allocated now and at the peak.
///
/// A test binary that wants the memory contracts to run installs it as its
/// global allocator and hands it to [`Suite::allocator`](crate::Suite::allocator):
///
/// ```ignore
/// #[global_allocator]
/// static ALLOCATOR: endpoint_kit::PeakAlloc = endpoint_kit::PeakAlloc::new();
/// ```
///
/// It counts every thread in the process, so a measurement is only meaningful
/// while nothing else is running: put the suite in a test binary of its own,
/// with a single test function.
pub struct PeakAlloc {
    current: AtomicUsize,
    peak: AtomicUsize,
}

impl PeakAlloc {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            current: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    /// Bytes allocated and not yet freed.
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Relaxed)
    }

    /// The most bytes allocated at once since the last [`reset_peak`](Self::reset_peak).
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// Starts a new measurement: the peak becomes what is allocated now.
    pub fn reset_peak(&self) {
        self.peak.store(self.current(), Ordering::Relaxed);
    }

    fn grew(&self, bytes: usize) {
        let now = self.current.fetch_add(bytes, Ordering::Relaxed) + bytes;
        self.peak.fetch_max(now, Ordering::Relaxed);
    }

    fn shrank(&self, bytes: usize) {
        self.current.fetch_sub(bytes, Ordering::Relaxed);
    }
}

impl Default for PeakAlloc {
    fn default() -> Self {
        Self::new()
    }
}

// SAFETY: every call is forwarded to `System` unchanged; the counters only
// observe the sizes of the allocations that succeed.
unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc`'s contract.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            self.grew(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::alloc_zeroed`'s contract.
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            self.grew(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: the caller upholds `GlobalAlloc::dealloc`'s contract.
        unsafe { System.dealloc(ptr, layout) };
        self.shrank(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: the caller upholds `GlobalAlloc::realloc`'s contract.
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            if new_size >= layout.size() {
                self.grew(new_size - layout.size());
            } else {
                self.shrank(layout.size() - new_size);
            }
        }
        new_ptr
    }
}
