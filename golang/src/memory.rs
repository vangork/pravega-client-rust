use std::mem;
use std::slice;

#[derive(Copy, Clone)]
#[repr(C)]
pub struct Buffer {
    pub ptr: *mut u8,
    pub len: usize,
    pub cap: usize,
}

// this frees memory we released earlier
#[no_mangle]
pub extern "C" fn free_buffer(buf: Buffer) {
    unsafe {
        let _ = buf.consume();
    }
}

impl Buffer {
    // read provides a reference to the included data to be parsed or copied elsewhere
    // data is only guaranteed to live as long as the Buffer
    // (or the scope of the extern "C" call it came from)
    pub fn read(&self) -> Option<&[u8]> {
        if self.is_empty() {
            None
        } else {
            unsafe { Some(slice::from_raw_parts(self.ptr, self.len)) }
        }
    }
    // consume must only be used on memory previously released by from_vec
    // when the Vec is out of scope, it will deallocate the memory previously referenced by Buffer
    pub unsafe fn consume(self) -> Vec<u8> {
        if self.is_empty() {
            return Vec::new();
        }
        let mut v = Vec::from_raw_parts(self.ptr, self.len, self.cap);
        v.shrink_to_fit();
        v
    }

    // this releases our memory to the caller
    pub fn from_vec(mut v: Vec<u8>) -> Self {
        let buf = Buffer {
            ptr: v.as_mut_ptr(),
            len: v.len(),
            cap: v.capacity(),
        };
        mem::forget(v);
        buf
    }

    pub fn is_empty(&self) -> bool {
        self.ptr.is_null() || self.len == 0 || self.cap == 0
    }
}

/// A view into an externally owned byte slice (Go `[]byte`).
/// Use this for the current call only. A view cannot be copied for safety reasons.
/// If you need a copy, use [`ByteSliceView::to_owned`].
///
/// Go's nil value is fully supported, such that we can differentiate between nil and an empty slice.
#[repr(C)]
pub struct ByteSliceView {
    /// True if and only if the byte slice is nil in Go. If this is true, the other fields must be ignored.
    is_nil: bool,
    ptr: *const u8,
    len: usize,
}

impl ByteSliceView {
    /// Provides a reference to the included data to be parsed or copied elsewhere
    /// This is safe as long as the `ByteSliceView` is constructed correctly.
    pub fn read(&self) -> Option<&[u8]> {
        if self.is_nil {
            None
        } else {
            Some(unsafe { slice::from_raw_parts(self.ptr, self.len) })
        }
    }

    /// Creates an owned copy that can safely be stored and mutated.
    #[allow(dead_code)]
    pub fn to_owned(&self) -> Option<Vec<u8>> {
        self.read().map(|slice| slice.to_owned())
    }
}