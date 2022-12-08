use crate::memory::Buffer;
use errno::{set_errno, Errno};

pub fn clear_error() {
    set_errno(Errno(0));
}

pub fn set_error() {
    // TODO: should we set errno to something besides generic 1 always?
    set_errno(Errno(1));
}
