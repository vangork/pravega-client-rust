use libc::c_char;
use pravega_client::error::Error;
use pravega_client::event::writer::EventWriter;
use pravega_client::util::oneshot_holder::OneShotHolder;
use pravega_client_shared::ScopedStream;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::slice;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::oneshot::error::RecvError;
use tokio::time::timeout;

// The amount of time the python api will wait for the underlying write to be completed.
const TIMEOUT_IN_SECONDS: u64 = 120;

pub struct StreamWriter {
    writer: EventWriter,
    runtime_handle: Handle,
    stream: ScopedStream,
    inflight: OneShotHolder<Error>,
}

impl StreamWriter {
    pub fn new(writer: EventWriter, runtime_handle: Handle, stream: ScopedStream, max_inflight_count: usize,) -> Self {
        StreamWriter {
            writer,
            runtime_handle,
            stream,
            inflight: OneShotHolder::new(max_inflight_count),
        }
    }

    pub fn write_event(&mut self, event: &[u8], routing_key: Option<String>) -> Result<isize, String> {
        // to_vec creates an owned copy of the python byte array object.
        let write_future: tokio::sync::oneshot::Receiver<Result<(), Error>> = match routing_key {
            Option::None => {
                //trace!("Writing a single event with no routing key");
                self.runtime_handle
                    .block_on(self.writer.write_event(event.to_vec()))
            }
            Option::Some(key) => {
                //trace!("Writing a single event for a given routing key {:?}", key);
                self.runtime_handle
                    .block_on(self.writer.write_event_by_routing_key(key, event.to_vec()))
            }
        };
        let _guard = self.runtime_handle.enter();
        let timeout_fut = timeout(
            Duration::from_secs(TIMEOUT_IN_SECONDS),
            self.inflight.add(write_future),
        );

        let result: Result<Result<Result<(), Error>, RecvError>, _> =
            self.runtime_handle.block_on(timeout_fut);
        match result {
            Ok(t) => match t {
                Ok(t1) => match t1 {
                    Ok(_) => Ok(event.len() as isize),
                    Err(e) => Err(format!(
                        "Error observed while writing an event: {:?}",
                        e
                    )),
                },
                Err(e) => Err(format!(
                    "Error observed while writing an event: {:?}",
                    e
                )),
            },
            Err(_) => Err(format!("Write timed out, please check connectivity with Pravega.")),
        }
    }

    pub fn flush(&mut self) -> Result<(), String> {
        for x in self.inflight.drain() {
            let res = self.runtime_handle.block_on(x);
            // fail fast on error.
            if let Err(e) = res {
                return Err(format!(
                    "RecvError observed while flushing events on stream {:?}: {:?}",
                    self.stream,
                    e,
                ));
            } else if let Err(e) = res.unwrap() {
                return Err(format!(
                    "Error observed while flushing events on {:?}: {:?}",
                    self.stream,
                    e,
                ));
            }
        }
        Ok(())
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_write_event(
    writer: *mut StreamWriter,
    ptr: *mut u8,
    len: usize,
    routing_key: *const c_char,
) -> isize {
    let stream_writer = &mut *writer;
    match catch_unwind(AssertUnwindSafe(move || {
        let event = unsafe { Some(slice::from_raw_parts(ptr, len)) };
        let event = event.expect("read event");

        let value = CStr::from_ptr(routing_key).to_str().unwrap().to_owned();
        let routing_key = if value.len() != 0 {
            Some(value)
        } else {
            None
        };

        stream_writer.write_event(event, routing_key)
    })) {
        Ok(result) => {
            if let Ok(val) = result {
                val
            } else {
                -1
            }
        }
        Err(_) => {
            -1
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_flush(writer: *mut StreamWriter) -> bool {
    let stream_writer = &mut *writer;

    if catch_unwind(AssertUnwindSafe(move || stream_writer.flush())).is_err() {
        false
    } else {
        true
    }
}

#[no_mangle]
pub extern "C" fn destroy_stream_writer(writer: *mut StreamWriter) {
    if !writer.is_null() {
        unsafe {
            Box::from_raw(writer);
        }
    }
}
