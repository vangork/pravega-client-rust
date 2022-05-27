use libc::c_char;
use pravega_client::client_factory::ClientFactory;
use pravega_client_config::ClientConfigBuilder;
use pravega_client_shared::*;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use crate::error::{set_error, clear_error};
use crate::memory::Buffer;
use crate::stream_writer::StreamWriter;

pub struct StreamManager {
    cf: ClientFactory,
}

impl StreamManager {
    fn new(
        controller_uri: &str,
    ) -> Self {
        // make sure the error number is 0 during startup
        clear_error();

        let mut builder = ClientConfigBuilder::default();
        builder
            .controller_uri(controller_uri)
            .is_auth_enabled(false)
            .is_tls_enabled(false);
        let config = builder.build().expect("creating config");
        let client_factory = ClientFactory::new(config.clone());

        StreamManager {
            cf: client_factory,
        }
    }

    pub fn create_scope(&self, scope_name: &str) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();
    
        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());
    
        handle.block_on(controller.create_scope(&scope_name)).map_err(|e| format!("{:?}", e))
    }

    pub fn create_stream(
        &self,
        scope_name: &str,
        stream_name: &str,
        initial_segments: i32,
    ) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();
        let stream_cfg = StreamConfiguration {
            scoped_stream: ScopedStream {
                scope: Scope::from(scope_name.to_string()),
                stream: Stream::from(stream_name.to_string()),
            },
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
            retention: Default::default(),
            tags: None,
        };
        let controller = self.cf.controller_client();
        handle.block_on(controller.create_stream(&stream_cfg)).map_err(|e| format!("{:?}", e))
    }

    pub fn create_writer(
        &self,
        scope_name: &str,
        stream_name: &str,
        max_inflight_events: usize,
    ) -> StreamWriter {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };
        StreamWriter::new(
            self.cf.create_event_writer(scoped_stream.clone()),
            self.cf.runtime_handle(),
            scoped_stream,
            max_inflight_events,
        )
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_new(uri: *const c_char, err: Option<&mut Buffer>) -> *mut StreamManager {
    let raw = CStr::from_ptr(uri);

    let uri_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse uri".to_string(), err);
            return ptr::null_mut();
        }
    };

    match catch_unwind(|| StreamManager::new(uri_as_str)) {
        Ok(manager) => Box::into_raw(Box::new(manager)),
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn stream_manager_destroy(manager: *mut StreamManager) {
    if !manager.is_null() {
        unsafe {
            Box::from_raw(manager);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_scope(manager: *const StreamManager, scope: *const c_char, err: Option<&mut Buffer>) -> bool {
    let raw = CStr::from_ptr(scope);

    let scope_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return false;
        },
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || stream_manager.create_scope(scope_as_str))) {
        Ok(result) => {
            match result {
                Ok(val) => val,
                Err(e) => {
                    set_error(e, err);
                    false
                }
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_stream(manager: *const StreamManager, scope: *const c_char, stream: *const c_char, num: i32, err: Option<&mut Buffer>) -> bool {
    let raw = CStr::from_ptr(scope);
    let scope_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return false;
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse stream".to_string(), err);
            return false;
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || stream_manager.create_stream(scope_as_str, stream_as_str, num))) {
        Ok(result) => {
            match result {
                Ok(val) => val,
                Err(e) => {
                    set_error(e, err);
                    false
                }
            }
        },
        Err(_) => {
            set_error("caught panic".to_string(), err);
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_writer_new(manager: *const StreamManager, scope: *const c_char, stream: *const c_char, max_inflight_events: usize, err: Option<&mut Buffer>) -> *mut StreamWriter {
    let raw = CStr::from_ptr(scope);
    let scope_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse scope".to_string(), err);
            return ptr::null_mut();
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_as_str = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            set_error("failed to parse stream".to_string(), err);
            return ptr::null_mut();
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || stream_manager.create_writer(scope_as_str, stream_as_str, max_inflight_events))) {
        Ok(writer) => Box::into_raw(Box::new(writer)),
        Err(_) => {
            set_error("caught panic".to_string(), err);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn stream_writer_destroy(writer: *mut StreamWriter) {
    if !writer.is_null() {
        unsafe {
            Box::from_raw(writer);
        }
    }
}

pub struct StreamScalingPolicy {
    scaling: Scaling,
}

impl StreamScalingPolicy {
    fn fixed_scaling_policy(initial_segments: i32) -> StreamScalingPolicy {
        StreamScalingPolicy {
            scaling: Scaling {
                scale_type: ScaleType::FixedNumSegments,
                target_rate: 0,
                scale_factor: 0,
                min_num_segments: initial_segments,
            },
        }
    }
}

#[no_mangle]
pub extern "C" fn fixed_scaling_policy(num: i32) -> *mut StreamScalingPolicy {
    if num <= 0 {
        return ptr::null_mut();
    }

    let policy = StreamScalingPolicy::fixed_scaling_policy(num);
    Box::into_raw(Box::new(policy))
}

#[no_mangle]
pub extern "C" fn scaling_policy_destroy(policy: *mut StreamScalingPolicy) {
    if !policy.is_null() {
        unsafe {
            Box::from_raw(policy);
        }
    }
}
