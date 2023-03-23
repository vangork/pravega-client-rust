use crate::stream_writer::StreamWriter;
use libc::c_char;
use pravega_client::client_factory::ClientFactory;
use pravega_client_config::*;
use pravega_client_config::credentials::Credentials;
use pravega_client_shared::*;
use std::ffi::CStr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;

pub struct StreamManager {
    cf: ClientFactory,
}

impl StreamManager {
    fn new(
        controller_uri: &str,
        keycloak_file: &str,
        auth_enabled: bool,
        disable_cert_verification: bool,
    ) -> Self {
        let mut builder = ClientConfigBuilder::default();
        builder.controller_uri(controller_uri.to_string());
            
        if keycloak_file.is_empty() {
            builder.is_auth_enabled(auth_enabled)
                .disable_cert_verification(disable_cert_verification);
        } else {
            let credentials = Credentials::keycloak(keycloak_file, disable_cert_verification);
            builder.is_auth_enabled(true)
                .credentials(credentials);
        }
            
        let config = builder.build().expect("creating config");
        let client_factory = ClientFactory::new(config);

        StreamManager {
            cf: client_factory,
        }
    }

    pub fn create_scope(&self, scope_name: &str) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();

        let controller = self.cf.controller_client();
        let scope_name = Scope::from(scope_name.to_string());

        handle
            .block_on(controller.create_scope(&scope_name))
            .map_err(|e| format!("{:?}", e))
    }

    pub fn create_stream_with_config(&self, stream_config: StreamConfiguration) -> Result<bool, String> {
        let handle = self.cf.runtime_handle();
        let controller = self.cf.controller_client();
        handle
            .block_on(controller.create_stream(&stream_config))
            .map_err(|e| format!("{:?}", e))
    }

    pub fn create_stream(
        &self,
        scope_name: &str,
        stream_name: &str,
        initial_segments: i32,
    ) -> Result<bool, String> {
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
        let handle = self.cf.runtime_handle();
        let controller = self.cf.controller_client();
        handle.
            block_on(controller.create_stream(&stream_cfg))
            .map_err(|e| format!("{:?}", e))
    }

    pub fn create_writer(&self, scope_name: &str, stream_name: &str, max_inflight_events: usize) -> StreamWriter {
        let scoped_stream = ScopedStream {
            scope: Scope::from(scope_name.to_string()),
            stream: Stream::from(stream_name.to_string()),
        };

        let writer = self.cf.create_event_writer(scoped_stream.clone());
        let handle = self.cf.runtime_handle();

        StreamWriter::new(writer, handle, scoped_stream, max_inflight_events)
    }

}

#[no_mangle]
pub unsafe extern "C" fn create_stream_manager(
    controller_uri: *const c_char,
    auth_enabled: bool,
    disable_cert_verification: bool,
) -> *mut StreamManager {
    let raw = CStr::from_ptr(controller_uri);
    let controller_uri = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return ptr::null_mut();
        }
    };
    match catch_unwind(|| {
        StreamManager::new(
            controller_uri,
            "",
            auth_enabled,
            disable_cert_verification,
        )
    }) {
        Ok(manager) => {
            Box::into_raw(Box::new(manager))
        }
        Err(_) => {
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn create_stream_manager_with_keyclock(
    controller_uri: *const c_char,
    keycloak_file: *const c_char,
    auth_enabled: bool,
    disable_cert_verification: bool,
) -> *mut StreamManager {
    let controller_uri = {
        let cstr = CStr::from_ptr(controller_uri);
        match cstr.to_str() {
            Ok(s) => s,
            Err(_) => {
                return ptr::null_mut();
            }
        }
    };
    let keycloak_file = {
        let cstr = CStr::from_ptr(keycloak_file);
        match cstr.to_str() {
            Ok(s) => s,
            Err(_) => {
                return ptr::null_mut();
            }
        }
    };
    match catch_unwind(|| {
        StreamManager::new(
            controller_uri,
            keycloak_file,
            auth_enabled,
            disable_cert_verification,
        )
    }) {
        Ok(manager) => {
            Box::into_raw(Box::new(manager))
        }
        Err(_) => {
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn destroy_stream_manager(manager: *mut StreamManager) {
    if !manager.is_null() {
        unsafe {
            Box::from_raw(manager);
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_scope(
    manager: *const StreamManager,
    scope: *const c_char,
) -> bool {
    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return false;
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || stream_manager.create_scope(scope_name))) {
        Ok(result) => match result {
            Ok(val) => val,
            Err(_) => {
                false
            }
        },
        Err(_) => {
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn stream_manager_create_stream(
    manager: *const StreamManager,
    scope: *const c_char,
    stream: *const c_char,
    initial_segments: i32,
) -> bool {
    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return false;
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return false;
        }
    };
    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || {
        stream_manager.create_stream(scope_name, stream_name, initial_segments)
    })) {
        Ok(result) => match result {
            Ok(val) => val,
            Err(_) => {
                false
            }
        },
        Err(_) => {
            false
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn create_stream_writer(
    manager: *const StreamManager,
    scope: *const c_char,
    stream: *const c_char,
    max_inflight_events: usize,
) -> *mut StreamWriter {
    let raw = CStr::from_ptr(scope);
    let scope_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return ptr::null_mut();
        }
    };

    let raw = CStr::from_ptr(stream);
    let stream_name = match raw.to_str() {
        Ok(s) => s,
        Err(_) => {
            return ptr::null_mut();
        }
    };

    let stream_manager = &*manager;
    match catch_unwind(AssertUnwindSafe(move || {
        stream_manager.create_writer(scope_name, stream_name, max_inflight_events)
    })) {
        Ok(writer) => Box::into_raw(Box::new(writer)),
        Err(_) => {
            ptr::null_mut()
        }
    }
}
