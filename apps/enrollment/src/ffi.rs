use crate::Node;
use serde_json::{Value, json};
use std::{
    ffi::{CStr, CString, c_char},
    sync::{Mutex, OnceLock},
};

struct Engine {
    runtime: tokio::runtime::Runtime,
    node: Node,
}
static ENGINE: OnceLock<Mutex<Option<Engine>>> = OnceLock::new();

fn command(input: &str) -> anyhow::Result<Value> {
    anyhow::ensure!(input.len() <= 128 * 1024, "Command too large.");
    let command: Value = serde_json::from_str(input)?;
    let mut engine = ENGINE
        .get_or_init(|| Mutex::new(None))
        .lock()
        .map_err(|_| anyhow::anyhow!("Enrollment engine unavailable."))?;
    match command["op"].as_str() {
        Some("open") if engine.is_none() => {
            let directory = command["directory"]
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("Missing private directory."))?;
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()?;
            let node = runtime.block_on(Node::open(directory))?;
            *engine = Some(Engine { runtime, node });
        }
        Some("close") => {
            if let Some(existing) = engine.take() {
                existing.runtime.block_on(existing.node.close());
            }
            return Ok(json!({"closed": true}));
        }
        _ => {}
    }
    let engine = engine
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("Enrollment is paused. Reopen the app."))?;
    if command["op"] == "open" {
        engine.runtime.block_on(engine.node.snapshot())
    } else {
        engine.runtime.block_on(engine.node.command(command))
    }
}

fn response(input: &str) -> String {
    let outcome = std::panic::catch_unwind(|| command(input));
    match outcome {
        Ok(Ok(state)) => json!({"ok": true, "state": state}),
        Ok(Err(error)) => json!({"ok": false, "error": error.to_string()}),
        Err(_) => {
            json!({"ok": false, "error": "Enrollment stopped unexpectedly. Restart the app."})
        }
    }
    .to_string()
}

/// Input is a valid, NUL-terminated UTF-8 string for the duration of this call.
/// Returned memory belongs to Rust; release it with mesh_enrollment_free.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mesh_enrollment_command(input: *const c_char) -> *mut c_char {
    if input.is_null() {
        return std::ptr::null_mut();
    }
    let result = match unsafe { CStr::from_ptr(input) }.to_str() {
        Ok(input) => response(input),
        Err(_) => json!({"ok":false,"error":"Invalid command encoding."}).to_string(),
    };
    CString::new(result).unwrap().into_raw()
}

/// Only pass a pointer returned by mesh_enrollment_command, exactly once.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn mesh_enrollment_free(output: *mut c_char) {
    if !output.is_null() {
        drop(unsafe { CString::from_raw(output) });
    }
}

#[cfg(target_os = "android")]
mod android {
    use super::*;
    use jni::{
        JNIEnv,
        objects::{GlobalRef, JClass, JObject, JString},
        sys::jstring,
    };
    static CONTEXT: OnceLock<GlobalRef> = OnceLock::new();

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_org_meshthing_mobile_EnrollmentNative_initialize(
        env: JNIEnv,
        _class: JClass,
        context: JObject,
    ) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Ok(reference) = env.new_global_ref(context) {
                if let Ok(vm) = env.get_java_vm() {
                    let reference = CONTEXT.get_or_init(|| {
                        unsafe {
                            ndk_context::initialize_android_context(
                                vm.get_java_vm_pointer().cast(),
                                reference.as_obj().as_raw().cast(),
                            );
                        }
                        reference
                    });
                    let _ = reference;
                }
            }
        }));
    }

    #[unsafe(no_mangle)]
    pub extern "system" fn Java_org_meshthing_mobile_EnrollmentNative_command(
        mut env: JNIEnv,
        _class: JClass,
        input: JString,
    ) -> jstring {
        let input = match env.get_string(&input) {
            Ok(value) => value.to_string_lossy().into_owned(),
            Err(_) => return std::ptr::null_mut(),
        };
        env.new_string(response(&input))
            .map(JString::into_raw)
            .unwrap_or(std::ptr::null_mut())
    }
}
