use jni::{JNIEnv, JavaVM};
use std::sync::OnceLock;

static JAVA_VM: OnceLock<JavaVM> = OnceLock::new();

/// Initialize the logger with the JVM instance
pub fn init_logger(jvm: JavaVM) {
    JAVA_VM.set(jvm).ok();
}

/// Log an info message through JNI callback to Java
pub fn log_info(message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            call_java_logger(&mut env, "logInfo", message);
        }
    }
}

/// Log a warning message through JNI callback to Java
pub fn log_warn(message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            call_java_logger(&mut env, "logWarn", message);
        }
    }
}

/// Log an error message through JNI callback to Java
pub fn log_error(message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            call_java_logger(&mut env, "logError", message);
        }
    }
}

/// Log a debug message through JNI callback to Java
pub fn log_debug(message: &str) {
    if let Some(jvm) = JAVA_VM.get() {
        if let Ok(mut env) = jvm.attach_current_thread() {
            call_java_logger(&mut env, "logDebug", message);
        }
    }
}

/// Internal function to call the Java logger method
fn call_java_logger(env: &mut JNIEnv, method_name: &str, message: &str) {
    let result = (|| -> Result<(), Box<dyn std::error::Error>> {
        // Find the RustLoggerBridge class
        let class = env.find_class("com/parquet/parquetdataformat/bridge/RustLoggerBridge")?;

        // Convert Rust string to Java string
        let java_message = env.new_string(message)?;

        // Call the static method
        env.call_static_method(
            class,
            method_name,
            "(Ljava/lang/String;)V",
            &[(&java_message).into()],
        )?;

        Ok(())
    })();

    // If logging fails, fall back to println as last resort
    if result.is_err() {
        println!("[RUST_LOG_FALLBACK] {}: {}", method_name, message);
    }
}

/// Macro for easy info logging
#[macro_export]
macro_rules! rust_log_info {
    ($($arg:tt)*) => {
        $crate::logger::log_info(&format!($($arg)*))
    };
}

/// Macro for easy warning logging
#[macro_export]
macro_rules! rust_log_warn {
    ($($arg:tt)*) => {
        $crate::logger::log_warn(&format!($($arg)*))
    };
}

/// Macro for easy error logging
#[macro_export]
macro_rules! rust_log_error {
    ($($arg:tt)*) => {
        $crate::logger::log_error(&format!($($arg)*))
    };
}

/// Macro for easy debug logging
#[macro_export]
macro_rules! rust_log_debug {
    ($($arg:tt)*) => {
        $crate::logger::log_debug(&format!($($arg)*))
    };
}
