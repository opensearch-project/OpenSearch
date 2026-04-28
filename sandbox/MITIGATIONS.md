# OpenSearch Sandbox - Security Mitigations Implementation Guide

**Date:** April 19, 2026  
**Priority:** Critical mitigations for immediate implementation

---

## 1. Fix Silent Ref Count Underflow (CRITICAL)

### Current Issue
```rust
// VULNERABLE CODE
pub fn release(&self) {
    self.active_reads
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            if current <= 0 {
                None  // ⚠️ Silently ignores underflow
            } else {
                Some(current - 1)
            }
        })
        .ok();  // Discards error
}
```

### Fixed Implementation
```rust
// SECURE CODE
pub fn release(&self) {
    let prev = self.active_reads.fetch_sub(1, Ordering::AcqRel);
    
    #[cfg(debug_assertions)]
    {
        assert!(prev > 0, "BUG: release() called with ref_count={} for path={:?}", 
                prev, self.remote_path);
    }
    
    #[cfg(not(debug_assertions))]
    {
        if prev <= 0 {
            log_error!(
                "BUG: release() called with ref_count={} for path={:?}. \
                 This indicates unbalanced acquire/release calls.",
                prev, self.remote_path
            );
            // Restore to prevent negative ref_count
            self.active_reads.store(0, Ordering::Release);
        }
    }
}
```

### Also Update acquire() for Better Performance
```rust
// IMPROVED: Use AcqRel instead of SeqCst
pub fn acquire(&self) {
    self.active_reads.fetch_add(1, Ordering::AcqRel);  // Was SeqCst
}

pub fn ref_count(&self) -> i64 {
    self.active_reads.load(Ordering::Acquire)  // Was SeqCst
}
```

---

## 2. Add FFM Pointer Validation (CRITICAL)

### Current Issue
```rust
// VULNERABLE CODE
#[no_mangle]
pub extern "C" fn ts_destroy_tiered_object_store(ptr: i64) -> i64 {
    if ptr == 0 {
        return Err("null pointer".to_string());
    }
    let _store = unsafe { Arc::from_raw(ptr as *const TieredObjectStore) };
    Ok(0)
}
```

### Fixed Implementation
```rust
// SECURE CODE
const FFM_SUCCESS: i64 = 0;
const FFM_ERR_NULL_PTR: i64 = -1;
const FFM_ERR_INVALID_PTR: i64 = -2;

// Valid pointer range (typical user space on 64-bit systems)
const MIN_VALID_PTR: i64 = 0x1000;
const MAX_VALID_PTR: i64 = 0x7FFF_FFFF_FFFF;

#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_destroy_tiered_object_store(ptr: i64) -> i64 {
    // Validate null pointer
    if ptr == 0 {
        log_error!("FFM: ts_destroy called with null pointer");
        return Err("null pointer (0)".to_string());
    }
    
    // Validate pointer range
    if ptr < MIN_VALID_PTR || ptr > MAX_VALID_PTR {
        log_error!("FFM: ts_destroy called with invalid pointer {:#x}", ptr);
        return Err(format!("invalid pointer {:#x}", ptr));
    }
    
    // Safe to dereference
    let _store = unsafe { Arc::from_raw(ptr as *const TieredObjectStore) };
    log_info!("FFM: destroyed TieredObjectStore ptr={:#x}", ptr);
    Ok(0)
}

// Apply same validation to ts_create
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_object_store() -> i64 {
    let file_registry = Arc::new(TieredStorageRegistry::new());
    let local = Arc::new(object_store::local::LocalFileSystem::new());
    let store = Arc::new(TieredObjectStore::new(file_registry, local));
    let ptr = Arc::into_raw(store) as i64;
    
    // Validate created pointer
    if ptr < MIN_VALID_PTR || ptr > MAX_VALID_PTR {
        log_error!("FFM: ts_create returned invalid pointer {:#x}", ptr);
        return Err("failed to create valid pointer".to_string());
    }
    
    log_info!("FFM: created TieredObjectStore ptr={:#x}", ptr);
    Ok(ptr)
}
```

---

## 3. Add Ref Count Limits (HIGH)

### Implementation
```rust
// Add to TieredFileEntry
const MAX_REF_COUNT: i64 = 10_000;

pub fn acquire(&self) -> Result<(), &'static str> {
    let prev = self.active_reads.fetch_add(1, Ordering::AcqRel);
    
    if prev >= MAX_REF_COUNT {
        // Rollback the increment
        self.active_reads.fetch_sub(1, Ordering::AcqRel);
        log_error!(
            "ref_count limit exceeded for path={:?}: current={}",
            self.remote_path, prev
        );
        return Err("ref_count limit exceeded");
    }
    
    Ok(())
}

// Update ReadGuard::new() to handle error
impl<'a> ReadGuard<'a> {
    pub(crate) fn new(entry: Ref<'a, String, TieredFileEntry>) -> Result<Self, &'static str> {
        entry.value().acquire()?;  // Propagate error
        Ok(Self { entry })
    }
}

// Update FileRegistry::get() signature
fn get(&self, key: &str) -> Result<ReadGuard<'_>, &'static str> {
    let entry = self.files.get(key)
        .ok_or("file not found")?;
    self.acquire_count.fetch_add(1, Ordering::Relaxed);
    ReadGuard::new(entry)
}
```

---

## 4. Add Validation to transition() (HIGH)

### Current Issue
```rust
// VULNERABLE CODE
pub fn transition(&self, path: &str, location: FileLocation, ...) {
    // No validation!
    self.registry.update(path, move |e| {
        e.location = location;
        // ...
    });
}
```

### Fixed Implementation
```rust
// SECURE CODE
pub fn transition(
    &self,
    path: &str,
    location: FileLocation,
    remote_path: Option<String>,
    repo_key: Option<String>,
    store: Option<Arc<dyn ObjectStore>>,
) -> Result<(), FileRegistryError> {
    // Validate required fields for Remote/Both
    if matches!(location, FileLocation::Remote | FileLocation::Both) {
        if remote_path.is_none() {
            return Err(FileRegistryError::InvalidStateTransition {
                path: path.to_string(),
                reason: "remote_path required for Remote/Both location".to_string(),
            });
        }
        if repo_key.is_none() {
            return Err(FileRegistryError::InvalidStateTransition {
                path: path.to_string(),
                reason: "repo_key required for Remote/Both location".to_string(),
            });
        }
        if store.is_none() {
            return Err(FileRegistryError::InvalidStateTransition {
                path: path.to_string(),
                reason: "store required for Remote/Both location".to_string(),
            });
        }
    }
    
    let remote_arc = remote_path.map(Arc::from);
    let repo_arc = repo_key.map(Arc::from);
    
    // Check if entry exists and update atomically
    let updated = self.registry.update(path, move |e| {
        e.location = location;
        e.remote_path = remote_arc;
        e.repo_key = repo_arc;
        e.remote_store = store;
    });
    
    if !updated {
        return Err(FileRegistryError::InvalidStateTransition {
            path: path.to_string(),
            reason: "file not found in registry".to_string(),
        });
    }
    
    log_info!(
        "AUDIT: transition path='{}' to location={:?}",
        path, location
    );
    
    Ok(())
}

// Update FileRegistry::update() to return bool
fn update(&self, key: &str, f: impl FnOnce(&mut TieredFileEntry)) -> bool {
    if let Some(mut entry) = self.files.get_mut(key) {
        f(entry.value_mut());
        true
    } else {
        false
    }
}
```

---

## 5. Add Retry Logic for Remote Stores (HIGH)

### Implementation
```rust
use std::time::Duration;
use tokio::time::sleep;

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

async fn get_opts_with_retry(
    &self,
    location: &Path,
    options: GetOptions,
) -> OsResult<GetResult> {
    let mut attempt = 0;
    let mut last_error = None;
    
    while attempt <= MAX_RETRIES {
        match self.get_opts(location, options.clone()).await {
            Ok(result) => {
                if attempt > 0 {
                    log_info!(
                        "Remote store request succeeded after {} retries for path='{}'",
                        attempt, location
                    );
                }
                return Ok(result);
            }
            Err(e) => {
                last_error = Some(e);
                
                if attempt < MAX_RETRIES {
                    let backoff = INITIAL_BACKOFF_MS * 2_u64.pow(attempt);
                    log_warn!(
                        "Remote store request failed (attempt {}/{}), retrying in {}ms: {}",
                        attempt + 1, MAX_RETRIES + 1, backoff, last_error.as_ref().unwrap()
                    );
                    sleep(Duration::from_millis(backoff)).await;
                }
                
                attempt += 1;
            }
        }
    }
    
    log_error!(
        "Remote store request failed after {} attempts for path='{}'",
        MAX_RETRIES + 1, location
    );
    Err(last_error.unwrap())
}
```

---

## 6. Add Comprehensive Audit Logging (MEDIUM)

### Implementation
```rust
// In TieredStorageRegistry
impl FileRegistry for TieredStorageRegistry {
    fn register(&self, key: &str, value: TieredFileEntry) {
        log_info!(
            "AUDIT: register file='{}' location={:?} remote_path={:?} repo_key={:?}",
            key, value.location(), value.remote_path(), value.repo_key()
        );
        self.files.insert(key.to_string(), value);
    }
    
    fn remove(&self, key: &str, force: bool) -> bool {
        let removed = if force {
            self.files.remove(key).is_some()
        } else {
            match self.files.entry(key.to_string()) {
                Entry::Occupied(entry) => {
                    if entry.get().ref_count() == 0 {
                        entry.remove();
                        true
                    } else {
                        false
                    }
                }
                Entry::Vacant(_) => false,
            }
        };
        
        if removed {
            self.remove_count.fetch_add(1, Ordering::Relaxed);
            log_info!(
                "AUDIT: remove file='{}' force={} result=success",
                key, force
            );
        } else {
            log_warn!(
                "AUDIT: remove file='{}' force={} result=failed (ref_count > 0 or not found)",
                key, force
            );
        }
        
        removed
    }
}

// In FFM bridge
#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_create_tiered_object_store() -> i64 {
    log_info!("AUDIT: FFM call ts_create_tiered_object_store");
    // ... implementation
    log_info!("AUDIT: FFM return ts_create_tiered_object_store ptr={:#x}", ptr);
    Ok(ptr)
}

#[ffm_safe]
#[no_mangle]
pub extern "C" fn ts_destroy_tiered_object_store(ptr: i64) -> i64 {
    log_info!("AUDIT: FFM call ts_destroy_tiered_object_store ptr={:#x}", ptr);
    // ... implementation
    log_info!("AUDIT: FFM return ts_destroy_tiered_object_store result=success");
    Ok(0)
}
```

---

## 7. Document ReadGuard Lock Behavior (MEDIUM)

### Add to trait documentation
```rust
/// File registry trait for tiered storage.
///
/// Implementations must be thread-safe (`Send + Sync`). Ref counting is
/// managed by [`ReadGuard`] — `get()` returns a guard that auto-acquires
/// on creation and auto-releases on drop.
///
/// # ⚠️ IMPORTANT: ReadGuard Lock Behavior
///
/// `ReadGuard` holds a DashMap shard lock for the duration of its lifetime.
/// **DO NOT** hold a `ReadGuard` while performing I/O operations, as this
/// will block other threads from accessing the same shard.
///
/// ## ✅ CORRECT Usage
/// ```rust
/// // Extract data before I/O
/// let (remote_path, store) = {
///     let guard = registry.get("/file.parquet")?;
///     (
///         guard.remote_path().map(String::from),
///         guard.remote_store().map(Arc::clone)
///     )
/// };  // Guard dropped here
///
/// // Now safe to perform I/O
/// if let (Some(path), Some(store)) = (remote_path, store) {
///     let data = store.get(&path).await?;
/// }
/// ```
///
/// ## ❌ INCORRECT Usage
/// ```rust
/// // DON'T DO THIS - holds lock during I/O
/// let guard = registry.get("/file.parquet")?;
/// let store = guard.remote_store().unwrap();
/// let data = store.get(guard.remote_path().unwrap()).await?;  // Lock held!
/// ```
pub trait FileRegistry: Send + Sync {
    /// Get an entry with auto-acquired ref count. Guard releases on drop.
    ///
    /// ⚠️ **WARNING:** Do not hold the returned guard during I/O operations.
    /// Extract needed data and drop the guard before performing I/O.
    fn get(&self, key: &str) -> Option<ReadGuard<'_>>;
    
    // ... rest of trait
}
```

---

## 8. Add Unit Tests for Security

### Test File: `tests/security_tests.rs`
```rust
#[cfg(test)]
mod security_tests {
    use super::*;
    
    #[test]
    fn test_ref_count_underflow_detection() {
        let entry = TieredFileEntry::new(
            FileLocation::Local, None, None, None, None
        );
        
        // Release without acquire should be detected
        entry.release();
        
        // In debug mode, this would panic
        // In release mode, ref_count should be clamped at 0
        assert_eq!(entry.ref_count(), 0);
    }
    
    #[test]
    fn test_ref_count_limit() {
        let entry = TieredFileEntry::new(
            FileLocation::Local, None, None, None, None
        );
        
        // Acquire up to limit
        for _ in 0..MAX_REF_COUNT {
            assert!(entry.acquire().is_ok());
        }
        
        // Next acquire should fail
        assert!(entry.acquire().is_err());
        assert_eq!(entry.ref_count(), MAX_REF_COUNT);
    }
    
    #[test]
    fn test_ffm_pointer_validation() {
        // Null pointer
        assert_eq!(
            ts_destroy_tiered_object_store(0),
            FFM_ERR_NULL_PTR
        );
        
        // Invalid low pointer
        assert_eq!(
            ts_destroy_tiered_object_store(0x100),
            FFM_ERR_INVALID_PTR
        );
        
        // Invalid high pointer
        assert_eq!(
            ts_destroy_tiered_object_store(0x8000_0000_0000),
            FFM_ERR_INVALID_PTR
        );
    }
    
    #[test]
    fn test_transition_validation() {
        let registry = Arc::new(TieredStorageRegistry::new());
        let local = Arc::new(object_store::local::LocalFileSystem::new());
        let store = TieredObjectStore::new(registry, local);
        
        // Register a file
        store.register_file(
            "/file.parquet",
            FileLocation::Local,
            None, None, None
        ).unwrap();
        
        // Transition to Remote without required fields should fail
        assert!(store.transition(
            "/file.parquet",
            FileLocation::Remote,
            None,  // Missing remote_path
            None,  // Missing repo_key
            None,  // Missing store
        ).is_err());
        
        // Transition with all required fields should succeed
        let s3_store = Arc::new(object_store::memory::InMemory::new());
        assert!(store.transition(
            "/file.parquet",
            FileLocation::Remote,
            Some("s3://bucket/file.parquet".to_string()),
            Some("s3-repo".to_string()),
            Some(s3_store),
        ).is_ok());
    }
    
    #[test]
    fn test_concurrent_ref_counting() {
        use std::sync::Barrier;
        use std::thread;
        
        let registry = Arc::new(TieredStorageRegistry::new());
        registry.register("/file", TieredFileEntry::new(
            FileLocation::Local, None, None, None, None
        ));
        
        let num_threads = 16;
        let ops_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));
        
        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let reg = Arc::clone(&registry);
                let bar = Arc::clone(&barrier);
                thread::spawn(move || {
                    bar.wait();
                    for _ in 0..ops_per_thread {
                        if let Some(guard) = reg.get("/file") {
                            // Simulate work
                            std::hint::black_box(guard.location());
                            drop(guard);
                        }
                    }
                })
            })
            .collect();
        
        for h in handles {
            h.join().unwrap();
        }
        
        // All guards dropped, ref_count should be 0
        let guard = registry.get("/file").unwrap();
        assert_eq!(guard.ref_count(), 1); // Only this guard
    }
}
```

---

## 9. Add Metrics and Monitoring

### Metrics Implementation
```rust
use std::sync::atomic::{AtomicU64, Ordering};

pub struct SecurityMetrics {
    pub ref_count_underflows: AtomicU64,
    pub ref_count_limit_exceeded: AtomicU64,
    pub ffm_invalid_pointers: AtomicU64,
    pub transition_validation_failures: AtomicU64,
    pub remote_store_retries: AtomicU64,
}

impl SecurityMetrics {
    pub fn new() -> Self {
        Self {
            ref_count_underflows: AtomicU64::new(0),
            ref_count_limit_exceeded: AtomicU64::new(0),
            ffm_invalid_pointers: AtomicU64::new(0),
            transition_validation_failures: AtomicU64::new(0),
            remote_store_retries: AtomicU64::new(0),
        }
    }
    
    pub fn report(&self) -> String {
        format!(
            "SecurityMetrics {{ \
             ref_count_underflows: {}, \
             ref_count_limit_exceeded: {}, \
             ffm_invalid_pointers: {}, \
             transition_validation_failures: {}, \
             remote_store_retries: {} \
             }}",
            self.ref_count_underflows.load(Ordering::Relaxed),
            self.ref_count_limit_exceeded.load(Ordering::Relaxed),
            self.ffm_invalid_pointers.load(Ordering::Relaxed),
            self.transition_validation_failures.load(Ordering::Relaxed),
            self.remote_store_retries.load(Ordering::Relaxed),
        )
    }
}

// Add to TieredStorageRegistry
pub struct TieredStorageRegistry {
    files: DashMap<String, TieredFileEntry>,
    acquire_count: AtomicU64,
    remove_count: AtomicU64,
    security_metrics: Arc<SecurityMetrics>,  // NEW
}

// Update methods to record metrics
pub fn release(&self) {
    let prev = self.active_reads.fetch_sub(1, Ordering::AcqRel);
    if prev <= 0 {
        SECURITY_METRICS.ref_count_underflows.fetch_add(1, Ordering::Relaxed);
        log_error!("ref_count underflow detected");
    }
}
```

---

## 10. Implementation Checklist

### Phase 1: Critical (This Week)
- [ ] Fix ref_count underflow in `types.rs::TieredFileEntry::release()`
- [ ] Add FFM pointer validation in `ffm.rs`
- [ ] Add ref_count limits in `types.rs::TieredFileEntry::acquire()`
- [ ] Add validation to `tiered_object_store.rs::transition()`
- [ ] Add unit tests for all above

### Phase 2: High Priority (Next Week)
- [ ] Implement retry logic for remote stores
- [ ] Add comprehensive audit logging
- [ ] Document ReadGuard lock behavior
- [ ] Add security metrics
- [ ] Update `FileRegistry::update()` to return bool

### Phase 3: Testing (Week 3)
- [ ] Run all security unit tests
- [ ] Perform stress testing on ref counting
- [ ] Test FFM boundary with invalid pointers
- [ ] Test remote store failure scenarios
- [ ] Review audit logs for completeness

### Phase 4: Documentation (Week 4)
- [ ] Update API documentation
- [ ] Create security best practices guide
- [ ] Document monitoring and alerting
- [ ] Create incident response runbook

---

## Quick Start: Apply Critical Fixes Now

1. **Backup your code:**
```bash
cd ~/workspace/open-source/OpenSearch/sandbox
git checkout -b security-mitigations
```

2. **Apply the fixes** to the relevant files (see sections 1-4 above)

3. **Run tests:**
```bash
cd libs/dataformat-native/rust
cargo test
cargo test --release
```

4. **Commit:**
```bash
git add .
git commit -m "security: implement critical mitigations for tiered storage

- Fix silent ref_count underflow
- Add FFM pointer validation
- Add ref_count limits
- Add transition() validation
- Add comprehensive audit logging

Addresses threats: T-T-01, T-S-01, T-D-01, T-T-02"
```

---

## Need Help?

If you need assistance implementing any of these mitigations:
1. Share the specific file you're working on
2. Let me know which mitigation you want to implement
3. I'll provide the exact code changes needed

**Priority Order:** Implement mitigations 1-4 first (all CRITICAL/HIGH severity).
