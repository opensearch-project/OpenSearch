use std::future::{IntoFuture, Ready};

use arrow::array::{
    Array, ArrayData, ArrayRef, BinaryViewArray, BooleanArray, StringViewArray, make_array,
};
use arrow::buffer::BooleanBuffer;

use super::cached_batch::CacheEntry;
use super::core::{CacheFull, LiquidCache};
use super::policies::{CachePolicy, SqueezePolicy, TranscodeEvict};
use super::{EntryID, LiquidExpr, LiquidPolicy};
use crate::sync::Arc;

/// Builder for [LiquidCache].
///
/// Example:
/// ```rust
/// use liquid_cache::cache::LiquidCacheBuilder;
/// use liquid_cache::cache_policies::LiquidPolicy;
///
/// let _storage = LiquidCacheBuilder::new()
///     .with_batch_size(8192)
///     .with_max_memory_bytes(1024 * 1024 * 1024)
///     .with_cache_policy(Box::new(LiquidPolicy::new()))
///     .build();
/// ```
pub struct LiquidCacheBuilder {
    batch_size: usize,
    max_memory_bytes: usize,
    cache_policy: Box<dyn CachePolicy>,
    squeeze_policy: Box<dyn SqueezePolicy>,
}

impl Default for LiquidCacheBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Default max memory when none is provided: 1 GiB.
const DEFAULT_MAX_MEMORY_BYTES: usize = 1 << 30;

impl LiquidCacheBuilder {
    /// Create a new instance of [LiquidCacheBuilder].
    pub fn new() -> Self {
        Self {
            batch_size: 8192,
            max_memory_bytes: DEFAULT_MAX_MEMORY_BYTES,
            cache_policy: Box::new(LiquidPolicy::new()),
            squeeze_policy: Box::new(TranscodeEvict),
        }
    }

    /// Set the batch size for the cache.
    /// Default is 8192.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the max memory bytes for the cache.
    /// Default is 1 GiB.
    pub fn with_max_memory_bytes(mut self, max_memory_bytes: usize) -> Self {
        self.max_memory_bytes = max_memory_bytes;
        self
    }

    /// Set the cache policy for the cache.
    /// Default is [LiquidPolicy].
    pub fn with_cache_policy(mut self, policy: Box<dyn CachePolicy>) -> Self {
        self.cache_policy = policy;
        self
    }

    /// Set the squeeze policy for the cache.
    /// Default is [TranscodeEvict].
    pub fn with_squeeze_policy(mut self, policy: Box<dyn SqueezePolicy>) -> Self {
        self.squeeze_policy = policy;
        self
    }

    /// Build the cache storage.
    ///
    /// The cache storage is wrapped in an [Arc] to allow for concurrent access.
    pub fn build(self) -> Arc<LiquidCache> {
        Arc::new(LiquidCache::new(
            self.batch_size,
            self.max_memory_bytes,
            self.squeeze_policy,
            self.cache_policy,
        ))
    }
}

/// Builder returned by [`LiquidCache::insert`] for configuring cache writes.
#[derive(Debug)]
pub struct Insert<'a> {
    pub(super) storage: &'a LiquidCache,
    pub(super) entry_id: EntryID,
    pub(super) batch: ArrayRef,
    pub(super) skip_gc: bool,
}

impl<'a> Insert<'a> {
    pub(super) fn new(storage: &'a LiquidCache, entry_id: EntryID, batch: ArrayRef) -> Self {
        Self {
            storage,
            entry_id,
            batch,
            skip_gc: false,
        }
    }

    /// Skip garbage collection of view arrays.
    pub fn with_skip_gc(mut self) -> Self {
        self.skip_gc = true;
        self
    }

    /// Insert the batch into the cache.
    pub fn execute(self) -> Result<(), CacheFull> {
        let batch = if self.skip_gc {
            self.batch.clone()
        } else {
            maybe_gc_view_arrays(&self.batch).unwrap_or_else(|| self.batch.clone())
        };
        let batch = CacheEntry::memory_arrow(batch);
        self.storage.insert_inner(self.entry_id, batch)
    }
}

impl<'a> IntoFuture for Insert<'a> {
    type Output = Result<(), CacheFull>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.execute())
    }
}

/// Builder returned by [`LiquidCache::get`] for configuring cache reads.
#[derive(Debug)]
pub struct Get<'a> {
    pub(super) storage: &'a LiquidCache,
    pub(super) entry_id: &'a EntryID,
    pub(super) selection: Option<&'a BooleanBuffer>,
}

impl<'a> Get<'a> {
    pub(super) fn new(storage: &'a LiquidCache, entry_id: &'a EntryID) -> Self {
        Self {
            storage,
            entry_id,
            selection: None,
        }
    }

    /// Attach a selection bitmap used to filter rows prior to materialization.
    pub fn with_selection(mut self, selection: &'a BooleanBuffer) -> Self {
        self.selection = Some(selection);
        self
    }

    /// Materialize the cached array as [`ArrayRef`].
    pub fn read(self) -> Option<ArrayRef> {
        self.storage.observer().on_get(self.selection.is_some());
        self.storage.read_arrow_array(self.entry_id, self.selection)
    }
}

impl<'a> IntoFuture for Get<'a> {
    type Output = Option<ArrayRef>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.read())
    }
}

/// Recursively garbage collects view arrays (BinaryView/StringView) within an array tree.
fn maybe_gc_view_arrays(array: &ArrayRef) -> Option<ArrayRef> {
    if let Some(binary_view) = array.as_any().downcast_ref::<BinaryViewArray>() {
        return Some(Arc::new(binary_view.gc()));
    }
    if let Some(utf8_view) = array.as_any().downcast_ref::<StringViewArray>() {
        return Some(Arc::new(utf8_view.gc()));
    }

    let data = array.to_data();
    if data.child_data().is_empty() {
        return None;
    }

    let mut changed = false;
    let mut children: Vec<ArrayData> = Vec::with_capacity(data.child_data().len());
    for child in data.child_data() {
        let child_array = make_array(child.clone());
        if let Some(gc_child) = maybe_gc_view_arrays(&child_array) {
            changed = true;
            children.push(gc_child.to_data());
        } else {
            children.push(child.clone());
        }
    }

    if !changed {
        return None;
    }

    let new_data = data.into_builder().child_data(children).build().ok()?;
    Some(make_array(new_data))
}

/// Builder for predicate evaluation on cached data.
#[derive(Debug)]
pub struct EvaluatePredicate<'a> {
    pub(super) storage: &'a LiquidCache,
    pub(super) entry_id: &'a EntryID,
    pub(super) predicate: &'a LiquidExpr,
    pub(super) selection: Option<&'a BooleanBuffer>,
}

impl<'a> EvaluatePredicate<'a> {
    pub(super) fn new(
        storage: &'a LiquidCache,
        entry_id: &'a EntryID,
        predicate: &'a LiquidExpr,
    ) -> Self {
        Self {
            storage,
            entry_id,
            predicate,
            selection: None,
        }
    }

    /// Attach a selection bitmap used to pre-filter rows before predicate evaluation.
    pub fn with_selection(mut self, selection: &'a BooleanBuffer) -> Self {
        self.selection = Some(selection);
        self
    }

    /// Evaluate the predicate against the cached data.
    pub fn read(self) -> Option<BooleanArray> {
        self.storage
            .eval_predicate_internal(self.entry_id, self.selection, self.predicate)
    }
}

impl<'a> IntoFuture for EvaluatePredicate<'a> {
    type Output = Option<BooleanArray>;
    type IntoFuture = Ready<Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        std::future::ready(self.read())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};

    #[test]
    fn insert_gcs_view_arrays_recursively() {
        // Build view arrays then slice to create non-zero offsets (and larger backing buffers).
        let bin = Arc::new(BinaryViewArray::from(vec![
            Some(b"long_prefix_m0" as &[u8]),
            Some(b"m1"),
        ])) as ArrayRef;
        let str_view = Arc::new(StringViewArray::from(vec![
            Some("long_prefix_s0"),
            Some("s1"),
        ])) as ArrayRef;
        let nested_metadata = Arc::new(BinaryViewArray::from(vec![
            Some(b"meta0" as &[u8]),
            Some(b"meta1"),
        ])) as ArrayRef;
        let nested_value = Arc::new(BinaryViewArray::from(vec![
            Some(b"value0" as &[u8]),
            Some(b"value1"),
        ])) as ArrayRef;

        // Slice to keep only the second element so buffers still reference unused bytes.
        let bin_slice = bin.slice(1, 1);
        let str_slice = str_view.slice(1, 1);
        let nested_metadata_slice = nested_metadata.slice(1, 1);
        let nested_value_slice = nested_value.slice(1, 1);

        // Nested struct: metadata (BinaryView), value (BinaryView), and a typed string view.
        let nested_typed_fields = Fields::from(vec![Arc::new(Field::new(
            "typed_str",
            DataType::Utf8View,
            true,
        ))]);
        let nested_struct_fields = Fields::from(vec![
            Arc::new(Field::new("metadata", DataType::BinaryView, true)),
            Arc::new(Field::new("value", DataType::BinaryView, true)),
            Arc::new(Field::new(
                "typed_value",
                DataType::Struct(nested_typed_fields.clone()),
                true,
            )),
        ]);
        let nested_struct = Arc::new(StructArray::new(
            nested_struct_fields.clone(),
            vec![
                nested_metadata_slice.clone(),
                nested_value_slice.clone(),
                Arc::new(StructArray::new(
                    nested_typed_fields.clone(),
                    vec![str_slice.clone()],
                    None,
                )) as ArrayRef,
            ],
            None,
        ));

        let root_fields = Fields::from(vec![
            Arc::new(Field::new("bin_view", DataType::BinaryView, true)),
            Arc::new(Field::new("str_view", DataType::Utf8View, true)),
            Arc::new(Field::new(
                "nested",
                DataType::Struct(nested_struct_fields.clone()),
                true,
            )),
        ]);
        let root = Arc::new(StructArray::new(
            root_fields,
            vec![
                bin_slice.clone(),
                str_slice.clone(),
                nested_struct.clone() as ArrayRef,
            ],
            None,
        )) as ArrayRef;

        let pre_size = root.get_array_memory_size();

        let cache = LiquidCacheBuilder::new().build();
        let entry_id = EntryID::from(123usize);
        cache.insert(entry_id, root.clone()).execute().unwrap();

        let stored = cache.get(&entry_id).read().expect("array present");
        let post_size = stored.get_array_memory_size();

        // GC should have compacted the view arrays, reducing memory footprint.
        assert!(post_size < pre_size, "expected gc to reduce memory usage");

        // Validate values are preserved.
        let struct_out = stored
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");

        assert_eq!(struct_out.len(), 1);

        let bin_out = struct_out
            .column_by_name("bin_view")
            .unwrap()
            .as_binary_view();
        assert_eq!(bin_out.value(0), b"m1");

        let str_out = struct_out
            .column_by_name("str_view")
            .unwrap()
            .as_string_view();
        assert_eq!(str_out.value(0), "s1");

        let nested_out = struct_out.column_by_name("nested").unwrap().as_struct();
        let meta_out = nested_out
            .column_by_name("metadata")
            .unwrap()
            .as_binary_view();
        assert_eq!(meta_out.value(0), b"meta1");

        let val_out = nested_out.column_by_name("value").unwrap().as_binary_view();
        assert_eq!(val_out.value(0), b"value1");

        let typed_out = nested_out
            .column_by_name("typed_value")
            .unwrap()
            .as_struct();
        let typed_str_out = typed_out
            .column_by_name("typed_str")
            .unwrap()
            .as_string_view();
        assert_eq!(typed_str_out.value(0), "s1");
    }
}
