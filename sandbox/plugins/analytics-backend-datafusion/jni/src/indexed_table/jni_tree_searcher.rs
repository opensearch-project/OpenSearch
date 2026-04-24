/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
JNI-backed tree searcher — calls back into Java's `FilterTreeCallbackBridge`.

Each Collector leaf in the boolean tree gets its own JniTreeShardSearcher,
identified by (context_id, provider_id, leaf_index). The provider_id routes
to the correct IndexFilterTreeProvider registered in the bridge.

```text
Java: FilterTreeCallbackBridge.createContext() → contextId
      FilterTreeCallbackBridge.registerProvider(contextId, providerId, provider, treeContext)
      NativeBridge.executeTreeQuery(..., treeBytes, contextId, ...)

Rust: JniTreeShardSearcher { context_id, provider_id, leaf_index, java_class }
        .collector(seg, min, max)  →  calls FilterTreeCallbackBridge.createCollector
        .collect(min, max)         →  calls FilterTreeCallbackBridge.collectDocs
```
**/

use std::sync::Arc;

use jni::objects::{GlobalRef, JClass, JValue};
use jni::JavaVM;

use super::index::{RowGroupDocsCollector, ShardSearcher};

/// A `ShardSearcher` for a single Collector leaf in the boolean tree.
///
/// Calls back into Java's `FilterTreeCallbackBridge` using the registered
/// context ID, provider ID, and this leaf's index within the provider.
pub struct JniTreeShardSearcher {
    jvm: Arc<JavaVM>,
    context_id: i64,
    provider_id: i32,
    leaf_index: i32,
    class_ref: GlobalRef,
    segment_count: usize,
    segment_max_docs: Vec<i64>,
}

impl JniTreeShardSearcher {
    pub fn new(
        jvm: Arc<JavaVM>,
        context_id: i64,
        provider_id: i32,
        leaf_index: i32,
        class_ref: GlobalRef,
        segment_count: usize,
        segment_max_docs: Vec<i64>,
    ) -> Self {
        Self { jvm, context_id, provider_id, leaf_index, class_ref, segment_count, segment_max_docs }
    }
}

impl std::fmt::Debug for JniTreeShardSearcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniTreeShardSearcher")
            .field("context_id", &self.context_id)
            .field("provider_id", &self.provider_id)
            .field("leaf_index", &self.leaf_index)
            .field("segment_count", &self.segment_count)
            .finish()
    }
}

impl ShardSearcher for JniTreeShardSearcher {
    fn segment_count(&self) -> usize {
        self.segment_count
    }

    fn segment_max_doc(&self, segment_ord: usize) -> Result<i64, String> {
        self.segment_max_docs.get(segment_ord).copied()
            .ok_or_else(|| format!("segment_ord {} out of range ({})", segment_ord, self.segment_count))
    }

    fn collector(
        &self,
        segment_ord: usize,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        let mut env = self.jvm.attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &JClass = self.class_ref.as_obj().into();

        // Call FilterTreeCallbackBridge.createCollector(contextId, providerId, leafIndex, segmentOrd, minDoc, maxDoc)
        let collector_key = env.call_static_method(
            class,
            "createCollector",
            "(JIIIII)I",
            &[
                JValue::Long(self.context_id),
                JValue::Int(self.provider_id),
                JValue::Int(self.leaf_index),
                JValue::Int(segment_ord as i32),
                JValue::Int(doc_min),
                JValue::Int(doc_max),
            ],
        )
        .map_err(|e| format!("createCollector failed: {}", e))?
        .i()
        .map_err(|e| format!("Failed to get int: {}", e))?;

        if collector_key < 0 {
            return Err("No matches in segment (collector_key < 0)".into());
        }

        let collector_class_ref = env.new_global_ref(class)
            .map_err(|e| format!("Failed to create global ref: {}", e))?;

        Ok(Arc::new(JniTreeCollector {
            jvm: Arc::clone(&self.jvm),
            context_id: self.context_id,
            provider_id: self.provider_id,
            leaf_index: self.leaf_index,
            collector_key,
            class_ref: collector_class_ref,
        }))
    }
}

/// A collector backed by Java's `FilterTreeCallbackBridge` for a specific Collector leaf.
struct JniTreeCollector {
    jvm: Arc<JavaVM>,
    context_id: i64,
    provider_id: i32,
    leaf_index: i32,
    collector_key: i32,
    class_ref: GlobalRef,
}

impl std::fmt::Debug for JniTreeCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniTreeCollector")
            .field("context_id", &self.context_id)
            .field("provider_id", &self.provider_id)
            .field("leaf_index", &self.leaf_index)
            .field("collector_key", &self.collector_key)
            .finish()
    }
}

impl Drop for JniTreeCollector {
    fn drop(&mut self) {
        if let Ok(mut env) = self.jvm.attach_current_thread() {
            let class: &JClass = self.class_ref.as_obj().into();
            let _ = env.call_static_method(
                class,
                "releaseCollector",
                "(JIII)V",
                &[
                    JValue::Long(self.context_id),
                    JValue::Int(self.provider_id),
                    JValue::Int(self.leaf_index),
                    JValue::Int(self.collector_key),
                ],
            );
        }
    }
}

impl RowGroupDocsCollector for JniTreeCollector {
    fn collect(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let mut env = self.jvm.attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &JClass = self.class_ref.as_obj().into();

        // Call FilterTreeCallbackBridge.collectDocs(contextId, providerId, leafIndex, collectorKey, minDoc, maxDoc)
        let result = env.call_static_method(
            class,
            "collectDocs",
            "(JIIIII)[J",
            &[
                JValue::Long(self.context_id),
                JValue::Int(self.provider_id),
                JValue::Int(self.leaf_index),
                JValue::Int(self.collector_key),
                JValue::Int(min_doc),
                JValue::Int(max_doc),
            ],
        )
        .map_err(|e| format!("collectDocs failed: {}", e))?;

        let array_obj = result.l()
            .map_err(|e| format!("Failed to get array: {}", e))?;

        let long_array = unsafe { jni::objects::JLongArray::from_raw(array_obj.as_raw()) };
        let len = env.get_array_length(&long_array)
            .map_err(|e| format!("Failed to get array length: {}", e))? as usize;

        if len == 0 {
            return Ok(Vec::new());
        }

        let mut buf = vec![0i64; len];
        env.get_long_array_region(&long_array, 0, &mut buf)
            .map_err(|e| format!("Failed to get array region: {}", e))?;

        Ok(buf.iter().map(|&v| v as u64).collect())
    }
}
