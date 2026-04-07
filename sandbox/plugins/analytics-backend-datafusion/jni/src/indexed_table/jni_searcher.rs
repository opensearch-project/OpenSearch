/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
JNI-backed `ShardSearcher` — wraps a Java-side Weight pointer.

This is the "flipped" version of `LuceneIndexProvider`:
- **Before**: Rust creates the Weight by calling into Java (`create_shard_weight`)
- **Now**: Java creates the Weight and passes the raw pointer to Rust

The `JniShardSearcher` implements `ShardSearcher` by calling back into Java
for scorer creation and doc collection — same JNI methods, just driven from
the Java side.

```text
Java: Weight weight = searcher.createWeight(query);
      long weightPtr = storeWeight(weight);
      nativeExecuteIndexedQuery(weightPtr, segmentInfos, ...);

Rust: JniShardSearcher { weight_ptr, java_class }
        .collector(seg, min, max)  →  calls Java createPartitionScorerFromShard
        .collect(min, max)         →  calls Java getNextRowGroupDocs
```
**/

use std::sync::Arc;

use jni::objects::{GlobalRef, JClass, JValue};
use jni::JavaVM;

use super::index::{RowGroupDocsCollector, ShardSearcher};


/// A `ShardSearcher` backed by a Java-side Weight pointer.
///
/// Created by the JNI entry point when Java passes a pre-built Weight.
/// The Weight pointer is NOT owned — Java manages its lifecycle.
pub struct JniShardSearcher {
    jvm: Arc<JavaVM>,
    weight_ptr: i64,
    class_ref: GlobalRef,
    segment_count: usize,
    segment_max_docs: Vec<i64>,
}

impl JniShardSearcher {
    /// Create from a Java-side Weight pointer.
    ///
    /// `segment_max_docs` is passed from Java (one entry per segment).
    /// The Weight pointer is borrowed — caller (Java) owns it.
    pub fn new(
        jvm: Arc<JavaVM>,
        weight_ptr: i64,
        class_ref: GlobalRef,
        segment_max_docs: Vec<i64>,
    ) -> Self {
        let segment_count = segment_max_docs.len();
        Self {
            jvm,
            weight_ptr,
            class_ref,
            segment_count,
            segment_max_docs,
        }
    }
}

impl std::fmt::Debug for JniShardSearcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniShardSearcher")
            .field("weight_ptr", &self.weight_ptr)
            .field("segment_count", &self.segment_count)
            .finish()
    }
}

impl ShardSearcher for JniShardSearcher {
    fn segment_count(&self) -> usize {
        self.segment_count
    }

    fn segment_max_doc(&self, segment_ord: usize) -> Result<i64, String> {
        self.segment_max_docs
            .get(segment_ord)
            .copied()
            .ok_or_else(|| {
                format!(
                    "segment_ord {} out of range ({})",
                    segment_ord, self.segment_count
                )
            })
    }

    fn collector(
        &self,
        segment_ord: usize,
        doc_min: i32,
        doc_max: i32,
    ) -> Result<Arc<dyn RowGroupDocsCollector>, String> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &JClass = self.class_ref.as_obj().into();

        let scorer_ptr = env
            .call_static_method(
                class,
                "createPartitionScorerFromShard",
                "(JIII)J",
                &[
                    JValue::Long(self.weight_ptr),
                    JValue::Int(segment_ord as i32),
                    JValue::Int(doc_min),
                    JValue::Int(doc_max),
                ],
            )
            .map_err(|e| format!("createPartitionScorerFromShard failed: {}", e))?
            .j()
            .map_err(|e| format!("Failed to get long: {}", e))?;

        if scorer_ptr < 0 {
            return Err("No matches in segment (scorer_ptr < 0)".to_string());
        }

        let scorer_class_ref = env
            .new_global_ref(class)
            .map_err(|e| format!("Failed to create global ref: {}", e))?;

        Ok(Arc::new(JniSegmentCollector {
            jvm: Arc::clone(&self.jvm),
            scorer_ptr,
            class_ref: scorer_class_ref,
        }))
    }
}

/// A `SegmentCollector` backed by a Java-side PartitionScorer pointer.
///
/// The scorer pointer IS owned — dropped via `releasePartitionScorer`.
struct JniSegmentCollector {
    jvm: Arc<JavaVM>,
    scorer_ptr: i64,
    class_ref: GlobalRef,
}

impl std::fmt::Debug for JniSegmentCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JniSegmentCollector")
            .field("scorer_ptr", &self.scorer_ptr)
            .finish()
    }
}

impl Drop for JniSegmentCollector {
    fn drop(&mut self) {
        if let Ok(mut env) = self.jvm.attach_current_thread() {
            let class: &JClass = self.class_ref.as_obj().into();
            let _ = env.call_static_method(
                class,
                "releasePartitionScorer",
                "(J)V",
                &[JValue::Long(self.scorer_ptr)],
            );
        }
    }
}

impl RowGroupDocsCollector for JniSegmentCollector {
    fn collect(&self, min_doc: i32, max_doc: i32) -> Result<Vec<u64>, String> {
        let mut env = self
            .jvm
            .attach_current_thread()
            .map_err(|e| format!("Failed to attach thread: {}", e))?;

        let class: &JClass = self.class_ref.as_obj().into();

        let result = env
            .call_static_method(
                class,
                "getNextRowGroupDocs",
                "(JII)[J",
                &[
                    JValue::Long(self.scorer_ptr),
                    JValue::Int(min_doc),
                    JValue::Int(max_doc),
                ],
            )
            .map_err(|e| format!("getNextRowGroupDocs failed: {}", e))?;

        let array_obj = result
            .l()
            .map_err(|e| format!("Failed to get array: {}", e))?;

        let long_array = unsafe { jni::objects::JLongArray::from_raw(array_obj.as_raw()) };
        let len = env
            .get_array_length(&long_array)
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
