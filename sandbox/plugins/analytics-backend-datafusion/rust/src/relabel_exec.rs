/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Zero-copy schema-relabel ExecutionPlan.
//!
//! Bridges a small gap between the LogicalPlan and physical-plan type systems on the
//! producer side: DataFusion's substrait consumer types `ROW_NUMBER OVER` as `Int64`
//! at the LogicalPlan level (matching what Calcite / isthmus declared on the wire),
//! but the physical `WindowAggExec` for `row_number()` emits `UInt64` regardless. The
//! same divergence affects any `Int8↔UInt8` … `Int64↔UInt64` pair where DataFusion's
//! physical operator chooses an unsigned representation for a Substrait `i64` declaration.
//!
//! Wrapping the physical plan with `RelabelExec` retags each batch's mismatched columns
//! to match the LogicalPlan's declared types. The rebuild is zero-copy: `ArrayData`
//! buffers are shared with the source array; only the type tag flips. Per-batch cost is
//! one `Arc` clone + one `ArrayData::into_builder().data_type(...).build()` per mismatched
//! column — no buffer alloc, no per-row work.
//!
//! Non-bit-compatible mismatches (e.g. cross-family) error at construction; those need
//! a proper `Cast` at the LogicalPlan level rather than a relabel.

use std::fmt;
use std::sync::Arc;

use arrow::array::{make_array, ArrayRef};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use futures::stream::TryStreamExt;

/// Wraps an `ExecutionPlan` and exposes a `target_schema` whose columns differ from the
/// input's only by bit-compatible Int↔UInt sign flips. Per-batch retags via zero-copy
/// `ArrayData` rebuild.
pub struct RelabelExec {
    input: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl fmt::Debug for RelabelExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RelabelExec")
            .field("target_schema", &self.target_schema)
            .finish()
    }
}

impl RelabelExec {
    pub fn try_new(input: Arc<dyn ExecutionPlan>, target_schema: SchemaRef) -> Result<Arc<Self>> {
        validate_bit_compatible(&input.schema(), &target_schema)?;
        let new_eq = EquivalenceProperties::new(Arc::clone(&target_schema));
        let input_props = input.properties();
        let properties = Arc::new(PlanProperties::new(
            new_eq,
            input_props.output_partitioning().clone(),
            input_props.emission_type,
            input_props.boundedness,
        ));
        Ok(Arc::new(Self {
            input,
            target_schema,
            properties,
        }))
    }
}

impl DisplayAs for RelabelExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "RelabelExec: schema={:?}", self.target_schema)
    }
}

impl ExecutionPlan for RelabelExec {
    fn name(&self) -> &str {
        "RelabelExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.target_schema)
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "RelabelExec expects exactly one child, got {}",
                children.len()
            )));
        }
        let new_input = children.remove(0);
        let target_schema = Arc::clone(&self.target_schema);
        Ok(Self::try_new(new_input, target_schema)? as Arc<dyn ExecutionPlan>)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let target_schema = Arc::clone(&self.target_schema);
        let target_schema_for_stream = Arc::clone(&target_schema);
        let mapped = input_stream.and_then(move |batch| {
            let target = Arc::clone(&target_schema);
            async move { relabel_batch(&batch, &target) }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            target_schema_for_stream,
            mapped,
        )))
    }
}

/// Wraps `physical_plan` with `RelabelExec` when its output schema diverges from
/// `target_schema` only on bit-compatible pairs. No-op when schemas already match
/// (returns the original plan unchanged so this is safe to call from any
/// physical-plan-build site). Errors when a mismatch isn't bit-compatible — those
/// should have been emitted as a `Cast` at the LogicalPlan level.
pub fn wrap_if_relabel_needed(
    physical_plan: Arc<dyn ExecutionPlan>,
    target_schema: SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    if physical_plan.schema().as_ref() == target_schema.as_ref() {
        return Ok(physical_plan);
    }
    Ok(RelabelExec::try_new(physical_plan, target_schema)? as Arc<dyn ExecutionPlan>)
}

fn validate_bit_compatible(src_schema: &SchemaRef, target_schema: &SchemaRef) -> Result<()> {
    if src_schema.fields().len() != target_schema.fields().len() {
        return Err(DataFusionError::Internal(format!(
            "RelabelExec field count mismatch: input has {}, target has {}",
            src_schema.fields().len(),
            target_schema.fields().len()
        )));
    }
    for (i, (src_f, dst_f)) in src_schema
        .fields()
        .iter()
        .zip(target_schema.fields().iter())
        .enumerate()
    {
        if src_f.data_type() != dst_f.data_type()
            && !is_bit_compatible(src_f.data_type(), dst_f.data_type())
        {
            return Err(DataFusionError::Internal(format!(
                "RelabelExec column {} ('{}') has non-bit-compatible types: {:?} → {:?} \
                 (use a LogicalPlan-level Cast for this conversion)",
                i,
                src_f.name(),
                src_f.data_type(),
                dst_f.data_type()
            )));
        }
    }
    Ok(())
}

/// Bit-compatible pairs share an identical Arrow buffer layout; retagging the
/// `DataType` produces a valid array with the same bytes. Currently covers the
/// signed/unsigned integer pairs at the same width, which are the divergences
/// the substrait→physical-plan lowering introduces today (notably DataFusion's
/// `row_number()` returning UInt64 against a Substrait `i64` declaration).
fn is_bit_compatible(a: &DataType, b: &DataType) -> bool {
    matches!(
        (a, b),
        (DataType::Int8, DataType::UInt8)
            | (DataType::UInt8, DataType::Int8)
            | (DataType::Int16, DataType::UInt16)
            | (DataType::UInt16, DataType::Int16)
            | (DataType::Int32, DataType::UInt32)
            | (DataType::UInt32, DataType::Int32)
            | (DataType::Int64, DataType::UInt64)
            | (DataType::UInt64, DataType::Int64)
    )
}

fn relabel_batch(batch: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, target_field) in target_schema.fields().iter().enumerate() {
        let src = batch.column(i);
        if src.data_type() == target_field.data_type() {
            columns.push(Arc::clone(src));
            continue;
        }
        // Zero-copy: rebuild ArrayData with the new DataType. Buffers are shared with
        // the source array; only the type tag flips.
        let data = src
            .to_data()
            .into_builder()
            .data_type(target_field.data_type().clone())
            .build()
            .map_err(|e| {
                DataFusionError::Internal(format!(
                    "RelabelExec failed to retag column {} ('{}'): {}",
                    i,
                    target_field.name(),
                    e
                ))
            })?;
        columns.push(make_array(data));
    }
    RecordBatch::try_new(Arc::clone(target_schema), columns)
        .map_err(|e| DataFusionError::Execution(format!("RelabelExec assemble batch: {}", e)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, UInt64Array};
    use arrow::datatypes::{Field, Schema};

    #[test]
    fn relabel_uint64_to_int64_shares_buffer() {
        let src: ArrayRef = Arc::new(UInt64Array::from(vec![1u64, 2, 3]));
        let target_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("x", DataType::UInt64, true)])),
            vec![src],
        )
        .unwrap();
        let out = relabel_batch(&batch, &target_schema).unwrap();
        assert_eq!(out.column(0).data_type(), &DataType::Int64);
        // Bit pattern preserved: u64 value 1 reinterprets as i64 value 1.
        let as_i64 = out.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(as_i64.values(), &[1i64, 2, 3]);
    }

    #[test]
    fn relabel_skips_no_op_when_schemas_match() {
        let src: ArrayRef = Arc::new(Int64Array::from(vec![10i64, 20]));
        let same_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("y", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(Arc::clone(&same_schema), vec![Arc::clone(&src)]).unwrap();
        let out = relabel_batch(&batch, &same_schema).unwrap();
        // Same Arc — the no-op branch in relabel_batch is the test target.
        assert!(Arc::ptr_eq(out.column(0), &src));
    }

    #[test]
    fn validate_rejects_cross_family() {
        let a: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let b: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Float64, true)]));
        assert!(validate_bit_compatible(&a, &b).is_err());
    }

    #[test]
    fn validate_accepts_uint64_to_int64() {
        let a: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::UInt64, true)]));
        let b: SchemaRef = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        assert!(validate_bit_compatible(&a, &b).is_ok());
    }
}
