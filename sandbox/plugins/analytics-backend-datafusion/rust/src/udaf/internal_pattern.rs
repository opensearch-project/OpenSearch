/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `internal_pattern(field, max_sample_count, buffer_limit, show_numbered_token,
//!  [threshold_percentage], [variable_count_threshold])` — BRAIN aggregate UDF
//! used by PPL's `patterns ... method=BRAIN` in aggregation mode.
//!
//! Mirrors `org.opensearch.sql.calcite.udf.udaf.LogPatternAggFunction` on the
//! SQL plugin side. Collects all source-field strings per group, then on
//! `evaluate()` runs `BrainLogParser::parse_all_log_patterns` and emits the
//! sorted list of pattern groups.
//!
//! Distributed correctness: PARTIAL emits its accumulated string buffer as
//! `List<Utf8>` state via `state()`; the coordinator's FINAL accumulator
//! un-nests received per-shard buffers in `merge_batch` and runs the parser at
//! `evaluate()` over the concatenated corpus. We do not pre-merge pattern
//! groups across shards because BRAIN's token-frequency histogram is global
//! and pattern equality is corpus-dependent — running the parser once on the
//! full corpus matches the Java single-pass behaviour. The `buffer_limit`
//! parameter is accepted for parity with the Java signature but currently
//! ignored (no partial pre-aggregation).
//!
//! Output Arrow shape:
//!   `List<Struct<pattern: Utf8, pattern_count: Int64,
//!                tokens: Map<Utf8, List<Utf8>>,
//!                sample_logs: List<Utf8>>>`
//!
//! The PPL Calcite visitor wraps this aggregate output in
//! `Uncollect` → `flattenParsedPattern` (ITEM on each struct field), so the
//! tuple shape after expansion is one row per pattern group with
//! `(pattern, pattern_count, tokens, sample_logs)` columns — matching the
//! Java path's `ImmutableMap.of(...)` rows.

use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Int64Builder, ListArray, ListBuilder, MapBuilder, MapFieldNames,
    StringArray, StringBuilder, StructArray,
};
use datafusion::arrow::buffer::OffsetBuffer;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::common::{exec_err, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, Signature, Volatility,
};
use datafusion::physical_expr::expressions::Literal;

use crate::patterns::brain::{BrainLogParser, PatternEntry};
use crate::patterns::eval::eval_samples;
use crate::patterns::{DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE, DEFAULT_VARIABLE_COUNT_THRESHOLD};

pub const NAME: &str = "internal_pattern";

const FIELD_PATTERN: &str = "pattern";
const FIELD_PATTERN_COUNT: &str = "pattern_count";
const FIELD_TOKENS: &str = "tokens";
const FIELD_SAMPLE_LOGS: &str = "sample_logs";

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udaf(AggregateUDF::from(InternalPatternUdaf::new()));
}

#[derive(Debug)]
pub struct InternalPatternUdaf {
    signature: Signature,
}

impl InternalPatternUdaf {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for InternalPatternUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for InternalPatternUdaf {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl Eq for InternalPatternUdaf {}
impl Hash for InternalPatternUdaf {
    fn hash<H: Hasher>(&self, state: &mut H) {
        NAME.hash(state);
    }
}

/// Arrow shape for one pattern-group element. Matches the Java path's
/// `ImmutableMap.of(PATTERN, ..., PATTERN_COUNT, ..., TOKENS, ..., SAMPLE_LOGS, ...)`
/// row but as a typed Struct.
fn struct_element_type() -> DataType {
    DataType::Struct(struct_fields())
}

fn struct_fields() -> Fields {
    Fields::from(vec![
        Field::new(FIELD_PATTERN, DataType::Utf8, true),
        Field::new(FIELD_PATTERN_COUNT, DataType::Int64, true),
        Field::new(FIELD_TOKENS, tokens_map_type(), true),
        Field::new(
            FIELD_SAMPLE_LOGS,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
    ])
}

fn tokens_map_type() -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new(
                    "value",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                ),
            ])),
            false,
        )),
        false,
    )
}

/// The complete return type of `internal_pattern`: List<Struct<…>>.
fn list_struct_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", struct_element_type(), true)))
}

/// Per-shard state shape — `List<Utf8>` carrying the raw collected log lines.
fn state_list_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

impl AggregateUDFImpl for InternalPatternUdaf {
    fn name(&self) -> &str {
        NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(list_struct_type())
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let arg0_type = acc_args
            .exprs
            .first()
            .map(|e| e.data_type(acc_args.schema))
            .transpose()?
            .ok_or_else(|| {
                datafusion::common::DataFusionError::Execution(format!(
                    "{}: missing first argument",
                    NAME
                ))
            })?;
        let arg0_is_list = matches!(
            arg0_type,
            DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _)
        );
        let config = AggConfig::from_args(&acc_args)?;
        Ok(Box::new(InternalPatternAccumulator::new(
            arg0_is_list,
            config,
        )))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        // Per-shard state is the raw collected log lines so the coordinator
        // can run the BRAIN parser once over the full corpus.
        Ok(vec![Arc::new(Field::new(
            format!("{}[buf]", args.name),
            state_list_type(),
            true,
        ))])
    }
}

/// Tunable BRAIN parameters extracted from the aggregate call's literal
/// arguments. The PPL layer always passes the four required positional args
/// (field, max_sample_count, buffer_limit, show_numbered_token); the optional
/// trailing pair (threshold_percentage, variable_count_threshold) is sorted by
/// argument name and appended only when set.
#[derive(Debug, Clone)]
struct AggConfig {
    max_sample_count: usize,
    show_numbered_token: bool,
    variable_count_threshold: usize,
    threshold_percentage: f64,
}

impl AggConfig {
    fn defaults() -> Self {
        Self {
            max_sample_count: 10,
            show_numbered_token: false,
            variable_count_threshold: DEFAULT_VARIABLE_COUNT_THRESHOLD,
            threshold_percentage: DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
        }
    }

    fn from_args(acc_args: &AccumulatorArgs) -> Result<Self> {
        let mut config = Self::defaults();
        // arg layout (positional):
        //   0 = field column
        //   1 = max_sample_count (int)
        //   2 = buffer_limit     (int)   — currently ignored
        //   3 = show_numbered_token (bool)
        //   trailing in sorted Argument order:
        //     frequency_threshold_percentage (double)
        //     variable_count_threshold       (int)
        if let Some(expr) = acc_args.exprs.get(1) {
            if let Some(lit) = expr.downcast_ref::<Literal>() {
                config.max_sample_count = scalar_to_usize(lit.value())?;
            }
        }
        if let Some(expr) = acc_args.exprs.get(3) {
            if let Some(lit) = expr.downcast_ref::<Literal>() {
                config.show_numbered_token = scalar_to_bool(lit.value())?;
            }
        }
        // Optional tail: prefer parsing by Arrow type rather than position so
        // the alphabetical PPL ordering (frequency_threshold_percentage first,
        // variable_count_threshold second) doesn't lock us in.
        for expr in acc_args.exprs.iter().skip(4) {
            let Some(lit) = expr.downcast_ref::<Literal>() else {
                continue;
            };
            match lit.value() {
                ScalarValue::Float32(_)
                | ScalarValue::Float64(_)
                | ScalarValue::Decimal128(_, _, _)
                | ScalarValue::Decimal256(_, _, _) => {
                    config.threshold_percentage = scalar_to_f64(lit.value())?;
                }
                ScalarValue::Int8(_)
                | ScalarValue::Int16(_)
                | ScalarValue::Int32(_)
                | ScalarValue::Int64(_)
                | ScalarValue::UInt8(_)
                | ScalarValue::UInt16(_)
                | ScalarValue::UInt32(_)
                | ScalarValue::UInt64(_) => {
                    config.variable_count_threshold = scalar_to_usize(lit.value())?;
                }
                _ => continue,
            }
        }
        Ok(config)
    }
}

fn scalar_to_usize(scalar: &ScalarValue) -> Result<usize> {
    match scalar {
        ScalarValue::Int8(Some(v)) => Ok(*v as usize),
        ScalarValue::Int16(Some(v)) => Ok(*v as usize),
        ScalarValue::Int32(Some(v)) => Ok(*v as usize),
        ScalarValue::Int64(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt8(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt16(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt32(Some(v)) => Ok(*v as usize),
        ScalarValue::UInt64(Some(v)) => Ok(*v as usize),
        _ => exec_err!("{}: expected integer literal, got {scalar:?}", NAME),
    }
}

fn scalar_to_bool(scalar: &ScalarValue) -> Result<bool> {
    match scalar {
        ScalarValue::Boolean(Some(v)) => Ok(*v),
        _ => exec_err!("{}: expected boolean literal, got {scalar:?}", NAME),
    }
}

fn scalar_to_f64(scalar: &ScalarValue) -> Result<f64> {
    match scalar {
        ScalarValue::Float32(Some(v)) => Ok(*v as f64),
        ScalarValue::Float64(Some(v)) => Ok(*v),
        ScalarValue::Decimal128(Some(v), _, scale) => Ok(*v as f64 / 10f64.powi(*scale as i32)),
        _ => exec_err!("{}: expected double literal, got {scalar:?}", NAME),
    }
}

pub struct InternalPatternAccumulator {
    arg0_is_list: bool,
    config: AggConfig,
    /// Collected raw log lines — order-preserving so sample_logs match the
    /// Java path's arrival-order behaviour.
    buffer: Vec<String>,
}

impl InternalPatternAccumulator {
    pub fn new(arg0_is_list: bool, config: AggConfig) -> Self {
        Self {
            arg0_is_list,
            config,
            buffer: Vec::new(),
        }
    }

    fn append_strings(&mut self, arr: &ArrayRef) -> Result<()> {
        if let Some(strs) = arr.as_any().downcast_ref::<StringArray>() {
            for i in 0..strs.len() {
                if !strs.is_null(i) {
                    self.buffer.push(strs.value(i).to_string());
                }
            }
            return Ok(());
        }
        if let Some(strs) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::LargeStringArray>()
        {
            for i in 0..strs.len() {
                if !strs.is_null(i) {
                    self.buffer.push(strs.value(i).to_string());
                }
            }
            return Ok(());
        }
        if let Some(strs) = arr
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringViewArray>()
        {
            for i in 0..strs.len() {
                if !strs.is_null(i) {
                    self.buffer.push(strs.value(i).to_string());
                }
            }
            return Ok(());
        }
        exec_err!(
            "{}: expected string column, got {:?}",
            NAME,
            arr.data_type()
        )
    }

    fn append_list(&mut self, arr: &ArrayRef) -> Result<()> {
        let lists: &ListArray = arr.as_list();
        for i in 0..lists.len() {
            if lists.is_null(i) {
                continue;
            }
            let inner = lists.value(i);
            self.append_strings(&inner)?;
        }
        Ok(())
    }

    fn current_state(&self) -> ScalarValue {
        let scalars: Vec<ScalarValue> = self
            .buffer
            .iter()
            .map(|s| ScalarValue::Utf8(Some(s.clone())))
            .collect();
        ScalarValue::List(ScalarValue::new_list_nullable(&scalars, &DataType::Utf8))
    }
}

impl Debug for InternalPatternAccumulator {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalPatternAccumulator")
            .field("len", &self.buffer.len())
            .field("arg0_is_list", &self.arg0_is_list)
            .finish()
    }
}

impl Accumulator for InternalPatternAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let col = &values[0];
        if self.arg0_is_list {
            // FINAL form: arg 0 is List<Utf8> carrying each shard's PARTIAL state.
            self.append_list(col)
        } else {
            self.append_strings(col)
        }
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.buffer.is_empty() {
            return Ok(empty_list_struct_scalar()?);
        }
        let mut parser = BrainLogParser::with_thresholds(
            self.config.variable_count_threshold,
            self.config.threshold_percentage,
        );
        let stats = parser.parse_all_log_patterns(&self.buffer, self.config.max_sample_count);
        let entries = sorted_pattern_entries(stats);
        build_list_struct_scalar(&entries, self.config.show_numbered_token)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.buffer.iter().map(|s| s.capacity()).sum::<usize>()
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.current_state()])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        self.append_list(&states[0])
    }
}

/// Sort the BRAIN stats by descending pattern_count to match
/// `LogPatternAggFunction.value()`'s ordering.
fn sorted_pattern_entries(stats: HashMap<String, PatternEntry>) -> Vec<PatternEntry> {
    let mut entries: Vec<PatternEntry> = stats.into_values().collect();
    entries.sort_by(|a, b| b.pattern_count.cmp(&a.pattern_count));
    entries
}

/// Build a single-row `List<Struct<…>>` scalar containing the per-pattern
/// groups. When `show_numbered_token` is true, the pattern string is rewritten
/// to numbered-token form (`<token1>@<token2>.<token3>`) and the tokens map
/// captures the extracted variables; otherwise tokens is empty and pattern
/// stays raw (`<*>@<*>.<*>`).
fn build_list_struct_scalar(
    entries: &[PatternEntry],
    show_numbered_token: bool,
) -> Result<ScalarValue> {
    let array = build_list_struct_array(entries, show_numbered_token)?;
    Ok(ScalarValue::List(Arc::new(array)))
}

/// Empty result — caller returns this when the accumulator never saw any non-null input.
fn empty_list_struct_scalar() -> Result<ScalarValue> {
    let array = build_list_struct_array(&[], false)?;
    Ok(ScalarValue::List(Arc::new(array)))
}

/// Builds a one-row `ListArray` whose single element is the per-pattern
/// StructArray with `entries.len()` rows.
fn build_list_struct_array(
    entries: &[PatternEntry],
    show_numbered_token: bool,
) -> Result<ListArray> {
    let struct_array = build_struct_array(entries, show_numbered_token)?;
    let offsets = OffsetBuffer::new(vec![0i32, entries.len() as i32].into());
    let item_field = Field::new("item", struct_element_type(), true);
    Ok(ListArray::new(
        Arc::new(item_field),
        offsets,
        Arc::new(struct_array),
        None,
    ))
}

fn build_struct_array(entries: &[PatternEntry], show_numbered_token: bool) -> Result<StructArray> {
    let n = entries.len();
    let mut pattern_builder = StringBuilder::with_capacity(n, n * 64);
    let mut count_builder = Int64Builder::with_capacity(n);
    let mut tokens_builder = MapBuilder::with_capacity(
        Some(MapFieldNames {
            entry: "entries".to_string(),
            key: "key".to_string(),
            value: "value".to_string(),
        }),
        StringBuilder::new(),
        ListBuilder::new(StringBuilder::new()),
        n,
    );
    let mut sample_logs_builder = ListBuilder::new(StringBuilder::new());

    for entry in entries {
        // When show_numbered_token is true, re-run eval_samples to produce
        // the numbered pattern string and tokens map from the BRAIN-derived
        // wildcard pattern + this group's sample logs.
        let (pattern_str, tokens_map) = if show_numbered_token {
            let result = eval_samples(&entry.pattern, &entry.sample_logs);
            (result.pattern, result.tokens)
        } else {
            (entry.pattern.clone(), HashMap::new())
        };

        pattern_builder.append_value(&pattern_str);
        count_builder.append_value(entry.pattern_count as i64);

        for (label, values) in tokens_map.iter() {
            tokens_builder.keys().append_value(label);
            let value_builder = tokens_builder.values().values();
            for v in values {
                value_builder.append_value(v);
            }
            tokens_builder.values().append(true);
        }
        tokens_builder
            .append(true)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(Box::new(e), None))?;

        for s in &entry.sample_logs {
            sample_logs_builder.values().append_value(s);
        }
        sample_logs_builder.append(true);
    }

    let pattern_array: ArrayRef = Arc::new(pattern_builder.finish());
    let count_array: ArrayRef = Arc::new(count_builder.finish());
    let tokens_array: ArrayRef = Arc::new(tokens_builder.finish());
    let sample_logs_array: ArrayRef = Arc::new(sample_logs_builder.finish());

    Ok(StructArray::new(
        struct_fields(),
        vec![pattern_array, count_array, tokens_array, sample_logs_array],
        None,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;

    fn run(logs: &[&str], show_numbered_token: bool) -> Vec<PatternEntry> {
        let mut acc = InternalPatternAccumulator::new(
            false,
            AggConfig {
                max_sample_count: 5,
                show_numbered_token,
                variable_count_threshold: DEFAULT_VARIABLE_COUNT_THRESHOLD,
                threshold_percentage: DEFAULT_FREQUENCY_THRESHOLD_PERCENTAGE,
            },
        );
        let arr: ArrayRef = Arc::new(StringArray::from(
            logs.iter().map(|s| Some(s.to_string())).collect::<Vec<_>>(),
        ));
        acc.update_batch(&[arr]).unwrap();
        let result = acc.evaluate().unwrap();
        let list = match result {
            ScalarValue::List(l) => l,
            other => panic!("expected List, got {other:?}"),
        };
        let inner = list.value(0);
        let s = inner.as_any().downcast_ref::<StructArray>().unwrap();
        let pattern_col = s.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let count_col = s
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        let sample_logs_col = s.column(3).as_list::<i32>();
        (0..s.len())
            .map(|i| {
                let sample_inner = sample_logs_col.value(i);
                let samples = sample_inner.as_any().downcast_ref::<StringArray>().unwrap();
                PatternEntry {
                    pattern: pattern_col.value(i).to_string(),
                    pattern_count: count_col.value(i) as u64,
                    sample_logs: (0..samples.len())
                        .map(|j| samples.value(j).to_string())
                        .collect(),
                }
            })
            .collect()
    }

    #[test]
    fn returns_empty_list_when_no_logs_seen() {
        let mut acc = InternalPatternAccumulator::new(false, AggConfig::defaults());
        let out = acc.evaluate().unwrap();
        match out {
            ScalarValue::List(l) => assert_eq!(l.value(0).len(), 0),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn discovers_repeated_pattern_across_inputs() {
        // Same shape repeated several times — BRAIN should merge them into
        // one group with count = number of repeats.
        let logs = [
            "user 1 logged in",
            "user 2 logged in",
            "user 3 logged in",
            "user 4 logged in",
            "user 5 logged in",
            "unrelated noise",
        ];
        let entries = run(&logs, false);
        let top = &entries[0];
        assert_eq!(
            top.pattern_count, 5,
            "expected 5 in top group, got {entries:?}"
        );
    }

    #[test]
    fn merge_batch_concatenates_partial_states() {
        let mut a = InternalPatternAccumulator::new(false, AggConfig::defaults());
        let arr1: ArrayRef = Arc::new(StringArray::from(vec![
            "foo@bar.com".to_string(),
            "baz@qux.org".to_string(),
        ]));
        a.update_batch(&[arr1]).unwrap();
        let state_a = a.state().unwrap().pop().unwrap();

        let mut b = InternalPatternAccumulator::new(false, AggConfig::defaults());
        let arr2: ArrayRef = Arc::new(StringArray::from(vec![
            "hello@world.net".to_string(),
            "alpha@beta.io".to_string(),
            "ck@example.dev".to_string(),
        ]));
        b.update_batch(&[arr2]).unwrap();
        let state_b = b.state().unwrap().pop().unwrap();

        // Stitch the two per-shard list states into one ListArray and feed to FINAL.
        let s_a: ArrayRef = match state_a {
            ScalarValue::List(l) => l,
            o => panic!("expected list, got {o:?}"),
        };
        let s_b: ArrayRef = match state_b {
            ScalarValue::List(l) => l,
            o => panic!("expected list, got {o:?}"),
        };
        let combined: ArrayRef =
            datafusion::arrow::compute::concat(&[s_a.as_ref(), s_b.as_ref()]).unwrap();

        let mut coord = InternalPatternAccumulator::new(false, AggConfig::defaults());
        coord.merge_batch(&[combined]).unwrap();
        match coord.evaluate().unwrap() {
            ScalarValue::List(l) => {
                let inner = l.value(0);
                let s = inner.as_any().downcast_ref::<StructArray>().unwrap();
                let count_col = s
                    .column(1)
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .unwrap();
                let total: i64 = (0..s.len()).map(|i| count_col.value(i)).sum();
                assert_eq!(total, 5, "merge should preserve all 5 lines");
            }
            o => panic!("expected list, got {o:?}"),
        }
    }
}
