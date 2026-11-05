/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Build and probe primitives for the join runtime-filter Bloom.
//!
//! A join's build side contributes one fixed-size Bloom per shard; the coordinator
//! ORs those into one filter; the probe-side producer then tests each row against
//! it and drops non-matching rows **before they enter the shuffle**. Measurement on
//! the sf=100 cluster put that reduction at roughly 3.4x on a five-way join whose
//! cost is dominated by shipping the full fact table into the join tier, with no
//! change in bytes read — so this path targets shuffle and join volume, not IO.
//!
//! # Why the parquet `Sbbf`
//!
//! Reusing [`Sbbf`] rather than writing a Bloom means the build and probe sides
//! cannot disagree on hash function or block layout, which is the failure mode that
//! would be silent and would drop rows. It is already a dependency, and
//! [`super::indexed_table::bloom_pruner`] already reads the same structure out of
//! parquet footers.
//!
//! # The two invariants
//!
//! **No false negatives.** Every key inserted must answer `true`. This is what
//! licenses dropping a probe row at all: a `false` is a proof of absence.
//!
//! **Union is a superset.** OR-ing two contributions yields a filter that answers
//! `true` for every key in either. A merge that lost a bit would drop rows that
//! belong in the result, so [`merge_bitsets`] refuses any length mismatch rather
//! than truncating.
//!
//! # Key normalisation
//!
//! `Sbbf` hashes the raw native-endian bytes of whatever it is handed, so an `i32`
//! and an `i64` holding the same logical value hash **differently** — 4 bytes
//! against 8. Every key on both sides therefore goes through [`insert_i64`] /
//! [`may_contain_i64`], which widen to `i64` first. Callers must not hand `Sbbf`
//! a narrower integer directly.
//!
//! [`insert_i64`]: RuntimeFilterBloom::insert_i64
//! [`may_contain_i64`]: RuntimeFilterBloom::may_contain_i64

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder, Int64Array,
};
use datafusion::arrow::compute::kernels::cast::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Int64Type};
use datafusion::common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::{
    Accumulator, AggregateUDF, AggregateUDFImpl, ColumnarValue, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use datafusion::parquet::bloom_filter::{Sbbf, BITSET_MAX_LENGTH, BITSET_MIN_LENGTH};
use parking_lot::RwLock;

/// Bytes per SBBF block. Fixed by the parquet spec: eight 4-byte words.
const BLOCK_BYTES: usize = 32;

/// A fixed-size Bloom filter over `i64` join keys.
///
/// Fixed-size on purpose: contributions from different shards are merged by OR-ing
/// their bitsets, which is only defined when every contribution was built to the
/// same size. Sizing is therefore a query-level decision made once and shipped to
/// every build task, never derived per shard from local cardinality.
pub struct RuntimeFilterBloom {
    sbbf: Sbbf,
}

/// Summarised rather than derived: a filter is up to 128 MiB of bitset, and a
/// derived `Debug` would dump all of it into whatever log line printed it.
impl std::fmt::Debug for RuntimeFilterBloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeFilterBloom")
            .field("bytes", &(self.sbbf.num_blocks() * BLOCK_BYTES))
            .finish()
    }
}

impl RuntimeFilterBloom {
    /// Builds an empty filter of `num_bytes`, rounded to a whole number of blocks
    /// and clamped to the parquet bitset bounds.
    ///
    /// Returns the effective size alongside the filter so a caller can record what
    /// it actually got: rounding and clamping mean the request is a hint, and two
    /// contributions only merge if they made the *same* request.
    pub fn with_num_bytes(num_bytes: usize) -> (Self, usize) {
        let effective = effective_bitset_bytes(num_bytes);
        (
            Self {
                sbbf: Sbbf::new_with_num_of_bytes(effective),
            },
            effective,
        )
    }

    /// Rebuilds a filter from a previously serialised bitset.
    ///
    /// Rejects a length that is not a whole number of blocks or falls outside the
    /// parquet bounds — a bitset of the wrong shape would be reinterpreted rather
    /// than rejected, and every answer from it would be meaningless.
    pub fn from_bitset(bitset: &[u8]) -> Result<Self, String> {
        validate_bitset_len(bitset.len())?;
        Ok(Self {
            sbbf: Sbbf::new(bitset),
        })
    }

    /// Adds a key. Widening to `i64` first is required — see the module's key
    /// normalisation note.
    #[inline]
    pub fn insert_i64(&mut self, value: i64) {
        self.sbbf.insert(&value);
    }

    /// `true` if `value` may have been inserted, `false` if it definitely was not.
    /// Only `false` is a proof, and only `false` licenses dropping a row.
    #[inline]
    pub fn may_contain_i64(&self, value: i64) -> bool {
        self.sbbf.check(&value)
    }

    /// The raw bitset, without the parquet header — the form contributions are
    /// merged and shipped in.
    pub fn to_bitset(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(self.sbbf.num_blocks() * BLOCK_BYTES);
        self.sbbf
            .write_bitset(&mut out)
            .map_err(|e| format!("failed to serialise runtime-filter bitset: {e}"))?;
        Ok(out)
    }
}

/// Size `Sbbf::new_with_num_of_bytes` will actually use for this request.
///
/// Mirrors its rounding so a caller can predict the size rather than discover it,
/// which matters because merging requires two contributions to agree on it. The
/// rounding is to the next **power of two**, not to the next block — clamping
/// happens first, so a request above the maximum lands on the maximum rather than
/// overflowing.
pub fn effective_bitset_bytes(num_bytes: usize) -> usize {
    num_bytes
        .clamp(BITSET_MIN_LENGTH, BITSET_MAX_LENGTH)
        .next_power_of_two()
}

/// Rejects a bitset whose length is not a whole number of blocks or is out of
/// bounds.
///
/// Load-bearing rather than defensive: `Sbbf::new` splits with `chunks_exact` and
/// therefore **silently drops a trailing partial block**, so a wrong length would
/// yield a smaller filter than the caller believes it has — and one that answers
/// `false` for keys the dropped block covered, which drops rows.
fn validate_bitset_len(len: usize) -> Result<(), String> {
    if len == 0 || len % BLOCK_BYTES != 0 {
        return Err(format!(
            "runtime-filter bitset length {len} is not a positive multiple of {BLOCK_BYTES}"
        ));
    }
    if len < BITSET_MIN_LENGTH || len > BITSET_MAX_LENGTH {
        return Err(format!(
            "runtime-filter bitset length {len} outside [{BITSET_MIN_LENGTH}, {BITSET_MAX_LENGTH}]"
        ));
    }
    Ok(())
}

/// Merges `other` into `into` by bytewise OR — the union of two build-side
/// contributions.
///
/// Errors on any length mismatch. Silently merging the common prefix would produce
/// a filter missing part of one contribution, and a filter that says `false` for a
/// key some shard actually holds drops rows from the result.
pub fn merge_bitsets(into: &mut [u8], other: &[u8]) -> Result<(), String> {
    if into.len() != other.len() {
        return Err(format!(
            "cannot merge runtime-filter bitsets of {} and {} bytes",
            into.len(),
            other.len()
        ));
    }
    validate_bitset_len(into.len())?;
    for (dst, src) in into.iter_mut().zip(other.iter()) {
        *dst |= *src;
    }
    Ok(())
}

// ── Build side: the aggregate ─────────────────────────────────────────────

/// `os_bloom_agg(key, num_bytes) -> binary` — one build-side contribution.
///
/// Runs on the build table's shards inside the pre-pass fragment and emits the raw
/// bitset, which the coordinator ORs with every other shard's before shipping the
/// result to the probe side.
///
/// `num_bytes` is a literal rather than a session setting because every
/// contribution must be built to the **same** size or the union is undefined, and a
/// literal in the plan is what makes that agreement visible and per-query. Note
/// that `Sbbf` rounds it up to a power of two, so two different requests can still
/// agree — and two adjacent ones can fail to.
#[derive(Debug)]
pub struct BloomAggUdaf {
    signature: Signature,
}

impl BloomAggUdaf {
    pub const NAME: &'static str = "os_bloom_agg";

    pub fn new() -> Self {
        let key_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Date32,
            DataType::Date64,
        ];
        let mut variants = Vec::new();
        for key in key_types {
            for size in [DataType::Int32, DataType::Int64] {
                variants.push(TypeSignature::Exact(vec![key.clone(), size]));
            }
        }
        Self {
            signature: Signature::one_of(variants, Volatility::Immutable),
        }
    }
}

impl Default for BloomAggUdaf {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for BloomAggUdaf {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}
impl Eq for BloomAggUdaf {}
impl std::hash::Hash for BloomAggUdaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Self::NAME.hash(state);
    }
}

impl AggregateUDFImpl for BloomAggUdaf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Binary)
    }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(BloomAggAccumulator::new(literal_size(&args)?)))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        Ok(vec![Arc::new(Field::new(
            format!("{}[bitset]", args.name),
            DataType::Binary,
            true,
        ))])
    }
}

/// The filter size when it arrives as a literal, `None` when it arrives as a column.
///
/// Both shapes occur. Calcite cannot put a literal in an `AggregateCall` argument list — those are
/// field ordinals — so it materialises the size as a constant column in a Project below the
/// aggregate. Some plans still inline it. `take` handles the same two shapes for its `n` argument;
/// this mirrors it rather than inventing a second convention.
fn literal_size(args: &AccumulatorArgs) -> Result<Option<usize>> {
    let Some(expr) = args.exprs.get(1) else {
        return exec_err!("{}: missing num_bytes argument", BloomAggUdaf::NAME);
    };
    let Some(literal) = expr.downcast_ref::<datafusion::physical_expr::expressions::Literal>()
    else {
        // A constant column, resolved from row 0 on the first batch.
        return Ok(None);
    };
    scalar_size(literal.value()).map(Some)
}

fn scalar_size(scalar: &ScalarValue) -> Result<usize> {
    match scalar {
        ScalarValue::Int32(Some(v)) if *v > 0 => Ok(*v as usize),
        ScalarValue::Int64(Some(v)) if *v > 0 => Ok(*v as usize),
        other => exec_err!(
            "{}: num_bytes must be a positive integer, got {other:?}",
            BloomAggUdaf::NAME
        ),
    }
}

/// Accumulates keys into one Bloom of a size fixed for the whole query.
///
/// The Bloom is built lazily because its size may only be knowable once data arrives — see
/// [`literal_size`]. A shard that sees no rows therefore never builds one and emits a NULL state,
/// which is the honest answer: it contributes no keys, and the coordinator's union skips it. If every
/// shard does that the merged filter is empty and no filter is shipped, so the query runs unfiltered.
struct BloomAggAccumulator {
    /// `Some` once the size is known and the filter exists.
    bloom: Option<RuntimeFilterBloom>,
    /// Size from a literal argument, if it was one.
    literal_bytes: Option<usize>,
    /// Effective size of `bloom`, for `size()` reporting.
    num_bytes: usize,
}

impl BloomAggAccumulator {
    fn new(literal_bytes: Option<usize>) -> Self {
        Self {
            bloom: None,
            literal_bytes,
            num_bytes: 0,
        }
    }

    /// Creates the filter if it does not exist yet, taking the size from the literal or from row 0
    /// of the size column.
    fn ensure_bloom(&mut self, size_column: Option<&ArrayRef>) -> Result<()> {
        if self.bloom.is_some() {
            return Ok(());
        }
        let requested = match self.literal_bytes {
            Some(bytes) => bytes,
            None => {
                let column = size_column.ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "{}: num_bytes is neither a literal nor a column",
                        BloomAggUdaf::NAME
                    ))
                })?;
                if column.is_empty() || column.is_null(0) {
                    return exec_err!("{}: num_bytes column is empty or null", BloomAggUdaf::NAME);
                }
                // Every row carries the same constant, so row 0 defines it.
                scalar_size(&ScalarValue::try_from_array(column, 0)?)?
            }
        };
        let (bloom, effective) = RuntimeFilterBloom::with_num_bytes(requested);
        self.bloom = Some(bloom);
        self.num_bytes = effective;
        Ok(())
    }

    /// The current bitset, or NULL when this accumulator never saw a row.
    fn bitset(&self) -> Result<ScalarValue> {
        match &self.bloom {
            None => Ok(ScalarValue::Binary(None)),
            Some(bloom) => Ok(ScalarValue::Binary(Some(
                bloom.to_bitset().map_err(DataFusionError::Execution)?,
            ))),
        }
    }
}

impl std::fmt::Debug for BloomAggAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BloomAggAccumulator")
            .field("bytes", &self.num_bytes)
            .field("built", &self.bloom.is_some())
            .finish()
    }
}

impl Accumulator for BloomAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some(keys) = values.first() else {
            return Ok(());
        };
        if keys.is_empty() {
            return Ok(());
        }
        self.ensure_bloom(values.get(1))?;
        let bloom = self
            .bloom
            .as_mut()
            .expect("ensure_bloom built the filter or returned an error");
        // Widen before hashing — a narrower representation hashes differently and the
        // probe side would never match it.
        let keys = widen_to_i64(keys)?;
        for key in keys.iter().flatten() {
            // Nulls are skipped: a null key matches nothing under equi-join semantics,
            // so it is not a candidate and must not widen the filter.
            bloom.insert_i64(key);
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        self.bitset()
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + self.num_bytes
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.bitset()?])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // Required, not optional: DataFusion splits this aggregate PARTIAL/FINAL
        // across a task's partitions, so per-partition bitsets are merged here before
        // the coordinator ever sees one.
        let Some(states) = states.first() else {
            return Ok(());
        };
        let incoming = states.as_binary::<i32>();
        // The first non-null contribution defines the size when this accumulator has not
        // built a filter yet — a partition that saw no rows has no size of its own.
        let mut merged: Option<Vec<u8>> = match &self.bloom {
            Some(bloom) => Some(bloom.to_bitset().map_err(DataFusionError::Execution)?),
            None => None,
        };
        for i in 0..incoming.len() {
            if incoming.is_null(i) {
                continue;
            }
            match merged.as_mut() {
                None => merged = Some(incoming.value(i).to_vec()),
                Some(into) => {
                    merge_bitsets(into, incoming.value(i)).map_err(DataFusionError::Execution)?
                }
            }
        }
        if let Some(bytes) = merged {
            self.num_bytes = bytes.len();
            self.bloom =
                Some(RuntimeFilterBloom::from_bitset(&bytes).map_err(DataFusionError::Execution)?);
        }
        Ok(())
    }
}

// ── Per-session registry and the probe UDF ───────────────────────────────

/// Per-session map from runtime-filter id to its decoded Bloom.
///
/// The plan carries only the **id**; the bitset arrives separately as an
/// instruction and is decoded into this registry once, before the fragment runs.
/// That split is the same one `index_filter` already uses, and here it is a
/// performance requirement rather than a preference: passing the bitset as a plan
/// literal would make `Sbbf::new` rebuild it on **every record batch**, which for a
/// megabyte-sized filter over a fact-table scan is tens of gigabytes of copying.
/// Caching that decode would need an identity check on the literal, and any
/// cheap-but-approximate check risks using the wrong filter — which drops correct
/// rows. An id is exact and O(1).
pub type RuntimeFilterRegistry = Arc<RwLock<HashMap<i32, Arc<InstalledFilter>>>>;

/// An installed filter and what it has actually done.
///
/// The counters exist because every way this feature fails is silent. A filter sized too small for its
/// build side admits nearly every row and reports nothing: no error, no warning, only a query that did
/// not get faster. Measured on sf=100, that is not hypothetical — 22.7M keys in a 1 MiB filter is ~2.7
/// bits per key and eliminated nothing, while the same query with an adequate filter ran 4.6x faster.
/// `rows_in` against `rows_kept` is the one signal that separates "the filter did nothing" from "there
/// was nothing to do".
#[derive(Debug)]
pub struct InstalledFilter {
    bloom: RuntimeFilterBloom,
    rows_in: std::sync::atomic::AtomicU64,
    rows_kept: std::sync::atomic::AtomicU64,
}

impl InstalledFilter {
    fn new(bloom: RuntimeFilterBloom) -> Self {
        Self {
            bloom,
            rows_in: std::sync::atomic::AtomicU64::new(0),
            rows_kept: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Relaxed ordering: these are counters read once after the stream is exhausted, never used to
    /// synchronise anything, and the cost of a stronger ordering would land on every batch.
    fn record(&self, rows: u64, kept: u64) {
        use std::sync::atomic::Ordering::Relaxed;
        self.rows_in.fetch_add(rows, Relaxed);
        self.rows_kept.fetch_add(kept, Relaxed);
    }
}

/// Per-filter `(id, rows_in, rows_kept)` for every filter installed on a session.
///
/// Read after execution and folded into the per-fragment metrics JSON, which the coordinator already
/// logs per shard and returns under `profile` — so no new transport exists for this.
pub fn stats(registry: &RuntimeFilterRegistry) -> Vec<(i32, u64, u64)> {
    use std::sync::atomic::Ordering::Relaxed;
    let mut out: Vec<(i32, u64, u64)> = registry
        .read()
        .iter()
        .map(|(id, f)| (*id, f.rows_in.load(Relaxed), f.rows_kept.load(Relaxed)))
        .collect();
    out.sort_by_key(|(id, _, _)| *id);
    out
}

/// A fresh, empty registry for one session.
pub fn new_registry() -> RuntimeFilterRegistry {
    Arc::new(RwLock::new(HashMap::new()))
}

/// Registers both halves of the feature on one session and returns the registry the
/// instruction handler must populate.
///
/// Both are registered together deliberately: the build aggregate and the probe UDF
/// only agree because they share [`RuntimeFilterBloom`], and registering one without
/// the other yields a plan that cannot be executed on this session.
pub fn register(ctx: &SessionContext) -> RuntimeFilterRegistry {
    let registry = new_registry();
    ctx.register_udaf(AggregateUDF::from(BloomAggUdaf::new()));
    ctx.register_udf(ScalarUDF::from(RuntimeFilterUdf::new(Arc::clone(
        &registry,
    ))));
    registry
}

/// Decodes `bitset` and installs it under `filter_id`, replacing any previous entry.
pub fn install_filter(
    registry: &RuntimeFilterRegistry,
    filter_id: i32,
    bitset: &[u8],
) -> Result<(), String> {
    let bloom = RuntimeFilterBloom::from_bitset(bitset)?;
    registry
        .write()
        .insert(filter_id, Arc::new(InstalledFilter::new(bloom)));
    Ok(())
}

/// `os_runtime_filter(filter_id, key) -> boolean` — the probe-side application
/// point.
///
/// `true` keeps the row, `false` drops it. Placed on the probe producer's fragment,
/// so a dropped row never enters the shuffle; that is where the measured win comes
/// from, not from reading less.
///
/// Fails open in the one way that matters: an id absent from the registry yields
/// `true` for every row. A filter that failed to arrive therefore costs the
/// optimization, never a row.
#[derive(Debug)]
pub struct RuntimeFilterUdf {
    signature: Signature,
    registry: RuntimeFilterRegistry,
}

impl RuntimeFilterUdf {
    pub const NAME: &'static str = "os_runtime_filter";

    pub fn new(registry: RuntimeFilterRegistry) -> Self {
        // Accepts the integral key types a join key can arrive as, all widened to
        // i64 before hashing — see the module's key normalisation note.
        let key_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Date32,
            DataType::Date64,
        ];
        Self {
            signature: Signature::one_of(
                key_types
                    .into_iter()
                    .map(|k| TypeSignature::Exact(vec![DataType::Int32, k]))
                    .collect(),
                Volatility::Immutable,
            ),
            registry,
        }
    }
}

impl PartialEq for RuntimeFilterUdf {
    /// Two instances are interchangeable only when they share a registry; a plan
    /// rewritten against a different session's filters would probe the wrong Bloom.
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.registry, &other.registry)
    }
}
impl Eq for RuntimeFilterUdf {}
impl std::hash::Hash for RuntimeFilterUdf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Self::NAME.hash(state);
    }
}

impl ScalarUDFImpl for RuntimeFilterUdf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if args.args.len() != 2 {
            return exec_err!(
                "{} expects (filter_id, key), got {} arguments",
                Self::NAME,
                args.args.len()
            );
        }
        let filter_id = match &args.args[0] {
            ColumnarValue::Scalar(ScalarValue::Int32(Some(id))) => *id,
            other => {
                return exec_err!(
                    "{}: filter_id must be an Int32 literal, got {other:?}",
                    Self::NAME
                )
            }
        };
        // One lock acquisition and one lookup per batch, then a lock-free probe per
        // row off the cloned Arc.
        let Some(installed) = self.registry.read().get(&filter_id).cloned() else {
            // Fail open: the filter never arrived, so nothing can be excluded.
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))));
        };

        match &args.args[1] {
            ColumnarValue::Scalar(scalar) => {
                let keep = match scalar_key_as_i64(scalar) {
                    Some(key) => installed.bloom.may_contain_i64(key),
                    // A null key matches nothing under the equi-join semantics that
                    // permitted this filter at all, so dropping it is sound.
                    None => false,
                };
                installed.record(1, u64::from(keep));
                Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(keep))))
            }
            ColumnarValue::Array(array) => {
                let keys = widen_to_i64(array)?;
                let decisions = probe_array(&installed.bloom, &keys);
                // Counted from the result rather than by re-walking: `true_count` is a popcount over the
                // buffer already produced, so observability costs one pass over bits and not one per row.
                installed.record(decisions.len() as u64, decisions.true_count() as u64);
                Ok(ColumnarValue::Array(Arc::new(decisions) as ArrayRef))
            }
        }
    }
}

/// Probes every row, returning one boolean per row.
///
/// Two paths because this runs once per probe-side row and the null check is not
/// free: with no nulls the values slice is walked directly, which is the common case
/// for a join key. The output carries no null mask — a null key yields `false`
/// rather than null, so the caller sees a decision for every row.
fn probe_array(bloom: &RuntimeFilterBloom, keys: &Int64Array) -> BooleanArray {
    let mut decisions = BooleanBufferBuilder::new(keys.len());
    match keys.nulls() {
        None => {
            for key in keys.values() {
                decisions.append(bloom.may_contain_i64(*key));
            }
        }
        Some(nulls) => {
            for (i, key) in keys.values().iter().enumerate() {
                // A null key matches nothing under the equi-join semantics that
                // permitted this filter, so dropping it is sound.
                decisions.append(nulls.is_valid(i) && bloom.may_contain_i64(*key));
            }
        }
    }
    BooleanArray::new(decisions.finish(), None)
}

/// Widens any accepted integral key array to `Int64` so hashing sees one
/// representation. See the module's key normalisation note for why this cannot be
/// skipped even when the values would fit.
///
/// Already-`Int64` input is returned without a cast: `cast` copies the whole array,
/// and paying that on every batch of a fact-table scan for a no-op conversion is the
/// kind of cost that eats the win this feature exists to deliver.
fn widen_to_i64(array: &ArrayRef) -> Result<Int64Array> {
    if array.data_type() == &DataType::Int64 {
        return Ok(array.as_primitive::<Int64Type>().clone());
    }
    let widened = cast(array, &DataType::Int64)
        .map_err(|e| DataFusionError::Execution(format!("{}: {e}", RuntimeFilterUdf::NAME)))?;
    Ok(widened.as_primitive::<Int64Type>().clone())
}

fn scalar_key_as_i64(scalar: &ScalarValue) -> Option<i64> {
    match scalar {
        ScalarValue::Int8(v) => v.map(i64::from),
        ScalarValue::Int16(v) => v.map(i64::from),
        ScalarValue::Int32(v) => v.map(i64::from),
        ScalarValue::Int64(v) => *v,
        ScalarValue::Date32(v) => v.map(i64::from),
        ScalarValue::Date64(v) => *v,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn built_from(values: &[i64], num_bytes: usize) -> Vec<u8> {
        let (mut bloom, _) = RuntimeFilterBloom::with_num_bytes(num_bytes);
        for v in values {
            bloom.insert_i64(*v);
        }
        bloom.to_bitset().unwrap()
    }

    #[test]
    fn no_false_negatives() {
        // The invariant that licenses dropping a probe row.
        let keys: Vec<i64> = (0..5000).map(|i| i * 7 + 1).collect();
        let bitset = built_from(&keys, 64 * 1024);
        let bloom = RuntimeFilterBloom::from_bitset(&bitset).unwrap();
        for k in &keys {
            assert!(bloom.may_contain_i64(*k), "false negative for {k}");
        }
    }

    #[test]
    fn absent_keys_are_mostly_rejected() {
        // Not an invariant — a Bloom may answer true for an absent key. Asserted
        // loosely, only to catch a filter that says yes to everything, which would
        // pass `no_false_negatives` while pruning nothing.
        let keys: Vec<i64> = (0..1000).collect();
        let bitset = built_from(&keys, 64 * 1024);
        let bloom = RuntimeFilterBloom::from_bitset(&bitset).unwrap();
        let absent = (1_000_000..1_010_000)
            .filter(|k| !bloom.may_contain_i64(*k))
            .count();
        assert!(
            absent > 9_000,
            "expected most absent keys rejected, got {absent}/10000"
        );
    }

    #[test]
    fn round_trips_through_a_bitset() {
        let bitset = built_from(&[1, 2, 3], 4096);
        let reloaded = RuntimeFilterBloom::from_bitset(&bitset).unwrap();
        for k in [1, 2, 3] {
            assert!(reloaded.may_contain_i64(k));
        }
        // Serialising the reloaded filter must reproduce the same bytes, or a
        // second merge round would operate on a different structure.
        assert_eq!(bitset, reloaded.to_bitset().unwrap());
    }

    #[test]
    fn union_is_a_superset_of_both_contributions() {
        let left: Vec<i64> = (0..500).collect();
        let right: Vec<i64> = (10_000..10_500).collect();
        let mut merged = built_from(&left, 64 * 1024);
        merge_bitsets(&mut merged, &built_from(&right, 64 * 1024)).unwrap();

        let bloom = RuntimeFilterBloom::from_bitset(&merged).unwrap();
        for k in left.iter().chain(right.iter()) {
            assert!(bloom.may_contain_i64(*k), "merged filter lost {k}");
        }
    }

    #[test]
    fn merge_is_order_independent_and_idempotent() {
        // Union has to be commutative and idempotent for per-shard contributions to
        // be merged in arrival order without coordination.
        let a = built_from(&[1, 2, 3], 4096);
        let b = built_from(&[4, 5, 6], 4096);
        let mut ab = a.clone();
        merge_bitsets(&mut ab, &b).unwrap();
        let mut ba = b.clone();
        merge_bitsets(&mut ba, &a).unwrap();
        assert_eq!(ab, ba, "merge must be commutative");

        let mut twice = ab.clone();
        merge_bitsets(&mut twice, &a).unwrap();
        assert_eq!(
            ab, twice,
            "merging a contribution twice must change nothing"
        );
    }

    #[test]
    fn merge_refuses_a_length_mismatch() {
        // Merging the common prefix would drop part of one contribution, and a
        // filter missing a shard's keys drops rows from the result.
        let mut small = built_from(&[1], 4096);
        let large = built_from(&[2], 64 * 1024);
        assert_ne!(small.len(), large.len(), "fixture sizes must differ");
        let err = merge_bitsets(&mut small, &large).expect_err("mismatch must be refused");
        assert!(err.contains("cannot merge"), "unexpected: {err}");
    }

    #[test]
    fn narrow_integers_must_be_widened_before_hashing() {
        // Sbbf hashes raw native bytes, so 4- and 8-byte forms of the same value land in
        // different blocks. This is why the API only accepts i64.
        let bitset = built_from(&[42], 4096);
        let bloom = RuntimeFilterBloom::from_bitset(&bitset).unwrap();
        let widened: i64 = i64::from(42i32);
        assert!(bloom.may_contain_i64(widened));
    }

    // ── The build aggregate ──────────────────────────────────────────────

    fn bitset_of(scalar: ScalarValue) -> Vec<u8> {
        match scalar {
            ScalarValue::Binary(Some(b)) => b,
            other => panic!("expected Binary, got {other:?}"),
        }
    }

    #[test]
    fn accumulator_builds_a_filter_covering_every_key_it_saw() {
        let mut acc = BloomAggAccumulator::new(Some(64 * 1024));
        let keys: ArrayRef = Arc::new(I64Arr::from(vec![Some(1), Some(2), None, Some(3)]));
        acc.update_batch(&[keys]).unwrap();
        let bloom = RuntimeFilterBloom::from_bitset(&bitset_of(acc.evaluate().unwrap())).unwrap();
        for k in [1, 2, 3] {
            assert!(bloom.may_contain_i64(k), "lost key {k}");
        }
    }

    #[test]
    fn accumulator_widens_narrow_keys_so_the_probe_side_matches() {
        // The probe UDF widens too; if either side skipped it, the same logical value
        // would hash to different blocks and every row would be dropped.
        let mut acc = BloomAggAccumulator::new(Some(4096));
        let narrow: ArrayRef = Arc::new(Int32Array::from(vec![7]));
        acc.update_batch(&[narrow]).unwrap();
        let bloom = RuntimeFilterBloom::from_bitset(&bitset_of(acc.evaluate().unwrap())).unwrap();
        assert!(bloom.may_contain_i64(7));
    }

    #[test]
    fn accumulator_merges_partition_states() {
        // DataFusion splits this aggregate PARTIAL/FINAL across a task's partitions,
        // so merge_batch is on the correctness path, not an optional extra.
        let mut left = BloomAggAccumulator::new(Some(64 * 1024));
        left.update_batch(&[Arc::new(I64Arr::from(vec![1, 2])) as ArrayRef])
            .unwrap();
        let mut right = BloomAggAccumulator::new(Some(64 * 1024));
        right
            .update_batch(&[Arc::new(I64Arr::from(vec![100, 200])) as ArrayRef])
            .unwrap();

        let incoming: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&bitset_of(
            right.state().unwrap().remove(0),
        )]));
        left.merge_batch(&[incoming]).unwrap();

        let bloom = RuntimeFilterBloom::from_bitset(&bitset_of(left.evaluate().unwrap())).unwrap();
        for k in [1, 2, 100, 200] {
            assert!(bloom.may_contain_i64(k), "merge lost key {k}");
        }
    }

    #[test]
    fn accumulator_merge_refuses_two_differently_sized_contributions() {
        // Mismatched sizes must never merge silently: the union would be missing part of one bitset, and
        // a filter missing keys drops rows. An empty accumulator adopts the first size, so the check has
        // to bite on the SECOND contribution.
        let mut small = BloomAggAccumulator::new(None);
        small
            .update_batch(&[
                Arc::new(I64Arr::from(vec![1])) as ArrayRef,
                Arc::new(I64Arr::from(vec![4096])) as ArrayRef,
            ])
            .unwrap();
        let mut large = BloomAggAccumulator::new(Some(64 * 1024));
        large
            .update_batch(&[Arc::new(I64Arr::from(vec![2])) as ArrayRef])
            .unwrap();

        let incoming: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&bitset_of(
            large.state().unwrap().remove(0),
        )]));
        assert!(
            small.merge_batch(&[incoming]).is_err(),
            "a 4 KiB accumulator must refuse a 64 KiB contribution"
        );
    }

    #[test]
    fn accumulator_takes_its_size_from_a_constant_column() {
        // The shape Calcite actually emits: an AggregateCall argument list holds field ordinals, not
        // literals, so the size is materialised as a constant column in a Project below the aggregate.
        // The first draft errored on this and would have failed every real pre-pass.
        let mut acc = BloomAggAccumulator::new(None);
        let keys: ArrayRef = Arc::new(I64Arr::from(vec![1, 2]));
        let sizes: ArrayRef = Arc::new(I64Arr::from(vec![4096, 4096]));
        acc.update_batch(&[keys, sizes]).unwrap();
        let bitset = bitset_of(acc.evaluate().unwrap());
        assert_eq!(
            effective_bitset_bytes(4096),
            bitset.len(),
            "size came from the column"
        );
        let bloom = RuntimeFilterBloom::from_bitset(&bitset).unwrap();
        for k in [1, 2] {
            assert!(bloom.may_contain_i64(k));
        }
    }

    #[test]
    fn a_shard_with_no_rows_contributes_null_not_an_empty_filter() {
        // An empty bitset would be a filter that rejects every key — it would drop every probe row.
        // NULL says "no contribution", and the union skips it.
        let mut acc = BloomAggAccumulator::new(Some(4096));
        acc.update_batch(&[Arc::new(I64Arr::from(Vec::<i64>::new())) as ArrayRef])
            .unwrap();
        assert_eq!(acc.evaluate().unwrap(), ScalarValue::Binary(None));
        assert_eq!(acc.state().unwrap(), vec![ScalarValue::Binary(None)]);
    }

    #[test]
    fn merge_adopts_the_size_of_the_first_contribution() {
        // A partition that saw no rows has no size of its own, so it must take the incoming one rather
        // than refusing the merge.
        let mut empty = BloomAggAccumulator::new(None);
        let mut seen = BloomAggAccumulator::new(Some(64 * 1024));
        seen.update_batch(&[Arc::new(I64Arr::from(vec![9])) as ArrayRef])
            .unwrap();
        let incoming: ArrayRef = Arc::new(BinaryArray::from_vec(vec![&bitset_of(
            seen.state().unwrap().remove(0),
        )]));
        empty.merge_batch(&[incoming]).unwrap();
        let bloom = RuntimeFilterBloom::from_bitset(&bitset_of(empty.evaluate().unwrap())).unwrap();
        assert!(bloom.may_contain_i64(9));
    }

    #[test]
    fn merge_of_only_null_contributions_stays_null() {
        let mut acc = BloomAggAccumulator::new(None);
        let nulls: ArrayRef = Arc::new(BinaryArray::from_opt_vec(vec![None, None]));
        acc.merge_batch(&[nulls]).unwrap();
        assert_eq!(
            acc.evaluate().unwrap(),
            ScalarValue::Binary(None),
            "no contribution at all"
        );
    }

    #[test]
    fn udaf_is_registrable_and_named_for_the_planner() {
        let udaf = AggregateUDF::from(BloomAggUdaf::new());
        assert_eq!(udaf.name(), "os_bloom_agg");
        assert_eq!(
            udaf.return_type(&[DataType::Int64, DataType::Int32])
                .unwrap(),
            DataType::Binary
        );
    }

    // ── Cost of the probe path ───────────────────────────────────────────

    /// The probe runs once per probe-side row, so its per-row cost is the only
    /// deduction from the measured end-to-end win, and the deduction is what decides
    /// whether the feature pays for itself.
    ///
    /// Times only `invoke_with_args` — building a `Vec<Option<bool>>` from the result,
    /// as the other tests do for readability, costs more than the probe itself and
    /// would inflate the figure well past what the plan actually pays. Batches are
    /// sized like production rather than as one giant array, so per-batch overhead
    /// (the registry lock, the lookup, the cast) is amortised the way it will be.
    ///
    /// Reported rather than asserted tightly: the bound only catches an
    /// order-of-magnitude regression, such as rebuilding the filter per batch.
    #[test]
    /// The performance property the feature exists for, stated as an assertion.
    ///
    /// The measured 24% end-to-end win came from removing probe rows before they entered a
    /// shuffle, so what has to hold is that the probe *actually removes them* at the default
    /// filter size — and that the removal rate tracks the build side's share of the key domain
    /// rather than being eroded by false positives. That is checkable deterministically here,
    /// whereas the end-to-end speedup only exists at sf=100 and above (a 60M-row cluster shows
    /// nothing, because nothing there is expensive enough to be worth removing).
    ///
    /// Bounds rather than exact counts, because the false-positive rate is a property of the
    /// hash and the fill factor, not something to pin to a golden number.
    /// The counters that make an undersized filter visible instead of silent.
    ///
    /// Without them the two outcomes "the filter removed nothing because it was too small" and "the
    /// filter removed nothing because there was nothing to remove" look identical from outside — which is
    /// exactly the confusion that cost a measurement round on sf=100.
    #[test]
    fn the_probe_counts_what_it_examined_and_kept() {
        let registry = new_registry();
        install_filter(&registry, 1, &built_from(&[10, 20, 30], 64 * 1024)).unwrap();
        let udf = RuntimeFilterUdf::new(Arc::clone(&registry));

        // Two present, three absent.
        let keys: ArrayRef = Arc::new(I64Arr::from(vec![10i64, 20, 999, 1000, 1001]));
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                ColumnarValue::Array(keys),
            ],
            arg_fields: vec![
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ],
            number_rows: 5,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(Default::default()),
        })
        .unwrap();

        let stats = stats(&registry);
        assert_eq!(stats.len(), 1, "one installed filter, one row of stats");
        let (id, rows_in, rows_kept) = stats[0];
        assert_eq!(id, 1);
        assert_eq!(rows_in, 5, "every row examined is counted");
        assert_eq!(
            rows_kept, 2,
            "only the two present keys survive at this size"
        );
    }

    /// A filter far too small for its build side keeps almost everything — and the counters say so.
    ///
    /// This is the failure the counters exist for, reproduced deliberately: 200k keys in the smallest
    /// bitset is a fraction of a bit per key, so `rows_kept` approaches `rows_in` and the ratio is the
    /// evidence. Measured at scale the same shape cost a 4.6x speedup.
    #[test]
    fn an_undersized_filter_shows_up_in_the_counters() {
        let registry = new_registry();
        let keys: Vec<i64> = (0..200_000).collect();
        install_filter(&registry, 7, &built_from(&keys, BITSET_MIN_LENGTH)).unwrap();
        let udf = RuntimeFilterUdf::new(Arc::clone(&registry));

        // Probe keys that are all ABSENT, so a filter doing its job would keep none of them.
        let absent: ArrayRef = Arc::new(I64Arr::from(
            (1_000_000i64..1_010_000).collect::<Vec<i64>>(),
        ));
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(7))),
                ColumnarValue::Array(absent),
            ],
            arg_fields: vec![
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ],
            number_rows: 10_000,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(Default::default()),
        })
        .unwrap();

        let (_, rows_in, rows_kept) = stats(&registry)[0];
        assert_eq!(rows_in, 10_000);
        assert!(
            rows_kept * 2 > rows_in,
            "an undersized filter should keep most absent rows, kept {rows_kept} of {rows_in};              if this ever fails the filter got better, not the test worse"
        );
    }

    #[test]
    fn the_probe_eliminates_the_rows_the_speedup_comes_from() {
        const DOMAIN: i64 = 1_000_000;
        const DEFAULT_BYTES: usize = 1024 * 1024; // analytics.mpp.runtime_filter.bloom.bytes

        // Build side holds every fifth key, the shape of a dimension whose local predicate keeps
        // 20% of it. A probe scan over the whole domain should therefore keep about 20%.
        let build: Vec<i64> = (0..DOMAIN).filter(|k| k % 5 == 0).collect();
        let expected_kept = build.len();

        let registry = new_registry();
        install_filter(&registry, 1, &built_from(&build, DEFAULT_BYTES)).unwrap();
        let udf = RuntimeFilterUdf::new(registry);

        let keys: ArrayRef = Arc::new(I64Arr::from((0..DOMAIN).collect::<Vec<i64>>()));
        let kept = match udf
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                    ColumnarValue::Array(Arc::clone(&keys)),
                ],
                arg_fields: vec![
                    Arc::new(Field::new("id", DataType::Int32, false)),
                    Arc::new(Field::new("k", DataType::Int64, true)),
                ],
                number_rows: DOMAIN as usize,
                return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap()
        {
            ColumnarValue::Array(a) => a.as_boolean().true_count(),
            other => panic!("unexpected {other:?}"),
        };

        let false_positives = kept - expected_kept;
        let fp_rate = false_positives as f64 / (DOMAIN as usize - expected_kept) as f64;
        let eliminated = (DOMAIN as usize - kept) as f64 / DOMAIN as f64;
        println!(
            "[runtime-filter] 1 MiB filter, {expected_kept} of {DOMAIN} keys present: \
             eliminated {:.1}% of rows, false-positive rate {:.4}%",
            eliminated * 100.0,
            fp_rate * 100.0
        );

        // Every present key must survive: a false negative would drop a joinable row.
        assert!(
            kept >= expected_kept,
            "kept {kept} < {expected_kept} present keys — false negative, which loses results"
        );
        // And the absent 80% must actually be removed, which is where the shuffle saving is. A
        // filter that admits most absent keys is the failure mode that looks like it works and
        // buys nothing — exactly what an undersized filter produces.
        assert!(
            fp_rate < 0.01,
            "false-positive rate {:.4}% at the default size; the filter is admitting absent keys \
             and would not reduce shuffle volume",
            fp_rate * 100.0
        );
        assert!(
            eliminated > 0.79,
            "eliminated only {:.1}% of rows; expected ~80% for a build side holding 20% of the domain",
            eliminated * 100.0
        );
    }

    #[test]
    fn probe_cost_per_row_is_reported_and_bounded() {
        const BATCH: usize = 8192;
        const BATCHES: usize = 128;
        let batches: Vec<ArrayRef> = (0..BATCHES)
            .map(|b| {
                let base = (b * BATCH) as i64;
                Arc::new(I64Arr::from(
                    (base..base + BATCH as i64).collect::<Vec<i64>>(),
                )) as ArrayRef
            })
            .collect();

        // Swept across sizes: the probe is one random access, so its cost tracks the cache level the
        // filter fits in. A smaller filter probes faster but admits more false positives.
        for num_bytes in [32 * 1024usize, 256 * 1024, 1024 * 1024, 8 * 1024 * 1024] {
            let registry = new_registry();
            install_filter(
                &registry,
                1,
                &built_from(&(0..200_000).collect::<Vec<i64>>(), num_bytes),
            )
            .unwrap();
            let udf = RuntimeFilterUdf::new(registry);

            let mut kept = 0usize;
            let start = std::time::Instant::now();
            for keys in &batches {
                let args = ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Int32(Some(1))),
                        ColumnarValue::Array(Arc::clone(keys)),
                    ],
                    arg_fields: vec![
                        Arc::new(Field::new("id", DataType::Int32, false)),
                        Arc::new(Field::new("k", DataType::Int64, true)),
                    ],
                    number_rows: BATCH,
                    return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
                    config_options: Arc::new(Default::default()),
                };
                match udf.invoke_with_args(args).unwrap() {
                    // Popcount rather than a per-row closure, so the timed region stays
                    // the probe plus one cheap aggregate.
                    ColumnarValue::Array(a) => kept += a.as_boolean().true_count(),
                    other => panic!("unexpected {other:?}"),
                }
            }
            let rows = BATCH * BATCHES;
            let ns_per_row = start.elapsed().as_nanos() as f64 / rows as f64;
            // 200k keys inserted; anything kept beyond those is a false positive.
            let false_positives = kept.saturating_sub(200_003);
            println!(
                "[runtime-filter] {:>5} KiB filter: {ns_per_row:>5.1} ns/row over {rows} rows, \
                 kept {kept} ({false_positives} false positive)",
                num_bytes / 1024
            );
            assert!(
                ns_per_row < 500.0,
                "probe cost {ns_per_row:.1} ns/row at {num_bytes} bytes is far above expectation; \
                 suspect the filter is being rebuilt per batch"
            );
        }
    }

    // ── The probe UDF ────────────────────────────────────────────────────

    use datafusion::arrow::array::{BinaryArray, Int32Array, Int64Array as I64Arr};
    use datafusion::arrow::datatypes::Field;
    use datafusion::logical_expr::ScalarUDF;

    /// Invokes the UDF over one key array, returning the per-row keep decision.
    fn probe(udf: &RuntimeFilterUdf, filter_id: i32, keys: ArrayRef) -> Vec<Option<bool>> {
        let rows = keys.len();
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Scalar(ScalarValue::Int32(Some(filter_id))),
                ColumnarValue::Array(keys),
            ],
            arg_fields: vec![
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ],
            number_rows: rows,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(Default::default()),
        };
        match udf.invoke_with_args(args).unwrap() {
            ColumnarValue::Array(a) => a.as_boolean().iter().collect(),
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => vec![v; rows],
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn udf_keeps_inserted_keys_and_drops_absent_ones() {
        let registry = new_registry();
        install_filter(&registry, 7, &built_from(&[10, 20, 30], 64 * 1024)).unwrap();
        let udf = RuntimeFilterUdf::new(registry);

        let keys: ArrayRef = Arc::new(I64Arr::from(vec![10, 20, 30]));
        assert_eq!(
            probe(&udf, 7, keys),
            vec![Some(true); 3],
            "no false negatives"
        );

        // Absent keys: not an invariant per row, but a filter that kept all of a
        // large absent sample would be pruning nothing.
        let absent: ArrayRef = Arc::new(I64Arr::from((1_000_000..1_002_000).collect::<Vec<i64>>()));
        let kept = probe(&udf, 7, absent)
            .into_iter()
            .filter(|k| k == &Some(true))
            .count();
        assert!(
            kept < 100,
            "expected most absent keys dropped, kept {kept}/2000"
        );
    }

    #[test]
    fn udf_fails_open_on_an_unknown_filter_id() {
        // A filter that never arrived must cost the optimization, not rows.
        let udf = RuntimeFilterUdf::new(new_registry());
        let keys: ArrayRef = Arc::new(I64Arr::from(vec![1, 2, 3]));
        assert_eq!(probe(&udf, 99, keys), vec![Some(true); 3]);
    }

    #[test]
    fn udf_drops_null_keys() {
        // A null key matches nothing under the equi-join semantics that permit a
        // probe-side filter at all, so dropping it is sound rather than merely safe.
        let registry = new_registry();
        install_filter(&registry, 1, &built_from(&[5], 4096)).unwrap();
        let udf = RuntimeFilterUdf::new(registry);
        let keys: ArrayRef = Arc::new(I64Arr::from(vec![Some(5), None]));
        assert_eq!(probe(&udf, 1, keys), vec![Some(true), Some(false)]);
    }

    #[test]
    fn udf_widens_narrower_key_types_consistently() {
        // The build side inserts i64; a probe column typed Int32 must still match,
        // which only holds because the UDF widens before hashing.
        let registry = new_registry();
        install_filter(&registry, 1, &built_from(&[42], 4096)).unwrap();
        let udf = RuntimeFilterUdf::new(registry);
        let narrow: ArrayRef = Arc::new(Int32Array::from(vec![42]));
        assert_eq!(
            probe(&udf, 1, narrow),
            vec![Some(true)],
            "Int32 key must widen to i64"
        );
    }

    #[test]
    fn udf_rejects_a_non_literal_filter_id() {
        // The id identifies which Bloom to probe; a per-row id would mean the plan
        // never pinned one, which is a planning bug rather than something to guess at.
        let udf = RuntimeFilterUdf::new(new_registry());
        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int32Array::from(vec![1, 1])) as ArrayRef),
                ColumnarValue::Array(Arc::new(I64Arr::from(vec![1, 2])) as ArrayRef),
            ],
            arg_fields: vec![
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Field::new("k", DataType::Int64, true)),
            ],
            number_rows: 2,
            return_field: Arc::new(Field::new("out", DataType::Boolean, true)),
            config_options: Arc::new(Default::default()),
        };
        assert!(udf.invoke_with_args(args).is_err());
    }

    #[test]
    fn udf_is_registrable_and_named_for_the_planner() {
        // The planner emits this name into the fragment, so it is part of the
        // contract rather than an implementation detail.
        let udf = ScalarUDF::from(RuntimeFilterUdf::new(new_registry()));
        assert_eq!(udf.name(), "os_runtime_filter");
        assert_eq!(
            udf.return_type(&[DataType::Int32, DataType::Int64])
                .unwrap(),
            DataType::Boolean
        );
    }

    #[test]
    fn installing_a_malformed_bitset_is_refused() {
        // Better to have no filter than one silently truncated by `Sbbf::new`.
        let registry = new_registry();
        assert!(install_filter(&registry, 1, &[0u8; 33]).is_err());
        assert!(registry.read().is_empty());
    }

    #[test]
    fn rejects_a_malformed_bitset() {
        assert!(RuntimeFilterBloom::from_bitset(&[]).is_err(), "empty");
        assert!(
            RuntimeFilterBloom::from_bitset(&[0u8; 33]).is_err(),
            "not a whole block"
        );
        assert!(
            RuntimeFilterBloom::from_bitset(&[0u8; 16]).is_err(),
            "below the minimum"
        );
    }

    #[test]
    fn effective_size_is_predictable_and_block_aligned() {
        // Callers must be able to predict the size, since contributions only merge at equal sizes.
        // `Sbbf` rounds to the next power of two, not the next block.
        for request in [
            0,
            1,
            31,
            32,
            33,
            4096,
            4097,
            64 * 1024,
            BITSET_MAX_LENGTH + 1,
        ] {
            let effective = effective_bitset_bytes(request);
            assert_eq!(
                effective % BLOCK_BYTES,
                0,
                "request {request} not block aligned"
            );
            assert!(
                effective >= BITSET_MIN_LENGTH,
                "request {request} below minimum"
            );
            assert!(
                effective <= BITSET_MAX_LENGTH,
                "request {request} above maximum"
            );
            let (bloom, reported) = RuntimeFilterBloom::with_num_bytes(request);
            assert_eq!(
                effective, reported,
                "prediction disagreed for request {request}"
            );
            assert_eq!(
                effective,
                bloom.to_bitset().unwrap().len(),
                "serialised size disagreed for request {request}"
            );
        }
    }

    #[test]
    fn requests_rounding_to_the_same_size_still_merge() {
        // Consequence of power-of-two rounding: 4097 and 8192 merge, 4096 and 4097 do not. Sizing is
        // therefore one query-level decision, not a per-task derivation.
        let a = built_from(&[1], 4097);
        let b = built_from(&[2], 8192);
        assert_eq!(
            a.len(),
            b.len(),
            "4097 and 8192 must round to the same size"
        );
        let mut merged = a.clone();
        merge_bitsets(&mut merged, &b).unwrap();

        let mut across_boundary = built_from(&[1], 4096);
        assert!(
            merge_bitsets(&mut across_boundary, &b).is_err(),
            "4096 and 8192 round differently and must not merge"
        );
    }
}
