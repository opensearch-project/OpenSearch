/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelReferentialConstraint;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.ArrayList;
import java.util.List;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.ConverterProvider;
import io.substrait.isthmus.SubstraitRelVisitor;
import io.substrait.isthmus.TypeConverter;
import io.substrait.isthmus.expression.AggregateFunctionConverter;
import io.substrait.isthmus.expression.FunctionMappings;
import io.substrait.isthmus.expression.ScalarFunctionConverter;
import io.substrait.isthmus.expression.WindowFunctionConverter;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.plan.ProtoPlanConverter;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.NamedScan;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;

/**
 * Converts Calcite RelNode fragments to Substrait protobuf bytes
 * for the DataFusion Rust runtime.
 *
 * <p>Dispatch summary:
 * <ul>
 *   <li>{@link #convertFragment(RelNode)} — full-fragment conversion via
 *       {@link #convertToSubstrait(RelNode)}, with StageInputScan rewriting
 *       for reduce-stage fragments.</li>
 *   <li>{@link #attachPartialAggOnTop(RelNode, byte[])} and
 *       {@link #attachFragmentOnTop(RelNode, byte[])} — convert the wrapping
 *       operator standalone, then rewire its input to the decoded inner plan's
 *       root via {@link #rewire(Plan, Rel, List)}.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    /**
     * Maps backend-specific Calcite operators to their Substrait extension names so Isthmus
     * serializes them through our {@code SimpleExtension} catalog. One entry per line so
     * parallel per-UDF PRs append without hotspot conflicts.
     * <ul>
     *   <li>{@link DelegatedPredicateFunction} → {@code delegated_predicate} (delegation to a peer backend).</li>
     *   <li>{@link SqlLibraryOperators#ILIKE} → {@code ilike} (case-insensitive LIKE; resolved by
     *       DataFusion's substrait consumer to a case-insensitive {@code LikeExpr}).</li>
     *   <li>{@link SqlLibraryOperators#DATE_PART} → {@code date_part} (target of DatePartAdapters' rewrite).</li>
     *   <li>{@link ConvertTzAdapter#LOCAL_CONVERT_TZ_OP} → {@code convert_tz} (Rust UDF).</li>
     *   <li>{@link UnixTimestampAdapter#LOCAL_TO_UNIXTIME_OP} → {@code to_unixtime} (DF native).</li>
     *   <li>{@link JsonFunctionAdapters.JsonAppendAdapter#LOCAL_JSON_APPEND_OP} →
     *       {@code json_append} (Rust UDF, homogeneous-string variadic path/value pairs).</li>
     *   <li>{@link JsonFunctionAdapters.JsonArrayLengthAdapter#LOCAL_JSON_ARRAY_LENGTH_OP} →
     *       {@code json_array_length} (Rust UDF).</li>
     *   <li>{@link JsonFunctionAdapters.JsonDeleteAdapter#LOCAL_JSON_DELETE_OP} →
     *       {@code json_delete} (Rust UDF, homogeneous-string variadic).</li>
     *   <li>{@link JsonFunctionAdapters.JsonExtendAdapter#LOCAL_JSON_EXTEND_OP} →
     *       {@code json_extend} (Rust UDF, homogeneous-string variadic path/value pairs).</li>
     *   <li>{@link JsonFunctionAdapters.JsonExtractAdapter#LOCAL_JSON_EXTRACT_OP} →
     *       {@code json_extract} (Rust UDF, homogeneous-string variadic).</li>
     *   <li>{@link JsonFunctionAdapters.JsonKeysAdapter#LOCAL_JSON_KEYS_OP} →
     *       {@code json_keys} (Rust UDF).</li>
     *   <li>{@link JsonFunctionAdapters.JsonSetAdapter#LOCAL_JSON_SET_OP} →
     *       {@code json_set} (Rust UDF, homogeneous-string variadic path/value pairs).</li>
     *   <li>{@link SqlLibraryOperators#REGEXP_CONTAINS} → {@code regex_match} (boolean regex match;
     *       resolved by DataFusion's substrait consumer to {@code Operator::RegexMatch}, the same
     *       binary operator that backs PostgreSQL's {@code ~} regex match). Lowering target for PPL
     *       {@code regex} command and {@code regexp_match()} function.</li>
     *   <li>{@link SqlStdOperatorTable#REPLACE} → {@code replace} (literal string replacement;
     *       lowering target for PPL `replace` command on non-wildcard patterns).</li>
     *   <li>{@link SqlLibraryOperators#REGEXP_REPLACE_3} → {@code regexp_replace} (regex string
     *       replacement; lowering target for PPL `replace` command on wildcard patterns and for
     *       PPL `replace()` / `regexp_replace()` functions in `eval`).</li>
     *   <li>{@link SqlLibraryOperators#REGEXP_REPLACE_PG_4} → {@code regexp_replace} (4-arg
     *       PostgreSQL-style with flags string; lowering target for PPL `rex mode=sed` with
     *       {@code g}/{@code i} flags. Reuses the same DataFusion {@code regexp_replace} UDF as
     *       the 3-arg form.</li>
     *   <li>{@link SqlLibraryOperators#TRANSLATE3} → {@code translate} (3-arg character
     *       transliteration; lowering target for PPL `rex mode=sed` with {@code y/from/to/}
     *       transliteration syntax). DataFusion's substrait consumer resolves the extension name
     *       "translate" to its native {@code translate} UDF
     *       (datafusion-functions/src/unicode/translate.rs).</li>
     *   <li>{@link RexExtractAdapter#LOCAL_REX_EXTRACT_OP} → {@code rex_extract} (Rust UDF;
     *       single-match named/numbered group extract). Lowering target for PPL
     *       {@code rex field=f "(?<g>...)"} extract command.</li>
     *   <li>{@link RexExtractMultiAdapter#LOCAL_REX_EXTRACT_MULTI_OP} → {@code rex_extract_multi}
     *       (Rust UDF; multi-match named/numbered group extract returning {@code list<varchar>}).
     *       Lowering target for PPL {@code rex ... max_match=N}.</li>
     *   <li>{@link RexOffsetAdapter#LOCAL_REX_OFFSET_OP} → {@code rex_offset} (Rust UDF;
     *       named-group position emission as a single string). Lowering target for PPL
     *       {@code rex ... offset_field=name}.</li>
     * </ul>
     */
    private static final List<FunctionMappings.Sig> ADDITIONAL_SCALAR_SIGS = List.of(
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(DelegationPossibleFunction.FUNCTION, DelegationPossibleFunction.NAME),
        FunctionMappings.s(SqlStdOperatorTable.ASCII, "ascii"),
        FunctionMappings.s(SqlStdOperatorTable.CHAR_LENGTH, "length"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_FUNCTION, "concat"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_WS, "concat_ws"),
        FunctionMappings.s(SqlLibraryOperators.ILIKE, "ilike"),
        FunctionMappings.s(SqlLibraryOperators.DATE_PART, "date_part"),
        // Engine-output cast rewrite target — see DatetimeOutputCastRewriter (issue #5420).
        // Routes Calcite's TO_CHAR call to DataFusion's native `to_char` so PPL's
        // documented space-separator timestamp output is preserved on the AE path.
        FunctionMappings.s(SqlLibraryOperators.TO_CHAR, "to_char"),
        FunctionMappings.s(SqlLibraryOperators.DATE_TRUNC, "date_trunc"),
        FunctionMappings.s(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, "convert_tz"),
        FunctionMappings.s(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, "to_unixtime"),
        // Niladic ops from DateTimeAdapters — each maps 1:1 to a DF builtin.
        FunctionMappings.s(DateTimeAdapters.LOCAL_NOW_OP, "now"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_DATE_OP, "current_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_TIME_OP, "current_time"),
        // PPL time(expr) → DF builtin to_time (TimeAdapter renames only).
        FunctionMappings.s(DateTimeAdapters.LOCAL_TIME_OP, "to_time"),
        // PPL date(expr) → DF builtin to_date (DateAdapter renames only).
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_OP, "to_date"),
        // PPL datetime(expr) → DF builtin to_timestamp (DatetimeAdapter renames only).
        FunctionMappings.s(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, "to_timestamp"),
        // PPL datetime + format functions → Rust UDFs registered in rust/src/udf/mod.rs.
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_EXTRACT_OP, "extract"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, "from_unixtime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKEDATE_OP, "makedate"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_MAKETIME_OP, "maketime"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_DATE_FORMAT_OP, "date_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_TIME_FORMAT_OP, "time_format"),
        FunctionMappings.s(RustUdfDateTimeAdapters.LOCAL_STR_TO_DATE_OP, "str_to_date"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_PG_4, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.REVERSE, "reverse"),
        FunctionMappings.s(SqlLibraryOperators.TRANSLATE3, "translate"),
        FunctionMappings.s(PositionAdapter.STRPOS, "strpos"),
        FunctionMappings.s(StrftimeFunctionAdapter.STRFTIME, "strftime"),
        FunctionMappings.s(ToNumberFunctionAdapter.TONUMBER, "tonumber"),
        FunctionMappings.s(ToStringFunctionAdapter.TOSTRING, "tostring"),
        FunctionMappings.s(SqlLibraryOperators.MD5, "md5"),
        FunctionMappings.s(SqlLibraryOperators.SHA1, "sha1"),
        FunctionMappings.s(SqlLibraryOperators.CRC32, "crc32"),
        FunctionMappings.s(Sha2FunctionAdapter.DIGEST, "digest"),
        FunctionMappings.s(Sha2FunctionAdapter.ENCODE, "encode"),
        FunctionMappings.s(RexExtractAdapter.LOCAL_REX_EXTRACT_OP, "rex_extract"),
        FunctionMappings.s(RexExtractMultiAdapter.LOCAL_REX_EXTRACT_MULTI_OP, "rex_extract_multi"),
        FunctionMappings.s(RexOffsetAdapter.LOCAL_REX_OFFSET_OP, "rex_offset"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(SqlStdOperatorTable.TRUNCATE, "trunc"),
        FunctionMappings.s(SqlStdOperatorTable.CBRT, "cbrt"),
        FunctionMappings.s(SqlStdOperatorTable.COT, "cot"),
        FunctionMappings.s(SqlStdOperatorTable.PI, "pi"),
        FunctionMappings.s(SqlStdOperatorTable.RAND, "random"),
        FunctionMappings.s(SqlLibraryOperators.LOG, "logb"),
        FunctionMappings.s(SignumFunction.FUNCTION, SignumFunction.NAME),
        FunctionMappings.s(JsonFunctionAdapters.JsonAppendAdapter.LOCAL_JSON_APPEND_OP, "json_append"),
        FunctionMappings.s(JsonFunctionAdapters.JsonArrayLengthAdapter.LOCAL_JSON_ARRAY_LENGTH_OP, "json_array_length"),
        FunctionMappings.s(JsonFunctionAdapters.JsonDeleteAdapter.LOCAL_JSON_DELETE_OP, "json_delete"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtendAdapter.LOCAL_JSON_EXTEND_OP, "json_extend"),
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAdapter.LOCAL_JSON_EXTRACT_OP, "json_extract"),
        FunctionMappings.s(JsonFunctionAdapters.JsonKeysAdapter.LOCAL_JSON_KEYS_OP, "json_keys"),
        FunctionMappings.s(JsonFunctionAdapters.JsonSetAdapter.LOCAL_JSON_SET_OP, "json_set"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        // Array S0 ladder — see DataFusionAnalyticsBackendPlugin.STANDARD_PROJECT_OPS /
        // ARRAY_RETURNING_PROJECT_OPS for the capability registration. ARRAY_LENGTH /
        // ARRAY_SLICE / ARRAY_DISTINCT pass through under their Calcite-stdlib names
        // (DataFusion's substrait consumer resolves them natively). MakeArrayAdapter /
        // ArrayToStringAdapter / ArrayElementAdapter rewrite PPL `array(...)` /
        // `mvjoin(...)` / `mvindex(...)` single-element to locally-declared SqlFunctions
        // so isthmus emits Substrait calls with DataFusion's native function names.
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_SLICE, "array_slice"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_DISTINCT, "array_distinct"),
        FunctionMappings.s(MakeArrayAdapter.LOCAL_MAKE_ARRAY_OP, "make_array"),
        FunctionMappings.s(ArrayToStringAdapter.LOCAL_ARRAY_TO_STRING_OP, "array_to_string"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_ARRAY_ELEMENT_OP, "array_element"),
        FunctionMappings.s(MvzipAdapter.LOCAL_MVZIP_OP, "mvzip"),
        FunctionMappings.s(MvfindAdapter.LOCAL_MVFIND_OP, "mvfind"),
        FunctionMappings.s(MvappendAdapter.LOCAL_MVAPPEND_OP, "mvappend"),
        // PPL bucketing UDFs — see DataFusionAnalyticsBackendPlugin.STANDARD_PROJECT_OPS for
        // capability registration and per-adapter Javadoc for semantics. Each LOCAL_*_OP is a
        // locally-declared SqlFunction (see *Adapter.java) so isthmus's ScalarFunctionConverter
        // binds the Sig by operator identity without importing sql/core's UDF declarations.
        FunctionMappings.s(SpanBucketAdapter.LOCAL_SPAN_BUCKET_OP, "span_bucket"),
        FunctionMappings.s(WidthBucketAdapter.LOCAL_WIDTH_BUCKET_OP, "width_bucket"),
        FunctionMappings.s(MinspanBucketAdapter.LOCAL_MINSPAN_BUCKET_OP, "minspan_bucket"),
        FunctionMappings.s(RangeBucketAdapter.LOCAL_RANGE_BUCKET_OP, "range_bucket")
    );

    /**
     * Maps aggregate operators to their Substrait extension names so isthmus serializes
     * them through our {@code SimpleExtension} catalog instead of the default Substrait
     * names.
     *
     * <p>{@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT} → {@code approx_distinct}
     * (declared in {@code opensearch_aggregate_functions.yaml}) routes to DataFusion's
     * native HyperLogLog {@code APPROX_DISTINCT} aggregate. Wiring this through isthmus'
     * {@code ADDITIONAL_AGGREGATE_SIGS} alone is not enough because isthmus's default
     * aggregate catalog already binds {@code APPROX_COUNT_DISTINCT} to substrait's
     * standard {@code approx_count_distinct} URN; when signatures merge, the default
     * binding overwrites ours in the matcher map. {@link OpenSearchAggregateFunctionConverter}
     * fixes that by filtering the stock sig out of the default list so our entry is the
     * only one that resolves to this operator.
     */
    private static final List<FunctionMappings.Sig> ADDITIONAL_AGGREGATE_SIGS = List.of(
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct")
    );

    /**
     * Subclassed {@link AggregateFunctionConverter} that removes isthmus's default binding
     * for {@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT} from the signature merge.
     * Without this, the default {@code approx_count_distinct} URN binding would shadow
     * our entry in {@link #ADDITIONAL_AGGREGATE_SIGS} and the YAML-declared
     * {@code approx_distinct} extension would never be reached.
     */
    private static final class OpenSearchAggregateFunctionConverter extends AggregateFunctionConverter {
        OpenSearchAggregateFunctionConverter(
            List<SimpleExtension.AggregateFunctionVariant> functions,
            List<FunctionMappings.Sig> additionalSignatures,
            RelDataTypeFactory typeFactory,
            TypeConverter typeConverter
        ) {
            super(functions, additionalSignatures, typeFactory, typeConverter);
        }

        @Override
        protected ImmutableList<FunctionMappings.Sig> getSigs() {
            return super.getSigs().stream()
                .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                .collect(ImmutableList.toImmutableList());
        }
    }

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        LOGGER.debug("Converting fragment [{}]", fragment.getClass().getSimpleName());
        // Rewrite any OpenSearchStageInputScan leaves to plain TableScan nodes so the
        // isthmus visitor (which only knows about Calcite core / Logical RelNodes)
        // emits a ReadRel with the stage-input-id as the named table. No-op when the
        // fragment has no StageInputScan leaves (shard-scan and Values cases).
        RelNode rewritten = rewriteStageInputScans(fragment);
        return convertToSubstrait(rewritten);
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        LOGGER.debug("Attaching partial aggregate on top of {} inner bytes", innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        Rel wrapper = convertStandalone(partialAggFragment);
        Plan rewired = rewire(
            inner,
            withAggregationPhase(wrapper, Expression.AggregationPhase.INITIAL_TO_INTERMEDIATE),
            fieldNames(partialAggFragment)
        );
        return serializePlan(SubstraitPlanRewriter.rewrite(rewired));
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        // Rewrite OpenSearchStageInputScans before standalone conversion so the isthmus
        // visitor can traverse the fragment without choking on planner-internal leaves.
        // The standalone conversion's children are discarded by rewire(...) anyway, but
        // the visitor still walks them top-down to build the wrapper rel.
        RelNode rewritten = rewriteStageInputScans(fragment);
        Rel wrapper = convertStandalone(rewritten);
        // SubstraitPlanRewriter must run on the assembled wrapper-over-inner plan, not
        // just on the inner bytes (those came in already rewritten from the leaf path).
        // The wrapper rel was just produced by isthmus and carries un-rewritten literals
        // (e.g. timestamp precision 6 vs Parquet's 3) — without this pass the rewritten
        // inner gets reattached under a non-rewritten wrapper, leaving the new wrapper
        // expressions out of sync with the rest of the plan and tripping DataFusion at
        // execution time. Same fix applied to attachPartialAggOnTop.
        return serializePlan(SubstraitPlanRewriter.rewrite(rewire(inner, wrapper, fieldNames(fragment))));
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        // Rewrite SqlTypeName.NULL literals (Calcite's untyped null, emitted for the
        // implicit ELSE arm of CASE) to typed nulls — isthmus' TypeConverter rejects NULL
        // with "Unable to convert the type NULL". The widening only changes literal type
        // tags; semantics and field names (used by Plan.Root.names) are unchanged.
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(fragment);
        // Rewrite DatetimeOutputCastRule's CAST(<TIMESTAMP> AS VARCHAR) to to_char(...) so
        // DataFusion emits PPL's space-separator timestamp format instead of Arrow's ISO-T.
        // See issue #5420.
        preprocessed = DatetimeOutputCastRewriter.rewrite(preprocessed);
        RelRoot root = RelRoot.of(preprocessed, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        Rel substraitRel;
        try {
            substraitRel = visitor.apply(root.rel);
        } catch (AssertionError e) {
            // Substrait validators (e.g. VariadicParameterConsistencyValidator,
            // RelOptUtil.eq via Litmus.THROW) throw AssertionError directly via Java
            // code rather than via the `assert` keyword, so JVM -da doesn't gate them.
            // If one fires inside a search thread, OpenSearchUncaughtExceptionHandler
            // exits the cluster JVM. Convert to IllegalStateException so the analytics-
            // engine error path treats it as a normal per-query failure (HTTP 500 with
            // a bucketable message) instead of taking down the cluster.
            throw new IllegalStateException("Substrait conversion rejected the plan: " + e.getMessage(), e);
        }

        List<String> fieldNames = root.fields.stream().map(field -> field.getValue()).toList();

        Plan.Root substraitRoot = Plan.Root.builder().input(substraitRel).names(fieldNames).build();
        Plan plan = Plan.builder().addRoots(substraitRoot).build();

        plan = SubstraitPlanRewriter.rewrite(plan);

        io.substrait.proto.Plan protoPlan = new PlanProtoConverter().toProto(plan);
        byte[] bytes = protoPlan.toByteArray();
        LOGGER.debug("Substrait plan: {} bytes", bytes.length);
        return bytes;
    }

    /**
     * Converts a single operator into a Substrait {@link Rel}. The operator may carry
     * children (e.g. the {@code attachPartialAggOnTop} caller passes a
     * {@code LogicalAggregate} whose input is the already-stripped inner tree); we
     * deliberately discard those children by taking only the outermost rel of the
     * conversion and rewiring its input during {@link #rewire(Plan, Rel, List)}.
     */
    private Rel convertStandalone(RelNode operator) {
        // Same untyped-NULL preprocessing rationale as convertToSubstrait — the standalone
        // wrapper conversion is just as susceptible to a SqlTypeName.NULL literal lurking in
        // a CASE call attached on top of an inner plan.
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(operator);
        // Same rationale as convertToSubstrait — issue #5420.
        preprocessed = DatetimeOutputCastRewriter.rewrite(preprocessed);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        return visitor.apply(preprocessed);
    }

    /**
     * Rewires the Substrait {@code wrapper} rel to sit above the root relation of
     * {@code inner}. Returns a new {@link Plan} whose single root is
     * {@code wrapper(inner.root)} with {@code wrapperNames} attached as the root's
     * names list. Supports the known single-input wrappers emitted by our SPI
     * methods ({@link Aggregate}, {@link Sort}, {@link Filter}, {@link Project},
     * {@link Fetch}).
     *
     * <p>{@code wrapperNames} must describe the wrapper's output schema — one entry
     * per leaf field in the wrapper's row type. For schema-preserving wrappers
     * (Sort, Filter, Fetch) these match the inner plan's names; for schema-reshaping
     * wrappers (Aggregate, Project) they don't. Using the inner's names where the
     * wrapper reshapes the schema causes DataFusion to reject the Plan with
     * "Names list must match exactly to nested schema" — surfaces with
     * Aggregate-over-Join over exchange-gathered Scan.
     */
    static Plan rewire(Plan inner, Rel wrapper, List<String> wrapperNames) {
        if (inner.getRoots().isEmpty()) {
            throw new IllegalArgumentException("Inner Substrait plan has no root relation to rewire under wrapper");
        }
        Plan.Root innerRoot = inner.getRoots().get(0);
        Rel innerRel = innerRoot.getInput();
        Rel rewired = replaceInput(wrapper, innerRel);
        return Plan.builder().addRoots(Plan.Root.builder().input(rewired).names(wrapperNames).build()).build();
    }

    /** Wrapper's output column names from its Calcite row type. */
    private static List<String> fieldNames(RelNode fragment) {
        return fragment.getRowType().getFieldList().stream().map(RelDataTypeField::getName).toList();
    }

    private static Rel replaceInput(Rel wrapper, Rel newInput) {
        if (wrapper instanceof Aggregate agg) {
            return Aggregate.builder().from(agg).input(newInput).build();
        }
        if (wrapper instanceof Sort sort) {
            return Sort.builder().from(sort).input(newInput).build();
        }
        if (wrapper instanceof Filter filter) {
            return Filter.builder().from(filter).input(newInput).build();
        }
        if (wrapper instanceof Project project) {
            return Project.builder().from(project).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // SystemLimit + LogicalSort with offset/fetch lower to a Substrait Fetch rel.
            // Used by the implicit query-size limit at the top of every analytics-engine plan
            // and by user-level `head N` clauses; both arrive here when attached above a Union.
            return Fetch.builder().from(fetch).input(newInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    /**
     * Overrides the {@link Expression.AggregationPhase} on every {@link Aggregate.Measure}
     * inside an {@link Aggregate} wrapper. No-op for non-aggregate wrappers.
     *
     * <p>Isthmus hardcodes {@code INITIAL_TO_RESULT} on every aggregate-function
     * invocation. For the partial-agg-attach-on-shard path we want
     * {@code INITIAL_TO_INTERMEDIATE}; the final-agg path stays at
     * {@code INITIAL_TO_RESULT} (isthmus's default) which the DataFusion
     * substrait deserialiser treats as the single-stage/final form.
     */
    private static Rel withAggregationPhase(Rel rel, Expression.AggregationPhase phase) {
        if (!(rel instanceof Aggregate agg)) {
            return rel;
        }
        List<Aggregate.Measure> newMeasures = new ArrayList<>(agg.getMeasures().size());
        for (Aggregate.Measure m : agg.getMeasures()) {
            AggregateFunctionInvocation fn = m.getFunction();
            AggregateFunctionInvocation rephased = AggregateFunctionInvocation.builder().from(fn).aggregationPhase(phase).build();
            newMeasures.add(Aggregate.Measure.builder().from(m).function(rephased).build());
        }
        return Aggregate.builder().from(agg).measures(newMeasures).build();
    }

    /**
     * Rewrites every {@link OpenSearchStageInputScan} in the RelNode tree to a plain
     * Calcite {@link TableScan} whose qualified name matches what the matching
     * {@link DatafusionReduceSink} input partition registers on the native session.
     *
     * <p>The table id is {@code "input-<childStageId>"}, mirroring
     * {@code AbstractDatafusionReduceSink.inputIdFor}. For a single-input fragment the
     * sole stage id (typically 0) reproduces the conventional {@code "input-0"} name; for
     * multi-input shapes (Union) each branch refers to its own child stage id and the
     * isthmus visitor emits one {@link NamedScan} per branch.
     */
    private static RelNode rewriteStageInputScans(RelNode node) {
        if (node instanceof OpenSearchStageInputScan scan) {
            return new StageInputTableScan(scan.getCluster(), scan.getTraitSet(), "input-" + scan.getChildStageId(), scan.getRowType());
        }
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean changed = false;
        for (RelNode input : node.getInputs()) {
            RelNode rewritten = rewriteStageInputScans(input);
            newInputs.add(rewritten);
            if (rewritten != input) {
                changed = true;
            }
        }
        if (changed) {
            return node.copy(node.getTraitSet(), newInputs);
        }
        return node;
    }

    // ── Visitor wiring ──────────────────────────────────────────────────────────

    private SubstraitRelVisitor createVisitor(RelNode relNode) {
        RelDataTypeFactory typeFactory = relNode.getCluster().getTypeFactory();
        TypeConverter typeConverter = TypeConverter.DEFAULT;
        ScalarFunctionConverter scalarConverter = new ScalarFunctionConverter(
            extensions.scalarFunctions(),
            ADDITIONAL_SCALAR_SIGS,
            typeFactory,
            typeConverter
        );
        AggregateFunctionConverter aggConverter = new OpenSearchAggregateFunctionConverter(
            extensions.aggregateFunctions(),
            ADDITIONAL_AGGREGATE_SIGS,
            typeFactory,
            typeConverter
        );
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(extensions.windowFunctions(), typeFactory);
        ConverterProvider converterProvider = new ConverterProvider(
            typeFactory,
            extensions,
            scalarConverter,
            aggConverter,
            windowConverter,
            typeConverter
        );
        return new SubstraitRelVisitor(converterProvider);
    }

    // ── Plan serde helpers ──────────────────────────────────────────────────────

    /** Decodes serialized Substrait bytes into a model-level {@link Plan}. */
    private Plan decodePlan(byte[] bytes) {
        try {
            io.substrait.proto.Plan proto = io.substrait.proto.Plan.parseFrom(bytes);
            return new ProtoPlanConverter(extensions).from(proto);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to decode Substrait plan bytes", e);
        }
    }

    /** Serializes a model-level {@link Plan} to proto bytes. */
    private static byte[] serializePlan(Plan plan) {
        return new PlanProtoConverter().toProto(plan).toByteArray();
    }

    // ── Calcite TableScan wrappers for OpenSearchStageInputScan rewrite ─────────

    /**
     * Minimal {@link TableScan} representing a stage-input source. The backing
     * {@link StageInputRelOptTable} reports the stage-input id as its single qualified
     * name; isthmus converts this to a {@link NamedScan} with that one-element name.
     */
    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

    /**
     * Minimal {@link RelOptTable} implementation — only {@code getQualifiedName()} and
     * {@code getRowType()} are consulted by the isthmus visitor.
     */
    static final class StageInputRelOptTable implements RelOptTable {
        private final List<String> qualifiedName;
        private final RelDataType rowType;

        StageInputRelOptTable(String stageInputId, RelDataType rowType) {
            this.qualifiedName = List.of(stageInputId);
            this.rowType = rowType;
        }

        @Override
        public List<String> getQualifiedName() {
            return qualifiedName;
        }

        @Override
        public RelDataType getRowType() {
            return rowType;
        }

        @Override
        public double getRowCount() {
            return 100;
        }

        @Override
        public RelOptSchema getRelOptSchema() {
            return null;
        }

        @Override
        public RelNode toRel(ToRelContext context) {
            throw new UnsupportedOperationException("StageInputRelOptTable.toRel not supported");
        }

        @Override
        public List<ColumnStrategy> getColumnStrategies() {
            return List.of();
        }

        @Override
        public <C> C unwrap(Class<C> aClass) {
            return null;
        }

        @Override
        public boolean isKey(ImmutableBitSet columns) {
            return false;
        }

        @Override
        public List<ImmutableBitSet> getKeys() {
            return List.of();
        }

        @Override
        public List<RelReferentialConstraint> getReferentialConstraints() {
            return List.of();
        }

        @Override
        public List<RelCollation> getCollationList() {
            return List.of();
        }

        @Override
        public RelDistribution getDistribution() {
            return RelDistributions.ANY;
        }

        @Override
        @SuppressWarnings("rawtypes")
        public org.apache.calcite.linq4j.tree.Expression getExpression(Class clazz) {
            return null;
        }

        @Override
        public RelOptTable extend(List<RelDataTypeField> extendedFields) {
            return this;
        }
    }
}
