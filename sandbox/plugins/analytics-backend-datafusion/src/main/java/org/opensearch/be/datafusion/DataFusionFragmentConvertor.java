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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;
import org.opensearch.analytics.spi.DelegatedPredicateFunction;
import org.opensearch.analytics.spi.DelegationPossibleFunction;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.be.datafusion.planner.adapter.NumericConversionFunctionAdapter;
import org.opensearch.be.datafusion.planner.adapter.TimeConversionFunctionAdapter;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.FunctionArg;
import io.substrait.expression.ImmutableAggregateFunctionInvocation;
import io.substrait.extension.ExtensionCollector;
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
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.relation.Aggregate;
import io.substrait.relation.Fetch;
import io.substrait.relation.Filter;
import io.substrait.relation.Project;
import io.substrait.relation.Rel;
import io.substrait.relation.Sort;
import io.substrait.type.NamedStruct;
import io.substrait.type.Type;
import io.substrait.type.proto.TypeProtoConverter;

/** Converts Calcite RelNode fragments to Substrait protobuf bytes for the DataFusion Rust runtime. */
public class DataFusionFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(DataFusionFragmentConvertor.class);

    /** Per-field accessors for {@code pattern_parser}'s STRUCT output; see {@link ItemTypeRebuilder}. */
    static final SqlOperator LOCAL_PATTERN_PARSER_GET_PATTERN_OP = new SqlFunction(
        "pattern_parser_get_pattern",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    static final SqlOperator LOCAL_PATTERN_PARSER_GET_TOKENS_OP = new SqlFunction(
        "pattern_parser_get_tokens",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    /** TopK reduce expression: evaluates opaque aggregate state to a sortable scalar. */
    static final SqlOperator REDUCE_EVAL_OP = new SqlFunction(
        "reduce_eval",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BIGINT_NULLABLE,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private static final List<FunctionMappings.Sig> ADDITIONAL_SCALAR_SIGS = List.of(
        FunctionMappings.s(DelegatedPredicateFunction.FUNCTION, DelegatedPredicateFunction.NAME),
        FunctionMappings.s(REDUCE_EVAL_OP, "reduce_eval"),
        FunctionMappings.s(DelegationPossibleFunction.FUNCTION, DelegationPossibleFunction.NAME),
        FunctionMappings.s(SqlStdOperatorTable.ASCII, "ascii"),
        FunctionMappings.s(SqlStdOperatorTable.CHAR_LENGTH, "length"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_FUNCTION, "concat"),
        FunctionMappings.s(SqlLibraryOperators.CONCAT_WS, "concat_ws"),
        FunctionMappings.s(SqlLibraryOperators.ILIKE, "ilike"),
        FunctionMappings.s(SqlLibraryOperators.DATE_PART, "date_part"),
        FunctionMappings.s(SqlLibraryOperators.TO_CHAR, "to_char"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.IP_TO_STRING_OP, "ip_to_string"),
        FunctionMappings.s(IpBinaryCastFunctionAdapter.BINARY_TO_BASE64_OP, "binary_to_base64"),
        FunctionMappings.s(SqlLibraryOperators.DATE_TRUNC, "date_trunc"),
        FunctionMappings.s(SpanAdapter.LOCAL_DATE_BIN_OP, "date_bin"),
        FunctionMappings.s(PatternParserAdapter.LOCAL_PATTERN_PARSER_OP, "pattern_parser"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_PATTERN_OP, "pattern_parser_get_pattern"),
        FunctionMappings.s(LOCAL_PATTERN_PARSER_GET_TOKENS_OP, "pattern_parser_get_tokens"),
        FunctionMappings.s(ConvertTzAdapter.LOCAL_CONVERT_TZ_OP, "convert_tz"),
        FunctionMappings.s(ParseAdapter.LOCAL_PARSE_OP, "parse"),
        FunctionMappings.s(SqlStdOperatorTable.ITEM, "item"),
        FunctionMappings.s(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, "to_unixtime"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_NOW_OP, "now"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_DATE_OP, "current_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_CURRENT_TIME_OP, "current_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TIME_OP, "to_time"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_OP, "to_date"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP, "to_timestamp"),
        FunctionMappings.s(DateTimeAdapters.LOCAL_DATE_TRUNC_OP, "date_trunc"),
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
        FunctionMappings.s(NumericConversionFunctionAdapter.NUM, "num"),
        FunctionMappings.s(NumericConversionFunctionAdapter.AUTO, "auto"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MEMK, "memk"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMCOMMA, "rmcomma"),
        FunctionMappings.s(NumericConversionFunctionAdapter.RMUNIT, "rmunit"),
        FunctionMappings.s(NumericConversionFunctionAdapter.DUR2SEC, "dur2sec"),
        FunctionMappings.s(NumericConversionFunctionAdapter.MSTIME, "mstime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.CTIME, "ctime"),
        FunctionMappings.s(TimeConversionFunctionAdapter.MKTIME, "mktime"),
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
        FunctionMappings.s(JsonFunctionAdapters.JsonExtractAllAdapter.LOCAL_JSON_EXTRACT_ALL_OP, "json_extract_all"),
        FunctionMappings.s(JsonFunctionAdapters.JsonKeysAdapter.LOCAL_JSON_KEYS_OP, "json_keys"),
        FunctionMappings.s(JsonFunctionAdapters.JsonSetAdapter.LOCAL_JSON_SET_OP, "json_set"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_CONTAINS, "regex_match"),
        FunctionMappings.s(SqlStdOperatorTable.REPLACE, "replace"),
        FunctionMappings.s(SqlLibraryOperators.REGEXP_REPLACE_3, "regexp_replace"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_LENGTH, "array_length"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_SLICE, "array_slice"),
        FunctionMappings.s(SqlLibraryOperators.ARRAY_DISTINCT, "array_distinct"),
        FunctionMappings.s(MakeArrayAdapter.LOCAL_MAKE_ARRAY_OP, "make_array"),
        FunctionMappings.s(ArrayToStringAdapter.LOCAL_ARRAY_TO_STRING_OP, "array_to_string"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_ARRAY_ELEMENT_OP, "array_element"),
        FunctionMappings.s(ArrayElementAdapter.LOCAL_MAP_EXTRACT_OP, "map_extract"),
        FunctionMappings.s(MvzipAdapter.LOCAL_MVZIP_OP, "mvzip"),
        FunctionMappings.s(MvfindAdapter.LOCAL_MVFIND_OP, "mvfind"),
        FunctionMappings.s(MvappendAdapter.LOCAL_MVAPPEND_OP, "mvappend"),
        FunctionMappings.s(SpanBucketAdapter.LOCAL_SPAN_BUCKET_OP, "span_bucket"),
        FunctionMappings.s(WidthBucketAdapter.LOCAL_WIDTH_BUCKET_OP, "width_bucket"),
        FunctionMappings.s(MinspanBucketAdapter.LOCAL_MINSPAN_BUCKET_OP, "minspan_bucket"),
        FunctionMappings.s(RangeBucketAdapter.LOCAL_RANGE_BUCKET_OP, "range_bucket"),
        FunctionMappings.s(ConvAdapter.LOCAL_CONV_OP, "conv")
    );

    /** Local stubs for PPL state-expanding aggregates; swapped in by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_TAKE_OP = new SqlAggFunction(
        "take",
        null,
        SqlKind.OTHER_FUNCTION,
        // FORCE_NULLABLE so AggregateCall.create accepts a nullable explicit return type.
        ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_FIRST_OP = new SqlAggFunction(
        "first_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    static final SqlAggFunction LOCAL_LAST_OP = new SqlAggFunction(
        "last_value",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** Backs both LIST (call's isDistinct) and VALUES (forces isDistinct=true). */
    static final SqlAggFunction LOCAL_ARRAY_AGG_OP = new SqlAggFunction(
        "array_agg",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.TO_ARRAY.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** FINAL-side merge for LIST; un-nests per-shard list states. */
    static final SqlAggFunction LOCAL_LIST_MERGE_OP = new SqlAggFunction(
        "list_merge",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** FINAL-side merge for VALUES — re-deduplicates after concatenation. */
    static final SqlAggFunction LOCAL_LIST_MERGE_DISTINCT_OP = new SqlAggFunction(
        "list_merge_distinct",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.ANY,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /**
     * PPL {@code percentile_approx(field, percentile)} → DataFusion's builtin
     * {@code approx_percentile_cont(field, percentile)}. PPL's trailing field-type-flag
     * arg is stripped by {@link PplAggregateCallRewriter} before binding; the percentile
     * literal is rescaled from PPL's [0, 100] to DataFusion's [0, 1] convention via
     * {@link LocalAggOp#normaliseLiteralArg} at substrait emission.
     */
    static final LocalAggOp LOCAL_PERCENTILE_APPROX_OP = new LocalAggOp(
        "approx_percentile_cont",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0.andThen(SqlTypeTransforms.FORCE_NULLABLE),
        OperandTypes.ANY_ANY
    ) {
        @Override
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            if (argIndex == 1 && lit.getValue() instanceof BigDecimal bd) {
                BigDecimal scaled = bd.divide(BigDecimal.valueOf(100), MathContext.DECIMAL64);
                RelDataType doubleType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.DOUBLE), true);
                return rexBuilder.makeLiteral(scaled, doubleType);
            }
            return lit;
        }
    };

    /** BRAIN window stub for {@code patterns ... method=BRAIN mode=label}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_WINDOW_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_FORCE_NULLABLE,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    /** BRAIN aggregate stub; return type is supplied by {@link PplAggregateCallRewriter}. */
    static final SqlAggFunction LOCAL_INTERNAL_PATTERN_OP = new SqlAggFunction(
        "internal_pattern",
        null,
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.USER_DEFINED_FUNCTION,
        false,
        false,
        Optionality.FORBIDDEN
    ) {
    };

    private static final List<FunctionMappings.Sig> ADDITIONAL_AGGREGATE_SIGS = List.of(
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct"),
        FunctionMappings.s(LOCAL_TAKE_OP, "take"),
        FunctionMappings.s(LOCAL_FIRST_OP, "first_value"),
        FunctionMappings.s(LOCAL_LAST_OP, "last_value"),
        FunctionMappings.s(LOCAL_ARRAY_AGG_OP, "array_agg"),
        FunctionMappings.s(LOCAL_LIST_MERGE_OP, "list_merge"),
        FunctionMappings.s(LOCAL_LIST_MERGE_DISTINCT_OP, "list_merge_distinct"),
        FunctionMappings.s(LOCAL_PERCENTILE_APPROX_OP, "approx_percentile_cont"),
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_OP, "internal_pattern")
    );

    private static final List<FunctionMappings.Sig> ADDITIONAL_WINDOW_SIGS = List.of(
        FunctionMappings.s(LOCAL_INTERNAL_PATTERN_WINDOW_OP, "internal_pattern"),
        // Mirror ADDITIONAL_AGGREGATE_SIGS: rename APPROX_COUNT_DISTINCT to DataFusion's `approx_distinct`.
        FunctionMappings.s(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, "approx_distinct")
    );

    /**
     * Shared {@link TypeProtoConverter} for schema-only conversions. Safe as a singleton
     * because schema-only Reads convert primitive Calcite types to primitive Substrait
     * protos — no functions or user-defined types touch the inner {@link ExtensionCollector},
     * so it never accumulates per-call state. Avoids re-allocating both objects on every
     * {@link #convertSchemaOnlyRead} call.
     */
    private static final TypeProtoConverter SCHEMA_ONLY_TYPE_PROTO_CONVERTER = new TypeProtoConverter(new ExtensionCollector());

    private final SimpleExtension.ExtensionCollection extensions;

    public DataFusionFragmentConvertor(SimpleExtension.ExtensionCollection extensions) {
        this.extensions = extensions;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        LOGGER.debug("Converting fragment [{}]", fragment.getClass().getSimpleName());
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

    /**
     * Builds a schema-only stub plan directly via Substrait protos — no isthmus, no
     * Calcite RelNode round-trip. Output:
     * <pre>
     *   Plan { relations: [PlanRel { Root { input: Rel { Read { named_table: "input-&lt;id&gt;";
     *                                                          base_schema: rowType } },
     *                                 names: rowType.fieldNames }}] }
     * </pre>
     *
     * <p>Used by the LM stage path: LM runs Java-only scatter/gather/stitch and emits no
     * Substrait compute, but the parent reduce sink (Stage 3) still calls
     * {@code registerPartitionStream} which needs the partition's named-table id and base
     * schema. This stub is the minimum proto that satisfies that path. Bypassing isthmus
     * avoids unnecessary {@code SubstraitRelVisitor} setup and keeps the produced bytes
     * tightly scoped to the schema we care about.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Fully-qualified names below: io.substrait.proto.{Plan,Rel,NamedStruct,RelRoot} clash with already-imported single-name imports.
        NamedStruct ns = TypeConverter.DEFAULT.toNamedStruct(rowType);
        io.substrait.proto.NamedStruct nsProto = ns.toProto(SCHEMA_ONLY_TYPE_PROTO_CONVERTER);

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(nsProto)
            .build();

        io.substrait.proto.Rel inputRel = io.substrait.proto.Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(io.substrait.proto.RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = io.substrait.proto.Plan.newBuilder().addRelations(planRel).build().toByteArray();
        LOGGER.debug("Schema-only Read for stage [{}]: {} bytes", childStageId, bytes.length);
        return bytes;
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LOGGER.debug("Attaching generic fragment [{}] on top of {} inner bytes", fragment.getClass().getSimpleName(), innerBytes.length);
        Plan inner = decodePlan(innerBytes);
        RelNode rewritten = rewriteStageInputScans(fragment);
        Rel wrapper = convertStandalone(rewritten);
        // Rewriter must run on the assembled plan so wrapper literals get rewritten alongside the inner.
        return serializePlan(SubstraitPlanRewriter.rewrite(rewire(inner, wrapper, fieldNames(fragment))));
    }

    private byte[] convertToSubstrait(RelNode fragment) {
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(fragment);
        preprocessed = PplAggregateCallRewriter.rewrite(preprocessed);
        preprocessed = PplWindowCallRewriter.rewrite(preprocessed);
        preprocessed = ItemTypeRebuilder.rewrite(preprocessed);
        RelRoot root = RelRoot.of(preprocessed, SqlKind.SELECT);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        Rel substraitRel;
        try {
            substraitRel = visitor.apply(root.rel);
        } catch (AssertionError e) {
            // Substrait validators throw AssertionError directly (not via `assert`), so -da
            // doesn't gate them; convert to a normal exception so we don't crash the cluster.
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

    /** Converts a single operator into a Substrait {@link Rel}; children are discarded and rewired by {@link #rewire}. */
    private Rel convertStandalone(RelNode operator) {
        RelNode preprocessed = UntypedNullPreprocessor.rewrite(operator);
        preprocessed = PplAggregateCallRewriter.rewrite(preprocessed);
        preprocessed = PplWindowCallRewriter.rewrite(preprocessed);
        preprocessed = ItemTypeRebuilder.rewrite(preprocessed);
        SubstraitRelVisitor visitor = createVisitor(preprocessed);
        return visitor.apply(preprocessed);
    }

    /** Rewires {@code wrapper} above {@code inner}'s root; {@code wrapperNames} must match the wrapper's output schema. */
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
            // Lifted-window shape: outer Project references a window column from the lower Project.
            if (project.getInput() instanceof Project lower && containsWindowFunction(lower)) {
                Rel rewiredLower = replaceInput(lower, newInput);
                return Project.builder().from(project).input(rewiredLower).build();
            }
            return Project.builder().from(project).input(newInput).build();
        }
        if (wrapper instanceof Fetch fetch) {
            // A single Calcite LogicalSort carrying both a collation AND a fetch/offset lowers to
            // Fetch(Sort(input)) — two Substrait rels from one node. Rewiring the Fetch's input
            // directly would drop the Sort and lose global order before the limit. Descend into
            // the Sort so the shape becomes Fetch(Sort(newInput)): gather, sort globally, then limit.
            Rel rewiredInput = fetch.getInput() instanceof Sort ? replaceInput(fetch.getInput(), newInput) : newInput;
            return Fetch.builder().from(fetch).input(rewiredInput).build();
        }
        throw new UnsupportedOperationException(
            "Cannot attach-on-top a Substrait Rel of type " + wrapper.getClass().getSimpleName() + " — no single-input rewire defined"
        );
    }

    private static boolean containsWindowFunction(Project project) {
        for (Expression expr : project.getExpressions()) {
            if (expr instanceof Expression.WindowFunctionInvocation) {
                return true;
            }
        }
        return false;
    }

    /** Forces {@code phase} on every measure of an Aggregate wrapper (isthmus hardcodes INITIAL_TO_RESULT). */
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

    /** Rewrites {@link OpenSearchStageInputScan} leaves to TableScan with {@code "input-<childStageId>"} names. */
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
        // Filter isthmus's default APPROX_COUNT_DISTINCT binding so our `approx_distinct` entry wins.
        // The convert() override inlines literal-Project columns into the AggregateFunctionInvocation
        // as Substrait literals so two-stage UDAFs (e.g. TAKE's N) see the constant on the Final side.
        AggregateFunctionConverter aggConverter = new AggregateFunctionConverter(
            extensions.aggregateFunctions(),
            ADDITIONAL_AGGREGATE_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }

            @Override
            public Optional<AggregateFunctionInvocation> convert(
                RelNode input,
                Type.Struct inputType,
                AggregateCall call,
                Function<RexNode, Expression> rexConverter
            ) {
                Optional<AggregateFunctionInvocation> bound = super.convert(input, inputType, call, rexConverter);
                if (bound.isEmpty() || !(input instanceof org.apache.calcite.rel.core.Project project)) {
                    return bound;
                }
                AggregateFunctionInvocation fn = bound.get();
                List<RexNode> projects = project.getProjects();
                List<FunctionArg> args = fn.arguments();
                List<FunctionArg> rewritten = null;
                RexBuilder rexBuilder = project.getCluster().getRexBuilder();
                for (int i = 0; i < args.size(); i++) {
                    FunctionArg arg = args.get(i);
                    if (!(arg instanceof io.substrait.expression.FieldReference fr)) continue;
                    Integer offset = simpleStructOffset(fr);
                    if (offset == null || offset < 0 || offset >= projects.size()) continue;
                    if (!(projects.get(offset) instanceof RexLiteral rexLit)) continue;
                    if (rewritten == null) rewritten = new ArrayList<>(args);
                    RexNode toConvert = call.getAggregation() instanceof LocalAggOp localOp
                        ? localOp.normaliseLiteralArg(i, rexLit, rexBuilder, typeFactory)
                        : rexLit;
                    rewritten.set(i, rexConverter.apply(toConvert));
                }
                if (rewritten == null) return bound;
                return Optional.of(ImmutableAggregateFunctionInvocation.builder().from(fn).arguments(rewritten).build());
            }
        };
        // Same APPROX_COUNT_DISTINCT filter as aggConverter — let our `approx_distinct` entry win.
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
            extensions.windowFunctions(),
            ADDITIONAL_WINDOW_SIGS,
            typeFactory,
            typeConverter
        ) {
            @Override
            protected ImmutableList<FunctionMappings.Sig> getSigs() {
                return super.getSigs().stream()
                    .filter(sig -> sig.operator != SqlStdOperatorTable.APPROX_COUNT_DISTINCT)
                    .collect(ImmutableList.toImmutableList());
            }
        };
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

    /** Column offset for a simple input-rooted single-segment {@code StructField}, else null. */
    private static Integer simpleStructOffset(io.substrait.expression.FieldReference fr) {
        if (fr.isOuterReference() || fr.isLambdaParameterReference()) return null;
        if (!fr.inputExpression().isEmpty()) return null;
        if (fr.segments().size() != 1) return null;
        io.substrait.expression.FieldReference.ReferenceSegment seg = fr.segments().get(0);
        if (!(seg instanceof io.substrait.expression.FieldReference.StructField sf)) return null;
        return sf.offset();
    }

    /**
     * Local aggregate stub that may transform inlined literal args before substrait emission.
     * Other local stubs without transformations stay as plain {@link SqlAggFunction}; the
     * {@code convert()} override only invokes {@link #normaliseLiteralArg} when the call's
     * operator is a {@code LocalAggOp}, so adding a new normalisation is purely a matter of
     * subclassing here next to the op's declaration.
     */
    abstract static class LocalAggOp extends SqlAggFunction {
        LocalAggOp(
            String name,
            SqlKind kind,
            org.apache.calcite.sql.type.SqlReturnTypeInference returnTypeInference,
            org.apache.calcite.sql.type.SqlOperandTypeChecker operandTypeChecker
        ) {
            super(
                name,
                null,
                kind,
                returnTypeInference,
                null,
                operandTypeChecker,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false,
                Optionality.FORBIDDEN
            );
        }

        /** Identity by default; override to transform the {@code argIndex}-th inlined literal arg. */
        public RexNode normaliseLiteralArg(int argIndex, RexLiteral lit, RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
            return lit;
        }
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

    static final class StageInputTableScan extends TableScan {
        StageInputTableScan(RelOptCluster cluster, RelTraitSet traitSet, String stageInputId, RelDataType rowType) {
            super(cluster, traitSet, List.of(), new StageInputRelOptTable(stageInputId, rowType));
        }
    }

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
