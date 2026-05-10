/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.AbstractNameMappingAdapter;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.BackendExecutionContext;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.ExchangeSinkProvider;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterDelegationHandle;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.analytics.spi.StdOperatorRewriteAdapter;
import org.opensearch.be.datafusion.indexfilter.FilterTreeCallbacks;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * SPI extension discovered by analytics-engine via {@code META-INF/services}.
 * <p>
 * Receives the fully-initialized {@link DataFusionPlugin} instance via its single-arg
 * constructor (supported by {@code PluginsService.createExtension()}), so it has access
 * to the {@link DataFusionService} created during plugin lifecycle.
 * <p>
 * Declares all analytics query capabilities (operators, filters, aggregates) and
 * creates per-shard execution engines.
 */
public class DataFusionAnalyticsBackendPlugin implements AnalyticsSearchBackendPlugin {

    private static final Set<EngineCapability> ENGINE_CAPS = Set.of(EngineCapability.SORT, EngineCapability.UNION);

    private static final Set<FieldType> SUPPORTED_FIELD_TYPES = new HashSet<>();
    static {
        SUPPORTED_FIELD_TYPES.addAll(FieldType.numeric());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.keyword());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.date());
        SUPPORTED_FIELD_TYPES.add(FieldType.BOOLEAN);
        SUPPORTED_FIELD_TYPES.add(FieldType.TEXT);
    }

    // Filter-side scalar functions DataFusion can evaluate natively. Comparisons, arithmetic
    // (for `where x + y > 0`-style predicates), and Calcite's SARG fold (IN/BETWEEN/range-union)
    // are all supported via the Substrait default extension catalog. AND/OR/NOT are recursed into
    // by {@link OpenSearchFilterRule} structurally and never looked up here, but registering them
    // keeps the capability declaration complete for auditing and symmetric with PROJECT_OPS.
    private static final Set<ScalarFunction> STANDARD_FILTER_OPS = Set.of(
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IS_NULL,
        ScalarFunction.IS_NOT_NULL,
        ScalarFunction.IN,
        ScalarFunction.LIKE,
        ScalarFunction.REGEXP_CONTAINS,
        ScalarFunction.SARG_PREDICATE,
        ScalarFunction.PLUS,
        ScalarFunction.MINUS,
        ScalarFunction.TIMES,
        ScalarFunction.DIVIDE,
        ScalarFunction.MOD
    );

    // Project-side scalar functions DataFusion can evaluate natively. Each entry corresponds to a
    // PPL command/function we want the analytics-engine planner to route through DataFusion. Add
    // here only after verifying the function deserializes through Substrait isthmus into a plan
    // DataFusion's native runtime can execute (see DataFusionFragmentConvertor for the conversion
    // path). COALESCE is the lowering target of PPL `fillnull`. CAST is required because
    // ReduceExpressionsRule.ProjectReduceExpressionsRule (in PlannerImpl) constant-folds field
    // references through equality filters into typed literals — e.g. after `where str0 = 'FURNITURE'`,
    // the projection `fields str0` is rewritten to `CAST('FURNITURE' AS VARCHAR)`. CAST is also the
    // implicit result-type narrowing PPL inserts after a UDF call whose declared return type differs
    // from the eval column's inferred type (e.g. JSON_ARRAY_LENGTH returns INTEGER_FORCE_NULLABLE).
    // CONCAT is the lowering target of PPL `eval`'s `+` for strings (Calcite emits `||`, resolved to
    // CONCAT in ScalarFunction); SAFE_CAST covers PPL `eval`'s explicit nullable `CAST(... AS ...)`
    // expressions. The remaining comparison / arithmetic / logical operators are project-capable
    // for eval-style projections.
    private static final Set<ScalarFunction> STANDARD_PROJECT_OPS = Set.of(
        ScalarFunction.COALESCE,
        ScalarFunction.CEIL,
        ScalarFunction.CAST,
        ScalarFunction.CONCAT,
        ScalarFunction.SAFE_CAST,
        // CASE — Calcite emits CASE WHEN ... THEN ... END for conditional expressions, including
        // PPL `count(eval(predicate))` (lowered to COUNT(CASE WHEN predicate THEN ... ELSE NULL END))
        // and explicit `eval x = case(cond, val, ...)`. Isthmus translates SqlKind.CASE structurally
        // to a Substrait IfThen rel — no extension lookup needed, no adapter required. DataFusion's
        // substrait consumer handles IfThen natively. Without this entry, the analytics planner
        // rejects the operator with "No backend supports scalar function [CASE] among [datafusion]"
        // before substrait emission.
        ScalarFunction.CASE,
        // ABS / SUBSTRING — PPL sort-pushdown moves these into the project tree; DataFusion has
        // both natively and isthmus's default catalog binds them, so no adapter needed.
        ScalarFunction.ABS,
        ScalarFunction.SUBSTRING,
        ScalarFunction.SARG_PREDICATE,
        ScalarFunction.MINUS,
        ScalarFunction.ACOS,
        ScalarFunction.ASIN,
        ScalarFunction.ATAN,
        ScalarFunction.ATAN2,
        ScalarFunction.CBRT,
        ScalarFunction.EQUALS,
        ScalarFunction.NOT_EQUALS,
        ScalarFunction.GREATER_THAN,
        ScalarFunction.GREATER_THAN_OR_EQUAL,
        ScalarFunction.LESS_THAN,
        ScalarFunction.LESS_THAN_OR_EQUAL,
        ScalarFunction.IN,
        ScalarFunction.LIKE,
        ScalarFunction.REGEXP_CONTAINS,
        ScalarFunction.REPLACE,
        ScalarFunction.REGEXP_REPLACE,
        ScalarFunction.PLUS,
        ScalarFunction.TIMES,
        ScalarFunction.DIVIDE,
        ScalarFunction.MOD,
        ScalarFunction.COS,
        ScalarFunction.COT,
        ScalarFunction.DEGREES,
        ScalarFunction.EXP,
        ScalarFunction.FLOOR,
        ScalarFunction.LN,
        ScalarFunction.LOG,
        ScalarFunction.LOG10,
        ScalarFunction.LOG2,
        ScalarFunction.PI,
        ScalarFunction.POWER,
        ScalarFunction.RADIANS,
        ScalarFunction.RAND,
        ScalarFunction.ROUND,
        ScalarFunction.SIGN,
        ScalarFunction.SIN,
        ScalarFunction.TAN,
        ScalarFunction.TRUNCATE,
        ScalarFunction.COSH,
        ScalarFunction.SINH,
        ScalarFunction.E,
        ScalarFunction.EXPM1,
        ScalarFunction.SCALAR_MAX,
        ScalarFunction.SCALAR_MIN,
        // Date-part extractors rewrite to date_part(<unit>, ts) via DatePartAdapters.
        // Functions whose DF-builtin semantics diverge from legacy PPL (SECOND / DAYOFWEEK /
        // SYSDATE / DATE_FORMAT / TIME_FORMAT / STRFTIME, plus 2-arg FROM_UNIXTIME / DATETIME)
        // are omitted here and stay on the legacy engine until a dedicated Rust UDF lands —
        // matching the convert_tz / to_unixtime pattern already in this plugin.
        ScalarFunction.YEAR,
        ScalarFunction.QUARTER,
        ScalarFunction.MONTH,
        ScalarFunction.MONTH_OF_YEAR,
        ScalarFunction.DAY,
        ScalarFunction.DAYOFMONTH,
        ScalarFunction.DAYOFYEAR,
        ScalarFunction.DAY_OF_YEAR,
        ScalarFunction.HOUR,
        ScalarFunction.HOUR_OF_DAY,
        ScalarFunction.MINUTE,
        ScalarFunction.MINUTE_OF_HOUR,
        ScalarFunction.MICROSECOND,
        ScalarFunction.WEEK,
        ScalarFunction.WEEK_OF_YEAR,
        // Niladic now/current_* family maps 1:1 to DF builtins.
        ScalarFunction.NOW,
        ScalarFunction.CURRENT_TIMESTAMP,
        ScalarFunction.CURRENT_DATE,
        ScalarFunction.CURDATE,
        ScalarFunction.CURRENT_TIME,
        ScalarFunction.CURTIME,
        ScalarFunction.CONVERT_TZ,
        ScalarFunction.UNIX_TIMESTAMP,
        // DATE(expr) / TIME(expr) / MAKETIME(h,m,s) are intentionally not advertised:
        // PPL's Calcite binding for these returns VARCHAR rather than DATE/TIME, so
        // downstream `year(date(ts))` / `hour(maketime(...))` lowers to
        // date_part(string, string?) — no matching DataFusion signature. Left on the
        // legacy engine until PPL wires them to produce real DATE/TIME types.
        // EXTRACT(unit FROM ts) is also not advertised: isthmus resolves SqlKind.EXTRACT
        // through scalar-function lookup rather than emitting a native Substrait extract,
        // and we'd need a dedicated adapter + yaml entry to route it to DataFusion's
        // date_part. Left on legacy engine until that adapter lands; PPL date-part
        // functions cover the same semantics.
        ScalarFunction.ASCII,
        ScalarFunction.CONCAT_WS,
        ScalarFunction.LEFT,
        ScalarFunction.LENGTH,
        ScalarFunction.CHAR_LENGTH,
        ScalarFunction.LOCATE,
        ScalarFunction.POSITION,
        ScalarFunction.LOWER,
        ScalarFunction.LTRIM,
        ScalarFunction.REVERSE,
        ScalarFunction.RIGHT,
        ScalarFunction.RTRIM,
        ScalarFunction.TRIM,
        ScalarFunction.SUBSTR,
        ScalarFunction.UPPER,
        ScalarFunction.STRCMP,
        ScalarFunction.TOSTRING,
        ScalarFunction.NUMBER_TO_STRING,
        ScalarFunction.TONUMBER,
        ScalarFunction.JSON_APPEND,
        ScalarFunction.JSON_ARRAY_LENGTH,
        ScalarFunction.JSON_DELETE,
        ScalarFunction.JSON_EXTEND,
        ScalarFunction.JSON_EXTRACT,
        ScalarFunction.JSON_KEYS,
        ScalarFunction.JSON_SET,
        // Array functions whose RETURN type is element-typed (not ARRAY itself), so the
        // capability lookup at OpenSearchProjectRule resolves the call's return type to a
        // standard scalar FieldType and matches against SUPPORTED_FIELD_TYPES.
        // ARRAY_LENGTH returns BIGINT → FieldType.LONG; ARRAY_JOIN returns VARCHAR →
        // FieldType.KEYWORD (renamed to DataFusion `array_to_string` via {@link ArrayToStringAdapter}).
        // ITEM returns the array's element type (any of the supported scalar types) — used by
        // PPL `mvindex(arr, N)` single-element form.
        ScalarFunction.ARRAY_LENGTH,
        ScalarFunction.ARRAY_JOIN,
        ScalarFunction.ITEM,
        // PPL `mvfind` returns INTEGER (the 0-based index of the first match, or NULL); backed
        // by a custom Rust UDF on the DataFusion session context (`udf::mvfind`), routed via
        // {@link MvfindAdapter}.
        ScalarFunction.MVFIND
    );

    /**
     * Project-side scalar functions whose return type is {@code ARRAY}. Registered separately
     * because the capability lookup keys on the call's return type, and for these the lookup
     * resolves to {@link FieldType#ARRAY} — which is intentionally not in
     * {@link #SUPPORTED_FIELD_TYPES} (filter and aggregate operators have no meaningful semantics
     * over array-typed values, so we don't want them claiming viability there).
     *
     * <p>{@code ARRAY} (PPL {@code array(a, b, …)} constructor) renames to DataFusion's
     * {@code make_array} via {@link MakeArrayAdapter}. {@code ARRAY_SLICE} and
     * {@code ARRAY_DISTINCT} pass through by name (Calcite stdlib operator names match
     * DataFusion's native names — isthmus default catalog binds them).
     */
    private static final Set<ScalarFunction> ARRAY_RETURNING_PROJECT_OPS = Set.of(
        ScalarFunction.ARRAY,
        ScalarFunction.ARRAY_SLICE,
        ScalarFunction.ARRAY_DISTINCT,
        // PPL `mvzip` returns ARRAY<VARCHAR>; backed by a custom Rust UDF on the DataFusion
        // session context (`udf::mvzip`), routed via {@link MvzipAdapter}.
        ScalarFunction.MVZIP,
        // PPL `mvappend` returns ARRAY<commonType>; backed by a custom Rust UDF
        // (`udf::mvappend`), routed via {@link MvappendAdapter}.
        ScalarFunction.MVAPPEND
    );

    private static final Set<AggregateFunction> AGG_FUNCTIONS = Set.of(
        AggregateFunction.SUM,
        AggregateFunction.SUM0,
        AggregateFunction.MIN,
        AggregateFunction.MAX,
        AggregateFunction.COUNT,
        AggregateFunction.AVG
    );

    private final DataFusionPlugin plugin;

    public DataFusionAnalyticsBackendPlugin(DataFusionPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public String name() {
        return plugin.name();
    }

    @Override
    public BackendCapabilityProvider getCapabilityProvider() {
        return new BackendCapabilityProvider() {
            @Override
            public Set<EngineCapability> supportedEngineCapabilities() {
                return ENGINE_CAPS;
            }

            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }

            @Override
            public Set<ScanCapability> scanCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                return Set.of(new ScanCapability.DocValues(formats, Set.copyOf(SUPPORTED_FIELD_TYPES)));
            }

            @Override
            public Set<FilterCapability> filterCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<FilterCapability> caps = new HashSet<>();
                for (ScalarFunction op : STANDARD_FILTER_OPS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(new FilterCapability.Standard(op, Set.of(type), formats));
                    }
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<ProjectCapability> projectCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<ProjectCapability> caps = new HashSet<>();
                for (ScalarFunction op : STANDARD_PROJECT_OPS) {
                    caps.add(new ProjectCapability.Scalar(op, Set.copyOf(SUPPORTED_FIELD_TYPES), formats, true));
                }
                for (ScalarFunction op : ARRAY_RETURNING_PROJECT_OPS) {
                    caps.add(new ProjectCapability.Scalar(op, Set.of(FieldType.ARRAY), formats, true));
                }
                return Set.copyOf(caps);
            }

            @Override
            public Set<AggregateCapability> aggregateCapabilities() {
                Set<String> formats = Set.copyOf(plugin.getSupportedFormats());
                Set<AggregateCapability> caps = new HashSet<>();
                for (AggregateFunction func : AGG_FUNCTIONS) {
                    for (FieldType type : SUPPORTED_FIELD_TYPES) {
                        caps.add(AggregateCapability.simple(func, Set.of(type), formats));
                    }
                }
                return Set.copyOf(caps);
            }

            @Override
            public Map<ScalarFunction, ScalarFunctionAdapter> scalarFunctionAdapters() {
                // Map entries are alphabetical (Map.ofEntries past 5 pairs, else spotless inlines).
                // Alias pairs share an adapter instance but need separate enum entries because
                // ScalarFunction.fromSqlFunction resolves by enum name.
                DatePartAdapters month = DatePartAdapters.month();
                DatePartAdapters day = DatePartAdapters.day();
                DatePartAdapters dayOfYear = DatePartAdapters.dayOfYear();
                DatePartAdapters hour = DatePartAdapters.hour();
                DatePartAdapters minute = DatePartAdapters.minute();
                DatePartAdapters week = DatePartAdapters.week();
                DateTimeAdapters.NowAdapter now = new DateTimeAdapters.NowAdapter();
                DateTimeAdapters.CurrentDateAdapter currentDate = new DateTimeAdapters.CurrentDateAdapter();
                DateTimeAdapters.CurrentTimeAdapter currentTime = new DateTimeAdapters.CurrentTimeAdapter();
                return Map.ofEntries(
                    Map.entry(ScalarFunction.ARRAY, new MakeArrayAdapter()),
                    Map.entry(ScalarFunction.ARRAY_JOIN, new ArrayToStringAdapter()),
                    Map.entry(ScalarFunction.ARRAY_SLICE, new ArraySliceAdapter()),
                    Map.entry(ScalarFunction.ITEM, new ArrayElementAdapter()),
                    Map.entry(ScalarFunction.MVFIND, new MvfindAdapter()),
                    Map.entry(ScalarFunction.MVZIP, new MvzipAdapter()),
                    Map.entry(ScalarFunction.MVAPPEND, new MvappendAdapter()),
                    Map.entry(ScalarFunction.CONCAT, new ConcatFunctionAdapter()),
                    Map.entry(ScalarFunction.CONVERT_TZ, new ConvertTzAdapter()),
                    Map.entry(ScalarFunction.COSH, new HyperbolicOperatorAdapter(SqlLibraryOperators.COSH)),
                    Map.entry(ScalarFunction.CURDATE, currentDate),
                    Map.entry(ScalarFunction.CURRENT_DATE, currentDate),
                    Map.entry(ScalarFunction.CURRENT_TIME, currentTime),
                    Map.entry(ScalarFunction.CURRENT_TIMESTAMP, now),
                    Map.entry(ScalarFunction.CURTIME, currentTime),
                    Map.entry(ScalarFunction.DAY, day),
                    Map.entry(ScalarFunction.DAYOFMONTH, day),
                    Map.entry(ScalarFunction.DAYOFYEAR, dayOfYear),
                    Map.entry(ScalarFunction.DAY_OF_YEAR, dayOfYear),
                    Map.entry(ScalarFunction.DIVIDE, new StdOperatorRewriteAdapter("DIVIDE", SqlStdOperatorTable.DIVIDE)),
                    Map.entry(ScalarFunction.E, new EConstantAdapter()),
                    Map.entry(ScalarFunction.EXPM1, new Expm1Adapter()),
                    Map.entry(ScalarFunction.HOUR, hour),
                    Map.entry(ScalarFunction.HOUR_OF_DAY, hour),
                    Map.entry(ScalarFunction.JSON_APPEND, new JsonFunctionAdapters.JsonAppendAdapter()),
                    Map.entry(ScalarFunction.JSON_ARRAY_LENGTH, new JsonFunctionAdapters.JsonArrayLengthAdapter()),
                    Map.entry(ScalarFunction.JSON_DELETE, new JsonFunctionAdapters.JsonDeleteAdapter()),
                    Map.entry(ScalarFunction.JSON_EXTEND, new JsonFunctionAdapters.JsonExtendAdapter()),
                    Map.entry(ScalarFunction.JSON_EXTRACT, new JsonFunctionAdapters.JsonExtractAdapter()),
                    Map.entry(ScalarFunction.JSON_KEYS, new JsonFunctionAdapters.JsonKeysAdapter()),
                    Map.entry(ScalarFunction.JSON_SET, new JsonFunctionAdapters.JsonSetAdapter()),
                    Map.entry(ScalarFunction.LIKE, new LikeAdapter()),
                    Map.entry(ScalarFunction.LOCATE, new PositionAdapter()),
                    Map.entry(ScalarFunction.MICROSECOND, DatePartAdapters.microsecond()),
                    Map.entry(ScalarFunction.MINUTE, minute),
                    Map.entry(ScalarFunction.MINUTE_OF_HOUR, minute),
                    Map.entry(ScalarFunction.MOD, new StdOperatorRewriteAdapter("MOD", SqlStdOperatorTable.MOD)),
                    Map.entry(ScalarFunction.MONTH, month),
                    Map.entry(ScalarFunction.MONTH_OF_YEAR, month),
                    Map.entry(ScalarFunction.NUMBER_TO_STRING, new ToStringFunctionAdapter()),
                    Map.entry(ScalarFunction.NOW, now),
                    Map.entry(ScalarFunction.POSITION, new PositionAdapter()),
                    Map.entry(ScalarFunction.QUARTER, DatePartAdapters.quarter()),
                    Map.entry(ScalarFunction.REGEXP_REPLACE, new RegexpReplaceAdapter()),
                    Map.entry(ScalarFunction.SARG_PREDICATE, new SargAdapter()),
                    Map.entry(ScalarFunction.SCALAR_MAX, nameMapping(SqlLibraryOperators.GREATEST)),
                    Map.entry(ScalarFunction.SCALAR_MIN, nameMapping(SqlLibraryOperators.LEAST)),
                    Map.entry(ScalarFunction.SIGN, nameMapping(SignumFunction.FUNCTION)),
                    Map.entry(ScalarFunction.SINH, new HyperbolicOperatorAdapter(SqlLibraryOperators.SINH)),
                    Map.entry(ScalarFunction.STRCMP, new StrcmpFunctionAdapter()),
                    Map.entry(ScalarFunction.SUBSTR, nameMapping(SqlStdOperatorTable.SUBSTRING)),
                    Map.entry(ScalarFunction.SUBSTRING, nameMapping(SqlStdOperatorTable.SUBSTRING)),
                    Map.entry(ScalarFunction.TIMESTAMP, new TimestampFunctionAdapter()),
                    Map.entry(ScalarFunction.TONUMBER, new ToNumberFunctionAdapter()),
                    Map.entry(ScalarFunction.TOSTRING, new ToStringFunctionAdapter()),
                    Map.entry(ScalarFunction.UNIX_TIMESTAMP, new UnixTimestampAdapter()),
                    Map.entry(ScalarFunction.WEEK, week),
                    Map.entry(ScalarFunction.WEEK_OF_YEAR, week),
                    Map.entry(ScalarFunction.YEAR, DatePartAdapters.year())
                );
            }
        };
    }

    /**
     * Pure rename from a PPL scalar to {@code target} — no prepend / append operands.
     * Concrete subclass of {@link AbstractNameMappingAdapter} because the abstract
     * base cannot be instantiated directly.
     */
    private static AbstractNameMappingAdapter nameMapping(SqlOperator target) {
        return new AbstractNameMappingAdapter(target, java.util.List.of(), java.util.List.of()) {
        };
    }

    @Override
    public FragmentConvertor getFragmentConvertor() {
        return new DataFusionFragmentConvertor(plugin.getSubstraitExtensions());
    }

    @Override
    public SearchExecEngineProvider getSearchExecEngineProvider() {
        return (ctx, backendContext) -> {
            DataFusionService dataFusionService = plugin.getDataFusionService();
            if (dataFusionService == null) {
                throw new IllegalStateException("DataFusionService not initialized — createComponents() may not have been called");
            }

            DatafusionReader dfReader = null;

            if (ctx.getReader() != null) {
                DataFormatRegistry registry = plugin.getDataFormatRegistry();
                for (String formatName : plugin.getSupportedFormats()) {
                    dfReader = ctx.getReader().getReader(registry.format(formatName), DatafusionReader.class);
                    if (dfReader != null) {
                        break;
                    }
                }
            }

            if (dfReader == null) {
                throw new IllegalStateException("No DatafusionReader available in the acquired reader");
            }
            DatafusionContext context = new DatafusionContext(ctx.getTask(), dfReader, dataFusionService.getNativeRuntime());
            if (backendContext != null) {
                DataFusionSessionState sessionState = (DataFusionSessionState) backendContext;
                context.setSessionContextHandle(sessionState.sessionContextHandle());
            }
            DatafusionSearchExecEngine engine = new DatafusionSearchExecEngine(context);
            engine.prepare(ctx);
            return engine;
        };
    }

    @Override
    public FragmentInstructionHandlerFactory getInstructionHandlerFactory() {
        return new DataFusionInstructionHandlerFactory(plugin);
    }

    @Override
    public ExchangeSinkProvider getExchangeSinkProvider() {
        return ctx -> {
            DataFusionService svc = plugin.getDataFusionService();
            if (svc == null) {
                throw new IllegalStateException("DataFusionService not initialized");
            }
            String mode = plugin.getClusterService() != null
                ? plugin.getClusterService().getClusterSettings().get(DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE)
                : "streaming";
            // Memtable mode is single-input only (DatafusionMemtableReduceSink registers
            // exactly one MemTable at close time). Multi-input shapes (Union, future Join)
            // need per-child input partitions, which only the streaming sink implements via
            // MultiInputExchangeSink#sinkForChild. Auto-fall-back to streaming so end users
            // don't have to flip the cluster setting per query.
            // TODO: lift this fallback once the memtable sink registers one MemTable per
            // child stage (see DatafusionMemtableReduceSink class javadoc).
            if ("memtable".equals(mode) && ctx.childInputs().size() == 1) {
                return new DatafusionMemtableReduceSink(ctx, svc.getNativeRuntime());
            }
            return new DatafusionReduceSink(ctx, svc.getNativeRuntime());
        };
    }

    @Override
    public void configureFilterDelegation(FilterDelegationHandle handle, BackendExecutionContext backendContext) {
        // Install the handle as the FFM upcall target. All Rust callbacks
        // (createProvider, createCollector, collectDocs, release*) route to it.
        FilterTreeCallbacks.setHandle(handle);
    }
}
