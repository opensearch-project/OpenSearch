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
import org.opensearch.analytics.spi.JoinCapability;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.analytics.spi.SearchExecEngineProvider;
import org.opensearch.analytics.spi.StdOperatorRewriteAdapter;
import org.opensearch.analytics.spi.WindowCapability;
import org.opensearch.analytics.spi.WindowFunction;
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

    private static final Set<EngineCapability> ENGINE_CAPS = Set.of(EngineCapability.SORT, EngineCapability.UNION, EngineCapability.VALUES);

    private static final Set<FieldType> SUPPORTED_FIELD_TYPES = new HashSet<>();
    static {
        SUPPORTED_FIELD_TYPES.addAll(FieldType.numeric());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.keyword());
        SUPPORTED_FIELD_TYPES.addAll(FieldType.date());
        SUPPORTED_FIELD_TYPES.add(FieldType.BOOLEAN);
        SUPPORTED_FIELD_TYPES.add(FieldType.TEXT);
        SUPPORTED_FIELD_TYPES.add(FieldType.BINARY);
        SUPPORTED_FIELD_TYPES.add(FieldType.IP);
        SUPPORTED_FIELD_TYPES.add(FieldType.MATCH_ONLY_TEXT);
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
        ScalarFunction.TRANSLATE,
        ScalarFunction.REX_EXTRACT,
        ScalarFunction.REX_EXTRACT_MULTI,
        ScalarFunction.REX_OFFSET,
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
        // SECOND / SECOND_OF_MINUTE / DAYOFWEEK / DAY_OF_WEEK use dedicated adapters
        // (FLOOR cast for SECOND, +1 offset for DAYOFWEEK) to preserve PPL's MySQL
        // semantics on top of DF's date_part; see SecondAdapter / DayOfWeekAdapter.
        ScalarFunction.YEAR,
        ScalarFunction.QUARTER,
        ScalarFunction.MONTH,
        ScalarFunction.MONTH_OF_YEAR,
        ScalarFunction.DAY,
        ScalarFunction.DAYOFMONTH,
        ScalarFunction.DAYOFWEEK,
        ScalarFunction.DAY_OF_WEEK,
        ScalarFunction.DAYOFYEAR,
        ScalarFunction.DAY_OF_YEAR,
        ScalarFunction.HOUR,
        ScalarFunction.HOUR_OF_DAY,
        ScalarFunction.MINUTE,
        ScalarFunction.MINUTE_OF_HOUR,
        ScalarFunction.SECOND,
        ScalarFunction.SECOND_OF_MINUTE,
        ScalarFunction.MICROSECOND,
        ScalarFunction.WEEK,
        ScalarFunction.WEEK_OF_YEAR,
        // Niladic now/current_* family maps 1:1 to DF builtins. SYSDATE is an
        // approximation — PPL SYSDATE uses the systemClock (call-time) while NOW
        // uses queryStartClock; the wall-clock difference is sub-millisecond on a
        // single-statement OLAP query so routing both to DF `now` is acceptable.
        ScalarFunction.NOW,
        ScalarFunction.CURRENT_TIMESTAMP,
        ScalarFunction.CURRENT_DATE,
        ScalarFunction.CURDATE,
        ScalarFunction.CURRENT_TIME,
        ScalarFunction.CURTIME,
        ScalarFunction.SYSDATE,
        ScalarFunction.CONVERT_TZ,
        ScalarFunction.UNIX_TIMESTAMP,
        ScalarFunction.STRFTIME,
        // PPL `time(expr)` / `date(expr)` — extract time-of-day / date component
        // from a TIMESTAMP / DATE / TIME / string value. Route to DataFusion's
        // builtins `to_time` / `to_date` via TimeAdapter / DateAdapter. Safe on
        // the analytics-engine path because sql-repo PR #5408
        // (DatetimeUdtNormalizeRule) rewrites EXPR_TIME / EXPR_DATE → standard
        // Calcite TIME / DATE on the RexCall return type, so downstream consumers
        // see a real time/date type and Isthmus serializes accordingly.
        ScalarFunction.TIME,
        ScalarFunction.DATE,
        // PPL `datetime(expr)` — parse/cast into a TIMESTAMP. Routes to DF's
        // builtin `to_timestamp` via DatetimeAdapter. The single-arg
        // `timestamp(expr)` form shares these semantics but its ScalarFunction
        // slot is already bound to TimestampFunctionAdapter for VARCHAR literal
        // folding, so it stays on the legacy engine.
        ScalarFunction.DATETIME,
        // PPL extract / make* / format / from_unixtime are implemented as Rust UDFs
        // to preserve MySQL semantics that DataFusion builtins don't match: EXTRACT
        // supports 10 composite units (DAY_SECOND → ddHHmmss etc.) that are not a
        // single date_part; MAKETIME / MAKEDATE / FROM_UNIXTIME need DOUBLE inputs
        // and PPL-specific NULL-on-negative / year-wraparound behavior; DATE_FORMAT
        // / TIME_FORMAT / STR_TO_DATE translate MySQL format tokens (%i / %s / %p …)
        // that DataFusion's `to_char` does not recognize.
        ScalarFunction.EXTRACT,
        ScalarFunction.FROM_UNIXTIME,
        ScalarFunction.MAKETIME,
        ScalarFunction.MAKEDATE,
        ScalarFunction.DATE_FORMAT,
        ScalarFunction.TIME_FORMAT,
        ScalarFunction.STR_TO_DATE,
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
        ScalarFunction.MVFIND,
        ScalarFunction.BINARY,
        // Logical connectives — emitted in projections where boolean expressions are composed:
        // `case(a = 0 and b = 0, …)`, `eval x = a or b`, `eval x = NOT y`. DataFusion's substrait
        // consumer handles them natively.
        ScalarFunction.AND,
        ScalarFunction.OR,
        ScalarFunction.NOT,
        ScalarFunction.MD5,
        ScalarFunction.SHA1,
        ScalarFunction.SHA2,
        ScalarFunction.CRC32,
        // PPL `span(field, interval, unit?)` — bucketing for `stats … by span(...)`. Numeric
        // span lowers to {@code floor(field/interval)*interval}; time span (interval=1) to
        // {@code date_trunc(unit, field)}. Both targets are substrait-default operators.
        ScalarFunction.SPAN,
        // PPL bucketing label functions — Rust UDFs returning VARCHAR labels (e.g. "0-100").
        // SPAN_BUCKET reachable today via `bin <f> span=N`. WIDTH_BUCKET / MINSPAN_BUCKET /
        // RANGE_BUCKET reach through `bin <f> bins=N` / `minspan=N` / `start=X end=Y`, which
        // lower to MIN/MAX OVER () empty-partition window aggregates — exercised end-to-end
        // by the bucket IT suites once the window-pushdown follow-up (#21668) lands. See
        // per-adapter Javadoc for semantics + collision notes.
        ScalarFunction.SPAN_BUCKET,
        ScalarFunction.WIDTH_BUCKET,
        ScalarFunction.MINSPAN_BUCKET,
        ScalarFunction.RANGE_BUCKET
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
        AggregateFunction.AVG,
        AggregateFunction.APPROX_COUNT_DISTINCT
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
            public Set<JoinCapability> joinCapabilities() {
                return Set.of(
                    new JoinCapability(
                        Set.of(
                            JoinCapability.JoinKind.INNER,
                            JoinCapability.JoinKind.LEFT,
                            JoinCapability.JoinKind.RIGHT,
                            JoinCapability.JoinKind.FULL,
                            JoinCapability.JoinKind.SEMI,
                            JoinCapability.JoinKind.ANTI,
                            JoinCapability.JoinKind.CROSS
                        ),
                        Set.copyOf(plugin.getSupportedFormats())
                    )
                );
            }

            @Override
            public Set<WindowCapability> windowCapabilities() {
                return Set.of(
                    new WindowCapability(
                        Set.of(WindowFunction.SUM, WindowFunction.AVG, WindowFunction.COUNT, WindowFunction.MIN, WindowFunction.MAX),
                        Set.copyOf(plugin.getSupportedFormats())
                    )
                );
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
                    // PPL rex extract-mode multi-match returns array<varchar>; the planner
                    // keys capability lookups on the call's return type, so this op must be
                    // registered against FieldType.ARRAY rather than the scalar set used by
                    // every other op (UPPER, ABS, ...) — those genuinely don't return arrays.
                    Set<FieldType> types = op == ScalarFunction.REX_EXTRACT_MULTI
                        ? Set.of(FieldType.ARRAY)
                        : Set.copyOf(SUPPORTED_FIELD_TYPES);
                    caps.add(new ProjectCapability.Scalar(op, types, formats, true));
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
                        // 3-arg constructor leaves decomposition=null so the
                        // AggregateDecompositionResolver falls back to the enum's
                        // intermediateFields + finalExpression — the single source of truth
                        // for per-function distributed-execution behavior. Accepts any
                        // AggregateFunction.Type (SIMPLE, APPROXIMATE, ...), unlike the
                        // per-type factory methods which assert on Type.
                        caps.add(new AggregateCapability(func, Set.of(type), formats));
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
                DayOfWeekAdapter dayOfWeek = new DayOfWeekAdapter();
                SecondAdapter second = new SecondAdapter();
                return Map.ofEntries(
                    Map.entry(ScalarFunction.ARRAY, new MakeArrayAdapter()),
                    Map.entry(ScalarFunction.ARRAY_JOIN, new ArrayToStringAdapter()),
                    Map.entry(ScalarFunction.ARRAY_SLICE, new ArraySliceAdapter()),
                    Map.entry(ScalarFunction.ITEM, new ArrayElementAdapter()),
                    Map.entry(ScalarFunction.MVFIND, new MvfindAdapter()),
                    Map.entry(ScalarFunction.MVZIP, new MvzipAdapter()),
                    Map.entry(ScalarFunction.MVAPPEND, new MvappendAdapter()),
                    Map.entry(ScalarFunction.BINARY, new BinaryFunctionAdapter()),
                    Map.entry(ScalarFunction.CONCAT, new ConcatFunctionAdapter()),
                    Map.entry(ScalarFunction.CONVERT_TZ, new ConvertTzAdapter()),
                    Map.entry(ScalarFunction.COSH, new HyperbolicOperatorAdapter(SqlLibraryOperators.COSH)),
                    Map.entry(ScalarFunction.CURDATE, currentDate),
                    Map.entry(ScalarFunction.CURRENT_DATE, currentDate),
                    Map.entry(ScalarFunction.CURRENT_TIME, currentTime),
                    Map.entry(ScalarFunction.CURRENT_TIMESTAMP, now),
                    Map.entry(ScalarFunction.CURTIME, currentTime),
                    Map.entry(ScalarFunction.DATE, new DateTimeAdapters.DateAdapter()),
                    Map.entry(ScalarFunction.DATETIME, new DateTimeAdapters.DatetimeAdapter()),
                    Map.entry(ScalarFunction.DATE_FORMAT, new RustUdfDateTimeAdapters.DateFormatAdapter()),
                    Map.entry(ScalarFunction.DAY, day),
                    Map.entry(ScalarFunction.DAYOFMONTH, day),
                    Map.entry(ScalarFunction.DAYOFWEEK, dayOfWeek),
                    Map.entry(ScalarFunction.DAYOFYEAR, dayOfYear),
                    Map.entry(ScalarFunction.DAY_OF_WEEK, dayOfWeek),
                    Map.entry(ScalarFunction.DAY_OF_YEAR, dayOfYear),
                    Map.entry(ScalarFunction.DIVIDE, new StdOperatorRewriteAdapter("DIVIDE", SqlStdOperatorTable.DIVIDE)),
                    Map.entry(ScalarFunction.E, new EConstantAdapter()),
                    // Math functions whose substrait yaml impls are fp64-only — wrap integer/float
                    // operands in CAST(DOUBLE) before substrait conversion so isthmus can bind. See
                    // NumericToDoubleAdapter javadoc for the type-widening rules. Without these the
                    // path fails with "Unable to convert call EXP(i32?)" / "POWER(i32?, fp64)" etc.
                    Map.entry(ScalarFunction.EXP, new NumericToDoubleAdapter(SqlStdOperatorTable.EXP)),
                    Map.entry(ScalarFunction.EXPM1, new Expm1Adapter()),
                    Map.entry(ScalarFunction.EXTRACT, new RustUdfDateTimeAdapters.ExtractAdapter()),
                    Map.entry(ScalarFunction.FROM_UNIXTIME, new RustUdfDateTimeAdapters.FromUnixtimeAdapter()),
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
                    Map.entry(ScalarFunction.LN, new NumericToDoubleAdapter(SqlStdOperatorTable.LN)),
                    Map.entry(ScalarFunction.LOCATE, new PositionAdapter()),
                    Map.entry(ScalarFunction.LOG, new NumericToDoubleAdapter(SqlLibraryOperators.LOG)),
                    Map.entry(ScalarFunction.LOG10, new NumericToDoubleAdapter(SqlStdOperatorTable.LOG10)),
                    Map.entry(ScalarFunction.LOG2, new NumericToDoubleAdapter(SqlLibraryOperators.LOG2)),
                    Map.entry(ScalarFunction.MAKEDATE, new RustUdfDateTimeAdapters.MakedateAdapter()),
                    Map.entry(ScalarFunction.MAKETIME, new RustUdfDateTimeAdapters.MaketimeAdapter()),
                    Map.entry(ScalarFunction.MICROSECOND, DatePartAdapters.microsecond()),
                    Map.entry(ScalarFunction.MINSPAN_BUCKET, new MinspanBucketAdapter()),
                    Map.entry(ScalarFunction.MINUTE, minute),
                    Map.entry(ScalarFunction.MINUTE_OF_HOUR, minute),
                    Map.entry(ScalarFunction.MOD, new StdOperatorRewriteAdapter("MOD", SqlStdOperatorTable.MOD)),
                    Map.entry(ScalarFunction.MONTH, month),
                    Map.entry(ScalarFunction.MONTH_OF_YEAR, month),
                    Map.entry(ScalarFunction.NUMBER_TO_STRING, new ToStringFunctionAdapter()),
                    Map.entry(ScalarFunction.NOW, now),
                    Map.entry(ScalarFunction.POSITION, new PositionAdapter()),
                    Map.entry(ScalarFunction.POWER, new NumericToDoubleAdapter(SqlStdOperatorTable.POWER)),
                    Map.entry(ScalarFunction.QUARTER, DatePartAdapters.quarter()),
                    Map.entry(ScalarFunction.RANGE_BUCKET, new RangeBucketAdapter()),
                    Map.entry(ScalarFunction.REGEXP_REPLACE, new RegexpReplaceAdapter()),
                    Map.entry(ScalarFunction.REX_EXTRACT, new RexExtractAdapter()),
                    Map.entry(ScalarFunction.REX_EXTRACT_MULTI, new RexExtractMultiAdapter()),
                    Map.entry(ScalarFunction.REX_OFFSET, new RexOffsetAdapter()),
                    Map.entry(ScalarFunction.SARG_PREDICATE, new SargAdapter()),
                    Map.entry(ScalarFunction.SCALAR_MAX, nameMapping(SqlLibraryOperators.GREATEST)),
                    Map.entry(ScalarFunction.SCALAR_MIN, nameMapping(SqlLibraryOperators.LEAST)),
                    Map.entry(ScalarFunction.SECOND, second),
                    Map.entry(ScalarFunction.SECOND_OF_MINUTE, second),
                    Map.entry(ScalarFunction.SHA2, new Sha2FunctionAdapter()),
                    Map.entry(ScalarFunction.SIGN, nameMapping(SignumFunction.FUNCTION)),
                    Map.entry(ScalarFunction.SINH, new HyperbolicOperatorAdapter(SqlLibraryOperators.SINH)),
                    Map.entry(ScalarFunction.SPAN, new SpanAdapter()),
                    Map.entry(ScalarFunction.SPAN_BUCKET, new SpanBucketAdapter()),
                    Map.entry(ScalarFunction.STRCMP, new StrcmpFunctionAdapter()),
                    Map.entry(ScalarFunction.STRFTIME, new StrftimeFunctionAdapter()),
                    Map.entry(ScalarFunction.STR_TO_DATE, new RustUdfDateTimeAdapters.StrToDateAdapter()),
                    Map.entry(ScalarFunction.SUBSTR, nameMapping(SqlStdOperatorTable.SUBSTRING)),
                    Map.entry(ScalarFunction.SUBSTRING, nameMapping(SqlStdOperatorTable.SUBSTRING)),
                    Map.entry(ScalarFunction.SYSDATE, now),
                    Map.entry(ScalarFunction.TIME, new DateTimeAdapters.TimeAdapter()),
                    Map.entry(ScalarFunction.TIME_FORMAT, new RustUdfDateTimeAdapters.TimeFormatAdapter()),
                    Map.entry(ScalarFunction.TIMESTAMP, new TimestampFunctionAdapter()),
                    Map.entry(ScalarFunction.TONUMBER, new ToNumberFunctionAdapter()),
                    Map.entry(ScalarFunction.TOSTRING, new ToStringFunctionAdapter()),
                    Map.entry(ScalarFunction.UNIX_TIMESTAMP, new UnixTimestampAdapter()),
                    Map.entry(ScalarFunction.WEEK, week),
                    Map.entry(ScalarFunction.WEEK_OF_YEAR, week),
                    Map.entry(ScalarFunction.WIDTH_BUCKET, new WidthBucketAdapter()),
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
        return (ctx, backendContext) -> {
            DataFusionService svc = plugin.getDataFusionService();
            if (svc == null) {
                throw new IllegalStateException("DataFusionService not initialized");
            }
            // When the FinalAggregateInstructionHandler has already prepared a plan on the
            // coordinator, it hands over a DataFusionReduceState carrying the session +
            // registered senders. The sink drives executeLocalPreparedPlan against that
            // state instead of re-decoding the fragment bytes.
            DataFusionReduceState preparedState = backendContext instanceof DataFusionReduceState s ? s : null;
            String mode = plugin.getClusterService() != null
                ? plugin.getClusterService().getClusterSettings().get(DataFusionPlugin.DATAFUSION_REDUCE_INPUT_MODE)
                : "streaming";
            // Memtable mode is single-input only (DatafusionMemtableReduceSink registers
            // exactly one MemTable at close time). Multi-input shapes (Union, future Join)
            // need per-child input partitions, which only the streaming sink implements via
            // MultiInputExchangeSink#sinkForChild. Auto-fall-back to streaming so end users
            // don't have to flip the cluster setting per query. Also fall back when a
            // prepared state is supplied (memtable sink does not yet support the
            // prepared-plan path).
            // TODO: lift this fallback once the memtable sink registers one MemTable per
            // child stage (see DatafusionMemtableReduceSink class javadoc).
            if ("memtable".equals(mode) && ctx.childInputs().size() == 1 && preparedState == null) {
                return new DatafusionMemtableReduceSink(ctx, svc.getNativeRuntime());
            }
            return new DatafusionReduceSink(ctx, svc.getNativeRuntime(), svc.getDrainExecutor(), preparedState);
        };
    }

    @Override
    public void configureFilterDelegation(FilterDelegationHandle handle, BackendExecutionContext backendContext) {
        // Install the handle as the FFM upcall target. All Rust callbacks
        // (createProvider, createCollector, collectDocs, release*) route to it.
        FilterTreeCallbacks.setHandle(handle);
    }

    @Override
    public void setDelegationThreadTracker(org.opensearch.analytics.spi.DelegationThreadTracker tracker) {
        FilterTreeCallbacks.setThreadTracker(tracker);
    }
}
