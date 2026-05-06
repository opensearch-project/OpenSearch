/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package io.substrait.isthmus.expression;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import io.substrait.expression.AggregateFunctionInvocation;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FunctionArg;
import io.substrait.extension.SimpleExtension;
import io.substrait.isthmus.TypeConverter;
import io.substrait.type.Type;

/**
 * Aggregate function converter that handles two issues with the stock isthmus matcher:
 *
 * <ol>
 *   <li><b>Identity-only operator lookup.</b> PPL emits its own {@code SqlAggFunction}
 *       subclasses (e.g. {@code TakeAggFunction}, {@code FirstAggFunction}). The default
 *       converter keys its matcher map by {@link SqlOperator} identity, so the PPL
 *       instances miss the entries seeded by the stub Sigs in
 *       {@code DataFusionFragmentConvertor}. {@link #getFunctionFinder} falls back to a
 *       case-insensitive name match against the seeded entries.</li>
 *
 *   <li><b>Wildcard signatures don't direct-match.</b> The stock {@code attemptMatch}
 *       does string-level key matching: it builds a key from the call's runtime types
 *       (e.g. {@code take:string_i32}) and looks it up in the YAML-derived directMap,
 *       which contains keys built from the YAML wildcards (e.g. {@code take:any_i32}).
 *       The two never match. The {@code singularInputType} fallback only handles
 *       uniform-arg signatures and so misses heterogeneous wildcard signatures like
 *       {@code take(any1, i32)}, {@code arg_min(any1, any2)}, {@code first(any1)}.
 *       {@link #convert} adds a name-keyed fallback that, on miss, picks the first
 *       extension variant whose name and arity match and constructs the
 *       {@link AggregateFunctionInvocation} directly — bypassing the strict directMap
 *       lookup. Argument-type validity is left to the substrait consumer (DataFusion).</li>
 * </ol>
 *
 * <h2>Alias rewrite mechanism</h2>
 *
 * <p>PPL frontend names sometimes don't match the DataFusion-side variant name, or need
 * shape tweaks (DISTINCT, ORDER BY) that the stock matcher can't infer from the call
 * shape alone. {@link #NAME_ALIASES} declares these per-name, and
 * {@link #rewriteAlias(RelNode, AggregateCall, Function)} applies the configured
 * shape changes before substrait emission. Current aliases:
 *
 * <ul>
 *   <li>{@code first → first_value}, {@code last → last_value} — pure rename. PPL's
 *       stats first(x)/last(x) map to DataFusion's native first_value/last_value.</li>
 *   <li>{@code list → array_agg}, {@code values → array_agg} — pure rename for list;
 *       rename + force-distinct + force-order-by for values (ascending on the operand
 *       itself, per PPL values() docs "sorted lexicographically, unique only").</li>
 *   <li>{@code arg_min → first_value}, {@code arg_max → last_value} — rename + pull
 *       the second arg out of argList into a synthesized ORDER BY sort field. PPL's
 *       stats earliest(field, ts) / latest(field, ts) lower to Calcite ARG_MIN / ARG_MAX
 *       at the frontend layer. DataFusion 52.x has no native min_by/max_by UDAF — the
 *       semantic equivalent is first_value/last_value with an ORDER BY on the key.
 *       ARG_MAX uses last_value with ASC (last row of ascending sort == max row).</li>
 * </ul>
 */
public class NameBasedAggregateFunctionConverter extends AggregateFunctionConverter {

    /**
     * Per-alias rewrite specification. A config with only {@link #target} set is a pure
     * rename — the aliased call uses the named YAML variant but keeps its own operands
     * and flags. Optional fields inject additional shape:
     *
     * <ul>
     *   <li>{@link #sortArgIndex} — pull the operand at this index out of the call's
     *       argList and emit it as a substrait sort field (ASC_NULLS_LAST) on the
     *       resulting measure. Used for ARG_MIN/ARG_MAX → first_value/last_value (sort
     *       by arg[1], the key) and VALUES → array_agg (sort by arg[0], the operand
     *       itself). When set, the operand at this index is NOT kept in the operand
     *       list — it appears only in the sort field.</li>
     *   <li>{@link #forceDistinct} — override {@link AggregateCall#isDistinct()} and
     *       emit DISTINCT on the substrait invocation. Used for VALUES which requires
     *       distinct-by-construction semantics but whose PPL-frontend SqlAggFunction
     *       doesn't carry the DISTINCT flag.</li>
     * </ul>
     *
     * <p>When {@code sortArgIndex} is set to the SAME index as a regular operand (i.e.
     * the sort key IS the aggregated value, like VALUES sorts by its own operand), the
     * operand is still emitted as a sort field AND kept as the primary operand. This
     * is the "sort by self" case. See {@link #rewriteAlias} for the implementation.
     */
    static final class AliasConfig {
        final String target;
        final Integer sortArgIndex;
        final boolean sortIsSelf;
        final boolean forceDistinct;

        private AliasConfig(String target, Integer sortArgIndex, boolean sortIsSelf, boolean forceDistinct) {
            this.target = target;
            this.sortArgIndex = sortArgIndex;
            this.sortIsSelf = sortIsSelf;
            this.forceDistinct = forceDistinct;
        }

        /** Pure rename: aliased call routes to {@code target} with operands/flags unchanged. */
        static AliasConfig rename(String target) {
            return new AliasConfig(target, null, false, false);
        }

        /** Rename + pull argList[sortArgIndex] into a synthesized ASC_NULLS_LAST sort field,
         *  removing it from the operand list. */
        static AliasConfig renameWithSortArg(String target, int sortArgIndex) {
            return new AliasConfig(target, sortArgIndex, false, false);
        }

        /** Rename + emit the SINGLE operand as both the value AND an ASC_NULLS_LAST sort
         *  field, also setting DISTINCT. For PPL values(): sort by operand, dedup by
         *  operand. Only valid when arity == 1. */
        static AliasConfig renameDistinctSortedBySelf(String target) {
            return new AliasConfig(target, 0, true, true);
        }

        boolean hasReshape() {
            return sortArgIndex != null || forceDistinct;
        }
    }

    /**
     * PPL operator name → alias config. Keys are case-insensitive (stored lowercase).
     * Entries without reshape flags (pure {@link AliasConfig#rename}) still flow
     * through the {@link #convertByName} fallback unchanged — their target name is
     * used for YAML variant lookup. Entries with reshape flags route through
     * {@link #rewriteAlias} which assembles the substrait invocation directly.
     */
    private static final Map<String, AliasConfig> NAME_ALIASES = Map.of(
        "first",
        AliasConfig.rename("first_value"),
        "last",
        AliasConfig.rename("last_value"),
        "list",
        AliasConfig.rename("array_agg"),
        "values",
        AliasConfig.renameDistinctSortedBySelf("array_agg"),
        "arg_min",
        AliasConfig.renameWithSortArg("first_value", 1),
        "arg_max",
        AliasConfig.renameWithSortArg("last_value", 1)
    );

    private final List<SimpleExtension.AggregateFunctionVariant> allVariants;
    private final TypeConverter typeConverter;

    public NameBasedAggregateFunctionConverter(
        List<SimpleExtension.AggregateFunctionVariant> functions,
        List<FunctionMappings.Sig> additionalSignatures,
        RelDataTypeFactory typeFactory,
        TypeConverter typeConverter
    ) {
        super(functions, additionalSignatures, typeFactory, typeConverter);
        this.allVariants = List.copyOf(functions);
        this.typeConverter = typeConverter;
    }

    @Override
    protected FunctionFinder getFunctionFinder(AggregateCall call) {
        FunctionFinder ff = super.getFunctionFinder(call);
        if (ff != null) {
            return ff;
        }
        String name = call.getAggregation().getName();
        if (name == null) {
            return null;
        }
        for (Map.Entry<SqlOperator, FunctionFinder> entry : signatures.entrySet()) {
            if (name.equalsIgnoreCase(entry.getKey().getName())) {
                return entry.getValue();
            }
        }
        return null;
    }

    @Override
    public Optional<AggregateFunctionInvocation> convert(
        RelNode input,
        Type.Struct inputType,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        // Reshape aliases (DISTINCT injection, ORDER BY synthesis) run BEFORE the stock
        // matcher so we can fully control the emitted call shape. Pure-rename aliases
        // don't need pre-emption — they flow through convertByName's NAME_ALIASES
        // lookup on target name.
        Optional<AggregateFunctionInvocation> aliasRewrite = rewriteAlias(input, call, topLevelConverter);
        if (aliasRewrite.isPresent()) {
            return aliasRewrite;
        }
        Optional<AggregateFunctionInvocation> result = super.convert(input, inputType, call, topLevelConverter);
        if (result.isPresent()) {
            return result;
        }
        return convertByName(input, call, topLevelConverter);
    }

    /**
     * Applies an {@link AliasConfig} to rewrite the call's emitted shape when the config
     * specifies reshape flags (sort field synthesis or forced DISTINCT). Pure-rename
     * configs return empty and fall through to {@link #convertByName}, which consults
     * {@link #NAME_ALIASES} for the target variant name.
     *
     * <p>Returns empty when:
     * <ul>
     *   <li>the call has no name (bare aggregate binding with no SqlAggFunction);</li>
     *   <li>the name has no entry in {@link #NAME_ALIASES};</li>
     *   <li>the entry is a pure rename with no reshape flags;</li>
     *   <li>the target YAML variant isn't present in the loaded extension collection;</li>
     *   <li>the call arity is incompatible with the config (e.g. sortArgIndex out of
     *       range, or sortIsSelf with arity != 1).</li>
     * </ul>
     */
    private Optional<AggregateFunctionInvocation> rewriteAlias(
        RelNode input,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        String callName = call.getAggregation().getName();
        if (callName == null) {
            return Optional.empty();
        }
        AliasConfig config = NAME_ALIASES.get(callName.toLowerCase(Locale.ROOT));
        if (config == null || !config.hasReshape()) {
            return Optional.empty();
        }

        int argCount = call.getArgList().size();
        if (config.sortArgIndex != null && config.sortArgIndex >= argCount) {
            return Optional.empty();
        }
        if (config.sortIsSelf && argCount != 1) {
            return Optional.empty();
        }

        // The post-rewrite operand count determines which YAML variant we need.
        // sortIsSelf keeps the operand (arity stays at 1). Plain sortArgIndex removes
        // the operand at that index from the operand list (arity drops by 1).
        int postRewriteArity = config.sortIsSelf ? argCount : (config.sortArgIndex != null ? argCount - 1 : argCount);
        SimpleExtension.AggregateFunctionVariant matched = null;
        for (SimpleExtension.AggregateFunctionVariant variant : allVariants) {
            if (!variant.name().equalsIgnoreCase(config.target)) {
                continue;
            }
            if (variant.requiredArguments().size() == postRewriteArity || variant.args().size() == postRewriteArity) {
                matched = variant;
                break;
            }
        }
        if (matched == null) {
            return Optional.empty();
        }

        // Build the operand list. If sortArgIndex is set and sortIsSelf is false, skip
        // that argList index — it's moved into the sort field list only.
        List<Expression> operands = new ArrayList<>(postRewriteArity);
        for (int i = 0; i < argCount; i++) {
            if (config.sortArgIndex != null && !config.sortIsSelf && i == config.sortArgIndex) {
                continue;
            }
            int colIdx = call.getArgList().get(i);
            operands.add(topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, colIdx)));
        }

        // Build the sort field list. ASC_NULLS_LAST for both directions — see the
        // class-level javadoc for the ARG_MIN/ARG_MAX reasoning. first_value picks the
        // smallest key (first row of ascending sort); last_value picks the largest
        // (last row of ascending sort).
        List<Expression.SortField> sorts = Collections.emptyList();
        if (config.sortArgIndex != null) {
            int sortColIdx = call.getArgList().get(config.sortArgIndex);
            Expression sortExpr = topLevelConverter.apply(input.getCluster().getRexBuilder().makeInputRef(input, sortColIdx));
            Expression.SortField sortField = Expression.SortField.builder()
                .expr(sortExpr)
                .direction(Expression.SortDirection.ASC_NULLS_LAST)
                .build();
            sorts = List.of(sortField);
        }

        Type outputType = typeConverter.toSubstrait(call.getType());

        boolean distinct = config.forceDistinct || call.isDistinct();
        Expression.AggregationInvocation invocation = distinct
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

        return Optional.of(
            ExpressionCreator.aggregateFunction(
                matched,
                outputType,
                Expression.AggregationPhase.INITIAL_TO_RESULT,
                sorts,
                invocation,
                operands.<FunctionArg>stream().toList()
            )
        );
    }

    /**
     * Looks up an extension variant by aggregate name and arity, then assembles an
     * {@link AggregateFunctionInvocation} directly. Used as a last resort when the
     * stock matcher cannot resolve the call (typically because the YAML signature uses
     * wildcards that the directMap lookup cannot expand against runtime types).
     *
     * <p>Consults {@link #NAME_ALIASES} for pure-rename aliases — e.g. a call named
     * "first" is looked up as "first_value" in the extension collection. Alias entries
     * with reshape flags are handled earlier in {@link #rewriteAlias}; by the time we
     * land here the call has either no alias or a pure rename.
     */
    private Optional<AggregateFunctionInvocation> convertByName(
        RelNode input,
        AggregateCall call,
        Function<RexNode, Expression> topLevelConverter
    ) {
        String callName = call.getAggregation().getName();
        if (callName == null) {
            return Optional.empty();
        }
        String lower = callName.toLowerCase(Locale.ROOT);
        AliasConfig aliasConfig = NAME_ALIASES.get(lower);
        String lookup = aliasConfig != null ? aliasConfig.target : lower;
        int argCount = call.getArgList().size();

        SimpleExtension.AggregateFunctionVariant matched = null;
        for (SimpleExtension.AggregateFunctionVariant variant : allVariants) {
            if (!variant.name().equalsIgnoreCase(lookup)) {
                continue;
            }
            if (variant.requiredArguments().size() == argCount || variant.args().size() == argCount) {
                matched = variant;
                break;
            }
        }
        if (matched == null) {
            return Optional.empty();
        }

        List<Expression> operands = call.getArgList()
            .stream()
            .map(idx -> input.getCluster().getRexBuilder().makeInputRef(input, idx))
            .map(topLevelConverter)
            .toList();

        Type outputType = typeConverter.toSubstrait(call.getType());

        Expression.AggregationInvocation invocation = call.isDistinct()
            ? Expression.AggregationInvocation.DISTINCT
            : Expression.AggregationInvocation.ALL;

        return Optional.of(
            ExpressionCreator.aggregateFunction(
                matched,
                outputType,
                Expression.AggregationPhase.INITIAL_TO_RESULT,
                Collections.<Expression.SortField>emptyList(),
                invocation,
                operands.<FunctionArg>stream().toList()
            )
        );
    }
}
