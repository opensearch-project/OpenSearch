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
 */
public class NameBasedAggregateFunctionConverter extends AggregateFunctionConverter {

    /**
     * PPL operator name → YAML extension name. Used when the PPL aggregate has a
     * different conventional name than the DataFusion built-in it maps to. Lookups
     * are case-insensitive (keys lowercase).
     */
    private static final Map<String, String> NAME_ALIASES = Map.of(
        "first", "first_value",
        "last", "last_value"
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
        Optional<AggregateFunctionInvocation> result = super.convert(input, inputType, call, topLevelConverter);
        if (result.isPresent()) {
            return result;
        }
        return convertByName(input, call, topLevelConverter);
    }

    /**
     * Looks up an extension variant by aggregate name and arity, then assembles an
     * {@link AggregateFunctionInvocation} directly. Used as a last resort when the
     * stock matcher cannot resolve the call (typically because the YAML signature uses
     * wildcards that the directMap lookup cannot expand against runtime types).
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
        String lookup = NAME_ALIASES.getOrDefault(lower, lower);
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

        List<Expression> operands = call.getArgList().stream()
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
