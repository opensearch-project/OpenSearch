/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Aggregate functions that a backend may support, categorized by {@link Type}.
 *
 * <p>Note: {@code COUNT} covers both {@code COUNT(*)} and {@code COUNT(DISTINCT x)}.
 * The distinction is on {@code AggregateCall.isDistinct()}, not on SqlKind.
 *
 * @opensearch.internal
 */
public enum AggregateFunction {
    // Simple — fixed-size state per key
    SUM(Type.SIMPLE, SqlKind.SUM),
    SUM0(Type.SIMPLE, SqlKind.SUM0),
    MIN(Type.SIMPLE, SqlKind.MIN),
    MAX(Type.SIMPLE, SqlKind.MAX),
    COUNT(Type.SIMPLE, SqlKind.COUNT, fields(IF("count", new ArrowType.Int(64, true), SUM))),
    // AVG's distributed decomposition (AVG(x) → CAST(SUM(x) / COUNT(x))) is handled by
    // OpenSearchAggregateReduceRule during HEP marking, not by the enum + resolver.
    // No intermediateFields needed here — the rule emits primitive SUM/COUNT calls and
    // a Project wrapper before the resolver sees the plan.
    AVG(Type.SIMPLE, SqlKind.AVG),

    // Statistical — fixed-size state, multi-pass or running stats. Handled by
    // OpenSearchAggregateReduceRule (once FUNCTIONS_TO_REDUCE is extended to include them)
    // — no intermediateFields here either.
    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    // State-expanding — state grows with input rows per key
    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),

    APPROX_COUNT_DISTINCT(Type.APPROXIMATE, SqlKind.OTHER, fields(IF("sketch", new ArrowType.Binary(), null))),
    TAKE(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("take_state", IntermediateTypeResolver.passThroughArg0(), null))),
    FIRST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("first_state", IntermediateTypeResolver.passThroughArg0(), null))),
    LAST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("last_state", IntermediateTypeResolver.passThroughArg0(), null))),
    LIST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("list_state", IntermediateTypeResolver.passThroughArg0(), null))),
    VALUES(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("values_state", IntermediateTypeResolver.passThroughArg0(), null))),
    // BRAIN aggregate from PPL's `patterns` command. The PPL Calcite layer registers the
    // SqlAggFunction with lower-case operator name "pattern", so we expose the enum as
    // PATTERN and rely on case-insensitive name lookup in {@link #fromNameOrError}. No
    // intermediate-field decomposition is registered: the analytics-engine backend rewrites
    // the call onto its local `internal_pattern` UDAF whose state shape is per-shard
    // List<Utf8> (the raw collected log lines), not derivable from the call's arg 0 type.
    PATTERN(Type.STATE_EXPANDING, SqlKind.OTHER);

    /** Category of aggregate function. Affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    /**
     * Describes one intermediate field emitted by a partial aggregate. A null reducer means
     * "self" (the owning enum constant).
     *
     * <p>The {@code typeResolver} produces the field's Calcite type given the FINAL aggregate
     * call's input arg types. For fixed-shape states (HLL sketch is always Binary, COUNT
     * counter is always Int64) the resolver ignores its input and returns a constant; for
     * input-parameterised states (e.g. {@code take(field, N)}'s buffer is {@code list<field>})
     * the resolver derives the shape from arg 0. Construct via
     * {@link IntermediateTypeResolver#fixed(ArrowType)} and
     * {@link IntermediateTypeResolver#passThroughArg0()}.
     */
    public record IntermediateField(String name, IntermediateTypeResolver typeResolver, AggregateFunction reducer) {
    }

    /**
     * Computes the intermediate-field type from an aggregate call's arg types. Implementations
     * must be pure: same input → same output. Two flavours:
     * <ul>
     *   <li><b>Fixed</b> ({@link #fixed(ArrowType)}) — returns a constant Arrow type wrapped
     *       in the corresponding Calcite type. Used for COUNT (Int64) and APPROX_COUNT_DISTINCT
     *       (Binary).</li>
     *   <li><b>Input-parameterised</b> (custom impls like {@link #passThroughArg0()}) — derives
     *       the type from {@code argTypes}. Used for {@code take}/{@code list}/{@code values}
     *       whose state shape is {@code list<arg0>}.</li>
     * </ul>
     */
    @FunctionalInterface
    public interface IntermediateTypeResolver {
        /** Resolve the intermediate field's Calcite type. */
        RelDataType resolve(List<RelDataType> argTypes, RelDataTypeFactory typeFactory);

        /** Resolver that always returns the same Arrow type, irrespective of arg types. */
        static IntermediateTypeResolver fixed(ArrowType arrowType) {
            return (argTypes, typeFactory) -> ArrowToCalciteTypeMapper.toCalcite(arrowType, typeFactory);
        }

        /**
         * Pass arg 0's type through unchanged. Used by state-expanding aggregates whose
         * FINAL re-aggregates over PARTIAL's output column — the column type already
         * equals the desired exchange shape.
         */
        static IntermediateTypeResolver passThroughArg0() {
            return (argTypes, typeFactory) -> {
                if (argTypes.isEmpty()) {
                    throw new IllegalStateException("passThroughArg0 resolver requires at least one arg type");
                }
                return argTypes.get(0);
            };
        }
    }

    /**
     * Internal Arrow → Calcite mapper used by {@link IntermediateTypeResolver#fixed}. Lives
     * in the SPI module so {@code IntermediateField} stays self-contained — no dependency
     * on the planner module. Add cases as new fixed-state shapes appear.
     */
    private static final class ArrowToCalciteTypeMapper {
        static RelDataType toCalcite(ArrowType t, RelDataTypeFactory f) {
            return switch (t) {
                case ArrowType.Int i when i.getBitWidth() == 64 -> f.createSqlType(SqlTypeName.BIGINT);
                case ArrowType.Binary b -> f.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
                default -> throw new IllegalArgumentException("Unsupported fixed Arrow type for IntermediateField: " + t);
            };
        }
    }

    private final Type type;
    private final SqlKind sqlKind;
    private final List<IntermediateField> intermediateFields;

    AggregateFunction(Type type, SqlKind sqlKind) {
        this(type, sqlKind, null);
    }

    AggregateFunction(Type type, SqlKind sqlKind, List<IntermediateField> intermediateFields) {
        this.type = type;
        this.sqlKind = sqlKind;
        this.intermediateFields = intermediateFields;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Returns intermediate fields with null reducers resolved to {@code this}. */
    public List<IntermediateField> intermediateFields() {
        if (intermediateFields == null) return null;
        return intermediateFields.stream()
            .map(f -> f.reducer() == null ? new IntermediateField(f.name(), f.typeResolver(), this) : f)
            .toList();
    }

    public boolean hasDecomposition() {
        return intermediateFields != null;
    }

    /** Maps a Calcite SqlKind to an AggregateFunction, or null if not recognized. Skips OTHER. */
    public static AggregateFunction fromSqlKind(SqlKind kind) {
        for (AggregateFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /**
     * Maps an aggregate function name to an AggregateFunction. Throws if not recognized.
     *
     * <p>Lookup is case-insensitive. PPL operator names are inconsistent: some are
     * registered upper-case ({@code TAKE}, {@code FIRST}) while others use lower-case
     * ({@code pattern}). Normalising to upper-case here means call sites can pass the
     * SqlAggFunction's raw name without worrying about which convention the operator
     * was registered under.
     */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }

    /**
     * Returns the Calcite {@link SqlAggFunction} equivalent of this enum constant.
     * Used when emitting rewritten aggregate calls (e.g. the resolver building a
     * FINAL-phase call for a function-swap or engine-native merge).
     */
    public SqlAggFunction toSqlAggFunction() {
        return switch (this) {
            case SUM -> SqlStdOperatorTable.SUM;
            case SUM0 -> SqlStdOperatorTable.SUM0;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            case COUNT -> SqlStdOperatorTable.COUNT;
            case AVG -> SqlStdOperatorTable.AVG;
            case APPROX_COUNT_DISTINCT -> SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
            default -> throw new IllegalStateException("No SqlAggFunction mapping for: " + this);
        };
    }

    /**
     * Resolves a Calcite {@link SqlAggFunction} back to an {@link AggregateFunction}.
     * Tries name-based lookup first (handles SqlKind.OTHER cases like APPROX_COUNT_DISTINCT)
     * and falls back to SqlKind matching. Throws if neither path succeeds.
     */
    public static AggregateFunction fromSqlAggFunction(SqlAggFunction op) {
        try {
            return fromNameOrError(op.getName());
        } catch (IllegalStateException e) {
            // Fall through to SqlKind-based resolution
        }
        AggregateFunction byKind = fromSqlKind(op.getKind());
        if (byKind != null) {
            return byKind;
        }
        throw new IllegalStateException("No AggregateFunction mapping for SqlAggFunction [" + op.getName() + "]");
    }

    // ── Helpers for readable enum-entry literals ──

    private static List<IntermediateField> fields(IntermediateField... fs) {
        return List.of(fs);
    }

    private static IntermediateField IF(String name, ArrowType arrowType, AggregateFunction reducer) {
        return new IntermediateField(name, IntermediateTypeResolver.fixed(arrowType), reducer);
    }

    private static IntermediateField IF(String name, IntermediateTypeResolver typeResolver, AggregateFunction reducer) {
        return new IntermediateField(name, typeResolver, reducer);
    }
}
