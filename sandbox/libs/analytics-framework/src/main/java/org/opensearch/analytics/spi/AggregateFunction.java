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

/** Aggregate functions a backend may support, categorized by {@link Type}. */
public enum AggregateFunction {
    SUM(Type.SIMPLE, SqlKind.SUM),
    SUM0(Type.SIMPLE, SqlKind.SUM0),
    MIN(Type.SIMPLE, SqlKind.MIN),
    MAX(Type.SIMPLE, SqlKind.MAX),
    COUNT(Type.SIMPLE, SqlKind.COUNT, fields(IF("count", new ArrowType.Int(64, true), SUM))) {
        /**
         * Single-arg {@code COUNT(DISTINCT x)} collapses onto {@link SqlStdOperatorTable#APPROX_COUNT_DISTINCT}.
         * Distinctness is global and cannot be reduced additively across shards;
         * {@link #APPROX_COUNT_DISTINCT} (Type.APPROXIMATE) carries the cross-shard merge.
         */
        @Override
        public SqlAggFunction resolveOperator(SqlAggFunction op, boolean isDistinct, int argCount) {
            if (isDistinct && argCount == 1) {
                return SqlStdOperatorTable.APPROX_COUNT_DISTINCT;
            }
            return op;
        }
    },
    AVG(Type.SIMPLE, SqlKind.AVG),

    STDDEV_POP(Type.STATISTICAL, SqlKind.STDDEV_POP),
    STDDEV_SAMP(Type.STATISTICAL, SqlKind.STDDEV_SAMP),
    VAR_POP(Type.STATISTICAL, SqlKind.VAR_POP),
    VAR_SAMP(Type.STATISTICAL, SqlKind.VAR_SAMP),

    PERCENTILE_CONT(Type.STATE_EXPANDING, SqlKind.PERCENTILE_CONT),
    PERCENTILE_DISC(Type.STATE_EXPANDING, SqlKind.PERCENTILE_DISC),
    PERCENTILE_APPROX(Type.STATE_EXPANDING, SqlKind.OTHER),
    COLLECT(Type.STATE_EXPANDING, SqlKind.COLLECT),
    LISTAGG(Type.STATE_EXPANDING, SqlKind.LISTAGG),

    APPROX_COUNT_DISTINCT(Type.APPROXIMATE, SqlKind.OTHER, fields(IF("sketch", new ArrowType.Binary(), null))),
    TAKE(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("take_state", IntermediateTypeResolver.passThroughArg0(), null))),
    FIRST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("first_state", IntermediateTypeResolver.passThroughArg0(), null))),
    LAST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("last_state", IntermediateTypeResolver.passThroughArg0(), null))),
    // earliest(value, ts) / latest(value, ts); rewritten to first_value/last_value by PplAggregateCallRewriter.
    ARG_MIN(Type.STATE_EXPANDING, SqlKind.ARG_MIN, fields(IF("arg_min_state", IntermediateTypeResolver.passThroughArg0(), null))),
    ARG_MAX(Type.STATE_EXPANDING, SqlKind.ARG_MAX, fields(IF("arg_max_state", IntermediateTypeResolver.passThroughArg0(), null))),
    LIST(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("list_state", IntermediateTypeResolver.passThroughArg0(), null))),
    VALUES(Type.STATE_EXPANDING, SqlKind.OTHER, fields(IF("values_state", IntermediateTypeResolver.passThroughArg0(), null))),
    PATTERN(Type.STATE_EXPANDING, SqlKind.OTHER);

    /** Category of aggregate function; affects execution strategy (shuffle vs map-reduce). */
    public enum Type {
        SIMPLE,
        STATISTICAL,
        STATE_EXPANDING,
        APPROXIMATE
    }

    /** Intermediate field of a partial aggregate; {@code null} reducer means "self". */
    public record IntermediateField(String name, IntermediateTypeResolver typeResolver, AggregateFunction reducer) {
    }

    /** Resolves the Calcite type of an intermediate field from a FINAL aggregate's arg types. */
    @FunctionalInterface
    public interface IntermediateTypeResolver {
        RelDataType resolve(List<RelDataType> argTypes, RelDataTypeFactory typeFactory);

        static IntermediateTypeResolver fixed(ArrowType arrowType) {
            return (argTypes, typeFactory) -> ArrowToCalciteTypeMapper.toCalcite(arrowType, typeFactory);
        }

        static IntermediateTypeResolver passThroughArg0() {
            return (argTypes, typeFactory) -> {
                if (argTypes.isEmpty()) {
                    throw new IllegalStateException("passThroughArg0 resolver requires at least one arg type");
                }
                return argTypes.get(0);
            };
        }
    }

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

    public List<IntermediateField> intermediateFields() {
        if (intermediateFields == null) return null;
        return intermediateFields.stream()
            .map(f -> f.reducer() == null ? new IntermediateField(f.name(), f.typeResolver(), this) : f)
            .toList();
    }

    public boolean hasDecomposition() {
        return intermediateFields != null;
    }

    public static AggregateFunction fromSqlKind(SqlKind kind) {
        for (AggregateFunction func : values()) {
            if (func.sqlKind == kind && func.sqlKind != SqlKind.OTHER) {
                return func;
            }
        }
        return null;
    }

    /**
     * Resolves an aggregate call shape (operator + flags) onto a standard {@link SqlAggFunction}.
     * Default returns {@code op} unchanged; override per enum value to encode rewrites.
     */
    public SqlAggFunction resolveOperator(SqlAggFunction op, boolean isDistinct, int argCount) {
        return op;
    }

    /** Case-insensitive name lookup; throws if not recognized. */
    public static AggregateFunction fromNameOrError(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Unrecognized aggregate function [" + name + "]", e);
        }
    }

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

    public static AggregateFunction fromSqlAggFunction(SqlAggFunction op) {
        try {
            return fromNameOrError(op.getName());
        } catch (IllegalStateException ignored) {
            // fall through
        }
        AggregateFunction byKind = fromSqlKind(op.getKind());
        if (byKind != null) {
            return byKind;
        }
        throw new IllegalStateException("No AggregateFunction mapping for SqlAggFunction [" + op.getName() + "]");
    }

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
