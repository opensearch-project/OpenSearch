/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Calcite type marker for an OpenSearch {@code unsigned_long} column. Backed by
 * {@link SqlTypeName#BIGINT} so planner coercion is unchanged; the subclass exists
 * only as an {@code instanceof}-dispatch marker for translators that need to apply
 * unsigned-long-specific bound semantics (negative clamping, decimal truncation,
 * overflow rejection for values above {@code Long.MAX_VALUE}).
 *
 * <p>Semantics mirror legacy {@code NumberFieldMapper.NumberType.UNSIGNED_LONG}:
 * <ul>
 *   <li>Storage: BigInteger [0, 2^64-1] — see {@code NumberFieldMapper.unsignedLongRangeQuery}</li>
 *   <li>DSL path: values above {@code Long.MAX_VALUE} are not representable because the
 *       schema_coerce.rs UInt64→Int64 narrowing loses them at the parquet read layer</li>
 *   <li>Negative bounds: clamped per {@code NumberFieldMapper.objectToUnsignedLong(lenientBound=true)}</li>
 * </ul>
 */
public final class UnsignedLongType extends BasicSqlType {

    /** OpenSearch type-name string this UDT preserves. */
    public static final String NAME = "unsigned_long";

    private final boolean nullable;

    public UnsignedLongType(RelDataTypeSystem typeSystem, boolean nullable) {
        super(typeSystem, SqlTypeName.BIGINT);
        this.nullable = nullable;
        computeDigest();
    }

    /** Convenience for the common nullable case used by {@link OpenSearchSchemaBuilder}. */
    public static UnsignedLongType nullable() {
        return new UnsignedLongType(RelDataTypeSystem.DEFAULT, true);
    }

    @Override
    public boolean isNullable() {
        return nullable;
    }

    @Override
    public BasicSqlType createWithNullability(boolean nullable) {
        if (nullable == this.nullable) {
            return this;
        }
        return new UnsignedLongType(typeSystem, nullable);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append("UNSIGNED_LONG");
    }
}
