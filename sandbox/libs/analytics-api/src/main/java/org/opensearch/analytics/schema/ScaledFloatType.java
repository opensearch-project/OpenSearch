/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Marker UDT for an OpenSearch {@code scaled_float} field carrying its per-field scaling factor.
 */
public final class ScaledFloatType extends BasicSqlType {

    private final double scalingFactor;
    private final boolean nullable;

    public ScaledFloatType(RelDataTypeSystem typeSystem, boolean nullable, double scalingFactor) {
        super(typeSystem, SqlTypeName.BIGINT);
        this.nullable = nullable;
        this.scalingFactor = scalingFactor;
        computeDigest();
    }

    /** Returns the scaling factor used to convert double values to stored longs. */
    public double getScalingFactor() {
        return scalingFactor;
    }

    /** Builds a nullable marker using the type system from the given factory. */
    public static ScaledFloatType nullable(RelDataTypeFactory typeFactory, double scalingFactor) {
        return new ScaledFloatType(typeFactory.getTypeSystem(), true, scalingFactor);
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
        return new ScaledFloatType(typeSystem, nullable, scalingFactor);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Factor MUST appear in digest — without it, type-factory canonicalization collapses
        // factor-10 and factor-100 fields into one shared type object.
        sb.append("SCALED_FLOAT(").append(scalingFactor).append(')');
    }
}
