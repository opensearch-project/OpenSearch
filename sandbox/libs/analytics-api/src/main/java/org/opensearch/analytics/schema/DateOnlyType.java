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

import java.util.Locale;

/**
 * Marker UDT for an OpenSearch {@code date} column whose mapping {@code format} declares
 * only date components. Substrait wire shape stays {@code Timestamp(ms)}; the marker only
 * downgrades the result-side type label to {@code date}. Sibling of {@link TimeOnlyType}.
 */
public final class DateOnlyType extends BasicSqlType {

    public static final String NAME = "date";

    private final boolean nullable;

    public DateOnlyType(RelDataTypeSystem typeSystem, boolean nullable, int precision) {
        super(typeSystem, SqlTypeName.TIMESTAMP, precision);
        this.nullable = nullable;
        computeDigest();
    }

    /** Builds a nullable marker with the precision required to match the parquet read shape. */
    public static DateOnlyType nullable(RelDataTypeFactory typeFactory, int precision) {
        return new DateOnlyType(typeFactory.getTypeSystem(), true, precision);
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
        return new DateOnlyType(typeSystem, nullable, getPrecision());
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Include precision in the digest — date→3 and date_nanos→9 must be distinct, else
        // type-factory canonicalization collapses them.
        sb.append(NAME.toUpperCase(Locale.ROOT)).append('(').append(getPrecision()).append(')');
    }
}
