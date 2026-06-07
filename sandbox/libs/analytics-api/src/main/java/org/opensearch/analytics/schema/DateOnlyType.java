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

    public DateOnlyType(RelDataTypeSystem typeSystem, boolean nullable) {
        super(typeSystem, SqlTypeName.TIMESTAMP);
        this.nullable = nullable;
        computeDigest();
    }

    public static DateOnlyType nullable(RelDataTypeFactory typeFactory) {
        return new DateOnlyType(typeFactory.getTypeSystem(), true);
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
        return new DateOnlyType(typeSystem, nullable);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
