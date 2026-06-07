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
 * only time components. Substrait wire shape stays {@code Timestamp(ms)}; the marker only
 * downgrades the result-side type label to {@code time}. Sibling of {@link DateOnlyType}.
 */
public final class TimeOnlyType extends BasicSqlType {

    public static final String NAME = "time";

    private final boolean nullable;

    public TimeOnlyType(RelDataTypeSystem typeSystem, boolean nullable) {
        super(typeSystem, SqlTypeName.TIMESTAMP);
        this.nullable = nullable;
        computeDigest();
    }

    public static TimeOnlyType nullable(RelDataTypeFactory typeFactory) {
        return new TimeOnlyType(typeFactory.getTypeSystem(), true);
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
        return new TimeOnlyType(typeSystem, nullable);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
