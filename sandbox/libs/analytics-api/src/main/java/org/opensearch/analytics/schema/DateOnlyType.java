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
 * Calcite type marker for an OpenSearch {@code date} column whose mapping {@code format}
 * declares only date components (no hour/minute/second). Backed by
 * {@link SqlTypeName#TIMESTAMP} so substrait wire shape remains {@code Timestamp(ms)} —
 * matching {@code DateParquetField} in the parquet-data-format plugin — while the
 * subclass acts as an {@code instanceof}-dispatch marker so result-side schema/value
 * formatting can downgrade the type label to {@code date} and strip the {@code 00:00:00}
 * time portion. Sibling of {@link IpType} / {@link BinaryType}.
 *
 * <p>Extends {@link BasicSqlType} (not {@code AbstractSqlType}) so the type carries an
 * explicit {@code TIMESTAMP} precision — substrait rejects
 * {@code PRECISION_NOT_SPECIFIED} for {@code PrecisionTimestamp}.
 */
public final class DateOnlyType extends BasicSqlType {

    /** OpenSearch logical-type label this UDT preserves. */
    public static final String NAME = "date";

    /** Local nullability flag — BasicSqlType's {@code isNullable} is set non-nullable by our ctor. */
    private final boolean nullable;

    public DateOnlyType(RelDataTypeSystem typeSystem, boolean nullable) {
        super(typeSystem, SqlTypeName.TIMESTAMP);
        this.nullable = nullable;
        computeDigest();
    }

    /** Convenience for the common nullable case used by {@link OpenSearchSchemaBuilder}. */
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
