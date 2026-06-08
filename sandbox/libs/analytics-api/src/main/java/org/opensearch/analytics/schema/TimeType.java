/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.Locale;

/**
 * Calcite type marker for an OpenSearch {@code date}/{@code date_nanos} column whose mapping
 * {@code format} is time-only (e.g. {@code hour}, {@code hour_minute_second}, {@code HH:mm:ss}).
 * Backed by {@link SqlTypeName#TIMESTAMP} so the storage, Substrait, and DataFusion layers are
 * unchanged — the parquet column is still {@code Timestamp(MILLISECOND)} on disk and the plan binds
 * against it exactly as a plain timestamp would. The subclass exists only as an
 * {@code instanceof}-dispatch marker so callers that build the response schema and materialize cell
 * values can distinguish a time-only column from a plain timestamp and render it as a SQL
 * {@code TIME} ({@code 09:00:00}) rather than a {@code TIMESTAMP} ({@code 1970-01-01 09:00:00}).
 *
 * <p>Mirrors {@link DateType}, which uses the same backing-type trick for date-only columns.
 */
public final class TimeType extends AbstractSqlType {

    /** Display name this UDT preserves. */
    public static final String NAME = "time";

    private final int precision;

    /**
     * @param nullable whether the column is nullable
     * @param precision the backing {@code TIMESTAMP} precision — must match what a plain
     *     {@code TIMESTAMP} column carries so the type serializes to the same Substrait
     *     {@code PrecisionTimestamp} (a {@code -1} precision is rejected by DataFusion).
     */
    public TimeType(boolean nullable, int precision) {
        super(SqlTypeName.TIMESTAMP, nullable, null);
        this.precision = precision;
        computeDigest();
    }

    @Override
    public int getPrecision() {
        return precision;
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
