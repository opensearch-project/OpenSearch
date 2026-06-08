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
 * {@code format} is date-only (e.g. {@code basic_date}, {@code year_month_day}, {@code yyyy-MM-dd}).
 * Backed by {@link SqlTypeName#TIMESTAMP} so the storage, Substrait, and DataFusion layers are
 * unchanged — the parquet column is still {@code Timestamp(MILLISECOND)} on disk and the plan binds
 * against it exactly as a plain timestamp would. The subclass exists only as an
 * {@code instanceof}-dispatch marker so callers that build the response schema and materialize cell
 * values can distinguish a date-only column from a plain timestamp and render it as a SQL
 * {@code DATE} ({@code 1984-04-12}) rather than a {@code TIMESTAMP} ({@code 1984-04-12 00:00:00}).
 *
 * <p>Mirrors {@link IpType} / {@link BinaryType}, which use the same backing-type trick for
 * {@code VARBINARY}-shaped {@code ip} and {@code binary} columns.
 */
public final class DateType extends AbstractSqlType {

    /** Display name this UDT preserves. */
    public static final String NAME = "date";

    private final int precision;

    /**
     * @param nullable whether the column is nullable
     * @param precision the backing {@code TIMESTAMP} precision — must match what a plain
     *     {@code TIMESTAMP} column carries so the type serializes to the same Substrait
     *     {@code PrecisionTimestamp}. Leaving it unspecified ({@code -1}) makes isthmus emit
     *     {@code PrecisionTimestamp(precision=-1)}, which DataFusion rejects.
     */
    public DateType(boolean nullable, int precision) {
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
