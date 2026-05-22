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
 * Calcite type marker for an OpenSearch {@code ip} column. Underlying SQL type is
 * {@link SqlTypeName#VARBINARY}, matching the on-disk 16-byte ipv4-mapped-ipv6 encoding;
 * {@link AbstractSqlType} handles {@code getSqlTypeName()}, {@code isNullable()}, and
 * {@code getFamily()} for free, so existing operator dispatch (cidrmatch byte-range rewrite,
 * equality / IN / BETWEEN coercion, Substrait conversion) treats this identically to a plain
 * VARBINARY column. The dedicated subclass exists purely so the SQL plugin's response-schema
 * build can {@code instanceof}-dispatch to render the column type as {@code "ip"}, and so
 * {@code AnalyticsExecutionEngine.convertRows} can format the {@code byte[]} cell as a
 * dotted-quad / RFC 5952 string.
 */
public final class IpType extends AbstractSqlType {

    /** OpenSearch type-name string this UDT preserves. */
    public static final String NAME = "ip";

    public IpType(boolean nullable) {
        super(SqlTypeName.VARBINARY, nullable, null);
        computeDigest();
    }

    /** Convenience for the common nullable case used by {@link OpenSearchSchemaBuilder}. */
    public static IpType nullable() {
        return new IpType(true);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
