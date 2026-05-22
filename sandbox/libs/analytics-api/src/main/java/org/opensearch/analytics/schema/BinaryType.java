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
 * Calcite type marker for an OpenSearch {@code binary} column. Underlying SQL type is
 * {@link SqlTypeName#VARBINARY}, matching the on-disk encoding;
 * {@link AbstractSqlType} handles {@code getSqlTypeName()}, {@code isNullable()}, and
 * {@code getFamily()} for free. The dedicated subclass exists so the SQL plugin's
 * response-schema build can {@code instanceof}-dispatch to render the column type as
 * {@code "binary"}, and so {@code AnalyticsExecutionEngine.convertRows} can base64-encode
 * the {@code byte[]} cell to match the OpenSearch {@code binary} field wire contract.
 */
public final class BinaryType extends AbstractSqlType {

    /** OpenSearch type-name string this UDT preserves. */
    public static final String NAME = "binary";

    public BinaryType(boolean nullable) {
        super(SqlTypeName.VARBINARY, nullable, null);
        computeDigest();
    }

    /** Convenience for the common nullable case used by {@link OpenSearchSchemaBuilder}. */
    public static BinaryType nullable() {
        return new BinaryType(true);
    }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(NAME.toUpperCase(Locale.ROOT));
    }
}
