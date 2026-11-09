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
 * Calcite type marker for an OpenSearch {@code binary} column. Backed by
 * {@link SqlTypeName#VARBINARY} so planner coercion is unchanged; the subclass exists
 * only as an {@code instanceof}-dispatch marker for callers that need to distinguish a
 * {@code binary} column from a plain {@code VARBINARY}.
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
