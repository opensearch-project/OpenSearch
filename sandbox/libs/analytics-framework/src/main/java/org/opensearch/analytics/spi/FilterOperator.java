/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;

/**
 * Standard comparison/predicate operations that a backend may support.
 *
 * @opensearch.internal
 */
public enum FilterOperator {
    EQUALS(SqlKind.EQUALS),
    NOT_EQUALS(SqlKind.NOT_EQUALS),
    GREATER_THAN(SqlKind.GREATER_THAN),
    GREATER_THAN_OR_EQUAL(SqlKind.GREATER_THAN_OR_EQUAL),
    LESS_THAN(SqlKind.LESS_THAN),
    LESS_THAN_OR_EQUAL(SqlKind.LESS_THAN_OR_EQUAL),
    IS_NULL(SqlKind.IS_NULL),
    IS_NOT_NULL(SqlKind.IS_NOT_NULL),
    IN(SqlKind.IN),
    LIKE(SqlKind.LIKE),
    PREFIX(SqlKind.OTHER),
    REGEXP(SqlKind.OTHER),
    WILDCARD(SqlKind.OTHER);

    private final SqlKind sqlKind;

    FilterOperator(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    /** Maps a Calcite SqlKind to a FilterOperator, or null if not a standard filter op. */
    public static FilterOperator fromSqlKind(SqlKind kind) {
        for (FilterOperator op : values()) {
            if (op.sqlKind == kind && op.sqlKind != SqlKind.OTHER) {
                return op;
            }
        }
        return null;
    }
}
