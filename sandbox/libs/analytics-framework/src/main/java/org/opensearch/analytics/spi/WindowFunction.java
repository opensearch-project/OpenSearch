/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/** Window functions a backend may support. */
public enum WindowFunction {
    SUM(SqlKind.SUM),
    AVG(SqlKind.AVG),
    COUNT(SqlKind.COUNT),
    MIN(SqlKind.MIN),
    MAX(SqlKind.MAX),
    ROW_NUMBER(SqlKind.ROW_NUMBER),
    PATTERN(SqlKind.OTHER);

    private final SqlKind sqlKind;

    WindowFunction(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    public static WindowFunction resolveFunction(SqlOperator operator) {
        WindowFunction fn = fromSqlKind(operator.getKind());
        if (fn != null) {
            return fn;
        }
        return fromName(operator.getName());
    }

    public static WindowFunction fromSqlKind(SqlKind sqlKind) {
        if (sqlKind == SqlKind.OTHER || sqlKind == SqlKind.OTHER_FUNCTION) {
            return null;
        }
        for (WindowFunction fn : values()) {
            if (fn.sqlKind == sqlKind) return fn;
        }
        return null;
    }

    public static WindowFunction fromName(String name) {
        try {
            return valueOf(name.toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
