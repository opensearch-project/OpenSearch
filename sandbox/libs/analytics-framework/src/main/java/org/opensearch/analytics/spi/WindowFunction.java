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
 * Window functions that a backend may support, categorized by {@link Type}.
 *
 * @opensearch.internal
 */
public enum WindowFunction {
    // Ranking
    ROW_NUMBER(Type.RANKING, SqlKind.ROW_NUMBER),
    RANK(Type.RANKING, SqlKind.RANK),
    DENSE_RANK(Type.RANKING, SqlKind.DENSE_RANK),
    PERCENT_RANK(Type.RANKING, SqlKind.PERCENT_RANK),
    CUME_DIST(Type.RANKING, SqlKind.CUME_DIST),
    NTILE(Type.RANKING, SqlKind.NTILE),

    // Value
    LEAD(Type.VALUE, SqlKind.LEAD),
    LAG(Type.VALUE, SqlKind.LAG),
    FIRST_VALUE(Type.VALUE, SqlKind.FIRST_VALUE),
    LAST_VALUE(Type.VALUE, SqlKind.LAST_VALUE),
    NTH_VALUE(Type.VALUE, SqlKind.NTH_VALUE);

    /** Category of window function. */
    public enum Type {
        RANKING,
        VALUE
    }

    private final Type type;
    private final SqlKind sqlKind;

    WindowFunction(Type type, SqlKind sqlKind) {
        this.type = type;
        this.sqlKind = sqlKind;
    }

    public Type getType() {
        return type;
    }

    public SqlKind getSqlKind() {
        return sqlKind;
    }

    /** Maps a Calcite SqlKind to a WindowFunction, or null if not recognized. */
    public static WindowFunction fromSqlKind(SqlKind kind) {
        for (WindowFunction func : values()) {
            if (func.sqlKind == kind) {
                return func;
            }
        }
        return null;
    }
}
