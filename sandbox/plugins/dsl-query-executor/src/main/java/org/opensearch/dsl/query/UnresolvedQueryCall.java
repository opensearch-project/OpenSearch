/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;

/**
 * A RexCall that wraps a QueryBuilder that couldn't be converted to standard Calcite Rex expressions.
 *
 * <p>The analytics engine's optimizer can inspect these nodes via {@link #getQueryBuilder()} and
 * decide how to handle them — push to Lucene, absorb into a physical scan, or reject.
 *
 * <p>Reusable for any query type: wildcard, match, query_string, fuzzy, etc.
 */
public class UnresolvedQueryCall extends RexCall {

    /** Marker operator identifying unresolved query nodes in the plan. */
    public static final SqlFunction UNRESOLVED_QUERY = new SqlFunction(
        "UNRESOLVED_QUERY",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.BOOLEAN,
        null,
        null,
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    private final QueryBuilder queryBuilder;

    /**
     * Creates an unresolved query call.
     *
     * @param type the return type (boolean — it's a filter condition)
     * @param queryBuilder the original OpenSearch query that couldn't be converted
     */
    public UnresolvedQueryCall(RelDataType type, QueryBuilder queryBuilder) {
        super(type, UNRESOLVED_QUERY, List.of());
        this.queryBuilder = queryBuilder;
    }

    /** Returns the original OpenSearch query builder. */
    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }
}
