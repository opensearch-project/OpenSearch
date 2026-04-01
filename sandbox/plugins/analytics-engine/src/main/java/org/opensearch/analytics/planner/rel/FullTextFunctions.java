/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.FullTextOperator;

/**
 * Calcite SqlFunction markers for full-text search operations.
 * Used by frontend plugins (DSL converter, SQL, PPL) to represent
 * full-text queries in the RelNode tree. The filter rule recognizes
 * these and routes to backends that support the corresponding
 * {@link FullTextOperator}.
 *
 * @opensearch.internal
 */
public class FullTextFunctions {

    public static final SqlFunction MATCH = fullTextFunction("MATCH");
    public static final SqlFunction MATCH_PHRASE = fullTextFunction("MATCH_PHRASE");
    public static final SqlFunction MATCH_PHRASE_PREFIX = fullTextFunction("MATCH_PHRASE_PREFIX");
    public static final SqlFunction MATCH_BOOL_PREFIX = fullTextFunction("MATCH_BOOL_PREFIX");
    public static final SqlFunction MULTI_MATCH = fullTextFunction("MULTI_MATCH");
    public static final SqlFunction QUERY_STRING = fullTextFunction("QUERY_STRING");
    public static final SqlFunction SIMPLE_QUERY_STRING = fullTextFunction("SIMPLE_QUERY_STRING");
    public static final SqlFunction FUZZY = fullTextFunction("FUZZY");

    private FullTextFunctions() {}

    private static SqlFunction fullTextFunction(String name) {
        return new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    /** Maps a SqlFunction to a FullTextOperator, or null if not a full-text function. */
    public static FullTextOperator toFullTextOperator(SqlFunction function) {
        String name = function.getName();
        try {
            return FullTextOperator.valueOf(name);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
