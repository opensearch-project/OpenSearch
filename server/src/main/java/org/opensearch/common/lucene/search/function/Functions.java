/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search.function;

import org.apache.lucene.search.Explanation;
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;

/**
 * Helper utility class for functions
 *
 * @opensearch.internal
 */
public final class Functions {
    private Functions() {}

    /**
     * Return function name wrapped into brackets or empty string, for example: '(_name: func1)'
     * @param functionName function name
     * @return function name wrapped into brackets or empty string
     */
    public static String nameOrEmptyFunc(final String functionName) {
        if (Strings.isNullOrEmpty(functionName) == false) {
            return "(" + AbstractQueryBuilder.NAME_FIELD.getPreferredName() + ": " + functionName + ")";
        } else {
            return "";
        }
    }

    /**
     * Return function name as an argument or empty string, for example: ', _name: func1'
     * @param functionName function name
     * @return function name as an argument or empty string
     */
    public static String nameOrEmptyArg(final String functionName) {
        if (Strings.isNullOrEmpty(functionName) == false) {
            return ", " + FunctionScoreQueryBuilder.NAME_FIELD.getPreferredName() + ": " + functionName;
        } else {
            return "";
        }
    }

    /**
     * Enrich explanation with query name
     * @param explanation explanation
     * @param queryName query name
     * @return explanation enriched with query name
     */
    public static Explanation explainWithName(Explanation explanation, String queryName) {
        if (Strings.isNullOrEmpty(queryName)) {
            return explanation;
        } else {
            final String description = explanation.getDescription() + " " + nameOrEmptyFunc(queryName);
            if (explanation.isMatch()) {
                return Explanation.match(explanation.getValue(), description, explanation.getDetails());
            } else {
                return Explanation.noMatch(description, explanation.getDetails());
            }
        }
    }
}
