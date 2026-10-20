/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;

import java.util.Map;

/**
 * Serializer for the WILDCARD_QUERY relevance function.
 * Maps to OpenSearch WildcardQueryBuilder (single-field, wildcard pattern).
 */
public class WildcardQuerySerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "wildcard_query";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        String convertedPattern = convertSqlWildcardToLucene(operands.query());
        return new WildcardQueryBuilder(operands.fieldName(), convertedPattern);
    }

    /**
     * Converts SQL wildcard characters (% and _) to Lucene wildcard characters (* and ?).
     * Escaped wildcards (\% and \_) are treated as literal characters.
     * A backslash escaping another backslash (\\) produces a literal backslash.
     */
    private static String convertSqlWildcardToLucene(String text) {
        final char ESCAPE = '\\';
        StringBuilder result = new StringBuilder(text.length());
        boolean escaped = false;

        for (char c : text.toCharArray()) {
            if (escaped) {
                switch (c) {
                    case '%':
                        result.append('%');
                        break;
                    case '_':
                        result.append('_');
                        break;
                    case ESCAPE:
                        result.append(ESCAPE);
                        break;
                    default:
                        result.append(ESCAPE);
                        result.append(c);
                        break;
                }
                escaped = false;
            } else if (c == ESCAPE) {
                escaped = true;
            } else if (c == '%') {
                result.append('*');
            } else if (c == '_') {
                result.append('?');
            } else {
                result.append(c);
            }
        }
        // Trailing backslash with nothing to escape — preserve it
        if (escaped) {
            result.append(ESCAPE);
        }
        return result.toString();
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        WildcardQueryBuilder wildcardQb = (WildcardQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "boost" -> wildcardQb.boost(Float.parseFloat(entry.getValue()));
                case "rewrite" -> wildcardQb.rewrite(entry.getValue());
                case "case_insensitive" -> wildcardQb.caseInsensitive(Boolean.parseBoolean(entry.getValue()));
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
