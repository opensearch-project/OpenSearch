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
     * Escaped wildcards (\\% and \\_) are treated as literal characters.
     */
    private static String convertSqlWildcardToLucene(String text) {
        final char ESCAPE = '\\';
        StringBuilder result = new StringBuilder(text.length());
        boolean escaped = false;

        for (char c : text.toCharArray()) {
            switch (c) {
                case ESCAPE:
                    escaped = true;
                    result.append(c);
                    break;
                case '%':
                    if (escaped) {
                        result.deleteCharAt(result.length() - 1);
                        result.append('%');
                    } else {
                        result.append('*');
                    }
                    escaped = false;
                    break;
                case '_':
                    if (escaped) {
                        result.deleteCharAt(result.length() - 1);
                        result.append('_');
                    } else {
                        result.append('?');
                    }
                    escaped = false;
                    break;
                default:
                    result.append(c);
                    escaped = false;
            }
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
