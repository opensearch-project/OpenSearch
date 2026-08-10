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
import org.opensearch.index.query.RegexpQueryBuilder;

import java.util.Map;

/**
 * Serializer for the REGEXP_QUERY delegated relevance function.
 * Maps to OpenSearch {@link RegexpQueryBuilder} — pattern passes through verbatim.
 */
public class RegexpQueryDslSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "regexp_query";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new RegexpQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        RegexpQueryBuilder regexpQb = (RegexpQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "case_insensitive" -> regexpQb.caseInsensitive(parseStrictBoolean("case_insensitive", entry.getValue()));
                case "flags" -> {
                    // Raw int bitmask — lossless passthrough matching RegexpQueryBuilder.flags(int).
                    try {
                        regexpQb.flags(Integer.parseInt(entry.getValue()));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException("Invalid integer value for 'flags': [" + entry.getValue() + "]", e);
                    }
                }
                case "max_determinized_states" -> {
                    try {
                        regexpQb.maxDeterminizedStates(Integer.parseInt(entry.getValue()));
                    } catch (NumberFormatException e) {
                        throw new IllegalArgumentException(
                            "Invalid integer value for 'max_determinized_states': [" + entry.getValue() + "]",
                            e
                        );
                    }
                }
                case "rewrite" -> regexpQb.rewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }

    /** Rejects values other than "true" or "false" (case-insensitive). */
    private static boolean parseStrictBoolean(String paramName, String value) {
        if ("true".equalsIgnoreCase(value)) {
            return true;
        }
        if ("false".equalsIgnoreCase(value)) {
            return false;
        }
        throw new IllegalArgumentException("Invalid boolean value for '" + paramName + "': [" + value + "]; must be 'true' or 'false'");
    }
}
