/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.common.Booleans;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.query.FuzzyQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * Serializer for the FUZZY relevance function.
 */
public class FuzzySerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "fuzzy";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new FuzzyQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        FuzzyQueryBuilder fuzzyQb = (FuzzyQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "fuzziness" -> fuzzyQb.fuzziness(Fuzziness.build(entry.getValue()));
                case "prefix_length" -> fuzzyQb.prefixLength(parseIntParam("prefix_length", entry.getValue()));
                case "max_expansions" -> fuzzyQb.maxExpansions(parseIntParam("max_expansions", entry.getValue()));
                case "transpositions" -> fuzzyQb.transpositions(parseStrictBoolean("transpositions", entry.getValue()));
                case "rewrite" -> fuzzyQb.rewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }

    private int parseIntParam(String paramName, String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(functionName() + " invalid value for '" + paramName + "': " + value);
        }
    }

    // Strict: Booleans.parseBoolean rejects anything other than "true"/"false"; lenient Boolean.parseBoolean
    // would silently coerce e.g. "yes" to false, flipping the edit-distance model with no error signal.
    private boolean parseStrictBoolean(String paramName, String value) {
        try {
            return Booleans.parseBoolean(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(functionName() + " invalid value for '" + paramName + "': " + value);
        }
    }
}
