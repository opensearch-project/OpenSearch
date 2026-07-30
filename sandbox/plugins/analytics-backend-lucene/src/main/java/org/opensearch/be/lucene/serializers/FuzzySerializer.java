/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
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
                case "prefix_length" -> fuzzyQb.prefixLength(Integer.parseInt(entry.getValue()));
                case "max_expansions" -> fuzzyQb.maxExpansions(Integer.parseInt(entry.getValue()));
                case "transpositions" -> fuzzyQb.transpositions(Boolean.parseBoolean(entry.getValue()));
                case "rewrite" -> fuzzyQb.rewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
