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
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * Serializer for the MATCH_BOOL_PREFIX relevance function.
 */
public class MatchBoolPrefixSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "match_bool_prefix";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new MatchBoolPrefixQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        MatchBoolPrefixQueryBuilder matchBoolPrefixQb = (MatchBoolPrefixQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "analyzer" -> matchBoolPrefixQb.analyzer(entry.getValue());
                case "fuzziness" -> matchBoolPrefixQb.fuzziness(Fuzziness.build(entry.getValue()));
                case "operator" -> matchBoolPrefixQb.operator(Operator.fromString(entry.getValue()));
                case "minimum_should_match" -> matchBoolPrefixQb.minimumShouldMatch(entry.getValue());
                case "prefix_length" -> matchBoolPrefixQb.prefixLength(Integer.parseInt(entry.getValue()));
                case "max_expansions" -> matchBoolPrefixQb.maxExpansions(Integer.parseInt(entry.getValue()));
                case "fuzzy_transpositions" -> matchBoolPrefixQb.fuzzyTranspositions(Boolean.parseBoolean(entry.getValue()));
                case "fuzzy_rewrite" -> matchBoolPrefixQb.fuzzyRewrite(entry.getValue());
                case "boost" -> matchBoolPrefixQb.boost(Float.parseFloat(entry.getValue()));
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
