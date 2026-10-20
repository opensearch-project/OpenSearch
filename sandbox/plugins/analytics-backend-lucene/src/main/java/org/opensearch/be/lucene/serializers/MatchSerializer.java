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
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.search.MatchQuery;

import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the MATCH relevance function.
 */
public class MatchSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "match";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new MatchQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        MatchQueryBuilder matchQb = (MatchQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "operator" -> matchQb.operator(Operator.fromString(entry.getValue()));
                case "analyzer" -> matchQb.analyzer(entry.getValue());
                case "fuzziness" -> matchQb.fuzziness(Fuzziness.build(entry.getValue()));
                case "boost" -> matchQb.boost(Float.parseFloat(entry.getValue()));
                case "zero_terms_query" -> matchQb.zeroTermsQuery(
                    MatchQuery.ZeroTermsQuery.valueOf(entry.getValue().toUpperCase(Locale.ROOT))
                );
                case "minimum_should_match" -> matchQb.minimumShouldMatch(entry.getValue());
                case "max_expansions" -> matchQb.maxExpansions(Integer.parseInt(entry.getValue()));
                case "prefix_length" -> matchQb.prefixLength(Integer.parseInt(entry.getValue()));
                case "fuzzy_transpositions" -> matchQb.fuzzyTranspositions(Boolean.parseBoolean(entry.getValue()));
                case "fuzzy_rewrite" -> matchQb.fuzzyRewrite(entry.getValue());
                case "lenient" -> matchQb.lenient(Boolean.parseBoolean(entry.getValue()));
                case "auto_generate_synonyms_phrase_query" -> matchQb.autoGenerateSynonymsPhraseQuery(
                    Boolean.parseBoolean(entry.getValue())
                );
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
