/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.search.MatchQuery;

import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the MATCH_PHRASE_PREFIX relevance function.
 */
public class MatchPhrasePrefixSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "match_phrase_prefix";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new MatchPhrasePrefixQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQb = (MatchPhrasePrefixQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "slop" -> matchPhrasePrefixQb.slop(Integer.parseInt(entry.getValue()));
                case "analyzer" -> matchPhrasePrefixQb.analyzer(entry.getValue());
                case "max_expansions" -> matchPhrasePrefixQb.maxExpansions(Integer.parseInt(entry.getValue()));
                case "zero_terms_query" -> matchPhrasePrefixQb.zeroTermsQuery(
                    MatchQuery.ZeroTermsQuery.valueOf(entry.getValue().toUpperCase(Locale.ROOT))
                );
                case "boost" -> matchPhrasePrefixQb.boost(Float.parseFloat(entry.getValue()));
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
