/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.search.MatchQuery;

import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the MATCH_PHRASE relevance function.
 */
public class MatchPhraseSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "match_phrase";
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        return new MatchPhraseQueryBuilder(operands.fieldName(), operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        MatchPhraseQueryBuilder matchPhraseQb = (MatchPhraseQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "slop" -> matchPhraseQb.slop(Integer.parseInt(entry.getValue()));
                case "analyzer" -> matchPhraseQb.analyzer(entry.getValue());
                case "zero_terms_query" -> matchPhraseQb.zeroTermsQuery(
                    MatchQuery.ZeroTermsQuery.valueOf(entry.getValue().toUpperCase(Locale.ROOT))
                );
                case "boost" -> matchPhraseQb.boost(Float.parseFloat(entry.getValue()));
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
