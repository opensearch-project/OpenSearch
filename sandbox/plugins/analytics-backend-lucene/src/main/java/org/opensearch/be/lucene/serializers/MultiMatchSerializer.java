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
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.search.MatchQuery;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the MULTI_MATCH relevance function.
 */
public class MultiMatchSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "multi_match";
    }

    @Override
    protected void validate(ConversionUtils.RelevanceOperands operands) {
        if (operands.query() == null) {
            throw new IllegalArgumentException(functionName() + " requires a 'query' parameter");
        }
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        List<String> fields = operands.fields();
        if (fields != null) {
            return new MultiMatchQueryBuilder(operands.query(), fields.toArray(String[]::new));
        }
        return new MultiMatchQueryBuilder(operands.query());
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        MultiMatchQueryBuilder multiMatchQb = (MultiMatchQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "type" -> multiMatchQb.type(
                    MultiMatchQueryBuilder.Type.parse(entry.getValue().toLowerCase(Locale.ROOT), LoggingDeprecationHandler.INSTANCE)
                );
                case "operator" -> multiMatchQb.operator(Operator.fromString(entry.getValue()));
                case "analyzer" -> multiMatchQb.analyzer(entry.getValue());
                case "fuzziness" -> multiMatchQb.fuzziness(Fuzziness.build(entry.getValue()));
                case "minimum_should_match" -> multiMatchQb.minimumShouldMatch(entry.getValue());
                case "slop" -> multiMatchQb.slop(Integer.parseInt(entry.getValue()));
                case "zero_terms_query" -> multiMatchQb.zeroTermsQuery(
                    MatchQuery.ZeroTermsQuery.valueOf(entry.getValue().toUpperCase(Locale.ROOT))
                );
                case "boost" -> multiMatchQb.boost(Float.parseFloat(entry.getValue()));
                case "tie_breaker" -> multiMatchQb.tieBreaker(Float.parseFloat(entry.getValue()));
                case "max_expansions" -> multiMatchQb.maxExpansions(Integer.parseInt(entry.getValue()));
                case "prefix_length" -> multiMatchQb.prefixLength(Integer.parseInt(entry.getValue()));
                case "lenient" -> multiMatchQb.lenient(Boolean.parseBoolean(entry.getValue()));
                case "fuzzy_transpositions" -> multiMatchQb.fuzzyTranspositions(Boolean.parseBoolean(entry.getValue()));
                case "fuzzy_rewrite" -> multiMatchQb.fuzzyRewrite(entry.getValue());
                case "auto_generate_synonyms_phrase_query" -> multiMatchQb.autoGenerateSynonymsPhraseQuery(
                    Boolean.parseBoolean(entry.getValue())
                );
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
