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
import org.opensearch.index.query.QueryStringQueryBuilder;

import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the QUERY_STRING relevance function.
 */
public class QueryStringSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "query_string";
    }

    @Override
    protected void validate(ConversionUtils.RelevanceOperands operands) {
        if (operands.query() == null) {
            throw new IllegalArgumentException(functionName() + " requires a 'query' parameter");
        }
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(operands.query());
        if (operands.fields() != null) {
            for (String field : operands.fields()) {
                queryBuilder.field(field);
            }
        }
        return queryBuilder;
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        QueryStringQueryBuilder queryStringQb = (QueryStringQueryBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "default_operator" -> queryStringQb.defaultOperator(Operator.fromString(entry.getValue()));
                case "analyzer" -> queryStringQb.analyzer(entry.getValue());
                case "allow_leading_wildcard" -> queryStringQb.allowLeadingWildcard(Boolean.parseBoolean(entry.getValue()));
                case "boost" -> queryStringQb.boost(Float.parseFloat(entry.getValue()));
                case "fuzziness" -> queryStringQb.fuzziness(Fuzziness.build(entry.getValue()));
                case "minimum_should_match" -> queryStringQb.minimumShouldMatch(entry.getValue());
                case "type" -> queryStringQb.type(
                    MultiMatchQueryBuilder.Type.parse(entry.getValue().toLowerCase(Locale.ROOT), LoggingDeprecationHandler.INSTANCE)
                );
                case "tie_breaker" -> queryStringQb.tieBreaker(Float.parseFloat(entry.getValue()));
                case "phrase_slop" -> queryStringQb.phraseSlop(Integer.parseInt(entry.getValue()));
                case "lenient" -> queryStringQb.lenient(Boolean.parseBoolean(entry.getValue()));
                case "analyze_wildcard" -> queryStringQb.analyzeWildcard(Boolean.parseBoolean(entry.getValue()));
                case "fuzzy_max_expansions" -> queryStringQb.fuzzyMaxExpansions(Integer.parseInt(entry.getValue()));
                case "fuzzy_prefix_length" -> queryStringQb.fuzzyPrefixLength(Integer.parseInt(entry.getValue()));
                case "fuzzy_transpositions" -> queryStringQb.fuzzyTranspositions(Boolean.parseBoolean(entry.getValue()));
                case "max_determinized_states" -> queryStringQb.maxDeterminizedStates(Integer.parseInt(entry.getValue()));
                case "quote_analyzer" -> queryStringQb.quoteAnalyzer(entry.getValue());
                case "quote_field_suffix" -> queryStringQb.quoteFieldSuffix(entry.getValue());
                case "rewrite" -> queryStringQb.rewrite(entry.getValue());
                case "time_zone" -> queryStringQb.timeZone(entry.getValue());
                case "escape" -> queryStringQb.escape(Boolean.parseBoolean(entry.getValue()));
                case "enable_position_increments" -> queryStringQb.enablePositionIncrements(Boolean.parseBoolean(entry.getValue()));
                case "auto_generate_synonyms_phrase_query" -> queryStringQb.autoGenerateSynonymsPhraseQuery(
                    Boolean.parseBoolean(entry.getValue())
                );
                case "fuzzy_rewrite" -> queryStringQb.fuzzyRewrite(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
