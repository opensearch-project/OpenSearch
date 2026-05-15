/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.opensearch.be.lucene.ConversionUtils;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;
import org.opensearch.index.query.SimpleQueryStringFlag;

import java.util.Locale;
import java.util.Map;

/**
 * Serializer for the SIMPLE_QUERY_STRING relevance function.
 */
public class SimpleQueryStringSerializer extends AbstractRelevanceSerializer {

    @Override
    protected String functionName() {
        return "simple_query_string";
    }

    @Override
    protected void validate(ConversionUtils.RelevanceOperands operands) {
        if (operands.query() == null) {
            throw new IllegalArgumentException(functionName() + " requires a 'query' parameter");
        }
    }

    @Override
    protected QueryBuilder createQueryBuilder(ConversionUtils.RelevanceOperands operands) {
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringBuilder(operands.query());
        if (operands.fields() != null) {
            for (String field : operands.fields()) {
                queryBuilder.field(field);
            }
        }
        return queryBuilder;
    }

    @Override
    protected void applyParams(QueryBuilder qb, Map<String, String> params) {
        SimpleQueryStringBuilder simpleQsQb = (SimpleQueryStringBuilder) qb;
        for (Map.Entry<String, String> entry : params.entrySet()) {
            switch (entry.getKey()) {
                case "default_operator" -> simpleQsQb.defaultOperator(Operator.fromString(entry.getValue()));
                case "analyzer" -> simpleQsQb.analyzer(entry.getValue());
                case "flags" -> {
                    String[] flagNames = entry.getValue().toUpperCase(Locale.ROOT).split("\\|");
                    SimpleQueryStringFlag[] flags = new SimpleQueryStringFlag[flagNames.length];
                    for (int i = 0; i < flagNames.length; i++) {
                        flags[i] = SimpleQueryStringFlag.valueOf(flagNames[i].trim());
                    }
                    simpleQsQb.flags(flags);
                }
                case "minimum_should_match" -> simpleQsQb.minimumShouldMatch(entry.getValue());
                case "boost" -> simpleQsQb.boost(Float.parseFloat(entry.getValue()));
                case "analyze_wildcard" -> simpleQsQb.analyzeWildcard(Boolean.parseBoolean(entry.getValue()));
                case "auto_generate_synonyms_phrase_query" -> simpleQsQb.autoGenerateSynonymsPhraseQuery(
                    Boolean.parseBoolean(entry.getValue())
                );
                case "fuzzy_max_expansions" -> simpleQsQb.fuzzyMaxExpansions(Integer.parseInt(entry.getValue()));
                case "fuzzy_prefix_length" -> simpleQsQb.fuzzyPrefixLength(Integer.parseInt(entry.getValue()));
                case "fuzzy_transpositions" -> simpleQsQb.fuzzyTranspositions(Boolean.parseBoolean(entry.getValue()));
                case "lenient" -> simpleQsQb.lenient(Boolean.parseBoolean(entry.getValue()));
                case "quote_field_suffix" -> simpleQsQb.quoteFieldSuffix(entry.getValue());
                default -> {
                    /* ignore unrecognized params for forward compatibility */ }
            }
        }
    }
}
