/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.index.query.MatchBoolPrefixQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.index.query.SimpleQueryStringBuilder;

import java.util.List;
import java.util.Map;

/**
 * Registry of per-function query serializers for delegated predicates.
 * Each serializer converts a Calcite RexCall into serialized QueryBuilder bytes
 * that the Lucene backend can deserialize at the data node.
 */
final class QuerySerializerRegistry {

    private static final Map<ScalarFunction, DelegatedPredicateSerializer> SERIALIZERS = Map.ofEntries(
        Map.entry(ScalarFunction.MATCH, QuerySerializerRegistry::serializeMatch),
        Map.entry(ScalarFunction.MATCH_PHRASE, QuerySerializerRegistry::serializeMatchPhrase),
        Map.entry(ScalarFunction.MATCH_BOOL_PREFIX, QuerySerializerRegistry::serializeMatchBoolPrefix),
        Map.entry(ScalarFunction.MATCH_PHRASE_PREFIX, QuerySerializerRegistry::serializeMatchPhrasePrefix),
        Map.entry(ScalarFunction.MULTI_MATCH, QuerySerializerRegistry::serializeMultiMatch),
        Map.entry(ScalarFunction.QUERY_STRING, QuerySerializerRegistry::serializeQueryString),
        Map.entry(ScalarFunction.SIMPLE_QUERY_STRING, QuerySerializerRegistry::serializeSimpleQueryString)
    );

    private QuerySerializerRegistry() {}

    static Map<ScalarFunction, DelegatedPredicateSerializer> getSerializers() {
        return SERIALIZERS;
    }

    // TODO: Extract each serialize* method into its own dedicated class once we handle more parameters.
    // These methods are expected to grow significantly as optional parameters are added.

    private static byte[] serializeMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.fieldName() == null || operands.query() == null) {
            throw new IllegalArgumentException("match requires 'field' and 'query' parameters, got: " + call);
        }
        // TODO: extract optional params (operator, analyzer, fuzziness, boost)
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(operands.fieldName(), operands.query());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrase(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.fieldName() == null || operands.query() == null) {
            throw new IllegalArgumentException("match_phrase requires 'field' and 'query' parameters, got: " + call);
        }
        // TODO: extract optional params (slop, analyzer, zero_terms_query)
        MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder(operands.fieldName(), operands.query());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchBoolPrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.fieldName() == null || operands.query() == null) {
            throw new IllegalArgumentException("match_bool_prefix requires 'field' and 'query' parameters, got: " + call);
        }
        // TODO: extract optional params (analyzer, fuzziness, operator, minimum_should_match)
        MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(operands.fieldName(), operands.query());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrasePrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.fieldName() == null || operands.query() == null) {
            throw new IllegalArgumentException("match_phrase_prefix requires 'field' and 'query' parameters, got: " + call);
        }
        // TODO: extract optional params (slop, analyzer, max_expansions, zero_terms_query)
        MatchPhrasePrefixQueryBuilder queryBuilder = new MatchPhrasePrefixQueryBuilder(operands.fieldName(), operands.query());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMultiMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.query() == null) {
            throw new IllegalArgumentException("multi_match requires a 'query' parameter, got: " + call);
        }
        // TODO: extract per-field boost values and optional params (type, operator, analyzer, fuzziness)
        List<String> fields = operands.fields();
        MultiMatchQueryBuilder queryBuilder = fields != null
            ? new MultiMatchQueryBuilder(operands.query(), fields.toArray(String[]::new))
            : new MultiMatchQueryBuilder(operands.query());
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.query() == null) {
            throw new IllegalArgumentException("query_string requires a 'query' parameter, got: " + call);
        }
        // TODO: extract optional params (default_operator, analyzer, allow_leading_wildcard)
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(operands.query());
        if (operands.fields() != null) {
            for (String field : operands.fields()) {
                queryBuilder.field(field);
            }
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeSimpleQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ConversionUtils.RelevanceOperands operands = ConversionUtils.extractRelevanceOperands(call, fieldStorage);
        if (operands.query() == null) {
            throw new IllegalArgumentException("simple_query_string requires a 'query' parameter, got: " + call);
        }
        // TODO: extract optional params (default_operator, analyzer, flags, minimum_should_match)
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringBuilder(operands.query());
        if (operands.fields() != null) {
            for (String field : operands.fields()) {
                queryBuilder.field(field);
            }
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }
}
