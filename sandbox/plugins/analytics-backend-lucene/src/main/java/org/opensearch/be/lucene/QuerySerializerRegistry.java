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
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (operator, analyzer, fuzziness, boost) and apply them to the QueryBuilder.
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrase(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (slop, analyzer, zero_terms_query) and apply them to the QueryBuilder.
        MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchBoolPrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (analyzer, fuzziness, operator, minimum_should_match) and apply them to the QueryBuilder.
        MatchBoolPrefixQueryBuilder queryBuilder = new MatchBoolPrefixQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMatchPhrasePrefix(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (slop, analyzer, max_expansions, zero_terms_query) and apply them to the QueryBuilder.
        MatchPhrasePrefixQueryBuilder queryBuilder = new MatchPhrasePrefixQueryBuilder(fieldName, fieldValue);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeMultiMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (type, operator, analyzer, fuzziness, minimum_should_match) and apply them to the QueryBuilder.
        MultiMatchQueryBuilder queryBuilder = new MultiMatchQueryBuilder(fieldValue, fields.toArray(String[]::new));
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (default_operator, analyzer, allow_leading_wildcard) and apply them to the QueryBuilder.
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(fieldValue);
        for (String field : fields) {
            queryBuilder.field(field);
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }

    private static byte[] serializeSimpleQueryString(RexCall call, List<FieldStorageInfo> fieldStorage) {
        List<String> fields = ConversionUtils.extractFieldsFromRelevanceMap(call, 0, fieldStorage);
        String fieldValue = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract per-field boost values from operand 0 and pass to QueryBuilder
        // TODO: Use ConversionUtils.extractOptionalParams(call, 2) to extract optional params
        // (default_operator, analyzer, flags, minimum_should_match) and apply them to the QueryBuilder.
        SimpleQueryStringBuilder queryBuilder = new SimpleQueryStringBuilder(fieldValue);
        for (String field : fields) {
            queryBuilder.field(field);
        }
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }
}
