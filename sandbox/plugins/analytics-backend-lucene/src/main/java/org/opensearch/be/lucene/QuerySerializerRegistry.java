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
import org.opensearch.index.query.MatchQueryBuilder;

import java.util.List;
import java.util.Map;

/**
 * Registry of per-function query serializers for delegated predicates.
 * Each serializer converts a Calcite RexCall into serialized QueryBuilder bytes
 * that the Lucene backend can deserialize at the data node.
 *
 * <p>TODO: add serializers for match_phrase, match_bool_prefix, match_phrase_prefix.
 * TODO: add multi-field relevance serializers for multi_match, query_string, simple_query_string.
 */
final class QuerySerializerRegistry {

    private static final Map<ScalarFunction, DelegatedPredicateSerializer> SERIALIZERS = Map.of(
        ScalarFunction.MATCH,
        QuerySerializerRegistry::serializeMatch
    );

    private QuerySerializerRegistry() {}

    static Map<ScalarFunction, DelegatedPredicateSerializer> getSerializers() {
        return SERIALIZERS;
    }

    private static byte[] serializeMatch(RexCall call, List<FieldStorageInfo> fieldStorage) {
        String fieldName = ConversionUtils.extractFieldFromRelevanceMap(call, 0, fieldStorage);
        String queryText = ConversionUtils.extractStringFromRelevanceMap(call, 1);
        // TODO: extract optional params (operator, analyzer, fuzziness) from operands 2+
        MatchQueryBuilder queryBuilder = new MatchQueryBuilder(fieldName, queryText);
        return ConversionUtils.serializeQueryBuilder(queryBuilder);
    }
}
