/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.be.lucene.serializers.EqualsSerializer;
import org.opensearch.be.lucene.serializers.MatchAllSerializer;
import org.opensearch.be.lucene.serializers.MatchBoolPrefixSerializer;
import org.opensearch.be.lucene.serializers.MatchPhrasePrefixSerializer;
import org.opensearch.be.lucene.serializers.MatchPhraseSerializer;
import org.opensearch.be.lucene.serializers.MatchSerializer;
import org.opensearch.be.lucene.serializers.MultiMatchSerializer;
import org.opensearch.be.lucene.serializers.QuerySerializer;
import org.opensearch.be.lucene.serializers.QueryStringSerializer;
import org.opensearch.be.lucene.serializers.SimpleQueryStringSerializer;
import org.opensearch.be.lucene.serializers.WildcardQuerySerializer;

import java.util.Map;

/**
 * Registry of per-function query serializers for delegated predicates.
 * Each serializer converts a Calcite RexCall into serialized QueryBuilder bytes
 * that the Lucene backend can deserialize at the data node.
 */
final class QuerySerializerRegistry {

    private static final Map<ScalarFunction, DelegatedPredicateSerializer> SERIALIZERS = Map.ofEntries(
        Map.entry(ScalarFunction.MATCH, new MatchSerializer()),
        Map.entry(ScalarFunction.MATCH_PHRASE, new MatchPhraseSerializer()),
        Map.entry(ScalarFunction.MATCH_BOOL_PREFIX, new MatchBoolPrefixSerializer()),
        Map.entry(ScalarFunction.MATCH_PHRASE_PREFIX, new MatchPhrasePrefixSerializer()),
        Map.entry(ScalarFunction.MULTI_MATCH, new MultiMatchSerializer()),
        Map.entry(ScalarFunction.QUERY_STRING, new QueryStringSerializer()),
        Map.entry(ScalarFunction.SIMPLE_QUERY_STRING, new SimpleQueryStringSerializer()),
        Map.entry(ScalarFunction.WILDCARD_QUERY, new WildcardQuerySerializer()),
        Map.entry(ScalarFunction.QUERY, new QuerySerializer()),
        Map.entry(ScalarFunction.MATCHALL, new MatchAllSerializer()),
        Map.entry(ScalarFunction.EQUALS, new EqualsSerializer())
    );

    private QuerySerializerRegistry() {}

    static Map<ScalarFunction, DelegatedPredicateSerializer> getSerializers() {
        return SERIALIZERS;
    }
}
