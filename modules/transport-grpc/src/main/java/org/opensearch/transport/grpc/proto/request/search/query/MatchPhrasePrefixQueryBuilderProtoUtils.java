/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.FuzzyQuery;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchPhrasePrefixQueryBuilder;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.index.search.MatchQuery.ZeroTermsQuery;
import org.opensearch.protobufs.MatchPhrasePrefixQuery;

/**
 * Utility class for converting MatchPhrasePrefixQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of match_phrase_prefix queries
 * into their corresponding OpenSearch MatchPhrasePrefixQueryBuilder implementations for search operations.
 */
public class MatchPhrasePrefixQueryBuilderProtoUtils {

    private MatchPhrasePrefixQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchPhrasePrefixQuery to an OpenSearch MatchPhrasePrefixQueryBuilder.
     * Similar to {@link MatchPhrasePrefixQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchPhrasePrefixQueryBuilder with the appropriate field name, value, analyzer, slop, max_expansions,
     * zero_terms_query, boost, and query name.
     *
     * @param matchPhrasePrefixQueryProto The Protocol Buffer MatchPhrasePrefixQuery object
     * @return A configured MatchPhrasePrefixQueryBuilder instance
     * @throws IllegalArgumentException if the field name or value is null or empty
     */
    protected static MatchPhrasePrefixQueryBuilder fromProto(MatchPhrasePrefixQuery matchPhrasePrefixQueryProto) {
        String fieldName = matchPhrasePrefixQueryProto.getField();
        Object value = matchPhrasePrefixQueryProto.getQuery();
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
        int maxExpansion = FuzzyQuery.defaultMaxExpansions;
        String queryName = null;
        ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;

        if (matchPhrasePrefixQueryProto.hasAnalyzer()) {
            analyzer = matchPhrasePrefixQueryProto.getAnalyzer();
        }

        if (matchPhrasePrefixQueryProto.hasBoost()) {
            boost = matchPhrasePrefixQueryProto.getBoost();
        }

        if (matchPhrasePrefixQueryProto.hasSlop()) {
            slop = matchPhrasePrefixQueryProto.getSlop();
        }

        if (matchPhrasePrefixQueryProto.hasMaxExpansions()) {
            maxExpansion = matchPhrasePrefixQueryProto.getMaxExpansions();
        }

        if (matchPhrasePrefixQueryProto.hasXName()) {
            queryName = matchPhrasePrefixQueryProto.getXName();
        }

        if (matchPhrasePrefixQueryProto.hasZeroTermsQuery()
            && matchPhrasePrefixQueryProto.getZeroTermsQuery() != org.opensearch.protobufs.ZeroTermsQuery.ZERO_TERMS_QUERY_UNSPECIFIED) {
            if (matchPhrasePrefixQueryProto.getZeroTermsQuery() == org.opensearch.protobufs.ZeroTermsQuery.ZERO_TERMS_QUERY_ALL) {
                zeroTermsQuery = MatchQuery.ZeroTermsQuery.ALL;
            } else if (matchPhrasePrefixQueryProto.getZeroTermsQuery() == org.opensearch.protobufs.ZeroTermsQuery.ZERO_TERMS_QUERY_NONE) {
                zeroTermsQuery = MatchQuery.ZeroTermsQuery.NONE;
            }
        }

        MatchPhrasePrefixQueryBuilder matchPhrasePrefixQueryBuilder = new MatchPhrasePrefixQueryBuilder(fieldName, value);
        matchPhrasePrefixQueryBuilder.analyzer(analyzer);
        matchPhrasePrefixQueryBuilder.slop(slop);
        matchPhrasePrefixQueryBuilder.maxExpansions(maxExpansion);
        matchPhrasePrefixQueryBuilder.queryName(queryName);
        matchPhrasePrefixQueryBuilder.boost(boost);
        matchPhrasePrefixQueryBuilder.zeroTermsQuery(zeroTermsQuery);
        return matchPhrasePrefixQueryBuilder;
    }
}
