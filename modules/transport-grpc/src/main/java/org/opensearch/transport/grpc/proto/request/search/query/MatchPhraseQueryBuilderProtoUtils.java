/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.protobufs.MatchPhraseQuery;
import org.opensearch.protobufs.ZeroTermsQuery;

/**
 * Utility class for converting Protocol Buffer MatchPhraseQuery objects to OpenSearch MatchPhraseQueryBuilder instances.
 * This class handles the detailed conversion logic, including parsing of all MatchPhraseQuery parameters,
 * analyzer settings, slop configuration, zero terms query behavior, boost values, and query names.
 *
 * @opensearch.internal
 */
class MatchPhraseQueryBuilderProtoUtils {
    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private MatchPhraseQueryBuilderProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer MatchPhraseQuery to a MatchPhraseQueryBuilder.
     * This method extracts all relevant parameters from the protobuf representation and
     * creates a properly configured MatchPhraseQueryBuilder with the appropriate field name,
     * query value, analyzer, slop, zero terms query behavior, boost, and query name.
     *
     * @param matchPhraseQueryProto The Protocol Buffer MatchPhraseQuery object
     * @return A properly configured MatchPhraseQueryBuilder
     * @throws IllegalArgumentException if the MatchPhraseQuery is null or if required fields are missing
     */
    static MatchPhraseQueryBuilder fromProto(MatchPhraseQuery matchPhraseQueryProto) {
        if (matchPhraseQueryProto == null) {
            throw new IllegalArgumentException("MatchPhraseQuery cannot be null");
        }

        String fieldName = matchPhraseQueryProto.getField();
        if (fieldName.isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty for match phrase query");
        }

        String value = matchPhraseQueryProto.getQuery();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Query value cannot be null or empty for match phrase query");
        }

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String analyzer = null;
        int slop = MatchQuery.DEFAULT_PHRASE_SLOP;
        MatchQuery.ZeroTermsQuery zeroTermsQuery = MatchQuery.DEFAULT_ZERO_TERMS_QUERY;
        String queryName = null;
        if (matchPhraseQueryProto.hasAnalyzer()) {
            analyzer = matchPhraseQueryProto.getAnalyzer();
        }

        if (matchPhraseQueryProto.hasSlop()) {
            int slopValue = matchPhraseQueryProto.getSlop();
            if (slopValue < 0) {
                throw new IllegalArgumentException("No negative slop allowed.");
            }
            slop = slopValue;
        }

        if (matchPhraseQueryProto.hasZeroTermsQuery()) {
            ZeroTermsQuery zeroTermsQueryProto = matchPhraseQueryProto.getZeroTermsQuery();
            MatchQuery.ZeroTermsQuery parsedZeroTermsQuery = parseZeroTermsQuery(zeroTermsQueryProto);
            if (parsedZeroTermsQuery != null) {
                zeroTermsQuery = parsedZeroTermsQuery;
            }
        }

        if (matchPhraseQueryProto.hasBoost()) {
            boost = matchPhraseQueryProto.getBoost();
        }

        if (matchPhraseQueryProto.hasXName()) {
            queryName = matchPhraseQueryProto.getXName();
        }

        MatchPhraseQueryBuilder matchQuery = new MatchPhraseQueryBuilder(fieldName, value);
        matchQuery.analyzer(analyzer);
        matchQuery.slop(slop);
        matchQuery.zeroTermsQuery(zeroTermsQuery);
        matchQuery.queryName(queryName);
        matchQuery.boost(boost);

        return matchQuery;
    }

    /**
     * Parses ZeroTermsQuery enum to MatchQuery.ZeroTermsQuery.
     *
     * @param zeroTermsQueryProto The ZeroTermsQuery enum value
     * @return The corresponding MatchQuery.ZeroTermsQuery, or null if unsupported
     */
    private static MatchQuery.ZeroTermsQuery parseZeroTermsQuery(ZeroTermsQuery zeroTermsQueryProto) {
        if (zeroTermsQueryProto == null) {
            return null;
        }

        switch (zeroTermsQueryProto) {
            case ZERO_TERMS_QUERY_ALL:
                return MatchQuery.ZeroTermsQuery.ALL;
            case ZERO_TERMS_QUERY_NONE:
                return MatchQuery.ZeroTermsQuery.NONE;
            default:
                return null;
        }
    }
}
