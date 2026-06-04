/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoolQuery;
import org.opensearch.protobufs.MinimumShouldMatch;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

import java.util.List;

/**
 * Utility class for converting BoolQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of bool queries
 * into their corresponding OpenSearch BoolQueryBuilder implementations for search operations.
 */
class BoolQueryBuilderProtoUtils {

    private BoolQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer BoolQuery to an OpenSearch BoolQueryBuilder.
     * Similar to {@link BoolQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * BoolQueryBuilder with the appropriate must, must_not, should, filter clauses,
     * boost, query name, and minimum_should_match.
     *
     * @param boolQueryProto The Protocol Buffer BoolQuery object
     * @param registry The registry to use for converting nested queries
     * @return A configured BoolQueryBuilder instance
     */
    static BoolQueryBuilder fromProto(BoolQuery boolQueryProto, QueryBuilderProtoConverterRegistry registry) {
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String minimumShouldMatch = null;
        boolean adjustPureNegative = BoolQueryBuilder.ADJUST_PURE_NEGATIVE_DEFAULT;

        // Create BoolQueryBuilder
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();

        // Process name
        if (boolQueryProto.hasXName()) {
            queryName = boolQueryProto.getXName();
            boolQuery.queryName(queryName);
        }

        // Process boost
        if (boolQueryProto.hasBoost()) {
            boost = boolQueryProto.getBoost();
            boolQuery.boost(boost);
        }

        // Process minimum_should_match
        if (boolQueryProto.hasMinimumShouldMatch()) {
            MinimumShouldMatch minimumShouldMatchProto = boolQueryProto.getMinimumShouldMatch();
            switch (minimumShouldMatchProto.getMinimumShouldMatchCase()) {
                case INT32:
                    minimumShouldMatch = String.valueOf(minimumShouldMatchProto.getInt32());
                    break;
                case STRING:
                    minimumShouldMatch = minimumShouldMatchProto.getString();
                    break;
                default:
                    // No minimum_should_match specified
                    break;
            }

            if (minimumShouldMatch != null) {
                boolQuery.minimumShouldMatch(minimumShouldMatch);
            }
        }

        // Process must clauses
        List<QueryContainer> mustClauses = boolQueryProto.getMustList();
        for (QueryContainer queryContainer : mustClauses) {
            QueryBuilder queryBuilder = registry.fromProto(queryContainer);
            if (queryBuilder != null) {
                boolQuery.must(queryBuilder);
            }
        }

        // Process must_not clauses
        List<QueryContainer> mustNotClauses = boolQueryProto.getMustNotList();
        for (QueryContainer queryContainer : mustNotClauses) {
            QueryBuilder queryBuilder = registry.fromProto(queryContainer);
            if (queryBuilder != null) {
                boolQuery.mustNot(queryBuilder);
            }
        }

        // Process should clauses
        List<QueryContainer> shouldClauses = boolQueryProto.getShouldList();
        for (QueryContainer queryContainer : shouldClauses) {
            QueryBuilder queryBuilder = registry.fromProto(queryContainer);
            if (queryBuilder != null) {
                boolQuery.should(queryBuilder);
            }
        }

        // Process filter clauses
        List<QueryContainer> filterClauses = boolQueryProto.getFilterList();
        for (QueryContainer queryContainer : filterClauses) {
            QueryBuilder queryBuilder = registry.fromProto(queryContainer);
            if (queryBuilder != null) {
                boolQuery.filter(queryBuilder);
            }
        }

        // Process adjust_pure_negative
        if (boolQueryProto.hasAdjustPureNegative()) {
            adjustPureNegative = boolQueryProto.getAdjustPureNegative();
            boolQuery.adjustPureNegative(adjustPureNegative);
        }

        return boolQuery;
    }
}
