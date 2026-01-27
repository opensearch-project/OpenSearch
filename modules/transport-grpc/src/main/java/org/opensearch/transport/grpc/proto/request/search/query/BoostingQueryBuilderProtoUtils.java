/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoostingQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.BoostingQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Utility class for converting BoostingQuery Protocol Buffers to OpenSearch query objects.
 * This class provides methods to transform Protocol Buffer representations of boosting queries
 * into their corresponding OpenSearch BoostingQueryBuilder implementations for search operations.
 */
class BoostingQueryBuilderProtoUtils {

    private BoostingQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer BoostingQuery to an OpenSearch BoostingQueryBuilder.
     * Similar to {@link BoostingQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * BoostingQueryBuilder with the appropriate positive, negative, negative_boost, boost, and name settings.
     *
     * @param boostingQueryProto The Protocol Buffer BoostingQuery to convert
     * @param registry The registry to use for converting nested queries
     * @return A configured BoostingQueryBuilder instance
     * @throws IllegalArgumentException if required fields are missing
     */
    static BoostingQueryBuilder fromProto(BoostingQuery boostingQueryProto, QueryBuilderProtoConverterRegistry registry) {
        // Extract fields in the order they appear in fromXContent
        QueryBuilder positiveQuery = null;
        QueryBuilder negativeQuery = null;
        float negativeBoost = -1;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;

        // Process positive query (required in proto)
        QueryContainer positiveContainer = boostingQueryProto.getPositive();
        positiveQuery = registry.fromProto(positiveContainer);
        if (positiveQuery == null) {
            throw new IllegalArgumentException(BoostingQueryBuilder.POSITIVE_QUERY_REQUIRED);
        }

        // Process negative query (required in proto)
        QueryContainer negativeContainer = boostingQueryProto.getNegative();
        negativeQuery = registry.fromProto(negativeContainer);
        if (negativeQuery == null) {
            throw new IllegalArgumentException(BoostingQueryBuilder.NEGATIVE_QUERY_REQUIRED);
        }

        // Process negative_boost (required in proto, but validate it's positive)
        negativeBoost = boostingQueryProto.getNegativeBoost();
        if (negativeBoost < 0) {
            throw new IllegalArgumentException(BoostingQueryBuilder.NEGATIVE_BOOST_POSITIVE_VALUE_REQUIRED);
        }

        // Process boost (optional)
        if (boostingQueryProto.hasBoost()) {
            boost = boostingQueryProto.getBoost();
        }

        // Process queryName (optional)
        if (boostingQueryProto.hasXName()) {
            queryName = boostingQueryProto.getXName();
        }

        // Build the query in the same order as fromXContent
        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positiveQuery, negativeQuery);
        boostingQuery.negativeBoost(negativeBoost);
        boostingQuery.boost(boost);
        boostingQuery.queryName(queryName);

        return boostingQuery;
    }
}
