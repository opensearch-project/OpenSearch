/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.ConstantScoreQuery;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Utility class for converting ConstantScoreQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of constant score queries
 * into their corresponding OpenSearch ConstantScoreQueryBuilder implementations for search operations.
 */
class ConstantScoreQueryBuilderProtoUtils {

    private ConstantScoreQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ConstantScoreQuery to an OpenSearch ConstantScoreQueryBuilder.
     * Similar to {@link ConstantScoreQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * ConstantScoreQueryBuilder with the appropriate filter query, boost, and query name.
     *
     * @param constantScoreQueryProto The Protocol Buffer ConstantScoreQuery object
     * @param registry The registry to use for converting the nested filter query
     * @return A configured ConstantScoreQueryBuilder instance
     */
    static ConstantScoreQueryBuilder fromProto(ConstantScoreQuery constantScoreQueryProto, QueryBuilderProtoConverterRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("QueryBuilderProtoConverterRegistry cannot be null");
        }
        String queryName = null;
        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        if (!constantScoreQueryProto.hasFilter()) {
            throw new IllegalArgumentException("ConstantScore query must have a filter query");
        }

        QueryBuilder query = registry.fromProto(constantScoreQueryProto.getFilter());
        if (query == null) {
            throw new IllegalArgumentException("Filter query cannot be null for constant_score query");
        }

        if (constantScoreQueryProto.hasBoost()) {
            boost = constantScoreQueryProto.getBoost();
        }

        if (constantScoreQueryProto.hasXName()) {
            queryName = constantScoreQueryProto.getXName();
        }

        ConstantScoreQueryBuilder constantScoreBuilder = new ConstantScoreQueryBuilder(query);
        constantScoreBuilder.boost(boost);
        constantScoreBuilder.queryName(queryName);
        return constantScoreBuilder;
    }
}
