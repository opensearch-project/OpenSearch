/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.ChildScoreMode;
import org.opensearch.protobufs.NestedQuery;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.transport.grpc.proto.request.search.InnerHitsBuilderProtoUtils;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Utility class for converting protobuf NestedQuery to OpenSearch NestedQueryBuilder.
 * Handles the conversion of nested query protobuf messages to OpenSearch NestedQueryBuilder objects.
 */
class NestedQueryBuilderProtoUtils {

    private NestedQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf NestedQuery to an OpenSearch NestedQueryBuilder.
     * Similar to {@link NestedQueryBuilder#fromXContent(org.opensearch.core.xcontent.XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * NestedQueryBuilder with the appropriate path, query, score mode, inner hits, boost, and query name.
     *
     * @param nestedQueryProto the protobuf NestedQuery to convert
     * @param registry The registry to use for converting nested queries
     * @return the converted OpenSearch NestedQueryBuilder
     * @throws IllegalArgumentException if the protobuf query is invalid
     */
    static NestedQueryBuilder fromProto(NestedQuery nestedQueryProto, QueryBuilderProtoConverterRegistry registry) {
        if (nestedQueryProto == null) {
            throw new IllegalArgumentException("NestedQuery cannot be null");
        }

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        ScoreMode scoreMode = ScoreMode.Avg;
        String queryName = null;
        QueryBuilder query = null;
        InnerHitBuilder innerHitBuilder = null;
        boolean ignoreUnmapped = NestedQueryBuilder.DEFAULT_IGNORE_UNMAPPED;

        String path = nestedQueryProto.getPath();
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Path is required for NestedQuery");
        }

        if (!nestedQueryProto.hasQuery()) {
            throw new IllegalArgumentException("Query is required for NestedQuery");
        }
        try {
            QueryContainer queryContainer = nestedQueryProto.getQuery();
            query = registry.fromProto(queryContainer);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to convert inner query for NestedQuery: " + e.getMessage(), e);
        }

        if (nestedQueryProto.hasScoreMode()) {
            scoreMode = parseScoreMode(nestedQueryProto.getScoreMode());
        }

        if (nestedQueryProto.hasIgnoreUnmapped()) {
            ignoreUnmapped = nestedQueryProto.getIgnoreUnmapped();
        }

        if (nestedQueryProto.hasBoost()) {
            boost = nestedQueryProto.getBoost();
        }

        if (nestedQueryProto.hasXName()) {
            queryName = nestedQueryProto.getXName();
        }

        if (nestedQueryProto.hasInnerHits()) {
            try {
                innerHitBuilder = InnerHitsBuilderProtoUtils.fromProto(nestedQueryProto.getInnerHits(), registry);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to convert inner hits for NestedQuery: " + e.getMessage(), e);
            }
        }

        NestedQueryBuilder queryBuilder = new NestedQueryBuilder(path, query, scoreMode, innerHitBuilder).ignoreUnmapped(ignoreUnmapped)
            .queryName(queryName)
            .boost(boost);

        return queryBuilder;
    }

    /**
     * Converts protobuf ChildScoreMode to Lucene ScoreMode.
     *
     * @param childScoreMode the protobuf ChildScoreMode
     * @return the converted Lucene ScoreMode
     */
    private static ScoreMode parseScoreMode(ChildScoreMode childScoreMode) {
        switch (childScoreMode) {
            case CHILD_SCORE_MODE_AVG:
                return ScoreMode.Avg;
            case CHILD_SCORE_MODE_MAX:
                return ScoreMode.Max;
            case CHILD_SCORE_MODE_MIN:
                return ScoreMode.Min;
            case CHILD_SCORE_MODE_NONE:
                return ScoreMode.None;
            case CHILD_SCORE_MODE_SUM:
                return ScoreMode.Total;
            default:
                return ScoreMode.Avg; // Default value
        }
    }
}
