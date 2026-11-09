/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryStringQueryBuilder;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;

/**
 * Utility class for converting REST-like actions between OpenSearch and Protocol Buffers formats.
 * This class provides methods to transform URL parameters from Protocol Buffer requests into
 * query builders and other OpenSearch constructs.
 */
public class ProtoActionsProtoUtils {

    private ProtoActionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link RestActions#urlParamsToQueryBuilder(RestRequest)}
     *
     * @param request
     * @return
     */
    protected static QueryBuilder urlParamsToQueryBuilder(SearchRequest request) {
        if (!request.hasQ()) {
            return null;
        }

        QueryStringQueryBuilder queryBuilder = QueryBuilders.queryStringQuery(request.getQ());
        queryBuilder.defaultField(request.hasDf() ? request.getDf() : null);
        queryBuilder.analyzeWildcard(request.hasAnalyzeWildcard() ? request.getAnalyzeWildcard() : false);
        if (request.hasDefaultOperator()) {
            queryBuilder.defaultOperator(OperatorProtoUtils.fromEnum(request.getDefaultOperator()));
        }
        return queryBuilder;
    }
}
