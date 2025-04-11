/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.protobufs.QueryContainer;

/**
 * Utility class for converting Protocol Buffer query representations to OpenSearch QueryBuilder objects.
 * This class provides methods to parse different types of query containers and transform them
 * into their corresponding OpenSearch QueryBuilder implementations for search operations.
 */
public class AbstractQueryBuilderProtoUtils {

    private AbstractQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Parse a query from its Protocol Buffer representation.
     * Similar to {@link AbstractQueryBuilder#parseInnerQueryBuilder(XContentParser)}, this method
     * determines the query type from the Protocol Buffer container and delegates to the appropriate
     * specialized parser.
     *
     * @param queryContainer The Protocol Buffer query container that holds various query type options
     * @return A QueryBuilder instance configured according to the input query parameters
     * @throws UnsupportedOperationException if the query type is not supported
     */
    public static QueryBuilder parseInnerQueryBuilderProto(QueryContainer queryContainer) throws UnsupportedOperationException {
        QueryBuilder result;

        if (queryContainer.hasMatchAll()) {
            result = MatchAllQueryBuilderProtoUtils.fromProto(queryContainer.getMatchAll());
        } else if (queryContainer.hasMatchNone()) {
            result = MatchNoneQueryBuilderProtoUtils.fromProto(queryContainer.getMatchNone());
        } else if (queryContainer.getTermCount() > 0) {
            result = TermQueryBuilderProtoUtils.fromProto(queryContainer.getTermMap());
        }
        // TODO add more query types
        else {
            throw new UnsupportedOperationException("Search query type not supported yet.");
        }

        return result;
    }
}
