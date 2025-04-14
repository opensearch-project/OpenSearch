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

import java.io.IOException;

/**
 * Utility class for converting AbstractQueryBuilder Protocol Buffers to objects.
 */
public class AbstractQueryBuilderProtoUtils {

    private AbstractQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Parse a query from its Protocol Buffer representation
     * Similar to {@link AbstractQueryBuilder#parseInnerQueryBuilder(XContentParser)}
     *
     * @param queryContainer The Protocol Buffer query container
     * @return A QueryBuilder instance
     * @throws IOException if there's an error during parsing
     */
    public static QueryBuilder parseInnerQueryBuilderProto(QueryContainer queryContainer) throws IOException {
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
