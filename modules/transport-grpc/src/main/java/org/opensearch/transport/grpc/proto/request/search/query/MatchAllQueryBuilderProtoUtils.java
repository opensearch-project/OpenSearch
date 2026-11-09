/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.protobufs.MatchAllQuery;

/**
 * Utility class for converting MatchAllQuery Protocol Buffers to OpenSearch query objects.
 */
class MatchAllQueryBuilderProtoUtils {

    private MatchAllQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchAllQuery to an OpenSearch MatchAllQueryBuilder.
     * Similar to {@link MatchAllQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchAllQueryBuilder with the appropriate boost and name settings.
     *
     * @param matchAllQueryProto The Protocol Buffer MatchAllQuery to convert
     * @return A configured MatchAllQueryBuilder instance
     */
    static MatchAllQueryBuilder fromProto(MatchAllQuery matchAllQueryProto) {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        if (matchAllQueryProto.hasBoost()) {
            matchAllQueryBuilder.boost(matchAllQueryProto.getBoost());
        }

        if (matchAllQueryProto.hasXName()) {
            matchAllQueryBuilder.queryName(matchAllQueryProto.getXName());
        }

        return matchAllQueryBuilder;
    }
}
