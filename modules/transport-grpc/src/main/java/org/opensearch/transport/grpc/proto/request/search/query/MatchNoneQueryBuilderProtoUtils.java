/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.protobufs.MatchNoneQuery;

/**
 * Utility class for converting MatchNoneQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of match_none queries
 * into their corresponding OpenSearch MatchNoneQueryBuilder implementations for search operations.
 */
class MatchNoneQueryBuilderProtoUtils {

    private MatchNoneQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer MatchNoneQuery to an OpenSearch MatchNoneQueryBuilder.
     * Similar to {@link MatchNoneQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * MatchNoneQueryBuilder with the appropriate boost and name settings.
     *
     * @param matchNoneQueryProto The Protocol Buffer MatchNoneQuery to convert
     * @return A configured MatchNoneQueryBuilder instance
     */
    static MatchNoneQueryBuilder fromProto(MatchNoneQuery matchNoneQueryProto) {
        MatchNoneQueryBuilder matchNoneQueryBuilder = new MatchNoneQueryBuilder();

        if (matchNoneQueryProto.hasBoost()) {
            matchNoneQueryBuilder.boost(matchNoneQueryProto.getBoost());
        }

        if (matchNoneQueryProto.hasXName()) {
            matchNoneQueryBuilder.queryName(matchNoneQueryProto.getXName());
        }

        return matchNoneQueryBuilder;
    }
}
