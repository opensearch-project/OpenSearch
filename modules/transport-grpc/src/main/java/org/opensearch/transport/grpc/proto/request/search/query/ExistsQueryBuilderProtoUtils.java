/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.protobufs.ExistsQuery;

/**
 * Utility class for converting ExistsQuery Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of exists queries
 * into their corresponding OpenSearch ExistsQueryBuilder implementations for search operations.
 */
class ExistsQueryBuilderProtoUtils {

    private ExistsQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer ExistsQuery to an OpenSearch ExistsQueryBuilder.
     * Similar to {@link ExistsQueryBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * ExistsQueryBuilder with the appropriate field name, boost, and query name.
     *
     * @param existsQueryProto The Protocol Buffer ExistsQuery object
     * @return A configured ExistsQueryBuilder instance
     * @throws IllegalArgumentException if the exists query is null or missing required fields
     */
    static ExistsQueryBuilder fromProto(ExistsQuery existsQueryProto) {
        if (existsQueryProto == null) {
            throw new IllegalArgumentException("ExistsQuery cannot be null");
        }

        String field = existsQueryProto.getField();

        ExistsQueryBuilder existsQueryBuilder = new ExistsQueryBuilder(field);

        // Set optional parameters
        if (existsQueryProto.hasBoost()) {
            existsQueryBuilder.boost(existsQueryProto.getBoost());
        }

        if (existsQueryProto.hasXName()) {
            existsQueryBuilder.queryName(existsQueryProto.getXName());
        }

        return existsQueryBuilder;
    }
}
