/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.protobufs.MatchAllQuery;

/**
 * Utility class for converting MatchAllQuery Protocol Buffers to objects
 *
 */
public class MatchAllQueryBuilderProtoUtils {

    private MatchAllQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link MatchAllQueryBuilder#fromXContent(XContentParser)}
     *
     * @param matchAllQueryProto
     */

    public static MatchAllQueryBuilder fromProto(MatchAllQuery matchAllQueryProto) {
        MatchAllQueryBuilder matchAllQueryBuilder = new MatchAllQueryBuilder();

        if (matchAllQueryProto.hasBoost()) {
            matchAllQueryBuilder.boost(matchAllQueryProto.getBoost());
        }

        if (matchAllQueryProto.hasName()) {
            matchAllQueryBuilder.queryName(matchAllQueryProto.getName());
        }

        return matchAllQueryBuilder;
    }
}
