/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.protobufs.MatchNoneQuery;

/**
 * Utility class for converting MatchAllQuery Protocol Buffers to objects
 *
 */
public class MatchNoneQueryBuilderProtoUtils {

    private MatchNoneQueryBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link MatchNoneQueryBuilder#fromXContent(XContentParser)}
     *
     * @param matchNoneQueryProto
     */

    public static MatchNoneQueryBuilder fromProto(MatchNoneQuery matchNoneQueryProto) {
        MatchNoneQueryBuilder matchNoneQueryBuilder = new MatchNoneQueryBuilder();

        if (matchNoneQueryProto.hasBoost()) {
            matchNoneQueryBuilder.boost(matchNoneQueryProto.getBoost());
        }

        if (matchNoneQueryProto.hasName()) {
            matchNoneQueryBuilder.queryName(matchNoneQueryProto.getName());
        }

        return matchNoneQueryBuilder;
    }
}
