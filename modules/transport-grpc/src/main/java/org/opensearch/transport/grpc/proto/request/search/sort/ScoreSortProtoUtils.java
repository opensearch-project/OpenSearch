/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.ScoreSort;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for converting ScoreSort Protocol Buffers to OpenSearch ScoreSortBuilder objects.
 * Similar to {@link ScoreSortBuilder#fromXContent}, this class handles the conversion of
 * Protocol Buffer representations to properly configured ScoreSortBuilder objects with
 * score-based sorting and sort order settings.
 *
 * @opensearch.internal
 */
public class ScoreSortProtoUtils {

    private ScoreSortProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer ScoreSort to a ScoreSortBuilder.
     * Similar to {@link ScoreSortBuilder#fromXContent}, this method parses the
     * Protocol Buffer representation and creates a properly configured ScoreSortBuilder
     * with the appropriate sort order settings.
     *
     * @param scoreSort The Protocol Buffer ScoreSort to convert
     * @return A configured ScoreSortBuilder
     * @throws IllegalArgumentException if scoreSort is null
     */
    public static ScoreSortBuilder fromProto(ScoreSort scoreSort) {
        if (scoreSort == null) {
            throw new IllegalArgumentException("ScoreSort cannot be null");
        }

        ScoreSortBuilder builder = new ScoreSortBuilder();

        if (scoreSort.getOrder() != org.opensearch.protobufs.SortOrder.SORT_ORDER_UNSPECIFIED) {
            SortOrder order = SortOrder.fromString(ProtobufEnumUtils.convertToString(scoreSort.getOrder()));
            builder.order(order);
        }

        return builder;
    }
}
