/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.PointInTimeReference;
import org.opensearch.search.builder.PointInTimeBuilder;

import static org.opensearch.search.builder.SearchSourceBuilder.POINT_IN_TIME;

/**
 * Utility class for converting PointInTimeBuilder Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of point-in-time
 * references into their corresponding OpenSearch PointInTimeBuilder implementations for
 * search operations with consistent snapshots.
 */
public class PointInTimeBuilderProtoUtils {

    private PointInTimeBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer PointInTimeReference to an OpenSearch PointInTimeBuilder.
     * Similar to {@link PointInTimeBuilder#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * PointInTimeBuilder with the appropriate ID and keep-alive settings.
     *
     * @param pointInTime The Protocol Buffer PointInTimeReference to convert
     * @return A configured PointInTimeBuilder instance
     */
    protected static PointInTimeBuilder fromProto(PointInTimeReference pointInTime) {

        TimeValue keepAlive = TimeValue.parseTimeValue(pointInTime.getKeepAlive(), null, POINT_IN_TIME.getPreferredName());
        return new PointInTimeBuilder(pointInTime.getId()).setKeepAlive(keepAlive);
    }

}
