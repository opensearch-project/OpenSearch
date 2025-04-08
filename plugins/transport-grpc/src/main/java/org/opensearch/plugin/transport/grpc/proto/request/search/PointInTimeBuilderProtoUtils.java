/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.PointInTimeReference;
import org.opensearch.search.builder.PointInTimeBuilder;

import static org.opensearch.search.builder.SearchSourceBuilder.POINT_IN_TIME;

/**
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class PointInTimeBuilderProtoUtils {

    private PointInTimeBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link PointInTimeBuilder#fromXContent(XContentParser)}
     *
     * @param pointInTime
     */

    public static PointInTimeBuilder fromProto(PointInTimeReference pointInTime) {

        TimeValue keepAlive = TimeValue.parseTimeValue(pointInTime.getKeepAlive(), null, POINT_IN_TIME.getPreferredName());
        return new PointInTimeBuilder(pointInTime.getId()).setKeepAlive(keepAlive);
    }

}
