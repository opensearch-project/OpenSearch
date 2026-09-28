/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.protobufs.SlicedScroll;
import org.opensearch.search.slice.SliceBuilder;

/**
 * Utility class for converting Highlight Protocol Buffers to objects
 *
 */
public class SliceBuilderProtoUtils {

    private SliceBuilderProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link SliceBuilder#fromXContent(XContentParser)}
     *
     * @param sliceProto
     */

    protected static SliceBuilder fromProto(SlicedScroll sliceProto) {
        SliceBuilder sliceBuilder;
        if (sliceProto.hasField()) {
            sliceBuilder = new SliceBuilder(sliceProto.getField(), sliceProto.getId(), sliceProto.getMax());
        } else {
            sliceBuilder = new SliceBuilder(sliceProto.getId(), sliceProto.getMax());
        }

        return sliceBuilder;
    }
}
