/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

/**
 * Utility class for converting SearchSourceBuilder Protocol Buffers to objects
 *
 */
public class FieldAndFormatProtoUtils {

    private FieldAndFormatProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Similar to {@link FieldAndFormat#fromXContent(XContentParser)}
     *
     * @param fieldAndFormatProto
     */

    public static FieldAndFormat fromProto(org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto) {

        // TODO how is this field used?
        // fieldAndFormatProto.getIncludeUnmapped();
        return new FieldAndFormat(fieldAndFormatProto.getField(), fieldAndFormatProto.getFormat());
    }
}
