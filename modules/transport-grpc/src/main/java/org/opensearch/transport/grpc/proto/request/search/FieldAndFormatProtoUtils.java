/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.fetch.subphase.FieldAndFormat;

/**
 * Utility class for converting FieldAndFormat Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of field and format
 * specifications into their corresponding OpenSearch FieldAndFormat implementations for search operations.
 */
public class FieldAndFormatProtoUtils {

    private FieldAndFormatProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer FieldAndFormat to an OpenSearch FieldAndFormat object.
     * Similar to {@link FieldAndFormat#fromXContent(XContentParser)}, this method
     * parses the Protocol Buffer representation and creates a properly configured
     * FieldAndFormat with the appropriate field name and format settings.
     *
     * @param fieldAndFormatProto The Protocol Buffer FieldAndFormat to convert
     * @return A configured FieldAndFormat instance
     */
    public static FieldAndFormat fromProto(org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto) {
        if (fieldAndFormatProto == null) {
            throw new IllegalArgumentException("FieldAndFormat protobuf cannot be null");
        }

        String fieldName = fieldAndFormatProto.getField();
        if (fieldName == null || fieldName.trim().isEmpty()) {
            throw new IllegalArgumentException("Field name cannot be null or empty");
        }
        String format = fieldAndFormatProto.hasFormat() ? fieldAndFormatProto.getFormat() : null;

        return new FieldAndFormat(fieldName, format);
    }

}
