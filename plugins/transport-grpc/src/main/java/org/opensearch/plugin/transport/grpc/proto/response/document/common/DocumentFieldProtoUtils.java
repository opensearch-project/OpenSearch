/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.document.common;

import com.google.protobuf.Value;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugin.transport.grpc.proto.response.common.StructProtoUtils;

import java.util.List;

/**
 * Utility class for converting DocumentField objects to Protocol Buffers.
 * This class handles the conversion of document get operation results to their
 * Protocol Buffer representation.
 */
public class DocumentFieldProtoUtils {

    private DocumentFieldProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a DocumentField values (list of objects) to its Protocol Buffer representation.
     * This method is equivalent to the  {@link DocumentField#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param fieldValues The list of DocumentField values to convert
     * @return A Protobuf Value representation
     */
    public static Value toProto(List<Object> fieldValues) {
        return StructProtoUtils.toProto(fieldValues);
    }

    /**
     * Converts a DocumentField value (object) to its Protocol Buffer representation.
     * This method is equivalent to the  {@link DocumentField#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param fieldValue The DocumentField value to convert
     * @return A Protobuf Value representation
     */
    public static Value toProto(Object fieldValue) {
        return StructProtoUtils.toProto(fieldValue);
    }

}
