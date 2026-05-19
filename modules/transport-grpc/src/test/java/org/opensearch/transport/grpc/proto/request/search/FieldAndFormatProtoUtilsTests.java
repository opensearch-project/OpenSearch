/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.search.fetch.subphase.FieldAndFormat;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for FieldAndFormatProtoUtils.
 */
public class FieldAndFormatProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProto_WithFieldOnly() {
        org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto = org.opensearch.protobufs.FieldAndFormat.newBuilder()
            .setField("test_field")
            .build();

        FieldAndFormat result = FieldAndFormatProtoUtils.fromProto(fieldAndFormatProto);

        assertNotNull(result);
        assertEquals("test_field", result.field);
        assertNull(result.format);
    }

    public void testFromProto_WithFieldAndFormat() {
        org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto = org.opensearch.protobufs.FieldAndFormat.newBuilder()
            .setField("date_field")
            .setFormat("yyyy-MM-dd")
            .build();

        FieldAndFormat result = FieldAndFormatProtoUtils.fromProto(fieldAndFormatProto);

        assertNotNull(result);
        assertEquals("date_field", result.field);
        assertEquals("yyyy-MM-dd", result.format);
    }

    public void testFromProto_WithEmptyFieldName() {
        org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto = org.opensearch.protobufs.FieldAndFormat.newBuilder()
            .setField("")
            .setFormat("yyyy-MM-dd")
            .build();

        try {
            FieldAndFormatProtoUtils.fromProto(fieldAndFormatProto);
            fail("Should have thrown IllegalArgumentException for empty field name");
        } catch (IllegalArgumentException e) {
            assertEquals("Field name cannot be null or empty", e.getMessage());
        }
    }

    public void testFromProto_WithNullProtobuf() {
        try {
            FieldAndFormatProtoUtils.fromProto(null);
            fail("Should have thrown IllegalArgumentException for null protobuf");
        } catch (IllegalArgumentException e) {
            assertEquals("FieldAndFormat protobuf cannot be null", e.getMessage());
        }
    }

    public void testFromProto_WithComplexFormat() {
        org.opensearch.protobufs.FieldAndFormat fieldAndFormatProto = org.opensearch.protobufs.FieldAndFormat.newBuilder()
            .setField("timestamp")
            .setFormat("epoch_millis")
            .build();

        FieldAndFormat result = FieldAndFormatProtoUtils.fromProto(fieldAndFormatProto);

        assertNotNull(result);
        assertEquals("timestamp", result.field);
        assertEquals("epoch_millis", result.format);
    }
}
