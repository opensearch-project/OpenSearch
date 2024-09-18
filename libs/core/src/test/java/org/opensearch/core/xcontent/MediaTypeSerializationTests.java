/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.xcontent;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class MediaTypeSerializationTests extends OpenSearchTestCase {

    public void testRoundtrip() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            XContentType.JSON.writeTo(output);
            XContentType.SMILE.writeTo(output);
            XContentType.YAML.writeTo(output);
            XContentType.CBOR.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                assertEquals(XContentType.JSON, MediaType.readFrom(in));
                assertEquals(XContentType.SMILE, MediaType.readFrom(in));
                assertEquals(XContentType.YAML, MediaType.readFrom(in));
                assertEquals(XContentType.CBOR, MediaType.readFrom(in));
            }
        }
    }

    public void testHardcodedOrdinals() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeVInt(0);
            output.writeVInt(1);
            output.writeVInt(2);
            output.writeVInt(3);
            try (StreamInput in = output.bytes().streamInput()) {
                assertEquals(XContentType.JSON, MediaType.readFrom(in));
                assertEquals(XContentType.SMILE, MediaType.readFrom(in));
                assertEquals(XContentType.YAML, MediaType.readFrom(in));
                assertEquals(XContentType.CBOR, MediaType.readFrom(in));
            }
        }
    }

    public void testBackwardsCompatibilityWithSerializedEnums() throws IOException {
        // Prior to version 2.10, OpenSearch would serialize XContentType as enums, which
        // writes the ordinal as a VInt. This test ensure the new MediaType.readFrom method is
        // functionally compatible with this previous approach.
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeEnum(XContentType.JSON);
            output.writeEnum(XContentType.SMILE);
            output.writeEnum(XContentType.YAML);
            output.writeEnum(XContentType.CBOR);
            try (StreamInput in = output.bytes().streamInput()) {
                assertEquals(XContentType.JSON, MediaType.readFrom(in));
                assertEquals(XContentType.SMILE, MediaType.readFrom(in));
                assertEquals(XContentType.YAML, MediaType.readFrom(in));
                assertEquals(XContentType.CBOR, MediaType.readFrom(in));
            }
        }
    }

    public void testStringVersion() throws IOException {
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.writeString(XContentType.JSON.mediaType());
            output.writeString(XContentType.SMILE.mediaType());
            output.writeString(XContentType.YAML.mediaType());
            output.writeString(XContentType.CBOR.mediaType());
            try (StreamInput in = output.bytes().streamInput()) {
                in.setVersion(Version.V_2_11_0);
                assertEquals(XContentType.JSON, MediaType.readFrom(in));
                assertEquals(XContentType.SMILE, MediaType.readFrom(in));
                assertEquals(XContentType.YAML, MediaType.readFrom(in));
                assertEquals(XContentType.CBOR, MediaType.readFrom(in));
            }
        }
    }
}
