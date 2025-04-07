/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.transport.grpc.proto.response.document.get;

import com.google.protobuf.ByteString;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.protobufs.InlineGetDictUserDefined;
import org.opensearch.protobufs.ResponseItem;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class GetResultProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithExistingDocument() throws IOException {
        // Create a GetResult for an existing document
        String index = "test-index";
        String id = "test-id";
        long version = 1;
        long seqNo = 2;
        long primaryTerm = 3;
        byte[] sourceBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        BytesReference source = new BytesArray(sourceBytes);

        GetResult getResult = new GetResult(
            index,
            id,
            seqNo,
            primaryTerm,
            version,
            true,
            source,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        // Convert to Protocol Buffer
        ResponseItem.Builder responseItemBuilder = ResponseItem.newBuilder();
        ResponseItem.Builder result = GetResultProtoUtils.toProto(getResult, responseItemBuilder);

        // Verify the conversion
        assertEquals("Should have the correct index", index, result.getIndex());
        assertEquals("Should have the correct id", id, result.getId().getString());
        assertEquals("Should have the correct version", version, result.getVersion());

        InlineGetDictUserDefined get = result.getGet();
        assertTrue("Should be found", get.getFound());
        assertEquals("Should have the correct sequence number", seqNo, get.getSeqNo());
        assertEquals("Should have the correct primary term", primaryTerm, get.getPrimaryTerm());
        assertEquals("Should have the correct source", ByteString.copyFrom(sourceBytes), get.getSource());
    }

    public void testToProtoWithNonExistingDocument() throws IOException {
        // Create a GetResult for a non-existing document
        String index = "test-index";
        String id = "test-id";

        GetResult getResult = new GetResult(
            index,
            id,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            -1,
            false,
            null,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        // Convert to Protocol Buffer
        ResponseItem.Builder responseItemBuilder = ResponseItem.newBuilder();
        ResponseItem.Builder result = GetResultProtoUtils.toProto(getResult, responseItemBuilder);

        // Verify the conversion
        assertEquals("Should have the correct index", index, result.getIndex());
        assertEquals("Should have the correct id", id, result.getId().getString());
        assertFalse("Should not be found", result.getGet().getFound());
    }

    public void testToProtoEmbeddedWithSequenceNumber() throws IOException {
        // Create a GetResult with sequence number and primary term
        String index = "test-index";
        String id = "test-id";
        long seqNo = 2;
        long primaryTerm = 3;
        byte[] sourceBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        BytesReference source = new BytesArray(sourceBytes);

        GetResult getResult = new GetResult(index, id, seqNo, primaryTerm, 1, true, source, Collections.emptyMap(), Collections.emptyMap());

        // Convert to Protocol Buffer
        InlineGetDictUserDefined.Builder builder = InlineGetDictUserDefined.newBuilder();
        InlineGetDictUserDefined.Builder result = GetResultProtoUtils.toProtoEmbedded(getResult, builder);

        // Verify the conversion
        assertTrue("Should be found", result.getFound());
        assertEquals("Should have the correct sequence number", seqNo, result.getSeqNo());
        assertEquals("Should have the correct primary term", primaryTerm, result.getPrimaryTerm());
        assertEquals("Should have the correct source", ByteString.copyFrom(sourceBytes), result.getSource());
    }

    public void testToProtoEmbeddedWithoutSequenceNumber() throws IOException {
        // Create a GetResult without sequence number and primary term
        String index = "test-index";
        String id = "test-id";
        byte[] sourceBytes = "{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8);
        BytesReference source = new BytesArray(sourceBytes);

        GetResult getResult = new GetResult(
            index,
            id,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            1,
            true,
            source,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        // Convert to Protocol Buffer
        InlineGetDictUserDefined.Builder builder = InlineGetDictUserDefined.newBuilder();
        InlineGetDictUserDefined.Builder result = GetResultProtoUtils.toProtoEmbedded(getResult, builder);

        // Verify the conversion
        assertTrue("Should be found", result.getFound());
        assertEquals("Should have the correct source", ByteString.copyFrom(source.toBytesRef().bytes), result.getSource());

        // Sequence number and primary term should not be set
        assertFalse("Should not have sequence number", result.hasSeqNo());
        assertFalse("Should not have primary term", result.hasPrimaryTerm());
    }

    public void testToProtoEmbeddedWithoutSource() throws IOException {
        // Create a GetResult without source
        String index = "test-index";
        String id = "test-id";

        GetResult getResult = new GetResult(
            index,
            id,
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            1,
            true,
            null,
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        // Convert to Protocol Buffer
        InlineGetDictUserDefined.Builder builder = InlineGetDictUserDefined.newBuilder();
        InlineGetDictUserDefined.Builder result = GetResultProtoUtils.toProtoEmbedded(getResult, builder);

        // Verify the conversion
        assertTrue("Should be found", result.getFound());

        // Source should not be set
        assertFalse("Should not have source", result.hasSource());
    }
}
