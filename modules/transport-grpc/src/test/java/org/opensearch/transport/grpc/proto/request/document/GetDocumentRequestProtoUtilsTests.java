/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document;

import org.opensearch.action.get.GetRequest;
import org.opensearch.protobufs.GetDocumentRequest;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Comprehensive unit tests for GetDocumentRequestProtoUtils.
 */
public class GetDocumentRequestProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoBasicFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", getRequest.index());
        assertEquals("test-id", getRequest.id());
    }

    public void testFromProtoWithAllFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("test-routing")
            .setPreference("_local")
            .setRealtime(false)
            .setRefresh(true)
            .addXSourceIncludes("field1")
            .addXSourceIncludes("field2")
            .addXSourceExcludes("field3")
            .addXSourceExcludes("field4")
            .addStoredFields("stored1")
            .addStoredFields("stored2")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("test-index", getRequest.index());
        assertEquals("test-id", getRequest.id());
        assertEquals("test-routing", getRequest.routing());
        assertEquals("_local", getRequest.preference());
        assertFalse(getRequest.realtime());
        assertTrue(getRequest.refresh());

        // Test source context
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();
        assertNotNull(sourceContext);
        assertArrayEquals(new String[] { "field1", "field2" }, sourceContext.includes());
        assertArrayEquals(new String[] { "field3", "field4" }, sourceContext.excludes());

        // Test stored fields
        assertArrayEquals(new String[] { "stored1", "stored2" }, getRequest.storedFields());
    }

    public void testFromProtoMissingIndex() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GetDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoMissingId() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GetDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document ID is required for get operations", exception.getMessage());
    }

    public void testFromProtoEmptyIndex() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("").setId("test-id").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GetDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Index name is required", exception.getMessage());
    }

    public void testFromProtoEmptyId() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").setId("").build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> GetDocumentRequestProtoUtils.fromProto(protoRequest)
        );
        assertEquals("Document ID is required for get operations", exception.getMessage());
    }

    public void testSourceContextIncludes() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("title")
            .addXSourceIncludes("content")
            .addXSourceIncludes("metadata.*")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();

        assertNotNull(sourceContext);
        assertArrayEquals(new String[] { "title", "content", "metadata.*" }, sourceContext.includes());
        assertNull(sourceContext.excludes());
        assertTrue(sourceContext.fetchSource());
    }

    public void testSourceContextExcludes() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceExcludes("password")
            .addXSourceExcludes("internal.*")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();

        assertNotNull(sourceContext);
        assertNull(sourceContext.includes());
        assertArrayEquals(new String[] { "password", "internal.*" }, sourceContext.excludes());
        assertTrue(sourceContext.fetchSource());
    }

    public void testSourceContextIncludesAndExcludes() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("user.*")
            .addXSourceIncludes("metadata")
            .addXSourceExcludes("user.password")
            .addXSourceExcludes("user.secret")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();

        assertNotNull(sourceContext);
        assertArrayEquals(new String[] { "user.*", "metadata" }, sourceContext.includes());
        assertArrayEquals(new String[] { "user.password", "user.secret" }, sourceContext.excludes());
        assertTrue(sourceContext.fetchSource());
    }

    public void testEmptySourceContext() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();

        // Should be null when no source filtering is specified
        assertNull(sourceContext);
    }

    public void testStoredFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addStoredFields("field1")
            .addStoredFields("field2")
            .addStoredFields("field3")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertArrayEquals(new String[] { "field1", "field2", "field3" }, getRequest.storedFields());
    }

    public void testEmptyStoredFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertNull(getRequest.storedFields());
    }

    public void testDefaultValues() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        // Test default values
        assertNull(getRequest.routing());
        assertNull(getRequest.preference());
        assertTrue(getRequest.realtime()); // Default is true
        assertFalse(getRequest.refresh()); // Default is false
        assertNull(getRequest.fetchSourceContext());
        assertNull(getRequest.storedFields());
    }

    public void testRealtimeAndRefreshCombinations() {
        // Test realtime=true, refresh=false (default)
        GetDocumentRequest protoRequest1 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRealtime(true)
            .setRefresh(false)
            .build();
        GetRequest getRequest1 = GetDocumentRequestProtoUtils.fromProto(protoRequest1);
        assertTrue(getRequest1.realtime());
        assertFalse(getRequest1.refresh());

        // Test realtime=false, refresh=true
        GetDocumentRequest protoRequest2 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRealtime(false)
            .setRefresh(true)
            .build();
        GetRequest getRequest2 = GetDocumentRequestProtoUtils.fromProto(protoRequest2);
        assertFalse(getRequest2.realtime());
        assertTrue(getRequest2.refresh());

        // Test realtime=false, refresh=false
        GetDocumentRequest protoRequest3 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRealtime(false)
            .setRefresh(false)
            .build();
        GetRequest getRequest3 = GetDocumentRequestProtoUtils.fromProto(protoRequest3);
        assertFalse(getRequest3.realtime());
        assertFalse(getRequest3.refresh());
    }

    public void testPreferenceValues() {
        // Test _local preference
        GetDocumentRequest protoRequest1 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setPreference("_local")
            .build();
        assertEquals("_local", GetDocumentRequestProtoUtils.fromProto(protoRequest1).preference());

        // Test _primary preference
        GetDocumentRequest protoRequest2 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setPreference("_primary")
            .build();
        assertEquals("_primary", GetDocumentRequestProtoUtils.fromProto(protoRequest2).preference());

        // Test custom node preference
        GetDocumentRequest protoRequest3 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setPreference("node1,node2")
            .build();
        assertEquals("node1,node2", GetDocumentRequestProtoUtils.fromProto(protoRequest3).preference());

        // Test empty preference (should be treated as null)
        GetDocumentRequest protoRequest4 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setPreference("")
            .build();
        assertNull(GetDocumentRequestProtoUtils.fromProto(protoRequest4).preference());
    }

    public void testRoutingValues() {
        // Test with routing
        GetDocumentRequest protoRequest1 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .setRouting("user123")
            .build();
        assertEquals("user123", GetDocumentRequestProtoUtils.fromProto(protoRequest1).routing());

        // Test with empty routing (should be treated as null)
        GetDocumentRequest protoRequest2 = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").setRouting("").build();
        assertNull(GetDocumentRequestProtoUtils.fromProto(protoRequest2).routing());

        // Test without routing
        GetDocumentRequest protoRequest3 = GetDocumentRequest.newBuilder().setIndex("test-index").setId("test-id").build();
        assertNull(GetDocumentRequestProtoUtils.fromProto(protoRequest3).routing());
    }

    public void testSpecialCharactersInFields() {
        // Test with special characters in index name
        GetDocumentRequest protoRequest1 = GetDocumentRequest.newBuilder()
            .setIndex("test-index-with-dashes_and_underscores.and.dots")
            .setId("test-id")
            .build();
        assertEquals("test-index-with-dashes_and_underscores.and.dots", GetDocumentRequestProtoUtils.fromProto(protoRequest1).index());

        // Test with special characters in ID
        GetDocumentRequest protoRequest2 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test:id/with\\special@characters#and$symbols%")
            .build();
        assertEquals("test:id/with\\special@characters#and$symbols%", GetDocumentRequestProtoUtils.fromProto(protoRequest2).id());

        // Test with Unicode characters
        GetDocumentRequest protoRequest3 = GetDocumentRequest.newBuilder()
            .setIndex("测试索引")
            .setId("测试文档ID")
            .setRouting("用户路由")
            .setPreference("节点偏好")
            .build();
        GetRequest getRequest3 = GetDocumentRequestProtoUtils.fromProto(protoRequest3);
        assertEquals("测试索引", getRequest3.index());
        assertEquals("测试文档ID", getRequest3.id());
        assertEquals("用户路由", getRequest3.routing());
        assertEquals("节点偏好", getRequest3.preference());
    }

    public void testSourceFieldPatterns() {
        // Test wildcard patterns
        GetDocumentRequest protoRequest1 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("user.*")
            .addXSourceIncludes("metadata.*.public")
            .addXSourceExcludes("*.password")
            .addXSourceExcludes("*.secret")
            .build();

        GetRequest getRequest1 = GetDocumentRequestProtoUtils.fromProto(protoRequest1);
        FetchSourceContext sourceContext1 = getRequest1.fetchSourceContext();

        assertArrayEquals(new String[] { "user.*", "metadata.*.public" }, sourceContext1.includes());
        assertArrayEquals(new String[] { "*.password", "*.secret" }, sourceContext1.excludes());

        // Test nested field patterns
        GetDocumentRequest protoRequest2 = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("user.profile.name")
            .addXSourceIncludes("user.profile.email")
            .addXSourceExcludes("user.profile.internal")
            .build();

        GetRequest getRequest2 = GetDocumentRequestProtoUtils.fromProto(protoRequest2);
        FetchSourceContext sourceContext2 = getRequest2.fetchSourceContext();

        assertArrayEquals(new String[] { "user.profile.name", "user.profile.email" }, sourceContext2.includes());
        assertArrayEquals(new String[] { "user.profile.internal" }, sourceContext2.excludes());
    }

    public void testDuplicateSourceFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addXSourceIncludes("field1")
            .addXSourceIncludes("field2")
            .addXSourceIncludes("field1") // Duplicate
            .addXSourceExcludes("field3")
            .addXSourceExcludes("field3") // Duplicate
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        FetchSourceContext sourceContext = getRequest.fetchSourceContext();

        // Should preserve duplicates as they come from protobuf
        assertArrayEquals(new String[] { "field1", "field2", "field1" }, sourceContext.includes());
        assertArrayEquals(new String[] { "field3", "field3" }, sourceContext.excludes());
    }

    public void testDuplicateStoredFields() {
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("test-index")
            .setId("test-id")
            .addStoredFields("field1")
            .addStoredFields("field2")
            .addStoredFields("field1") // Duplicate
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        // Should preserve duplicates as they come from protobuf
        assertArrayEquals(new String[] { "field1", "field2", "field1" }, getRequest.storedFields());
    }

    public void testComplexScenarios() {
        // Test with all optional fields set to non-default values
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("complex-test-index")
            .setId("complex-test-id")
            .setRouting("shard-routing-key")
            .setPreference("_primary_first")
            .setRealtime(false)
            .setRefresh(true)
            .addXSourceIncludes("user.*")
            .addXSourceIncludes("metadata.public.*")
            .addXSourceExcludes("user.password")
            .addXSourceExcludes("user.secret_key")
            .addXSourceExcludes("metadata.private.*")
            .addStoredFields("timestamp")
            .addStoredFields("version")
            .addStoredFields("checksum")
            .build();

        GetRequest getRequest = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals("complex-test-index", getRequest.index());
        assertEquals("complex-test-id", getRequest.id());
        assertEquals("shard-routing-key", getRequest.routing());
        assertEquals("_primary_first", getRequest.preference());
        assertFalse(getRequest.realtime());
        assertTrue(getRequest.refresh());

        FetchSourceContext sourceContext = getRequest.fetchSourceContext();
        assertArrayEquals(new String[] { "user.*", "metadata.public.*" }, sourceContext.includes());
        assertArrayEquals(new String[] { "user.password", "user.secret_key", "metadata.private.*" }, sourceContext.excludes());

        assertArrayEquals(new String[] { "timestamp", "version", "checksum" }, getRequest.storedFields());
    }

    public void testRequestConsistency() {
        // Test that multiple conversions of the same proto request yield identical results
        GetDocumentRequest protoRequest = GetDocumentRequest.newBuilder()
            .setIndex("consistency-test")
            .setId("doc-123")
            .setRouting("routing-key")
            .setPreference("_local")
            .setRealtime(false)
            .setRefresh(true)
            .addXSourceIncludes("field1")
            .addXSourceExcludes("field2")
            .addStoredFields("stored1")
            .build();

        GetRequest getRequest1 = GetDocumentRequestProtoUtils.fromProto(protoRequest);
        GetRequest getRequest2 = GetDocumentRequestProtoUtils.fromProto(protoRequest);

        assertEquals(getRequest1.index(), getRequest2.index());
        assertEquals(getRequest1.id(), getRequest2.id());
        assertEquals(getRequest1.routing(), getRequest2.routing());
        assertEquals(getRequest1.preference(), getRequest2.preference());
        assertEquals(getRequest1.realtime(), getRequest2.realtime());
        assertEquals(getRequest1.refresh(), getRequest2.refresh());

        // Compare source contexts
        FetchSourceContext sourceContext1 = getRequest1.fetchSourceContext();
        FetchSourceContext sourceContext2 = getRequest2.fetchSourceContext();
        if (sourceContext1 != null && sourceContext2 != null) {
            assertArrayEquals(sourceContext1.includes(), sourceContext2.includes());
            assertArrayEquals(sourceContext1.excludes(), sourceContext2.excludes());
        } else {
            assertEquals(sourceContext1, sourceContext2);
        }

        assertArrayEquals(getRequest1.storedFields(), getRequest2.storedFields());
    }
}
