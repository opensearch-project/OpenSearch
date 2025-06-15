/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.remotestore.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataTestHelper.createTestMetadata;
import static org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataTestHelper.createTestSegmentMetadata;
import static org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataTestHelper.createTestTranslogMetadata;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreMetadataResponseTests extends OpenSearchTestCase {

    public void testSerializationForSingleShard() throws Exception {
        RemoteStoreMetadata metadata = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(new RemoteStoreMetadata[] { metadata }, 1, 1, 0, List.of());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        validateResponseContent(jsonResponseObject, "test-index", 1);
    }

    public void testSerializationForMultipleShards() throws Exception {
        RemoteStoreMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);
        RemoteStoreMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 1);

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreMetadata[] { metadata1, metadata2 },
            2,
            2,
            0,
            List.of()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        validateResponseContent(jsonResponseObject, "test-index", 2);
    }

    @SuppressWarnings("unchecked")
    public void testSerializationForMultipleIndices() throws Exception {
        RemoteStoreMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index1", 0);
        RemoteStoreMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index2", 0);

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreMetadata[] { metadata1, metadata2 },
            2,
            2,
            0,
            List.of()
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> jsonResponseObject = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType())
            .v2();

        Map<String, Object> indicesObject = (Map<String, Object>) jsonResponseObject.get("indices");
        assertTrue(indicesObject.containsKey("index1"));
        assertTrue(indicesObject.containsKey("index2"));
    }

    public void testSerialization() throws Exception {
        RemoteStoreMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index1", 0);
        RemoteStoreMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index2", 1);

        List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        failures.add(new DefaultShardOperationFailedException("index1", 1, new Exception("test failure")));

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreMetadata[] { metadata1, metadata2 },
            3,
            2,
            1,
            failures
        );

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadataResponse deserializedResponse = new RemoteStoreMetadataResponse(in);

        assertEquals(3, deserializedResponse.getTotalShards());
        assertEquals(2, deserializedResponse.getSuccessfulShards());
        assertEquals(1, deserializedResponse.getFailedShards());
        assertEquals(1, deserializedResponse.getShardFailures().length);

        Map<String, Map<Integer, List<RemoteStoreMetadata>>> groupedMetadata = deserializedResponse.groupByIndexAndShards();
        assertTrue(groupedMetadata.containsKey("index1"));
        assertTrue(groupedMetadata.containsKey("index2"));
    }

    public void testToString() throws Exception {
        RemoteStoreMetadata metadata = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(new RemoteStoreMetadata[] { metadata }, 1, 1, 0, List.of());

        String responseString = response.toString();
        assertNotNull(responseString);
        assertTrue(responseString.contains("test-index"));
        assertTrue(responseString.contains("shards"));
        assertTrue(responseString.contains("indices"));
    }

    public void testGroupByIndexAndShards() {
        RemoteStoreMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index1", 0);
        RemoteStoreMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index1", 1);
        RemoteStoreMetadata metadata3 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index2", 0);

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreMetadata[] { metadata1, metadata2, metadata3 },
            3,
            3,
            0,
            List.of()
        );

        Map<String, Map<Integer, List<RemoteStoreMetadata>>> grouped = response.groupByIndexAndShards();

        assertEquals(2, grouped.get("index1").size());
        assertTrue(grouped.get("index1").containsKey(0));
        assertTrue(grouped.get("index1").containsKey(1));

        assertEquals(1, grouped.get("index2").size());
        assertTrue(grouped.get("index2").containsKey(0));
        assertEquals(1, grouped.get("index2").get(0).size());
    }

    public void testEmptyResponse() throws Exception {
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(new RemoteStoreMetadata[0], 0, 0, 0, List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadataResponse deserializedResponse = new RemoteStoreMetadataResponse(in);

        assertEquals(0, deserializedResponse.getTotalShards());
        assertTrue(deserializedResponse.groupByIndexAndShards().isEmpty());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> responseMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertTrue(((Map<?, ?>) responseMap.get("indices")).isEmpty());
    }

    @SuppressWarnings("unchecked")
    private void validateResponseContent(Map<String, Object> responseMap, String indexName, int expectedShards) {
        Map<String, Object> metadataShardsObject = (Map<String, Object>) responseMap.get("_shards");
        assertEquals(expectedShards, metadataShardsObject.get("total"));
        assertEquals(expectedShards, metadataShardsObject.get("successful"));
        assertEquals(0, metadataShardsObject.get("failed"));

        Map<String, Object> indicesObject = (Map<String, Object>) responseMap.get("indices");
        assertTrue(indicesObject.containsKey(indexName));

        Map<String, Object> indexData = (Map<String, Object>) indicesObject.get(indexName);
        assertNotNull(indexData.get("shards"));
    }
}
