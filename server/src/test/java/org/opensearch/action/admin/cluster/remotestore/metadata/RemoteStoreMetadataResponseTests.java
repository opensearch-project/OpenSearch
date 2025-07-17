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

    private static final String FIELD_INDICES = "indices";
    private static final String FIELD_SHARDS = "_shards";
    private static final String FIELD_TOTAL = "total";
    private static final String FIELD_SUCCESSFUL = "successful";
    private static final String FIELD_FAILED = "failed";

    public void testSerializationForSingleShard() throws Exception {
        RemoteStoreShardMetadata metadata = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreShardMetadata[] { metadata },
            1,
            1,
            0,
            List.of()
        );

        assertSerializationRoundTrip(response);
        assertXContentResponse(response, "test-index", 1);
    }

    public void testSerializationForMultipleShards() throws Exception {
        RemoteStoreShardMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);
        RemoteStoreShardMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 1);
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreShardMetadata[] { metadata1, metadata2 },
            2,
            2,
            0,
            List.of()
        );

        assertSerializationRoundTrip(response);
        assertXContentResponse(response, "test-index", 2);
    }

    public void testSerializationForMultipleIndices() throws Exception {
        RemoteStoreShardMetadata metadata1 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index1", 0);
        RemoteStoreShardMetadata metadata2 = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "index2", 0);
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreShardMetadata[] { metadata1, metadata2 },
            2,
            2,
            0,
            List.of()
        );

        assertSerializationRoundTrip(response);
        assertIndicesInResponse(response, "index1", "index2");
    }

    public void testFailures() throws Exception {
        List<DefaultShardOperationFailedException> failures = new ArrayList<>();
        failures.add(new DefaultShardOperationFailedException("index1", 1, new Exception("test failure")));
        failures.add(new DefaultShardOperationFailedException("index2", 0, new Exception("another failure")));

        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(new RemoteStoreShardMetadata[0], 3, 1, 2, failures);

        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadataResponse deserializedResponse = new RemoteStoreMetadataResponse(in);

        assertEquals("Total shards mismatch", response.getTotalShards(), deserializedResponse.getTotalShards());
        assertEquals("Successful shards mismatch", response.getSuccessfulShards(), deserializedResponse.getSuccessfulShards());
        assertEquals("Failed shards mismatch", response.getFailedShards(), deserializedResponse.getFailedShards());
        assertEquals("Failures count mismatch", response.getShardFailures().length, deserializedResponse.getShardFailures().length);

        for (int i = 0; i < failures.size(); i++) {
            DefaultShardOperationFailedException expected = failures.get(i);
            DefaultShardOperationFailedException actual = deserializedResponse.getShardFailures()[i];
            assertEquals("Index mismatch", expected.index(), actual.index());
            assertEquals("Shard ID mismatch", expected.shardId(), actual.shardId());
            assertTrue("Failure reason mismatch", actual.reason().contains(expected.getCause().getMessage()));
        }
    }

    public void testEmptyResponse() throws Exception {
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(new RemoteStoreShardMetadata[0], 0, 0, 0, List.of());

        assertSerializationRoundTrip(response);
        Map<String, Object> responseMap = convertToMap(response);
        validateEmptyResponse(responseMap);
    }

    private RemoteStoreMetadataResponse assertSerializationRoundTrip(RemoteStoreMetadataResponse response) throws Exception {
        BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadataResponse deserializedResponse = new RemoteStoreMetadataResponse(in);

        assertResponseEquals(response, deserializedResponse);
        return deserializedResponse;
    }

    private void assertXContentResponse(RemoteStoreMetadataResponse response, String indexName, int expectedShards) throws Exception {
        Map<String, Object> responseMap = convertToMap(response);
        validateResponseContent(responseMap, indexName, expectedShards);
    }

    @SuppressWarnings("unchecked")
    private void assertIndicesInResponse(RemoteStoreMetadataResponse response, String... expectedIndices) throws Exception {
        Map<String, Object> responseMap = convertToMap(response);
        Map<String, Object> indices = (Map<String, Object>) responseMap.get(FIELD_INDICES);

        for (String index : expectedIndices) {
            assertTrue("Missing index: " + index, indices.containsKey(index));
        }
        validateIndicesContent(indices);
    }

    private Map<String, Object> convertToMap(RemoteStoreMetadataResponse response) throws Exception {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, EMPTY_PARAMS);
        return XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
    }

    private void assertResponseEquals(RemoteStoreMetadataResponse expected, RemoteStoreMetadataResponse actual) {
        assertEquals("Total shards mismatch", expected.getTotalShards(), actual.getTotalShards());
        assertEquals("Successful shards mismatch", expected.getSuccessfulShards(), actual.getSuccessfulShards());
        assertEquals("Failed shards mismatch", expected.getFailedShards(), actual.getFailedShards());
        assertEquals("Failures count mismatch", expected.getShardFailures().length, actual.getShardFailures().length);

        Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> expectedGrouped = expected.groupByIndexAndShards();
        Map<String, Map<Integer, List<RemoteStoreShardMetadata>>> actualGrouped = actual.groupByIndexAndShards();

        assertEquals("Index set mismatch", expectedGrouped.keySet(), actualGrouped.keySet());

        for (String index : expectedGrouped.keySet()) {
            Map<Integer, List<RemoteStoreShardMetadata>> expectedShards = expectedGrouped.get(index);
            Map<Integer, List<RemoteStoreShardMetadata>> actualShards = actualGrouped.get(index);
            assertEquals("Shard set mismatch for index " + index, expectedShards.keySet(), actualShards.keySet());

            for (Integer shardId : expectedShards.keySet()) {
                assertMetadataListEquals(expectedShards.get(shardId), actualShards.get(shardId));
            }
        }
    }

    private void assertMetadataListEquals(List<RemoteStoreShardMetadata> expected, List<RemoteStoreShardMetadata> actual) {
        assertEquals("Metadata list size mismatch", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            RemoteStoreShardMetadata expectedMeta = expected.get(i);
            RemoteStoreShardMetadata actualMeta = actual.get(i);
            assertEquals("Index name mismatch", expectedMeta.getIndexName(), actualMeta.getIndexName());
            assertEquals("Shard ID mismatch", expectedMeta.getShardId(), actualMeta.getShardId());
            assertEquals(
                "Latest segment filename mismatch",
                expectedMeta.getLatestSegmentMetadataFileName(),
                actualMeta.getLatestSegmentMetadataFileName()
            );
            assertEquals(
                "Latest translog filename mismatch",
                expectedMeta.getLatestTranslogMetadataFileName(),
                actualMeta.getLatestTranslogMetadataFileName()
            );
        }
    }

    @SuppressWarnings("unchecked")
    private void validateResponseContent(Map<String, Object> responseMap, String indexName, int expectedShards) {
        Map<String, Object> shardsObject = (Map<String, Object>) responseMap.get(FIELD_SHARDS);
        assertEquals("Total shards mismatch", expectedShards, shardsObject.get(FIELD_TOTAL));
        assertEquals("Successful shards mismatch", expectedShards, shardsObject.get(FIELD_SUCCESSFUL));
        assertEquals("Failed shards mismatch", 0, shardsObject.get(FIELD_FAILED));

        Map<String, Object> indicesObject = (Map<String, Object>) responseMap.get(FIELD_INDICES);
        assertTrue("Missing index: " + indexName, indicesObject.containsKey(indexName));

        validateIndicesContent(indicesObject);
    }

    @SuppressWarnings("unchecked")
    private void validateIndicesContent(Map<String, Object> indicesObject) {
        for (Object indexData : indicesObject.values()) {
            Map<String, Object> indexMap = (Map<String, Object>) indexData;
            assertTrue("Missing shards section", indexMap.containsKey("shards"));

            Map<String, Object> shardsMap = (Map<String, Object>) indexMap.get("shards");
            for (Object shardData : shardsMap.values()) {
                List<Map<String, Object>> shardList = (List<Map<String, Object>>) shardData;
                for (Map<String, Object> shard : shardList) {
                    validateShardContent(shard);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void validateShardContent(Map<String, Object> shard) {
        assertTrue("Missing index field", shard.containsKey("index"));
        assertTrue("Missing shard field", shard.containsKey("shard"));
        assertTrue("Missing latest segment metadata filename", shard.containsKey("latest_segment_metadata_filename"));
        assertTrue("Missing latest translog metadata filename", shard.containsKey("latest_translog_metadata_filename"));
        assertTrue("Missing segment metadata files", shard.containsKey("available_segment_metadata_files"));
        assertTrue("Missing translog metadata files", shard.containsKey("available_translog_metadata_files"));

        Map<String, Object> segmentFiles = (Map<String, Object>) shard.get("available_segment_metadata_files");
        Map<String, Object> translogFiles = (Map<String, Object>) shard.get("available_translog_metadata_files");

        assertNotNull("Segment files should not be null", segmentFiles);
        assertFalse("Segment files should not be empty", segmentFiles.isEmpty());
        assertNotNull("Translog files should not be null", translogFiles);
        assertFalse("Translog files should not be empty", translogFiles.isEmpty());
    }

    @SuppressWarnings("unchecked")
    private void validateEmptyResponse(Map<String, Object> responseMap) {
        Map<String, Object> shardsObject = (Map<String, Object>) responseMap.get(FIELD_SHARDS);
        assertEquals("Total shards should be 0", 0, shardsObject.get(FIELD_TOTAL));
        assertEquals("Successful shards should be 0", 0, shardsObject.get(FIELD_SUCCESSFUL));
        assertEquals("Failed shards should be 0", 0, shardsObject.get(FIELD_FAILED));

        Map<String, Object> indicesObject = (Map<String, Object>) responseMap.get(FIELD_INDICES);
        assertTrue("Indices should be empty", indicesObject.isEmpty());
    }

    public void testToStringMethod() throws Exception {
        RemoteStoreShardMetadata metadata = createTestMetadata(createTestSegmentMetadata(), createTestTranslogMetadata(), "test-index", 0);
        RemoteStoreMetadataResponse response = new RemoteStoreMetadataResponse(
            new RemoteStoreShardMetadata[] { metadata },
            1,
            1,
            0,
            List.of()
        );

        String json = response.toString();
        assertNotNull("JSON string should not be null", json);
        assertTrue("JSON string should contain index name", json.contains("test-index"));
        assertTrue("JSON string should contain shard id", json.contains("\"0\""));
    }
}
