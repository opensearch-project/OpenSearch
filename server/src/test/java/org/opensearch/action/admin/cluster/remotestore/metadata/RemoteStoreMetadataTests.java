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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;

public class RemoteStoreMetadataTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        Map<String, Object> segmentMetadata = RemoteStoreMetadataTestHelper.createTestSegmentMetadata();
        Map<String, Object> translogMetadata = RemoteStoreMetadataTestHelper.createTestTranslogMetadata();
        RemoteStoreShardMetadata metadata = RemoteStoreMetadataTestHelper.createTestMetadata(
            segmentMetadata,
            translogMetadata,
            "test-index",
            0
        );

        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        RemoteStoreShardMetadata deserializedMetadata = new RemoteStoreShardMetadata(in);

        assertEquals(metadata.getIndexName(), deserializedMetadata.getIndexName());
        assertEquals(metadata.getShardId(), deserializedMetadata.getShardId());
        assertEquals(metadata.getLatestSegmentMetadataFileName(), deserializedMetadata.getLatestSegmentMetadataFileName());
        assertEquals(metadata.getLatestTranslogMetadataFileName(), deserializedMetadata.getLatestTranslogMetadataFileName());
        assertNestedMapEquals(metadata.getSegmentMetadataFiles(), deserializedMetadata.getSegmentMetadataFiles());
        assertNestedMapEquals(metadata.getTranslogMetadataFiles(), deserializedMetadata.getTranslogMetadataFiles());
    }

    @SuppressWarnings("unchecked")
    public void testXContent() throws Exception {
        Map<String, Object> segmentMetadata = RemoteStoreMetadataTestHelper.createTestSegmentMetadata();
        Map<String, Object> translogMetadata = RemoteStoreMetadataTestHelper.createTestTranslogMetadata();
        RemoteStoreShardMetadata metadata = RemoteStoreMetadataTestHelper.createTestMetadata(
            segmentMetadata,
            translogMetadata,
            "test-index",
            0
        );

        XContentBuilder builder = XContentFactory.jsonBuilder();
        metadata.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        assertEquals("test-index", xContentMap.get("index"));
        assertEquals(0, xContentMap.get("shard"));
        assertEquals(metadata.getLatestSegmentMetadataFileName(), xContentMap.get("latest_segment_metadata_filename"));
        assertEquals(metadata.getLatestTranslogMetadataFileName(), xContentMap.get("latest_translog_metadata_filename"));

        assertNotNull(xContentMap.get("available_segment_metadata_files"));
        assertNotNull(xContentMap.get("available_translog_metadata_files"));

        Map<String, Object> segmentFiles = (Map<String, Object>) xContentMap.get("available_segment_metadata_files");
        String segmentKey = metadata.getLatestSegmentMetadataFileName();
        assertTrue(segmentFiles.containsKey(segmentKey));

        Map<String, Object> segmentContent = (Map<String, Object>) segmentFiles.get(segmentKey);
        assertTrue(segmentContent.containsKey("files"));
        assertTrue(segmentContent.containsKey("replication_checkpoint"));

        Map<String, Object> translogFiles = (Map<String, Object>) xContentMap.get("available_translog_metadata_files");
        String translogKey = metadata.getLatestTranslogMetadataFileName();
        assertTrue(translogFiles.containsKey(translogKey));

        Map<String, Object> translogContent = (Map<String, Object>) translogFiles.get(translogKey);
        assertTrue(translogContent.containsKey("generation"));
        assertTrue(translogContent.containsKey("primary_term"));
        assertTrue(translogContent.containsKey("min_translog_gen"));
        assertTrue(translogContent.containsKey("generation_to_primary_term"));
    }

    public void testEmptyMetadata() throws Exception {
        RemoteStoreShardMetadata metadata = RemoteStoreMetadataTestHelper.createTestMetadata(Map.of(), Map.of(), "test-index", 0);

        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RemoteStoreShardMetadata deserializedMetadata = new RemoteStoreShardMetadata(in);

        assertEquals("test-index", deserializedMetadata.getIndexName());
        assertEquals(0, deserializedMetadata.getShardId());
        assertTrue(deserializedMetadata.getSegmentMetadataFiles().isEmpty());
        assertTrue(deserializedMetadata.getTranslogMetadataFiles().isEmpty());
        assertNull(deserializedMetadata.getLatestSegmentMetadataFileName());
        assertNull(deserializedMetadata.getLatestTranslogMetadataFileName());
    }

    private void assertNestedMapEquals(Map<String, Map<String, Object>> expected, Map<String, Map<String, Object>> actual) {
        assertEquals("Map size mismatch", expected.size(), actual.size());
        for (String key : expected.keySet()) {
            assertTrue("Missing key: " + key, actual.containsKey(key));
            assertFlatMapEquals(expected.get(key), actual.get(key));
        }
    }

    @SuppressWarnings("unchecked")
    private void assertFlatMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        assertEquals("Inner map size mismatch", expected.size(), actual.size());
        assertEquals("Inner map keys mismatch", expected.keySet(), actual.keySet());

        for (String key : expected.keySet()) {
            Object expectedVal = expected.get(key);
            Object actualVal = actual.get(key);

            if (expectedVal instanceof Map) {
                assertTrue("Expected value at key " + key + " should be a map", actualVal instanceof Map);
                assertFlatMapEquals((Map<String, Object>) expectedVal, (Map<String, Object>) actualVal);
            } else if (expectedVal instanceof Number) {
                assertEquals("Mismatch at key " + key, ((Number) expectedVal).longValue(), ((Number) actualVal).longValue());
            } else {
                assertEquals("Mismatch at key " + key, expectedVal, actualVal);
            }
        }
    }
}
