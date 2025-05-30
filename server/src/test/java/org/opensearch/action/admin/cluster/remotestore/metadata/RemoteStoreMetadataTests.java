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
        RemoteStoreMetadata metadata = new RemoteStoreMetadata(segmentMetadata, translogMetadata, "test-index", 0);

        // Test serialization
        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);

        // Test deserialization
        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadata deserializedMetadata = new RemoteStoreMetadata(in);

        // Verify
        assertEquals(metadata.getIndexName(), deserializedMetadata.getIndexName());
        assertEquals(metadata.getShardId(), deserializedMetadata.getShardId());
        assertMapEquals(metadata.getSegments(), deserializedMetadata.getSegments());
        assertMapEquals(metadata.getTranslog(), deserializedMetadata.getTranslog());
    }

    @SuppressWarnings("unchecked")
    public void testXContent() throws Exception {
        Map<String, Object> segmentMetadata = RemoteStoreMetadataTestHelper.createTestSegmentMetadata();
        Map<String, Object> translogMetadata = RemoteStoreMetadataTestHelper.createTestTranslogMetadata();
        RemoteStoreMetadata metadata = new RemoteStoreMetadata(segmentMetadata, translogMetadata, "test-index", 0);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        metadata.toXContent(builder, EMPTY_PARAMS);
        Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();

        // Verify structure
        assertEquals("test-index", xContentMap.get("index"));
        assertEquals(0, xContentMap.get("shard"));
        assertNotNull(xContentMap.get("segments"));
        assertNotNull(xContentMap.get("translog"));

        // Verify segment content
        Map<String, Object> segments = (Map<String, Object>) xContentMap.get("segments");
        assertMapEquals(segmentMetadata, segments);

        // Verify translog content
        Map<String, Object> translog = (Map<String, Object>) xContentMap.get("translog");
        assertMapEquals(translogMetadata, translog);
    }

    public void testEmptyMetadata() throws Exception {
        RemoteStoreMetadata metadata = new RemoteStoreMetadata(Map.of(), Map.of(), "test-index", 0);

        // Test serialization of empty metadata
        BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        RemoteStoreMetadata deserializedMetadata = new RemoteStoreMetadata(in);

        assertTrue(deserializedMetadata.getSegments().isEmpty());
        assertTrue(deserializedMetadata.getTranslog().isEmpty());
        assertEquals("test-index", deserializedMetadata.getIndexName());
        assertEquals(0, deserializedMetadata.getShardId());
    }

    @SuppressWarnings("unchecked")
    private void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        if (expected == null || actual == null) {
            assertEquals(expected, actual);
            return;
        }

        assertEquals("Maps have different sizes", expected.size(), actual.size());
        assertEquals("Maps have different key sets", expected.keySet(), actual.keySet());

        for (String key : expected.keySet()) {
            Object expectedValue = expected.get(key);
            Object actualValue = actual.get(key);

            if (expectedValue instanceof Map) {
                assertTrue("Value for key " + key + " should be a Map", actualValue instanceof Map);
                assertMapEquals((Map<String, Object>) expectedValue, (Map<String, Object>) actualValue);
            } else if (expectedValue instanceof Number) {
                // Handle number comparisons specifically
                assertEquals(
                    "Number value mismatch for key " + key,
                    ((Number) expectedValue).longValue(),
                    ((Number) actualValue).longValue()
                );
            } else {
                assertEquals("Value mismatch for key " + key, expectedValue, actualValue);
            }
        }
    }
}
