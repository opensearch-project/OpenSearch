/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.scale.searchonly;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ScaleIndexNodeRequestTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        String indexName = "test_index";
        List<ShardId> shardIds = createTestShardIds(indexName, 3);

        ScaleIndexNodeRequest request = new ScaleIndexNodeRequest(indexName, shardIds);

        assertEquals("Index name should match", indexName, request.getIndex());
        assertEquals("Shard IDs should match", shardIds, request.getShardIds());
    }

    public void testSerializationRoundTrip() throws IOException {
        String indexName = "test_index";
        List<ShardId> shardIds = createTestShardIds(indexName, 3);

        ScaleIndexNodeRequest originalRequest = new ScaleIndexNodeRequest(indexName, shardIds);

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ScaleIndexNodeRequest deserializedRequest = new ScaleIndexNodeRequest(input);

        assertEquals("Index name should survive serialization", originalRequest.getIndex(), deserializedRequest.getIndex());
        assertEquals("Shard IDs should survive serialization", originalRequest.getShardIds(), deserializedRequest.getShardIds());
    }

    public void testSerializationWithEmptyShardList() throws IOException {
        String indexName = "test_index";
        List<ShardId> emptyShardIds = new ArrayList<>();

        ScaleIndexNodeRequest originalRequest = new ScaleIndexNodeRequest(indexName, emptyShardIds);

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ScaleIndexNodeRequest deserializedRequest = new ScaleIndexNodeRequest(input);

        assertEquals("Index name should survive serialization", originalRequest.getIndex(), deserializedRequest.getIndex());
        assertTrue("Empty shard list should survive serialization", deserializedRequest.getShardIds().isEmpty());
    }

    public void testSerializationWithMultipleShards() throws IOException {
        String indexName = "test_index";
        List<ShardId> shardIds = createTestShardIds(indexName, 5);

        ScaleIndexNodeRequest originalRequest = new ScaleIndexNodeRequest(indexName, shardIds);

        BytesStreamOutput output = new BytesStreamOutput();
        originalRequest.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ScaleIndexNodeRequest deserializedRequest = new ScaleIndexNodeRequest(input);

        assertEquals(
            "Should have correct number of shards after deserialization",
            shardIds.size(),
            deserializedRequest.getShardIds().size()
        );

        for (int i = 0; i < shardIds.size(); i++) {
            ShardId original = shardIds.get(i);
            ShardId deserialized = deserializedRequest.getShardIds().get(i);

            assertEquals("Shard ID should match", original.id(), deserialized.id());
            assertEquals("Index name should match", original.getIndexName(), deserialized.getIndexName());
        }
    }

    private List<ShardId> createTestShardIds(String indexName, int count) {
        List<ShardId> shardIds = new ArrayList<>(count);
        Index index = new Index(indexName, "uuid");
        for (int i = 0; i < count; i++) {
            shardIds.add(new ShardId(index, i));
        }
        return shardIds;
    }
}
