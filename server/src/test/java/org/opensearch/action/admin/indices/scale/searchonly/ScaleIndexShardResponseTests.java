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

public class ScaleIndexShardResponseTests extends OpenSearchTestCase {

    public void testConstructorAndGetters() {
        ShardId shardId = createTestShardId();
        boolean needsSync = randomBoolean();
        int uncommittedOps = randomIntBetween(0, 100);

        ScaleIndexShardResponse response = new ScaleIndexShardResponse(shardId, needsSync, uncommittedOps);

        assertEquals("Shard ID should match", shardId, response.getShardId());
        assertEquals("Needs sync flag should match", needsSync, response.needsSync());
        assertEquals("Uncommitted operations status should match", uncommittedOps > 0, response.hasUncommittedOperations());
    }

    public void testSerializationRoundTrip() throws IOException {
        ShardId shardId = createTestShardId();
        boolean needsSync = randomBoolean();
        int uncommittedOps = randomIntBetween(0, 100);

        ScaleIndexShardResponse originalResponse = new ScaleIndexShardResponse(shardId, needsSync, uncommittedOps);

        BytesStreamOutput output = new BytesStreamOutput();
        originalResponse.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ScaleIndexShardResponse deserializedResponse = new ScaleIndexShardResponse(input);

        assertEquals("Shard ID should survive serialization", originalResponse.getShardId(), deserializedResponse.getShardId());
        assertEquals("Needs sync flag should survive serialization", originalResponse.needsSync(), deserializedResponse.needsSync());
        assertEquals(
            "Uncommitted operations status should survive serialization",
            originalResponse.hasUncommittedOperations(),
            deserializedResponse.hasUncommittedOperations()
        );
    }

    public void testZeroUncommittedOperations() {
        ShardId shardId = createTestShardId();
        ScaleIndexShardResponse response = new ScaleIndexShardResponse(shardId, randomBoolean(), 0);

        assertFalse("Should report no uncommitted operations when count is 0", response.hasUncommittedOperations());
    }

    public void testNonZeroUncommittedOperations() {
        ShardId shardId = createTestShardId();
        int uncommittedOps = randomIntBetween(1, 100);
        ScaleIndexShardResponse response = new ScaleIndexShardResponse(shardId, randomBoolean(), uncommittedOps);

        assertTrue("Should report uncommitted operations when count is > 0", response.hasUncommittedOperations());
    }

    public void testSerializationWithExtremeValues() throws IOException {
        ShardId shardId = createTestShardId();

        // Test with Integer.MAX_VALUE uncommitted operations
        ScaleIndexShardResponse originalResponse = new ScaleIndexShardResponse(shardId, true, Integer.MAX_VALUE);

        BytesStreamOutput output = new BytesStreamOutput();
        originalResponse.writeTo(output);

        StreamInput input = output.bytes().streamInput();
        ScaleIndexShardResponse deserializedResponse = new ScaleIndexShardResponse(input);

        assertTrue("Max value should be preserved and indicate uncommitted operations", deserializedResponse.hasUncommittedOperations());
    }

    public void testSerializationWithVariousShardIds() throws IOException {
        // Test with different shard numbers
        for (int shardNum : new int[] { 0, 1, 100, Integer.MAX_VALUE }) {
            ShardId shardId = new ShardId(new Index("test_index", "uuid"), shardNum);
            ScaleIndexShardResponse originalResponse = new ScaleIndexShardResponse(shardId, randomBoolean(), randomIntBetween(0, 100));

            BytesStreamOutput output = new BytesStreamOutput();
            originalResponse.writeTo(output);

            StreamInput input = output.bytes().streamInput();
            ScaleIndexShardResponse deserializedResponse = new ScaleIndexShardResponse(input);

            assertEquals("Shard number should survive serialization", shardId.id(), deserializedResponse.getShardId().id());
        }
    }

    private ShardId createTestShardId() {
        return new ShardId(new Index("test_index", "uuid"), randomIntBetween(0, 10));
    }
}
