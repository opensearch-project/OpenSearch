/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.indices;

import org.opensearch.common.Randomness;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Random;
import java.util.UUID;

public class IRCKeyWriteableSerializerTests extends OpenSearchSingleNodeTestCase {

    public void testSerializer() throws Exception {
        IndexService indexService = createIndex("test");
        IndexShard indexShard = indexService.getShardOrNull(0);
        IRCKeyWriteableSerializer ser = new IRCKeyWriteableSerializer();

        int NUM_KEYS = 1000;
        int[] valueLengths = new int[] { 1000, 6000 }; // test both branches in equals()
        Random rand = Randomness.get();
        for (int valueLength : valueLengths) {
            for (int i = 0; i < NUM_KEYS; i++) {
                IndicesRequestCache.Key key = getRandomIRCKey(valueLength, rand, indexShard.shardId(), System.identityHashCode(indexShard));
                byte[] serialized = ser.serialize(key);
                assertTrue(ser.equals(key, serialized));
                IndicesRequestCache.Key deserialized = ser.deserialize(serialized);
                assertTrue(key.equals(deserialized));
            }
        }
    }

    private IndicesRequestCache.Key getRandomIRCKey(int valueLength, Random random, ShardId shard, int indexShardHashCode) {
        byte[] value = new byte[valueLength];
        for (int i = 0; i < valueLength; i++) {
            value[i] = (byte) (random.nextInt(126 - 32) + 32);
        }
        BytesReference keyValue = new BytesArray(value);
        return new IndicesRequestCache.Key(shard, keyValue, UUID.randomUUID().toString(), indexShardHashCode); // same UUID
        // source as used in real key
    }
}
