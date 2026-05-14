/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.FieldMemoryStatsTests;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

public class FieldDataStatsTests extends OpenSearchTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        FieldDataStats stats = new FieldDataStats.Builder().memorySize(randomNonNegativeLong())
            .evictions(randomNonNegativeLong())
            .fieldMemoryStats(map)
            .build();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldDataStats read = new FieldDataStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getEvictions(), read.getEvictions());
        assertEquals(stats.getMemorySize(), read.getMemorySize());
        assertEquals(stats.getFields(), read.getFields());
    }

    // onRemoval without a matching onCache pushes memorySize negative; writeVLong then throws.
    public void testRemovalWithoutMatchingCacheGoesNegativeAndFailsSerialization() {
        ShardFieldData data = new ShardFieldData();
        ShardId shardId = new ShardId("index", "uuid", 0);
        long bytes = 2_895_512L;

        data.onRemoval(shardId, "foo", true, bytes);

        FieldDataStats stats = data.stats("*");
        assertEquals(-bytes, stats.getMemorySizeInBytes());

        BytesStreamOutput out = new BytesStreamOutput();
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> stats.writeTo(out));
        assertTrue(e.getMessage().contains("Negative longs unsupported"));
        assertTrue(e.getMessage().contains("-2895512"));
    }

    // With the identity guard, a stale removal is skipped — the new shard is not decremented.
    public void testIdentityGuardSkipsStaleDecrementOnReallocation() {
        AtomicReference<Object> currentShard = new AtomicReference<>();
        AtomicReference<ShardFieldData> currentShardFieldData = new AtomicReference<>();
        IndexFieldDataCache.Listener listener = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage, int shardIdentity) {
                ShardFieldData s = currentShardFieldData.get();
                if (s != null) s.onCache(shardId, fieldName, ramUsage);
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes, int shardIdentity) {
                Object shard = currentShard.get();
                ShardFieldData s = currentShardFieldData.get();
                if (shard == null || s == null) return;
                if (shardIdentity != 0 && shardIdentity != System.identityHashCode(shard)) return;
                s.onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
            }
        };

        ShardId shardId = new ShardId("idx", "uuid", 0);
        Object shardA = new Object();
        ShardFieldData fdA = new ShardFieldData();
        currentShard.set(shardA);
        currentShardFieldData.set(fdA);

        long bytes = 4_372_408L;
        int identityA = System.identityHashCode(shardA);
        listener.onCache(shardId, "user.name", accountableOf(bytes), identityA);
        assertEquals(bytes, fdA.stats("*").getMemorySizeInBytes());

        // Old shard is replaced. The stale removal still carries the old identity → guard skips it.
        Object shardAPrime = new Object();
        ShardFieldData fdAPrime = new ShardFieldData();
        currentShard.set(shardAPrime);
        currentShardFieldData.set(fdAPrime);

        listener.onRemoval(shardId, "user.name", false, bytes, identityA);

        assertEquals(0L, fdAPrime.stats("*").getMemorySizeInBytes());
    }

    // shardIdentity == 0 means "tracking disabled" — guard must let the removal through.
    public void testIdentityGuardAllowsThroughWhenIdentityIsZero() {
        AtomicReference<Object> currentShard = new AtomicReference<>();
        AtomicReference<ShardFieldData> currentShardFieldData = new AtomicReference<>();
        IndexFieldDataCache.Listener listener = new IndexFieldDataCache.Listener() {
            @Override
            public void onCache(ShardId shardId, String fieldName, Accountable ramUsage, int shardIdentity) {
                ShardFieldData s = currentShardFieldData.get();
                if (s != null) s.onCache(shardId, fieldName, ramUsage);
            }

            @Override
            public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes, int shardIdentity) {
                Object shard = currentShard.get();
                ShardFieldData s = currentShardFieldData.get();
                if (shard == null || s == null) return;
                if (shardIdentity != 0 && shardIdentity != System.identityHashCode(shard)) return;
                s.onRemoval(shardId, fieldName, wasEvicted, sizeInBytes);
            }
        };

        ShardId shardId = new ShardId("idx", "uuid", 0);
        Object shardA = new Object();
        ShardFieldData fdA = new ShardFieldData();
        currentShard.set(shardA);
        currentShardFieldData.set(fdA);

        long bytes = 500_000L;
        listener.onCache(shardId, "f", accountableOf(bytes), 0);
        listener.onRemoval(shardId, "f", false, bytes, 0);

        assertEquals(0L, fdA.stats("*").getMemorySizeInBytes());
    }

    private static Accountable accountableOf(long bytes) {
        return new Accountable() {
            @Override
            public long ramBytesUsed() {
                return bytes;
            }

            @Override
            public Collection<Accountable> getChildResources() {
                return Collections.emptyList();
            }
        };
    }
}
