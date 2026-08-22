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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.OriginalIndicesTests;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.routing.GroupShardsIteratorTests;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SearchShardIteratorTests extends OpenSearchTestCase {

    public void testShardId() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), OriginalIndices.NONE);
        assertSame(shardId, searchShardIterator.shardId());
    }

    public void testShardRoutingsAreRetainedOnlyWhenIncludedInShardInfo() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomIntBetween(0, 5));
        List<ShardRouting> shards = List.of(
            TestShardRouting.newShardRouting(shardId, "node-a", true, ShardRoutingState.STARTED),
            TestShardRouting.newShardRouting(shardId, "node-b", false, ShardRoutingState.STARTED)
        );

        // an ordinary search must not pay to retain routings it will never read
        assertNull(new SearchShardIterator(null, shardId, shards, OriginalIndices.NONE).getShardRoutings());
        assertFalse(new SearchShardIterator(null, shardId, shards, OriginalIndices.NONE).includeInShardInfo());
        assertNull(new SearchShardIterator(null, shardId, shards, OriginalIndices.NONE, false).getShardRoutings());

        // a shard described in shard_info needs them to report the primary flag and shard state
        SearchShardIterator capturing = new SearchShardIterator(null, shardId, shards, OriginalIndices.NONE, true);
        assertEquals(shards, capturing.getShardRoutings());
        assertTrue(capturing.includeInShardInfo());

        // target nodes are derived from the routings either way, so skipping the capture cannot change execution
        assertEquals(new SearchShardIterator(null, shardId, shards, OriginalIndices.NONE).getTargetNodeIds(), capturing.getTargetNodeIds());

        // point-in-time readers are targeted through plain node ids and never have routings
        assertNull(new SearchShardIterator(null, shardId, List.of("node-a"), OriginalIndices.NONE, null, null).getShardRoutings());
    }

    public void testGetOriginalIndices() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { randomAlphaOfLengthBetween(3, 10) },
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );
        SearchShardIterator searchShardIterator = new SearchShardIterator(null, shardId, Collections.emptyList(), originalIndices);
        assertSame(originalIndices, searchShardIterator.getOriginalIndices());
    }

    public void testGetClusterAlias() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            Collections.emptyList(),
            OriginalIndices.NONE
        );
        assertEquals(clusterAlias, searchShardIterator.getClusterAlias());
    }

    public void testNewSearchShardTarget() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomInt());
        OriginalIndices originalIndices = new OriginalIndices(
            new String[] { randomAlphaOfLengthBetween(3, 10) },
            IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
        );

        String nodeId = randomAlphaOfLengthBetween(3, 10);
        SearchShardIterator searchShardIterator = new SearchShardIterator(
            clusterAlias,
            shardId,
            Collections.singletonList(nodeId),
            originalIndices,
            null,
            null
        );
        final SearchShardTarget searchShardTarget = searchShardIterator.nextOrNull();
        assertNotNull(searchShardTarget);
        assertThat(searchShardTarget.getNodeId(), equalTo(nodeId));
        assertEquals(clusterAlias, searchShardTarget.getClusterAlias());
        assertSame(shardId, searchShardTarget.getShardId());
        assertEquals(nodeId, searchShardTarget.getNodeId());
        assertSame(originalIndices, searchShardTarget.getOriginalIndices());
    }

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            randomSearchShardIterator(),
            s -> new SearchShardIterator(
                s.getClusterAlias(),
                s.shardId(),
                s.getTargetNodeIds(),
                s.getOriginalIndices(),
                s.getSearchContextId(),
                s.getSearchContextKeepAlive()
            ),
            s -> {
                if (randomBoolean()) {
                    String clusterAlias;
                    if (s.getClusterAlias() == null) {
                        clusterAlias = randomAlphaOfLengthBetween(5, 10);
                    } else {
                        clusterAlias = randomBoolean() ? null : s.getClusterAlias() + randomAlphaOfLength(3);
                    }
                    return new SearchShardIterator(
                        clusterAlias,
                        s.shardId(),
                        s.getTargetNodeIds(),
                        s.getOriginalIndices(),
                        s.getSearchContextId(),
                        s.getSearchContextKeepAlive()
                    );
                } else {
                    ShardId shardId = new ShardId(
                        randomAlphaOfLengthBetween(5, 10),
                        randomAlphaOfLength(10),
                        randomIntBetween(0, Integer.MAX_VALUE)
                    );
                    return new SearchShardIterator(
                        s.getClusterAlias(),
                        shardId,
                        s.getTargetNodeIds(),
                        s.getOriginalIndices(),
                        s.getSearchContextId(),
                        s.getSearchContextKeepAlive()
                    );
                }
            }
        );
    }

    public void testCompareTo() {
        String[] clusters = generateRandomStringArray(2, 10, false, false);
        Arrays.sort(clusters);
        String[] indices = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(indices);
        String[] uuids = generateRandomStringArray(3, 10, false, false);
        Arrays.sort(uuids);
        List<SearchShardIterator> shardIterators = new ArrayList<>();
        int numShards = randomIntBetween(1, 5);
        for (int i = 0; i < numShards; i++) {
            for (String index : indices) {
                for (String uuid : uuids) {
                    ShardId shardId = new ShardId(index, uuid, i);
                    shardIterators.add(
                        new SearchShardIterator(
                            null,
                            shardId,
                            GroupShardsIteratorTests.randomShardRoutings(shardId),
                            OriginalIndicesTests.randomOriginalIndices()
                        )
                    );
                    for (String cluster : clusters) {
                        shardIterators.add(
                            new SearchShardIterator(
                                cluster,
                                shardId,
                                GroupShardsIteratorTests.randomShardRoutings(shardId),
                                OriginalIndicesTests.randomOriginalIndices()
                            )
                        );
                    }

                }
            }
        }
        for (int i = 0; i < shardIterators.size(); i++) {
            SearchShardIterator currentIterator = shardIterators.get(i);
            for (int j = i + 1; j < shardIterators.size(); j++) {
                SearchShardIterator greaterIterator = shardIterators.get(j);
                assertThat(currentIterator, Matchers.lessThan(greaterIterator));
                assertThat(greaterIterator, Matchers.greaterThan(currentIterator));
                assertNotEquals(currentIterator, greaterIterator);
            }
            for (int j = i - 1; j >= 0; j--) {
                SearchShardIterator smallerIterator = shardIterators.get(j);
                assertThat(smallerIterator, Matchers.lessThan(currentIterator));
                assertThat(currentIterator, Matchers.greaterThan(smallerIterator));
                assertNotEquals(currentIterator, smallerIterator);
            }
        }
    }

    public void testCompareToEqualItems() {
        SearchShardIterator shardIterator1 = randomSearchShardIterator();
        SearchShardIterator shardIterator2 = new SearchShardIterator(
            shardIterator1.getClusterAlias(),
            shardIterator1.shardId(),
            shardIterator1.getTargetNodeIds(),
            shardIterator1.getOriginalIndices(),
            shardIterator1.getSearchContextId(),
            shardIterator1.getSearchContextKeepAlive()
        );
        assertEquals(shardIterator1, shardIterator2);
        assertEquals(0, shardIterator1.compareTo(shardIterator2));
        assertEquals(0, shardIterator2.compareTo(shardIterator1));
    }

    private static SearchShardIterator randomSearchShardIterator() {
        String clusterAlias = randomBoolean() ? null : randomAlphaOfLengthBetween(5, 10);
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(5, 10), randomAlphaOfLength(10), randomIntBetween(0, Integer.MAX_VALUE));
        return new SearchShardIterator(
            clusterAlias,
            shardId,
            GroupShardsIteratorTests.randomShardRoutings(shardId),
            OriginalIndicesTests.randomOriginalIndices()
        );
    }
}
