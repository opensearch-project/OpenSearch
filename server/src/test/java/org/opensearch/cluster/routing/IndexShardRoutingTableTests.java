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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class IndexShardRoutingTableTests extends OpenSearchTestCase {
    public void testEqualsAttributesKey() {
        List<String> attr1 = Arrays.asList("a");
        List<String> attr2 = Arrays.asList("b");
        IndexShardRoutingTable.AttributesKey attributesKey1 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey2 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey3 = new IndexShardRoutingTable.AttributesKey(attr2);
        String s = "Some random other object";
        assertEquals(attributesKey1, attributesKey1);
        assertEquals(attributesKey1, attributesKey2);
        assertNotEquals(attributesKey1, null);
        assertNotEquals(attributesKey1, s);
        assertNotEquals(attributesKey1, attributesKey3);
    }

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, new ArrayList<>());
        IndexShardRoutingTable table3 = new IndexShardRoutingTable(shardId2, new ArrayList<>());
        String s = "Some other random object";
        assertEquals(table1, table1);
        assertEquals(table1, table2);
        assertNotEquals(table1, null);
        assertNotEquals(table1, s);
        assertNotEquals(table1, table3);

        ShardRouting primary = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        IndexShardRoutingTable table4 = new IndexShardRoutingTable(shardId, Arrays.asList(primary, replica));
        IndexShardRoutingTable table5 = new IndexShardRoutingTable(shardId, Arrays.asList(replica, primary));
        assertEquals(table4, table5);
    }

    public void testWriteVerifiableTo() throws IOException {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardRouting shard1 = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting shard2 = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        ShardRouting shard3 = TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED);

        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, Arrays.asList(shard1, shard2, shard3));
        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        IndexShardRoutingTable.Builder.writeVerifiableTo(table1, checksumOut);

        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, Arrays.asList(shard3, shard1, shard2));
        BytesStreamOutput out2 = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut2 = new BufferedChecksumStreamOutput(out2);
        IndexShardRoutingTable.Builder.writeVerifiableTo(table2, checksumOut2);
        assertEquals(checksumOut.getChecksum(), checksumOut2.getChecksum());
    }

    public void testWriteVerifiableToWithoutPrimary() throws IOException {
        // A shard can end up with no primary. For example, after the finalize step
        // of `POST /{index}/_scale {"search_only": true}`, only search-only replicas
        // remain, and search-only replicas have primary()==false, so primaryShard()
        // returns null.
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardRouting searchOnlyReplica = TestShardRouting.newShardRouting(
            shardId,
            null,
            null,
            false,
            true,
            ShardRoutingState.UNASSIGNED,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, List.of(searchOnlyReplica));
        assertNull("Precondition: this state has no primary", table.primaryShard());

        BytesStreamOutput out = new BytesStreamOutput();
        BufferedChecksumStreamOutput checksumOut = new BufferedChecksumStreamOutput(out);
        // Must not throw NullPointerException.
        IndexShardRoutingTable.Builder.writeVerifiableTo(table, checksumOut);
    }

    public void testShardsMatchingPredicate() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        ShardRouting primary = TestShardRouting.newShardRouting(shardId, "node-1", true, ShardRoutingState.STARTED);
        ShardRouting replica = TestShardRouting.newShardRouting(shardId, "node-2", false, ShardRoutingState.STARTED);
        ShardRouting unassignedReplica = ShardRouting.newUnassigned(
            shardId,
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        ShardRouting relocatingReplica1 = TestShardRouting.newShardRouting(
            shardId,
            "node-3",
            "node-4",
            false,
            ShardRoutingState.RELOCATING
        );
        ShardRouting relocatingReplica2 = TestShardRouting.newShardRouting(
            shardId,
            "node-4",
            "node-5",
            false,
            ShardRoutingState.RELOCATING
        );

        IndexShardRoutingTable table = new IndexShardRoutingTable(
            shardId,
            Arrays.asList(primary, replica, unassignedReplica, relocatingReplica1, relocatingReplica2)
        );
        assertEquals(List.of(primary), table.shardsMatchingPredicate(ShardRouting::primary));
        assertEquals(
            List.of(replica, unassignedReplica, relocatingReplica1, relocatingReplica2),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary())
        );
        assertEquals(
            List.of(unassignedReplica),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary() && shardRouting.unassigned())
        );
        assertEquals(
            Arrays.asList(relocatingReplica1, relocatingReplica2),
            table.shardsMatchingPredicate(shardRouting -> !shardRouting.primary() && shardRouting.relocating())
        );
    }

    /**
     * The search replica iterator must round-robin evenly across all active search replicas, even
     * when the replicas list also contains write replicas that the filter rejects. Rotating the
     * unfiltered list used to collapse two rotation offsets onto the same search replica, giving
     * it exactly twice the traffic of the others.
     */
    public void testSearchReplicaShardIteratorEvenDistribution() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        // zero write replicas must stay evenly distributed too, not just the mixed case
        int writeReplicas = randomIntBetween(0, 3);
        for (int i = 0; i < writeReplicas; i++) {
            shards.add(TestShardRouting.newShardRouting(shardId, "node-replica-" + i, false, ShardRoutingState.STARTED));
        }
        int searchReplicas = randomIntBetween(2, 5);
        for (int i = 0; i < searchReplicas; i++) {
            shards.add(TestShardRouting.newShardRouting(shardId, "node-search-" + i, null, false, true, ShardRoutingState.STARTED, null));
        }
        // the fix must hold regardless of where the write replicas sit in the list
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        int iterationsPerReplica = 100;
        Map<String, Integer> firstShardCounts = new HashMap<>();
        for (int i = 0; i < searchReplicas * iterationsPerReplica; i++) {
            ShardRouting first = table.searchReplicaActiveInitializingShardIt().nextOrNull();
            assertNotNull(first);
            assertTrue(first.isSearchOnly());
            firstShardCounts.merge(first.currentNodeId(), 1, Integer::sum);
        }
        assertEquals(searchReplicas, firstShardCounts.size());
        for (Map.Entry<String, Integer> count : firstShardCounts.entrySet()) {
            assertEquals(
                "search replica on " + count.getKey() + " should receive an equal share of requests",
                iterationsPerReplica,
                count.getValue().intValue()
            );
        }
    }

    /**
     * The replica-first iterator must round-robin evenly across active replicas even when some
     * replicas are initializing or unassigned. Rotating the full replicas list before dropping the
     * inactive ones used to favour the active replica that precedes them in list order.
     */
    public void testReplicaFirstShardIteratorEvenDistribution() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        int activeReplicas = randomIntBetween(2, 5);
        for (int i = 0; i < activeReplicas; i++) {
            shards.add(TestShardRouting.newShardRouting(shardId, "node-replica-" + i, false, ShardRoutingState.STARTED));
        }
        int initializingReplicas = randomIntBetween(0, 2);
        for (int i = 0; i < initializingReplicas; i++) {
            shards.add(TestShardRouting.newShardRouting(shardId, "node-initializing-" + i, false, ShardRoutingState.INITIALIZING));
        }
        int unassignedReplicas = randomIntBetween(initializingReplicas == 0 ? 1 : 0, 2);
        for (int i = 0; i < unassignedReplicas; i++) {
            shards.add(TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED));
        }
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        int iterationsPerReplica = 100;
        Map<String, Integer> firstShardCounts = new HashMap<>();
        for (int i = 0; i < activeReplicas * iterationsPerReplica; i++) {
            ShardRouting first = table.replicaFirstActiveInitializingShardsIt().nextOrNull();
            assertNotNull(first);
            assertTrue(first.active());
            assertFalse(first.primary());
            firstShardCounts.merge(first.currentNodeId(), 1, Integer::sum);
        }
        assertEquals(activeReplicas, firstShardCounts.size());
        for (Map.Entry<String, Integer> count : firstShardCounts.entrySet()) {
            assertEquals(
                "replica on " + count.getKey() + " should receive an equal share of requests",
                iterationsPerReplica,
                count.getValue().intValue()
            );
        }
    }

    public void testReplicaFirstShardIteratorOrdersActiveReplicasThenPrimaryThenInitializing() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-replica-0", false, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-replica-1", false, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-initializing", false, ShardRoutingState.INITIALIZING));
        shards.add(TestShardRouting.newShardRouting(shardId, null, false, ShardRoutingState.UNASSIGNED));
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        for (int i = 0; i < 10; i++) {
            ShardIterator iterator = table.replicaFirstActiveInitializingShardsIt();
            assertEquals(4, iterator.size());
            ShardRouting first = iterator.nextOrNull();
            ShardRouting second = iterator.nextOrNull();
            assertTrue(first.active() && first.primary() == false);
            assertTrue(second.active() && second.primary() == false);
            assertNotEquals(first.currentNodeId(), second.currentNodeId());
            assertEquals("node-primary", iterator.nextOrNull().currentNodeId());
            assertEquals("node-initializing", iterator.nextOrNull().currentNodeId());
            assertNull(iterator.nextOrNull());
        }
    }

    /**
     * With awareness attributes, the search replica iterator must return the search replicas in the local node's zone
     * first, round-robin evenly among them, and still return only search replicas.
     */
    public void testSearchReplicaPreferAttributesShardIteratorPrefersLocalZone() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(zoneNode("node-coordinator", "a"))
            .add(zoneNode("node-primary", "a"))
            .add(zoneNode("node-replica", "a"))
            .add(zoneNode("node-search-a0", "a"))
            .add(zoneNode("node-search-a1", "a"))
            .add(zoneNode("node-search-b0", "b"))
            .add(zoneNode("node-search-b1", "b"))
            .add(zoneNode("node-search-a-init", "a"))
            .add(zoneNode("node-search-b-init", "b"))
            .localNodeId("node-coordinator")
            .build();
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-replica", false, ShardRoutingState.STARTED));
        for (String nodeId : Arrays.asList("node-search-a0", "node-search-a1", "node-search-b0", "node-search-b1")) {
            shards.add(TestShardRouting.newShardRouting(shardId, nodeId, null, false, true, ShardRoutingState.STARTED, null));
        }
        for (String nodeId : Arrays.asList("node-search-a-init", "node-search-b-init")) {
            shards.add(
                TestShardRouting.newShardRouting(
                    shardId,
                    nodeId,
                    false,
                    true,
                    ShardRoutingState.INITIALIZING,
                    RecoverySource.PeerRecoverySource.INSTANCE
                )
            );
        }
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        int iterationsPerReplica = 100;
        Map<String, Integer> firstShardCounts = new HashMap<>();
        for (int i = 0; i < 2 * iterationsPerReplica; i++) {
            ShardIterator iterator = table.searchReplicaPreferAttributesActiveInitializingShardsIt(
                Collections.singletonList("zone"),
                nodes
            );
            assertEquals(6, iterator.size());
            List<String> order = new ArrayList<>();
            for (ShardRouting shard = iterator.nextOrNull(); shard != null; shard = iterator.nextOrNull()) {
                assertTrue(shard.isSearchOnly());
                order.add(shard.currentNodeId());
            }
            assertTrue(order.subList(0, 2).containsAll(Arrays.asList("node-search-a0", "node-search-a1")));
            assertTrue(order.subList(2, 4).containsAll(Arrays.asList("node-search-b0", "node-search-b1")));
            assertEquals(Arrays.asList("node-search-a-init", "node-search-b-init"), order.subList(4, 6));
            firstShardCounts.merge(order.get(0), 1, Integer::sum);
        }
        assertEquals(Map.of("node-search-a0", iterationsPerReplica, "node-search-a1", iterationsPerReplica), firstShardCounts);
    }

    /**
     * The awareness-preferring search replica iterator must return the same candidates as the plain one: the target of a
     * relocating search replica is not a candidate, only its source is.
     */
    public void testSearchReplicaPreferAttributesShardIteratorSkipsRelocationTargets() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(zoneNode("node-coordinator", "a"))
            .add(zoneNode("node-primary", "a"))
            .add(zoneNode("node-search-source", "a"))
            .add(zoneNode("node-search-target", "a"))
            .add(zoneNode("node-search-b", "b"))
            .localNodeId("node-coordinator")
            .build();
        IndexShardRoutingTable table = new IndexShardRoutingTable(
            shardId,
            Arrays.asList(
                TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED),
                TestShardRouting.newShardRouting(
                    shardId,
                    "node-search-source",
                    "node-search-target",
                    false,
                    true,
                    ShardRoutingState.RELOCATING,
                    null
                ),
                TestShardRouting.newShardRouting(shardId, "node-search-b", null, false, true, ShardRoutingState.STARTED, null)
            )
        );

        ShardIterator iterator = table.searchReplicaPreferAttributesActiveInitializingShardsIt(Collections.singletonList("zone"), nodes);
        assertEquals(2, iterator.size());
        assertEquals("node-search-source", iterator.nextOrNull().currentNodeId());
        assertEquals("node-search-b", iterator.nextOrNull().currentNodeId());
        assertNull(iterator.nextOrNull());
    }

    /**
     * When the local node has none of the awareness attributes, no search replica is preferred and all of them are
     * round-robined evenly, like the plain search replica iterator does.
     */
    public void testSearchReplicaPreferAttributesShardIteratorWithoutLocalAttribute() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(
                new DiscoveryNode(
                    "node-coordinator",
                    buildNewFakeTransportAddress(),
                    Collections.emptyMap(),
                    Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            )
            .add(zoneNode("node-primary", "a"))
            .add(zoneNode("node-search-a", "a"))
            .add(zoneNode("node-search-b", "b"))
            .add(zoneNode("node-search-c", "c"))
            .localNodeId("node-coordinator")
            .build();
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        for (String nodeId : Arrays.asList("node-search-a", "node-search-b", "node-search-c")) {
            shards.add(TestShardRouting.newShardRouting(shardId, nodeId, null, false, true, ShardRoutingState.STARTED, null));
        }
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        int iterationsPerReplica = 100;
        Map<String, Integer> firstShardCounts = new HashMap<>();
        for (int i = 0; i < 3 * iterationsPerReplica; i++) {
            ShardIterator iterator = table.searchReplicaPreferAttributesActiveInitializingShardsIt(
                Collections.singletonList("zone"),
                nodes
            );
            assertEquals(3, iterator.size());
            firstShardCounts.merge(iterator.nextOrNull().currentNodeId(), 1, Integer::sum);
        }
        assertEquals(
            Map.of("node-search-a", iterationsPerReplica, "node-search-b", iterationsPerReplica, "node-search-c", iterationsPerReplica),
            firstShardCounts
        );
    }

    private static DiscoveryNode zoneNode(String nodeId, String zone) {
        return new DiscoveryNode(
            nodeId,
            buildNewFakeTransportAddress(),
            Collections.singletonMap("zone", zone),
            Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
            Version.CURRENT
        );
    }

    public void testSearchReplicaShardIteratorReturnsActiveBeforeInitializing() {
        ShardId shardId = new ShardId(new Index("a", UUID.randomUUID().toString()), 0);
        List<ShardRouting> shards = new ArrayList<>();
        shards.add(TestShardRouting.newShardRouting(shardId, "node-primary", true, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-replica", false, ShardRoutingState.STARTED));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-search-0", null, false, true, ShardRoutingState.STARTED, null));
        shards.add(TestShardRouting.newShardRouting(shardId, "node-search-1", null, false, true, ShardRoutingState.STARTED, null));
        shards.add(
            TestShardRouting.newShardRouting(
                shardId,
                "node-search-2",
                false,
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.PeerRecoverySource.INSTANCE
            )
        );
        Collections.shuffle(shards, random());
        IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, shards);

        for (int i = 0; i < 10; i++) {
            ShardIterator iterator = table.searchReplicaActiveInitializingShardIt();
            assertEquals(3, iterator.size());
            ShardRouting first = iterator.nextOrNull();
            ShardRouting second = iterator.nextOrNull();
            ShardRouting third = iterator.nextOrNull();
            assertTrue(first.isSearchOnly() && first.active());
            assertTrue(second.isSearchOnly() && second.active());
            assertTrue(third.isSearchOnly() && third.initializing());
            assertEquals("node-search-2", third.currentNodeId());
            assertNull(iterator.nextOrNull());
        }
    }
}
