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
import org.opensearch.action.support.replication.ClusterStateCreationUtils;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.WeightedRoutingMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static java.util.Collections.singletonMap;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.common.util.FeatureFlags.WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.object.HasToString.hasToString;

public class OperationRoutingTests extends OpenSearchTestCase {
    public void testGenerateShardId() {
        int[][] possibleValues = new int[][] { { 8, 4, 2 }, { 20, 10, 2 }, { 36, 12, 3 }, { 15, 5, 1 } };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] / shardSplits[1]) * shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] / shardSplits[2]) * shardSplits[2]);
            IndexMetadata metadata = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[0])
                .numberOfReplicas(1)
                .build();
            String term = randomAlphaOfLength(10);
            final int shard = OperationRouting.generateShardId(metadata, term, null);
            IndexMetadata shrunk = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0])
                .build();
            int shrunkShard = OperationRouting.generateShardId(shrunk, term, null);

            Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(shrunkShard, metadata, shrunk.getNumberOfShards());
            assertEquals(1, shardIds.stream().filter((sid) -> sid.id() == shard).count());

            shrunk = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[2])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[0])
                .build();
            shrunkShard = OperationRouting.generateShardId(shrunk, term, null);
            shardIds = IndexMetadata.selectShrinkShards(shrunkShard, metadata, shrunk.getNumberOfShards());
            assertEquals(Arrays.toString(shardSplits), 1, shardIds.stream().filter((sid) -> sid.id() == shard).count());
        }
    }

    public void testGenerateShardIdSplit() {
        int[][] possibleValues = new int[][] { { 2, 4, 8 }, { 2, 10, 20 }, { 3, 12, 36 }, { 1, 5, 15 } };
        for (int i = 0; i < 10; i++) {
            int[] shardSplits = randomFrom(possibleValues);
            assertEquals(shardSplits[0], (shardSplits[0] * shardSplits[1]) / shardSplits[1]);
            assertEquals(shardSplits[1], (shardSplits[1] * shardSplits[2]) / shardSplits[2]);
            IndexMetadata metadata = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[0])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            String term = randomAlphaOfLength(10);
            final int shard = OperationRouting.generateShardId(metadata, term, null);
            IndexMetadata split = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[1])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            int shrunkShard = OperationRouting.generateShardId(split, term, null);

            ShardId shardId = IndexMetadata.selectSplitShard(shrunkShard, metadata, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());

            split = IndexMetadata.builder("test")
                .settings(settings(Version.CURRENT))
                .numberOfShards(shardSplits[2])
                .numberOfReplicas(1)
                .setRoutingNumShards(shardSplits[2])
                .build();
            shrunkShard = OperationRouting.generateShardId(split, term, null);
            shardId = IndexMetadata.selectSplitShard(shrunkShard, metadata, split.getNumberOfShards());
            assertNotNull(shardId);
            assertEquals(shard, shardId.getId());
        }
    }

    public void testPartitionedIndex() {
        // make sure the same routing value always has each _id fall within the configured partition size
        for (int shards = 1; shards < 5; shards++) {
            for (int partitionSize = 1; partitionSize == 1 || partitionSize < shards; partitionSize++) {
                IndexMetadata metadata = IndexMetadata.builder("test")
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(shards)
                    .routingPartitionSize(partitionSize)
                    .numberOfReplicas(1)
                    .build();

                for (int i = 0; i < 20; i++) {
                    String routing = randomUnicodeOfLengthBetween(1, 50);
                    Set<Integer> shardSet = new HashSet<>();

                    for (int k = 0; k < 150; k++) {
                        String id = randomUnicodeOfLengthBetween(1, 50);

                        shardSet.add(OperationRouting.generateShardId(metadata, id, routing));
                    }

                    assertEquals(partitionSize, shardSet.size());
                }
            }
        }
    }

    public void testPartitionedIndexShrunk() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 1);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 2);
        routingA.put("a_4", 1);
        routingA.put("a_5", 2);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 0);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingB.put("b_4", 3);
        routingB.put("b_5", 3);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 1);
        routingC.put("c_1", 1);
        routingC.put("c_2", 0);
        routingC.put("c_3", 0);
        routingC.put("c_4", 0);
        routingC.put("c_5", 1);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 2);
        routingD.put("d_1", 2);
        routingD.put("d_2", 3);
        routingD.put("d_3", 3);
        routingD.put("d_4", 3);
        routingD.put("d_5", 3);
        routingIdToShard.put("d", routingD);

        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .setRoutingNumShards(8)
            .numberOfShards(4)
            .routingPartitionSize(3)
            .numberOfReplicas(1)
            .build();

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, OperationRouting.generateShardId(metadata, id, routing));
            }
        }
    }

    public void testPartitionedIndexBWC() {
        Map<String, Map<String, Integer>> routingIdToShard = new HashMap<>();

        Map<String, Integer> routingA = new HashMap<>();
        routingA.put("a_0", 3);
        routingA.put("a_1", 2);
        routingA.put("a_2", 2);
        routingA.put("a_3", 3);
        routingIdToShard.put("a", routingA);

        Map<String, Integer> routingB = new HashMap<>();
        routingB.put("b_0", 5);
        routingB.put("b_1", 0);
        routingB.put("b_2", 0);
        routingB.put("b_3", 0);
        routingIdToShard.put("b", routingB);

        Map<String, Integer> routingC = new HashMap<>();
        routingC.put("c_0", 4);
        routingC.put("c_1", 4);
        routingC.put("c_2", 3);
        routingC.put("c_3", 4);
        routingIdToShard.put("c", routingC);

        Map<String, Integer> routingD = new HashMap<>();
        routingD.put("d_0", 3);
        routingD.put("d_1", 4);
        routingD.put("d_2", 4);
        routingD.put("d_3", 4);
        routingIdToShard.put("d", routingD);

        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(6)
            .routingPartitionSize(2)
            .numberOfReplicas(1)
            .build();

        for (Map.Entry<String, Map<String, Integer>> routingIdEntry : routingIdToShard.entrySet()) {
            String routing = routingIdEntry.getKey();

            for (Map.Entry<String, Integer> idEntry : routingIdEntry.getValue().entrySet()) {
                String id = idEntry.getKey();
                int shard = idEntry.getValue();

                assertEquals(shard, OperationRouting.generateShardId(metadata, id, routing));
            }
        }
    }

    /**
     * Ensures that all changes to the hash-function / shard selection are BWC
     */
    public void testBWC() {
        Map<String, Integer> termToShard = new TreeMap<>();
        termToShard.put("sEERfFzPSI", 1);
        termToShard.put("cNRiIrjzYd", 7);
        termToShard.put("BgfLBXUyWT", 5);
        termToShard.put("cnepjZhQnb", 3);
        termToShard.put("OKCmuYkeCK", 6);
        termToShard.put("OutXGRQUja", 5);
        termToShard.put("yCdyocKWou", 1);
        termToShard.put("KXuNWWNgVj", 2);
        termToShard.put("DGJOYrpESx", 4);
        termToShard.put("upLDybdTGs", 5);
        termToShard.put("yhZhzCPQby", 1);
        termToShard.put("EyCVeiCouA", 1);
        termToShard.put("tFyVdQauWR", 6);
        termToShard.put("nyeRYDnDQr", 6);
        termToShard.put("hswhrppvDH", 0);
        termToShard.put("BSiWvDOsNE", 5);
        termToShard.put("YHicpFBSaY", 1);
        termToShard.put("EquPtdKaBZ", 4);
        termToShard.put("rSjLZHCDfT", 5);
        termToShard.put("qoZALVcite", 7);
        termToShard.put("yDCCPVBiCm", 7);
        termToShard.put("ngizYtQgGK", 5);
        termToShard.put("FYQRIBcNqz", 0);
        termToShard.put("EBzEDAPODe", 2);
        termToShard.put("YePigbXgKb", 1);
        termToShard.put("PeGJjomyik", 3);
        termToShard.put("cyQIvDmyYD", 7);
        termToShard.put("yIEfZrYfRk", 5);
        termToShard.put("kblouyFUbu", 7);
        termToShard.put("xvIGbRiGJF", 3);
        termToShard.put("KWimwsREPf", 4);
        termToShard.put("wsNavvIcdk", 7);
        termToShard.put("xkWaPcCmpT", 0);
        termToShard.put("FKKTOnJMDy", 7);
        termToShard.put("RuLzobYixn", 2);
        termToShard.put("mFohLeFRvF", 4);
        termToShard.put("aAMXnamRJg", 7);
        termToShard.put("zKBMYJDmBI", 0);
        termToShard.put("ElSVuJQQuw", 7);
        termToShard.put("pezPtTQAAm", 7);
        termToShard.put("zBjjNEjAex", 2);
        termToShard.put("PGgHcLNPYX", 7);
        termToShard.put("hOkpeQqTDF", 3);
        termToShard.put("chZXraUPBH", 7);
        termToShard.put("FAIcSmmNXq", 5);
        termToShard.put("EZmDicyayC", 0);
        termToShard.put("GRIueBeIyL", 7);
        termToShard.put("qCChjGZYLp", 3);
        termToShard.put("IsSZQwwnUT", 3);
        termToShard.put("MGlxLFyyCK", 3);
        termToShard.put("YmscwrKSpB", 0);
        termToShard.put("czSljcjMop", 5);
        termToShard.put("XhfGWwNlng", 1);
        termToShard.put("cWpKJjlzgj", 7);
        termToShard.put("eDzIfMKbvk", 1);
        termToShard.put("WFFWYBfnTb", 0);
        termToShard.put("oDdHJxGxja", 7);
        termToShard.put("PDOQQqgIKE", 1);
        termToShard.put("bGEIEBLATe", 6);
        termToShard.put("xpRkJPWVpu", 2);
        termToShard.put("kTwZnPEeIi", 2);
        termToShard.put("DifcuqSsKk", 1);
        termToShard.put("CEmLmljpXe", 5);
        termToShard.put("cuNKtLtyJQ", 7);
        termToShard.put("yNjiAnxAmt", 5);
        termToShard.put("bVDJDCeaFm", 2);
        termToShard.put("vdnUhGLFtl", 0);
        termToShard.put("LnqSYezXbr", 5);
        termToShard.put("EzHgydDCSR", 3);
        termToShard.put("ZSKjhJlcpn", 1);
        termToShard.put("WRjUoZwtUz", 3);
        termToShard.put("RiBbcCdIgk", 4);
        termToShard.put("yizTqyjuDn", 4);
        termToShard.put("QnFjcpcZUT", 4);
        termToShard.put("agYhXYUUpl", 7);
        termToShard.put("UOjiTugjNC", 7);
        termToShard.put("nICGuWTdfV", 0);
        termToShard.put("NrnSmcnUVF", 2);
        termToShard.put("ZSzFcbpDqP", 3);
        termToShard.put("YOhahLSzzE", 5);
        termToShard.put("iWswCilUaT", 1);
        termToShard.put("zXAamKsRwj", 2);
        termToShard.put("aqGsrUPHFq", 5);
        termToShard.put("eDItImYWTS", 1);
        termToShard.put("JAYDZMRcpW", 4);
        termToShard.put("lmvAaEPflK", 7);
        termToShard.put("IKuOwPjKCx", 5);
        termToShard.put("schsINzlYB", 1);
        termToShard.put("OqbFNxrKrF", 2);
        termToShard.put("QrklDfvEJU", 6);
        termToShard.put("VLxKRKdLbx", 4);
        termToShard.put("imoydNTZhV", 1);
        termToShard.put("uFZyTyOMRO", 4);
        termToShard.put("nVAZVMPNNx", 3);
        termToShard.put("rPIdESYaAO", 5);
        termToShard.put("nbZWPWJsIM", 0);
        termToShard.put("wRZXPSoEgd", 3);
        termToShard.put("nGzpgwsSBc", 4);
        termToShard.put("AITyyoyLLs", 4);
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(8)
            .numberOfReplicas(1)
            .build();
        for (Map.Entry<String, Integer> entry : termToShard.entrySet()) {
            String key = entry.getKey();
            int shard = randomBoolean()
                ? OperationRouting.generateShardId(metadata, key, null)
                : OperationRouting.generateShardId(metadata, "foobar", key);
            assertEquals(shard, entry.getValue().intValue());
        }
    }

    public void testPreferNodes() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testPreferNodes");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            final ShardIterator it = new OperationRouting(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            ).getShards(clusterService.state(), indexName, 0, "_prefer_nodes:" + String.join(",", nodes));
            final List<ShardRouting> all = new ArrayList<>();
            ShardRouting shard;
            while ((shard = it.nextOrNull()) != null) {
                all.add(shard);
            }
            final Set<ShardRouting> preferred = new HashSet<>();
            preferred.addAll(all.subList(0, expected.size()));
            // the preferred shards should be at the front of the list
            assertThat(preferred, containsInAnyOrder(expected.toArray()));
            // verify all the shards are there
            assertThat(all.size(), equalTo(shards.size()));
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testFairSessionIdPreferences() throws InterruptedException, IOException {
        // Ensure that a user session is re-routed back to same nodes for
        // subsequent searches and that the nodes are selected fairly i.e.
        // given identically sorted lists of nodes across all shard IDs
        // each shard ID doesn't pick the same node.
        final int numIndices = randomIntBetween(1, 3);
        final int numShards = randomIntBetween(2, 10);
        final int numReplicas = randomIntBetween(1, 3);
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        final int numRepeatedSearches = 4;
        List<ShardRouting> sessionsfirstSearch = null;
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        String sessionKey = randomAlphaOfLength(10);
        for (int i = 0; i < numRepeatedSearches; i++) {
            List<ShardRouting> searchedShards = new ArrayList<>(numShards);
            Set<String> selectedNodes = new HashSet<>(numShards);
            final GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, sessionKey);

            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));
            for (ShardIterator shardIterator : groupIterator) {
                assertThat(shardIterator.size(), equalTo(numReplicas + 1));

                ShardRouting firstChoice = shardIterator.nextOrNull();
                assertNotNull(firstChoice);
                ShardRouting duelFirst = duelGetShards(state, firstChoice.shardId(), sessionKey).nextOrNull();
                assertThat("Regression test failure", duelFirst, equalTo(firstChoice));

                searchedShards.add(firstChoice);
                selectedNodes.add(firstChoice.currentNodeId());
            }
            if (sessionsfirstSearch == null) {
                sessionsfirstSearch = searchedShards;
            } else {
                assertThat("Sessions must reuse same replica choices", searchedShards, equalTo(sessionsfirstSearch));
            }

            // 2 is the bare minimum number of nodes we can reliably expect from
            // randomized tests in my experiments over thousands of iterations.
            // Ideally we would test for greater levels of machine utilisation
            // given a configuration with many nodes but the nature of hash
            // collisions means we can't always rely on optimal node usage in
            // all cases.
            assertThat("Search should use more than one of the nodes", selectedNodes.size(), greaterThan(1));
        }
    }

    // Regression test for the routing logic - implements same hashing logic
    private ShardIterator duelGetShards(ClusterState clusterState, ShardId shardId, String sessionId) {
        final IndexShardRoutingTable indexShard = clusterState.getRoutingTable().shardRoutingTable(shardId.getIndexName(), shardId.getId());
        int routingHash = Murmur3HashFunction.hash(sessionId);
        routingHash = 31 * routingHash + indexShard.shardId.hashCode();
        return indexShard.activeInitializingShardsIt(routingHash);
    }

    public void testThatOnlyNodesSupportNodeIds() throws InterruptedException, IOException {
        TestThreadPool threadPool = null;
        ClusterService clusterService = null;
        try {
            threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);
            final String indexName = "test";
            ClusterServiceUtils.setState(clusterService, ClusterStateCreationUtils.stateWithActivePrimary(indexName, true, randomInt(8)));
            final Index index = clusterService.state().metadata().index(indexName).getIndex();
            final List<ShardRouting> shards = clusterService.state().getRoutingNodes().assignedShards(new ShardId(index, 0));
            final int count = randomIntBetween(1, shards.size());
            int position = 0;
            final List<String> nodes = new ArrayList<>();
            final List<ShardRouting> expected = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                if (randomBoolean() && !shards.get(position).initializing()) {
                    nodes.add(shards.get(position).currentNodeId());
                    expected.add(shards.get(position));
                    position++;
                } else {
                    nodes.add("missing_" + i);
                }
            }
            if (expected.size() > 0) {
                final ShardIterator it = new OperationRouting(
                    Settings.EMPTY,
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                ).getShards(clusterService.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes));
                final List<ShardRouting> only = new ArrayList<>();
                ShardRouting shard;
                while ((shard = it.nextOrNull()) != null) {
                    only.add(shard);
                }
                assertThat(new HashSet<>(only), equalTo(new HashSet<>(expected)));
            } else {
                final ClusterService cs = clusterService;
                final IllegalArgumentException e = expectThrows(
                    IllegalArgumentException.class,
                    () -> new OperationRouting(
                        Settings.EMPTY,
                        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
                    ).getShards(cs.state(), indexName, 0, "_only_nodes:" + String.join(",", nodes))
                );
                if (nodes.size() == 1) {
                    assertThat(
                        e,
                        hasToString(
                            containsString("no data nodes with criteria [" + String.join(",", nodes) + "] found for shard: [test][0]")
                        )
                    );
                } else {
                    assertThat(
                        e,
                        hasToString(
                            containsString("no data nodes with criterion [" + String.join(",", nodes) + "] found for shard: [test][0]")
                        )
                    );
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testAdaptiveReplicaSelection() throws Exception {
        final int numIndices = 1;
        final int numShards = 1;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
        OperationRouting opRouting = new OperationRouting(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        opRouting.setUseAdaptiveReplicaSelection(true);
        List<ShardRouting> searchedShards = new ArrayList<>(numShards);
        Set<String> selectedNodes = new HashSet<>(numShards);
        TestThreadPool threadPool = new TestThreadPool("testThatOnlyNodesSupportNodeIds");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        Map<String, Long> outstandingRequests = new HashMap<>();
        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
            state,
            indexNames,
            null,
            null,
            collector,
            outstandingRequests,
            null
        );

        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // Test that the shards use a round-robin pattern when there are no stats
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));
        ShardRouting firstChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(firstChoice);
        searchedShards.add(firstChoice);
        selectedNodes.add(firstChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting secondChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(secondChoice);
        searchedShards.add(secondChoice);
        selectedNodes.add(secondChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        ShardRouting thirdChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(thirdChoice);
        searchedShards.add(thirdChoice);
        selectedNodes.add(thirdChoice.currentNodeId());

        // All three shards should have been separate, because there are no stats yet so they're all ranked equally.
        assertThat(searchedShards.size(), equalTo(3));

        // Now let's start adding node metrics, since that will affect which node is chosen
        collector.addNodeStatistics("node_0", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        collector.addNodeStatistics("node_1", 1, TimeValue.timeValueMillis(100).nanos(), TimeValue.timeValueMillis(50).nanos());
        collector.addNodeStatistics("node_2", 1, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(200).nanos());
        outstandingRequests.put("node_0", 1L);
        outstandingRequests.put("node_1", 1L);
        outstandingRequests.put("node_2", 1L);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        ShardRouting shardChoice = groupIterator.get(0).nextOrNull();
        // node 1 should be the lowest ranked node to start
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // node 1 starts getting more loaded...
        collector.addNodeStatistics("node_1", 2, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and more loaded...
        collector.addNodeStatistics("node_1", 3, TimeValue.timeValueMillis(250).nanos(), TimeValue.timeValueMillis(200).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        shardChoice = groupIterator.get(0).nextOrNull();
        assertThat(shardChoice.currentNodeId(), equalTo("node_1"));

        // and even more
        collector.addNodeStatistics("node_1", 4, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(250).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        shardChoice = groupIterator.get(0).nextOrNull();
        // finally, node 2 is chosen instead
        assertThat(shardChoice.currentNodeId(), equalTo("node_2"));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

    // Regression test to ignore awareness attributes. This test creates shards in different zones and simulates stress
    // on nodes in one zone to test if Adapative Replica Selection smartly routes the request to a node in different zone
    // by ignoring the zone awareness attributes.
    public void testAdaptiveReplicaSelectionWithZoneAwarenessIgnored() throws Exception {
        final int numIndices = 2;
        final int numShards = 1;
        final int numReplicas = 1;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }

        DiscoveryNode[] allNodes = setupNodes();
        ClusterState state = ClusterStateCreationUtils.state(allNodes[0], allNodes[3], allNodes);
        // Updates cluster state by assigning shard copies on nodes
        state = updateStatetoTestARS(indexNames, numShards, numReplicas, allNodes, state);

        Settings awarenessSetting = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build();
        TestThreadPool threadPool = new TestThreadPool("testThatOnlyNodesSupport");
        ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);

        OperationRouting opRouting = new OperationRouting(
            awarenessSetting,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        opRouting.setUseAdaptiveReplicaSelection(true);
        assertTrue(opRouting.ignoreAwarenessAttributes());
        List<ShardRouting> searchedShards = new ArrayList<>(numShards);
        Set<String> selectedNodes = new HashSet<>(numShards);
        ResponseCollectorService collector = new ResponseCollectorService(clusterService);
        Map<String, Long> outstandingRequests = new HashMap<>();

        GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
            state,
            indexNames,
            null,
            null,
            collector,
            outstandingRequests,
            null
        );
        assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

        // Test that the shards use a round-robin pattern when there are no stats
        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));

        ShardRouting firstChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(firstChoice);
        searchedShards.add(firstChoice);
        selectedNodes.add(firstChoice.currentNodeId());

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

        assertThat(groupIterator.size(), equalTo(numIndices * numShards));
        assertThat(groupIterator.get(0).size(), equalTo(numReplicas + 1));
        ShardRouting secondChoice = groupIterator.get(0).nextOrNull();
        assertNotNull(secondChoice);
        searchedShards.add(secondChoice);
        selectedNodes.add(secondChoice.currentNodeId());

        // All the shards should be ranked equally since there are no stats yet
        assertTrue(selectedNodes.contains("node_b2"));

        // Since the primary shards are divided randomly between node_a0 and node_a1
        assertTrue(selectedNodes.contains("node_a0") || selectedNodes.contains("node_a1"));

        // Now let's start adding node metrics, since that will affect which node is chosen. Adding more load to node_b2
        collector.addNodeStatistics("node_a0", 1, TimeValue.timeValueMillis(50).nanos(), TimeValue.timeValueMillis(50).nanos());
        collector.addNodeStatistics("node_a1", 20, TimeValue.timeValueMillis(100).nanos(), TimeValue.timeValueMillis(150).nanos());
        collector.addNodeStatistics("node_b2", 40, TimeValue.timeValueMillis(250).nanos(), TimeValue.timeValueMillis(250).nanos());
        outstandingRequests.put("node_a0", 1L);
        outstandingRequests.put("node_a1", 1L);
        outstandingRequests.put("node_b2", 1L);

        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        // node_a0 or node_a1 should be the lowest ranked node to start
        groupIterator.forEach(shardRoutings -> assertThat(shardRoutings.nextOrNull().currentNodeId(), containsString("node_a")));

        // Adding more load to node_a0
        collector.addNodeStatistics("node_a0", 10, TimeValue.timeValueMillis(200).nanos(), TimeValue.timeValueMillis(150).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

        // Adding more load to node_a0 and node_a1 from zone-a
        collector.addNodeStatistics("node_a1", 100, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(250).nanos());
        collector.addNodeStatistics("node_a0", 100, TimeValue.timeValueMillis(300).nanos(), TimeValue.timeValueMillis(250).nanos());
        groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);
        // ARS should pick node_b2 from zone-b since both node_a0 and node_a1 are overloaded
        groupIterator.forEach(shardRoutings -> assertThat(shardRoutings.nextOrNull().currentNodeId(), containsString("node_b")));

        IOUtils.close(clusterService);
        terminate(threadPool);
    }

    private ClusterState clusterStateForWeightedRouting(String[] indexNames, int numShards, int numReplicas) {
        DiscoveryNode[] allNodes = setUpNodesForWeightedRouting();
        ClusterState state = ClusterStateCreationUtils.state(allNodes[0], allNodes[6], allNodes);

        Map<String, List<DiscoveryNode>> discoveryNodeMap = new HashMap<>();
        List<DiscoveryNode> nodesZoneA = new ArrayList<>();
        nodesZoneA.add(allNodes[0]);
        nodesZoneA.add(allNodes[1]);

        List<DiscoveryNode> nodesZoneB = new ArrayList<>();
        nodesZoneB.add(allNodes[2]);
        nodesZoneB.add(allNodes[3]);

        List<DiscoveryNode> nodesZoneC = new ArrayList<>();
        nodesZoneC.add(allNodes[4]);
        nodesZoneC.add(allNodes[5]);
        discoveryNodeMap.put("a", nodesZoneA);
        discoveryNodeMap.put("b", nodesZoneB);
        discoveryNodeMap.put("c", nodesZoneC);

        // Updating cluster state with node, index and shard details
        state = updateStatetoTestWeightedRouting(indexNames, numShards, numReplicas, state, discoveryNodeMap);

        return state;

    }

    private ClusterState setWeightedRoutingWeights(ClusterState clusterState, Map<String, Double> weights) {
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, 0);
        Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
        metadataBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
        clusterState = ClusterState.builder(clusterState).metadata(metadataBuilder).build();
        return clusterState;
    }

    public void testWeightedOperationRouting() throws Exception {
        final int numIndices = 2;
        final int numShards = 3;
        final int numReplicas = 2;
        // setting up indices
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        ClusterService clusterService = null;
        TestThreadPool threadPool = null;
        try {
            ClusterState state = clusterStateForWeightedRouting(indexNames, numShards, numReplicas);

            Settings setting = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            OperationRouting opRouting = new OperationRouting(
                setting,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            assertTrue(opRouting.ignoreAwarenessAttributes());
            Set<String> selectedNodes = new HashSet<>();
            ResponseCollectorService collector = new ResponseCollectorService(clusterService);
            Map<String, Long> outstandingRequests = new HashMap<>();

            // Setting up weights for weighted round-robin in cluster state
            Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
            state = setWeightedRoutingWeights(state, weights);

            ClusterState.Builder builder = ClusterState.builder(state);
            ClusterServiceUtils.setState(clusterService, builder);

            // search shards call
            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
                state,
                indexNames,
                null,
                null,
                collector,
                outstandingRequests,
                null
            );

            for (ShardIterator it : groupIterator) {
                List<ShardRouting> shardRoutings = Collections.singletonList(it.nextOrNull());
                for (ShardRouting shardRouting : shardRoutings) {
                    selectedNodes.add(shardRouting.currentNodeId());
                }
            }
            // tests no shards are assigned to nodes in zone c
            for (String nodeID : selectedNodes) {
                // No shards are assigned to nodes in zone c since its weight is 0
                assertFalse(nodeID.contains("c"));
            }

            selectedNodes = new HashSet<>();
            setting = Settings.builder().put("cluster.routing.allocation.awareness.attributes", "zone").build();

            // Updating weighted round robin weights in cluster state
            weights = Map.of("a", 1.0, "b", 0.0, "c", 1.0);
            state = setWeightedRoutingWeights(state, weights);

            builder = ClusterState.builder(state);
            ClusterServiceUtils.setState(clusterService, builder);

            opRouting = new OperationRouting(setting, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

            // search shards call
            groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

            for (ShardIterator it : groupIterator) {
                List<ShardRouting> shardRoutings = Collections.singletonList(it.nextOrNull());
                for (ShardRouting shardRouting : shardRoutings) {
                    selectedNodes.add(shardRouting.currentNodeId());
                }
            }
            // tests that no shards are assigned to zone with weight zero
            for (String nodeID : selectedNodes) {
                // No shards are assigned to nodes in zone b since its weight is 0
                assertFalse(nodeID.contains("b"));
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testWeightedOperationRoutingWeightUndefinedForOneZone() throws Exception {
        final int numIndices = 2;
        final int numShards = 3;
        final int numReplicas = 2;
        // setting up indices
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }

        ClusterService clusterService = null;
        TestThreadPool threadPool = null;
        try {
            ClusterState state = clusterStateForWeightedRouting(indexNames, numShards, numReplicas);

            Settings setting = Settings.builder()
                .put("cluster.routing.allocation.awareness.attributes", "zone")
                .put("cluster.routing.allocation.awareness.force.zone.values", "a,b,c")
                .put("cluster.routing.weighted.fail_open", false)
                .build();

            threadPool = new TestThreadPool("testThatOnlyNodesSupport");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            OperationRouting opRouting = new OperationRouting(
                setting,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            assertTrue(opRouting.ignoreAwarenessAttributes());
            Set<String> selectedNodes = new HashSet<>();
            ResponseCollectorService collector = new ResponseCollectorService(clusterService);
            Map<String, Long> outstandingRequests = new HashMap<>();

            // Setting up weights for weighted round-robin in cluster state, weight for nodes in zone b is not set
            Map<String, Double> weights = Map.of("a", 1.0, "c", 0.0);
            state = setWeightedRoutingWeights(state, weights);
            ClusterServiceUtils.setState(clusterService, ClusterState.builder(state));

            // search shards call
            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(
                state,
                indexNames,
                null,
                null,
                collector,
                outstandingRequests,
                null
            );

            for (ShardIterator it : groupIterator) {
                while (it.remaining() > 0) {
                    ShardRouting shardRouting = it.nextOrNull();
                    assertNotNull(shardRouting);
                    selectedNodes.add(shardRouting.currentNodeId());
                }
            }
            boolean weighAwayNodesInUndefinedZone = true;
            // tests no shards are assigned to nodes in zone c
            // tests shards are assigned to nodes in zone b
            for (String nodeID : selectedNodes) {
                // shard from nodes in zone c is not selected since its weight is 0
                assertFalse(nodeID.contains("c"));
                if (nodeID.contains("b")) {
                    weighAwayNodesInUndefinedZone = false;
                }
            }
            assertFalse(weighAwayNodesInUndefinedZone);

            selectedNodes = new HashSet<>();

            // Updating weighted round-robin weights in cluster state
            weights = Map.of("a", 0.0, "b", 1.0);

            state = setWeightedRoutingWeights(state, weights);
            ClusterServiceUtils.setState(clusterService, ClusterState.builder(state));

            opRouting = new OperationRouting(setting, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));

            // search shards call
            groupIterator = opRouting.searchShards(state, indexNames, null, null, collector, outstandingRequests, null);

            for (ShardIterator it : groupIterator) {
                while (it.remaining() > 0) {
                    ShardRouting shardRouting = it.nextOrNull();
                    assertNotNull(shardRouting);
                    selectedNodes.add(shardRouting.currentNodeId());
                }
            }

            // tests that no shards are assigned to zone with weight zero
            // tests shards are assigned to nodes in zone c
            weighAwayNodesInUndefinedZone = true;
            for (String nodeID : selectedNodes) {
                // shard from nodes in zone a is not selected since its weight is 0
                assertFalse(nodeID.contains("a"));
                if (nodeID.contains("c")) {
                    weighAwayNodesInUndefinedZone = false;
                }
            }
            assertFalse(weighAwayNodesInUndefinedZone);
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testSearchableSnapshotPrimaryDefault() throws Exception {
        final int numIndices = 1;
        final int numShards = 2;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        // The first index is a searchable snapshot index
        final String searchableSnapshotIndex = indexNames[0];
        ClusterService clusterService = null;
        ThreadPool threadPool = null;

        try {
            OperationRouting opRouting = new OperationRouting(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );

            ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
            threadPool = new TestThreadPool("testSearchableSnapshotPreference");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            // Update the index config within the cluster state to modify the index to a searchable snapshot index
            IndexMetadata searchableSnapshotIndexMetadata = IndexMetadata.builder(searchableSnapshotIndex)
                .settings(
                    Settings.builder()
                        .put(state.metadata().index(searchableSnapshotIndex).getSettings())
                        .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.REMOTE_SNAPSHOT.getSettingsKey())
                        .build()
                )
                .build();
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata())
                .put(searchableSnapshotIndexMetadata, false)
                .generateClusterUuidIfNeeded();
            state = ClusterState.builder(state).metadata(metadataBuilder.build()).build();

            // Verify default preference is primary only
            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, null);
            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

            for (ShardIterator shardIterator : groupIterator) {
                assertThat("Only single shard will be returned with no preference", shardIterator.size(), equalTo(1));
                assertTrue("Only primary should exist with no preference", shardIterator.nextOrNull().primary());
            }

            // Verify alternative preference can be applied to a searchable snapshot index
            groupIterator = opRouting.searchShards(state, indexNames, null, "_replica");
            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

            for (ShardIterator shardIterator : groupIterator) {
                assertThat("Replica shards will be returned", shardIterator.size(), equalTo(numReplicas));
                assertFalse("Returned shard should be a replica", shardIterator.nextOrNull().primary());
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    @LockFeatureFlag(WRITABLE_WARM_INDEX_EXPERIMENTAL_FLAG)
    @SuppressForbidden(reason = "feature flag overrides")
    public void testPartialIndexPrimaryDefault() throws Exception {
        final int numIndices = 1;
        final int numShards = 2;
        final int numReplicas = 2;
        final String[] indexNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indexNames[i] = "test" + i;
        }
        // The first index is a partial index
        final String indexName = indexNames[0];
        ClusterService clusterService = null;
        ThreadPool threadPool = null;

        try {
            OperationRouting opRouting = new OperationRouting(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );

            ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(indexNames, numShards, numReplicas);
            threadPool = new TestThreadPool("testPartialIndexPrimaryDefault");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            // Update the index config within the cluster state to modify the index to a partial index
            IndexMetadata partialIndexMetadata = IndexMetadata.builder(indexName)
                .settings(
                    Settings.builder()
                        .put(state.metadata().index(indexName).getSettings())
                        .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL)
                        .build()
                )
                .build();
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata())
                .put(partialIndexMetadata, false)
                .generateClusterUuidIfNeeded();
            state = ClusterState.builder(state).metadata(metadataBuilder.build()).build();

            // Verify default preference is primary only
            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, null);
            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

            for (ShardIterator shardIterator : groupIterator) {
                assertTrue("Only primary should exist with no preference", shardIterator.nextOrNull().primary());
            }

            // Verify alternative preference can be applied to a partial index
            groupIterator = opRouting.searchShards(state, indexNames, null, "_replica");
            assertThat("One group per index shard", groupIterator.size(), equalTo(numIndices * numShards));

            for (ShardIterator shardIterator : groupIterator) {
                assertThat("Replica shards will be returned", shardIterator.size(), equalTo(numReplicas));
                assertFalse("Returned shard should be a replica", shardIterator.nextOrNull().primary());
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testSearchReplicaDefaultRouting() throws Exception {
        final int numShards = 1;
        final int numReplicas = 2;
        final int numSearchReplicas = 2;
        final String indexName = "test";
        final String[] indexNames = new String[] { indexName };

        ClusterService clusterService = null;
        ThreadPool threadPool = null;

        try {
            OperationRouting opRouting = new OperationRouting(
                Settings.builder().build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );

            ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
                indexNames,
                numShards,
                numReplicas,
                numSearchReplicas
            );
            IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().index(indexName).getShards().get(0);
            ShardId shardId = indexShardRoutingTable.searchOnlyReplicas().get(0).shardId();

            threadPool = new TestThreadPool("testSearchReplicaDefaultRouting");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            // add a search replica in initializing state:
            DiscoveryNode node = new DiscoveryNode(
                "node_initializing",
                OpenSearchTestCase.buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
                Version.CURRENT
            );

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(Settings.builder().put(state.metadata().index(indexName).getSettings()).build())
                .numberOfSearchReplicas(3)
                .numberOfReplicas(2)
                .build();
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(indexMetadata, false).generateClusterUuidIfNeeded();
            IndexRoutingTable.Builder indexShardRoutingBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            indexShardRoutingBuilder.addIndexShard(indexShardRoutingTable);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(shardId, node.getId(), null, false, true, ShardRoutingState.INITIALIZING, null)
            );
            state = ClusterState.builder(state)
                .routingTable(RoutingTable.builder().add(indexShardRoutingBuilder).build())
                .metadata(metadataBuilder.build())
                .build();

            // Verify default preference is primary only
            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, null);
            assertThat("one group per shard", groupIterator.size(), equalTo(numShards));
            for (ShardIterator shardIterator : groupIterator) {
                assertEquals("We should have 3 shards returned", shardIterator.size(), 3);
                int i = 0;
                for (ShardRouting shardRouting : shardIterator) {
                    assertTrue(
                        "Only search replicas should exist with preference SEARCH_REPLICA",
                        shardIterator.nextOrNull().isSearchOnly()
                    );
                    if (i == shardIterator.size()) {
                        assertTrue("Initializing shard should appear last", shardRouting.initializing());
                        assertFalse("Initializing shard should appear last", shardRouting.active());
                    }
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    public void testSearchReplicaRoutingWhenSearchOnlyStrictSettingIsFalse() throws Exception {
        final int numShards = 1;
        final int numReplicas = 2;
        final int numSearchReplicas = 2;
        final String indexName = "test";
        final String[] indexNames = new String[] { indexName };

        ClusterService clusterService = null;
        ThreadPool threadPool = null;

        try {
            OperationRouting opRouting = new OperationRouting(
                Settings.builder().build(),
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            opRouting.setStrictSearchOnlyShardRouting(false);

            ClusterState state = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
                indexNames,
                numShards,
                numReplicas,
                numSearchReplicas
            );
            IndexShardRoutingTable indexShardRoutingTable = state.getRoutingTable().index(indexName).getShards().get(0);
            ShardId shardId = indexShardRoutingTable.searchOnlyReplicas().get(0).shardId();

            threadPool = new TestThreadPool("testSearchReplicaDefaultRouting");
            clusterService = ClusterServiceUtils.createClusterService(threadPool);

            // add a search replica in initializing state:
            DiscoveryNode node = new DiscoveryNode(
                "node_initializing",
                OpenSearchTestCase.buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES),
                Version.CURRENT
            );

            IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
                .settings(Settings.builder().put(state.metadata().index(indexName).getSettings()).build())
                .numberOfSearchReplicas(3)
                .numberOfReplicas(2)
                .build();
            Metadata.Builder metadataBuilder = Metadata.builder(state.metadata()).put(indexMetadata, false).generateClusterUuidIfNeeded();
            IndexRoutingTable.Builder indexShardRoutingBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            indexShardRoutingBuilder.addIndexShard(indexShardRoutingTable);
            indexShardRoutingBuilder.addShard(
                TestShardRouting.newShardRouting(shardId, node.getId(), null, false, true, ShardRoutingState.INITIALIZING, null)
            );
            state = ClusterState.builder(state)
                .routingTable(RoutingTable.builder().add(indexShardRoutingBuilder).build())
                .metadata(metadataBuilder.build())
                .build();

            GroupShardsIterator<ShardIterator> groupIterator = opRouting.searchShards(state, indexNames, null, null);
            assertThat("one group per shard", groupIterator.size(), equalTo(numShards));
            for (ShardIterator shardIterator : groupIterator) {
                assertEquals("We should have all 6 shards returned", shardIterator.size(), 6);
                for (ShardRouting shardRouting : shardIterator) {
                    assertTrue(
                        "Any shard can exist with when cluster.routing.search_replica.strict is set as false",
                        shardRouting.isSearchOnly() || shardRouting.primary() || shardRouting.isSearchOnly() == false
                    );
                }
            }
        } finally {
            IOUtils.close(clusterService);
            terminate(threadPool);
        }
    }

    private DiscoveryNode[] setupNodes() {
        // Sets up two data nodes in zone-a and one data node in zone-b
        List<String> zones = Arrays.asList("a", "a", "b");
        DiscoveryNode[] allNodes = new DiscoveryNode[4];
        int i = 0;
        for (String zone : zones) {
            DiscoveryNode node = new DiscoveryNode(
                "node_" + zone + i,
                buildNewFakeTransportAddress(),
                singletonMap("zone", zone),
                Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            allNodes[i++] = node;
        }
        DiscoveryNode clusterManager = new DiscoveryNode(
            "cluster-manager",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        allNodes[i] = clusterManager;
        return allNodes;
    }

    private DiscoveryNode[] setUpNodesForWeightedRouting() {
        List<String> zones = Arrays.asList("a", "a", "b", "b", "c", "c");
        DiscoveryNode[] allNodes = new DiscoveryNode[7];
        int i = 0;
        for (String zone : zones) {
            DiscoveryNode node = new DiscoveryNode(
                "node_" + zone + "_" + i,
                buildNewFakeTransportAddress(),
                singletonMap("zone", zone),
                Collections.singleton(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            allNodes[i++] = node;
        }

        DiscoveryNode clusterManager = new DiscoveryNode(
            "cluster-manager",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        allNodes[i] = clusterManager;
        return allNodes;
    }

    public void testAllocationAwarenessDeprecation() {
        OperationRouting routing = new OperationRouting(
            Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "foo")
                .build(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    /**
     * The following setup is created to test ARS
     */
    private ClusterState updateStatetoTestARS(
        String[] indices,
        int numberOfShards,
        int numberOfReplicas,
        DiscoveryNode[] nodes,
        ClusterState state
    ) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        ClusterState.Builder clusterState = ClusterState.builder(state);

        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                        .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                )
                .build();
            metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(index, "_na_", i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                // Assign all the primary shards on nodes in zone-a (node_a0 or node_a1)
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(index, i, nodes[randomInt(1)].getId(), null, true, ShardRoutingState.STARTED)
                );
                for (int replica = 0; replica < numberOfReplicas; replica++) {
                    // Assign all the replicas on nodes in zone-b (node_b2)
                    indexShardRoutingBuilder.addShard(
                        TestShardRouting.newShardRouting(index, i, nodes[2].getId(), null, false, ShardRoutingState.STARTED)
                    );
                }
                indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        clusterState.metadata(metadataBuilder);
        clusterState.routingTable(routingTableBuilder.build());
        return clusterState.build();
    }

    private ClusterState updateStatetoTestWeightedRouting(
        String[] indices,
        int numberOfShards,
        int numberOfReplicas,
        ClusterState state,
        Map<String, List<DiscoveryNode>> discoveryNodeMap
    ) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Metadata.Builder metadataBuilder = Metadata.builder();
        ClusterState.Builder clusterState = ClusterState.builder(state);
        List<DiscoveryNode> nodesZoneA = discoveryNodeMap.get("a");
        List<DiscoveryNode> nodesZoneB = discoveryNodeMap.get("b");
        List<DiscoveryNode> nodesZoneC = discoveryNodeMap.get("c");
        for (String index : indices) {
            IndexMetadata indexMetadata = IndexMetadata.builder(index)
                .settings(
                    Settings.builder()
                        .put(SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(SETTING_NUMBER_OF_SHARDS, numberOfShards)
                        .put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
                        .put(SETTING_CREATION_DATE, System.currentTimeMillis())
                )
                .build();
            metadataBuilder.put(indexMetadata, false).generateClusterUuidIfNeeded();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(indexMetadata.getIndex());
            for (int i = 0; i < numberOfShards; i++) {
                final ShardId shardId = new ShardId(index, "_na_", i);
                IndexShardRoutingTable.Builder indexShardRoutingBuilder = new IndexShardRoutingTable.Builder(shardId);
                // Assign all the primary shards on nodes in zone-a (node_a0 or node_a1)
                indexShardRoutingBuilder.addShard(
                    TestShardRouting.newShardRouting(
                        index,
                        i,
                        nodesZoneA.get(randomInt(nodesZoneA.size() - 1)).getId(),
                        null,
                        true,
                        ShardRoutingState.STARTED
                    )
                );
                for (int replica = 0; replica < numberOfReplicas; replica++) {
                    // Assign all the replicas on nodes in zone-b (node_b2)
                    String nodeId = "";
                    if (replica == 0) {
                        nodeId = nodesZoneB.get(randomInt(nodesZoneB.size() - 1)).getId();
                    } else {
                        nodeId = nodesZoneC.get(randomInt(nodesZoneC.size() - 1)).getId();
                    }
                    indexShardRoutingBuilder.addShard(
                        TestShardRouting.newShardRouting(index, i, nodeId, null, false, ShardRoutingState.STARTED)
                    );
                }
                indexRoutingTableBuilder.addIndexShard(indexShardRoutingBuilder.build());
            }
            routingTableBuilder.add(indexRoutingTableBuilder.build());
        }
        // add weighted routing weights in metadata
        Map<String, Double> weights = Map.of("a", 1.0, "b", 1.0, "c", 0.0);
        WeightedRouting weightedRouting = new WeightedRouting("zone", weights);
        WeightedRoutingMetadata weightedRoutingMetadata = new WeightedRoutingMetadata(weightedRouting, 0);
        metadataBuilder.putCustom(WeightedRoutingMetadata.TYPE, weightedRoutingMetadata);
        clusterState.metadata(metadataBuilder);
        clusterState.routingTable(routingTableBuilder.build());
        return clusterState.build();
    }
}
