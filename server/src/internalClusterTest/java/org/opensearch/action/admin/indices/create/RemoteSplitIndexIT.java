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

package org.opensearch.action.admin.indices.create;

import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.Constants;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteSplitIndexIT extends RemoteStoreBaseIntegTestCase {
    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @After
    public void cleanUp() throws Exception {
        // Delete is async.
        assertAcked(
            client().admin().indices().prepareDelete("*").setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN).get()
        );
        // With pinned timestamp, we can have tlog files even after deletion.
        if (RemoteStoreSettings.isPinnedTimestampsEnabled() == false) {
            assertBusy(() -> {
                try {
                    assertEquals(0, getFileCount(translogRepoPath));
                } catch (IOException e) {
                    fail();
                }
            }, 30, TimeUnit.SECONDS);
        }
        super.teardown();
    }

    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
    }

    public void testCreateSplitIndexToN() throws IOException {
        int[][] possibleShardSplits = new int[][] { { 2, 4, 8 }, { 3, 6, 12 }, { 1, 2, 4 } };
        int[] shardSplits = randomFrom(possibleShardSplits);
        splitToN(shardSplits[0], shardSplits[1], shardSplits[2]);
    }

    public void testSplitFromOneToN() {

        assumeFalse("https://github.com/elastic/elasticsearch/issues/34080", Constants.WINDOWS);

        splitToN(1, 5, 10);
        client().admin().indices().prepareDelete("*").get();
        int randomSplit = randomIntBetween(2, 6);
        splitToN(1, randomSplit, randomSplit * 2);
    }

    private void splitToN(int sourceShards, int firstSplitShards, int secondSplitShards) {

        assertEquals(sourceShards, (sourceShards * firstSplitShards) / firstSplitShards);
        assertEquals(firstSplitShards, (firstSplitShards * secondSplitShards) / secondSplitShards);
        internalCluster().ensureAtLeastNumDataNodes(2);
        final boolean useRouting = randomBoolean();
        final boolean useNested = randomBoolean();
        final boolean useMixedRouting = useRouting ? randomBoolean() : false;
        CreateIndexRequestBuilder createInitialIndex = prepareCreate("source");
        Settings.Builder settings = Settings.builder().put(indexSettings()).put("number_of_shards", sourceShards);
        final boolean useRoutingPartition;
        if (randomBoolean()) {
            // randomly set the value manually
            int routingShards = secondSplitShards * randomIntBetween(1, 10);
            settings.put("index.number_of_routing_shards", routingShards);
            useRoutingPartition = false;
        } else {
            useRoutingPartition = randomBoolean();
        }
        if (useRouting && useMixedRouting == false && useRoutingPartition) {
            int numRoutingShards = MetadataCreateIndexService.calculateNumRoutingShards(secondSplitShards, Version.CURRENT) - 1;
            settings.put("index.routing_partition_size", randomIntBetween(1, numRoutingShards));
            if (useNested) {
                createInitialIndex.setMapping("_routing", "required=true", "nested1", "type=nested");
            } else {
                createInitialIndex.setMapping("_routing", "required=true");
            }
        } else if (useNested) {
            createInitialIndex.setMapping("nested1", "type=nested");
        }
        logger.info("use routing {} use mixed routing {} use nested {}", useRouting, useMixedRouting, useNested);
        createInitialIndex.setSettings(settings).get();

        int numDocs = randomIntBetween(10, 50);
        String[] routingValue = new String[numDocs];

        BiFunction<String, Integer, IndexRequestBuilder> indexFunc = (index, id) -> {
            try {
                return client().prepareIndex(index)
                    .setId(Integer.toString(id))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("foo", "bar")
                            .field("i", id)
                            .startArray("nested1")
                            .startObject()
                            .field("n_field1", "n_value1_1")
                            .field("n_field2", "n_value2_1")
                            .endObject()
                            .startObject()
                            .field("n_field1", "n_value1_2")
                            .field("n_field2", "n_value2_2")
                            .endObject()
                            .endArray()
                            .endObject()
                    );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
        for (int i = 0; i < numDocs; i++) {
            IndexRequestBuilder builder = indexFunc.apply("source", i);
            if (useRouting) {
                String routing = randomRealisticUnicodeOfCodepointLengthBetween(1, 10);
                if (useMixedRouting && randomBoolean()) {
                    routingValue[i] = null;
                } else {
                    routingValue[i] = routing;
                }
                builder.setRouting(routingValue[i]);
            }
            builder.get();
        }

        if (randomBoolean()) {
            for (int i = 0; i < numDocs; i++) { // let's introduce some updates / deletes on the index
                if (randomBoolean()) {
                    IndexRequestBuilder builder = indexFunc.apply("source", i);
                    if (useRouting) {
                        builder.setRouting(routingValue[i]);
                    }
                    builder.get();
                }
            }
        }

        ensureYellow();
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.blocks.write", true)).get();
        ensureGreen();
        Settings.Builder firstSplitSettingsBuilder = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", firstSplitShards)
            .putNull("index.blocks.write");
        if (sourceShards == 1 && useRoutingPartition == false && randomBoolean()) { // try to set it if we have a source index with 1 shard
            firstSplitSettingsBuilder.put("index.number_of_routing_shards", secondSplitShards);
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "first_split")
                .setResizeType(ResizeType.SPLIT)
                .setSettings(firstSplitSettingsBuilder.build())
                .get()
        );
        ensureGreen();
        assertHitCount(client().prepareSearch("first_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);

        for (int i = 0; i < numDocs; i++) { // now update
            IndexRequestBuilder builder = indexFunc.apply("first_split", i);
            if (useRouting) {
                builder.setRouting(routingValue[i]);
            }
            builder.get();
        }
        flushAndRefresh();
        assertHitCount(client().prepareSearch("first_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        for (int i = 0; i < numDocs; i++) {
            GetResponse getResponse = client().prepareGet("first_split", Integer.toString(i)).setRouting(routingValue[i]).get();
            assertTrue(getResponse.isExists());
        }

        client().admin()
            .indices()
            .prepareUpdateSettings("first_split")
            .setSettings(Settings.builder().put("index.blocks.write", true))
            .get();
        ensureGreen();
        // now split source into a new index
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("first_split", "second_split")
                .setResizeType(ResizeType.SPLIT)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put("index.number_of_shards", secondSplitShards)
                        .putNull("index.blocks.write")
                        .build()
                )
                .get()
        );
        ensureGreen();
        assertHitCount(client().prepareSearch("second_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        // let it be allocated anywhere and bump replicas
        client().admin()
            .indices()
            .prepareUpdateSettings("second_split")
            .setSettings(Settings.builder().put("index.number_of_replicas", 0))
            .get();
        ensureGreen();
        assertHitCount(client().prepareSearch("second_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);

        for (int i = 0; i < numDocs; i++) { // now update
            IndexRequestBuilder builder = indexFunc.apply("second_split", i);
            if (useRouting) {
                builder.setRouting(routingValue[i]);
            }
            builder.get();
        }
        flushAndRefresh();
        for (int i = 0; i < numDocs; i++) {
            GetResponse getResponse = client().prepareGet("second_split", Integer.toString(i)).setRouting(routingValue[i]).get();
            assertTrue(getResponse.isExists());
        }
        assertHitCount(client().prepareSearch("second_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        assertHitCount(client().prepareSearch("first_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        assertHitCount(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
        if (useNested) {
            assertNested("source", numDocs);
            assertNested("first_split", numDocs);
            assertNested("second_split", numDocs);
        }
        assertAllUniqueDocs(
            client().prepareSearch("second_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
            numDocs
        );
        assertAllUniqueDocs(
            client().prepareSearch("first_split").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
            numDocs
        );
        assertAllUniqueDocs(client().prepareSearch("source").setSize(100).setQuery(new TermsQueryBuilder("foo", "bar")).get(), numDocs);
    }

    public void assertNested(String index, int numDocs) {
        // now, do a nested query
        SearchResponse searchResponse = client().prepareSearch(index)
            .setQuery(nestedQuery("nested1", termQuery("nested1.n_field1", "n_value1_1"), ScoreMode.Avg))
            .get();
        assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numDocs));
    }

    public void assertAllUniqueDocs(SearchResponse response, int numDocs) {
        Set<String> ids = new HashSet<>();
        for (int i = 0; i < response.getHits().getHits().length; i++) {
            String id = response.getHits().getHits()[i].getId();
            assertTrue("found ID " + id + " more than once", ids.add(id));
        }
        assertEquals(numDocs, ids.size());
    }

    public void testSplitIndexPrimaryTerm() throws Exception {
        int numberOfTargetShards = randomIntBetween(2, 20);
        int numberOfShards = randomValueOtherThanMany(n -> numberOfTargetShards % n != 0, () -> between(1, numberOfTargetShards - 1));
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate("source").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("number_of_shards", numberOfShards)
                .put("index.number_of_routing_shards", numberOfTargetShards)
        ).get();
        ensureGreen(TimeValue.timeValueSeconds(120)); // needs more than the default to allocate many shards

        // fail random primary shards to force primary terms to increase
        final Index source = resolveIndex("source");
        final int iterations = scaledRandomIntBetween(0, 16);
        for (int i = 0; i < iterations; i++) {
            final String node = randomSubsetOf(1, internalCluster().nodesInclude("source")).get(0);
            final IndicesService indexServices = internalCluster().getInstance(IndicesService.class, node);
            final IndexService indexShards = indexServices.indexServiceSafe(source);
            for (final Integer shardId : indexShards.shardIds()) {
                final IndexShard shard = indexShards.getShard(shardId);
                if (shard.routingEntry().primary() && randomBoolean()) {
                    disableAllocation("source");
                    shard.failShard("test", new Exception("test"));
                    // this can not succeed until the shard is failed and a replica is promoted
                    int id = 0;
                    while (true) {
                        // find an ID that routes to the right shard, we will only index to the shard that saw a primary failure
                        final String s = Integer.toString(id);
                        final int hash = Math.floorMod(Murmur3HashFunction.hash(s), numberOfShards);
                        if (hash == shardId) {
                            final IndexRequest request = new IndexRequest("source").id(s)
                                .source("{ \"f\": \"" + s + "\"}", MediaTypeRegistry.JSON);
                            client().index(request).get();
                            break;
                        } else {
                            id++;
                        }
                    }
                    enableAllocation("source");
                    ensureGreen();
                }
            }
        }

        final Settings.Builder prepareSplitSettings = Settings.builder().put("index.blocks.write", true);
        client().admin().indices().prepareUpdateSettings("source").setSettings(prepareSplitSettings).get();
        ensureYellow();

        final IndexMetadata indexMetadata = indexMetadata(client(), "source");
        final long beforeSplitPrimaryTerm = IntStream.range(0, numberOfShards).mapToLong(indexMetadata::primaryTerm).max().getAsLong();

        // now split source into target
        final Settings splitSettings = Settings.builder()
            .put("index.number_of_replicas", 0)
            .put("index.number_of_shards", numberOfTargetShards)
            .putNull("index.blocks.write")
            .build();
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex("source", "target")
                .setResizeType(ResizeType.SPLIT)
                .setSettings(splitSettings)
                .get()
        );

        ensureGreen(TimeValue.timeValueSeconds(120)); // needs more than the default to relocate many shards

        final IndexMetadata aftersplitIndexMetadata = indexMetadata(client(), "target");
        for (int shardId = 0; shardId < numberOfTargetShards; shardId++) {
            assertThat(aftersplitIndexMetadata.primaryTerm(shardId), equalTo(beforeSplitPrimaryTerm + 1));
        }
    }

    private static IndexMetadata indexMetadata(final Client client, final String index) {
        final ClusterStateResponse clusterStateResponse = client.admin().cluster().state(new ClusterStateRequest()).actionGet();
        return clusterStateResponse.getState().metadata().index(index);
    }

    public void testCreateSplitIndex() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        prepareCreate("source").setSettings(
            Settings.builder().put(indexSettings()).put("number_of_shards", 1).put("index.version.created", version)
        ).get();
        final int docs = randomIntBetween(0, 128);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("source").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", MediaTypeRegistry.JSON).get();
        }
        // ensure all shards are allocated otherwise the ensure green below might not succeed since we require the merge node
        // if we change the setting too quickly we will end up with one replica unassigned which can't be assigned anymore due
        // to the require._name below.
        ensureGreen();
        // relocate all shards to one node such that we can merge it.
        client().admin().indices().prepareUpdateSettings("source").setSettings(Settings.builder().put("index.blocks.write", true)).get();
        ensureGreen();

        final IndicesStatsResponse sourceStats = client().admin().indices().prepareStats("source").setSegments(true).get();

        // disable rebalancing to be able to capture the right stats. balancing can move the target primary
        // making it hard to pin point the source shards.
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), "none"))
            .get();
        try {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareResizeIndex("source", "target")
                    .setResizeType(ResizeType.SPLIT)
                    .setSettings(
                        Settings.builder()
                            .put("index.number_of_replicas", 0)
                            .put("index.number_of_shards", 2)
                            .putNull("index.blocks.write")
                            .build()
                    )
                    .get()
            );
            ensureGreen();

            final ClusterState state = client().admin().cluster().prepareState().get().getState();
            DiscoveryNode mergeNode = state.nodes().get(state.getRoutingTable().index("target").shard(0).primaryShard().currentNodeId());
            logger.info("split node {}", mergeNode);

            final long maxSeqNo = Arrays.stream(sourceStats.getShards())
                .filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId()))
                .map(ShardStats::getSeqNoStats)
                .mapToLong(SeqNoStats::getMaxSeqNo)
                .max()
                .getAsLong();
            final long maxUnsafeAutoIdTimestamp = Arrays.stream(sourceStats.getShards())
                .filter(shard -> shard.getShardRouting().currentNodeId().equals(mergeNode.getId()))
                .map(ShardStats::getStats)
                .map(CommonStats::getSegments)
                .mapToLong(SegmentsStats::getMaxUnsafeAutoIdTimestamp)
                .max()
                .getAsLong();

            final IndicesStatsResponse targetStats = client().admin().indices().prepareStats("target").get();
            for (final ShardStats shardStats : targetStats.getShards()) {
                final SeqNoStats seqNoStats = shardStats.getSeqNoStats();
                final ShardRouting shardRouting = shardStats.getShardRouting();
                assertThat("failed on " + shardRouting, seqNoStats.getMaxSeqNo(), equalTo(maxSeqNo));
                assertThat("failed on " + shardRouting, seqNoStats.getLocalCheckpoint(), equalTo(maxSeqNo));
                assertThat(
                    "failed on " + shardRouting,
                    shardStats.getStats().getSegments().getMaxUnsafeAutoIdTimestamp(),
                    equalTo(maxUnsafeAutoIdTimestamp)
                );
            }

            final int size = docs > 0 ? 2 * docs : 1;
            assertHitCount(client().prepareSearch("target").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);

            for (int i = docs; i < 2 * docs; i++) {
                client().prepareIndex("target").setSource("{\"foo\" : \"bar\", \"i\" : " + i + "}", MediaTypeRegistry.JSON).get();
            }
            flushAndRefresh();
            assertHitCount(
                client().prepareSearch("target").setSize(2 * size).setQuery(new TermsQueryBuilder("foo", "bar")).get(),
                2 * docs
            );
            assertHitCount(client().prepareSearch("source").setSize(size).setQuery(new TermsQueryBuilder("foo", "bar")).get(), docs);
            GetSettingsResponse target = client().admin().indices().prepareGetSettings("target").get();
            assertEquals(version, target.getIndexToSettings().get("target").getAsVersion("index.version.created", null));
        } finally {
            // clean up
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), (String) null)
                )
                .get();
        }

    }

}
