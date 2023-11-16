/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.PitTestsUtil;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.termvectors.TermVectorsRequestBuilder;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.client.Requests;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexModule;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.NRTReplicationReaderManager;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.FileChunkRequest;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.NodeClosedException;
import org.opensearch.search.SearchService;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.internal.PitReaderContext;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.opensearch.action.search.PitTestsUtil.assertSegments;
import static org.opensearch.action.search.SearchContextId.decode;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.indices.replication.SegmentReplicationTarget.REPLICATION_PREFIX;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SegmentReplicationIT extends SegmentReplicationBaseIT {

    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    private static String indexOrAlias() {
        return randomBoolean() ? INDEX_NAME : "alias";
    }

    public void testPrimaryStopped_ReplicaPromoted() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(1, primary, replica);

        // index another doc but don't refresh, we will ensure this is searchable once replica is promoted.
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        // stop the primary node - we only have one shard on here.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());
        final SearchResponse response = client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get();
        // new primary should have at least the doc count from the first set of segments.
        assertTrue(response.getHits().getTotalHits().value >= 1);

        // assert we can index into the new primary.
        client().prepareIndex(INDEX_NAME).setId("3").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);

        // start another node, index another doc and replicate.
        String nodeC = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("4").setSource("baz", "baz").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(4, nodeC, replica);
        verifyStoreContent();
    }

    public void testRestartPrimary() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), primary);

        final int initialDocCount = 1;
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(initialDocCount, replica, primary);

        internalCluster().restartNode(primary);
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), replica);

        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, replica, primary);
        verifyStoreContent();
    }

    public void testCancelPrimaryAllocation() throws Exception {
        // this test cancels allocation on the primary - promoting the new replica and recreating the former primary as a replica.
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = 1;

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(initialDocCount, replica, primary);

        final IndexShard indexShard = getIndexShard(primary, INDEX_NAME);
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new CancelAllocationCommand(INDEX_NAME, indexShard.shardId().id(), primary, true))
            .execute()
            .actionGet();
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), replica);

        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, replica, primary);
        verifyStoreContent();
    }

    public void testReplicationAfterPrimaryRefreshAndFlush() throws Exception {
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        final Settings settings = Settings.builder()
            .put(indexSettings())
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomFrom(new ArrayList<>(CODECS) {
                {
                    add(CodecService.LUCENE_DEFAULT_CODEC);
                }
            }))
            .build();
        createIndex(INDEX_NAME, settings);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 10);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 10);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);
            verifyStoreContent();
        }
    }

    public void testIndexReopenClose() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        final String replica = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(1, 10);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            flush(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, primary, replica);
        }
        logger.info("--> Closing the index ");
        client().admin().indices().prepareClose(INDEX_NAME).get();

        logger.info("--> Opening the index");
        client().admin().indices().prepareOpen(INDEX_NAME).get();

        ensureGreen(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, primary, replica);
        verifyStoreContent();
    }

    public void testScrollWithConcurrentIndexAndSearch() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        final String replica = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        final List<ActionFuture<IndexResponse>> pendingIndexResponses = new ArrayList<>();
        final List<ActionFuture<SearchResponse>> pendingSearchResponse = new ArrayList<>();
        final int searchCount = randomIntBetween(1, 2);
        final WriteRequest.RefreshPolicy refreshPolicy = randomFrom(WriteRequest.RefreshPolicy.values());

        for (int i = 0; i < searchCount; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
            flush(INDEX_NAME);
            forceMerge();
        }

        final SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setIndices(INDEX_NAME)
            .setRequestCache(false)
            .setScroll(TimeValue.timeValueDays(1))
            .setSize(10)
            .get();

        for (int i = searchCount; i < searchCount * 2; i++) {
            pendingIndexResponses.add(
                client().prepareIndex(INDEX_NAME)
                    .setId(Integer.toString(i))
                    .setRefreshPolicy(refreshPolicy)
                    .setSource("field", "value" + i)
                    .execute()
            );
        }
        flush(INDEX_NAME);
        forceMerge();
        client().prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();

        assertBusy(() -> {
            client().admin().indices().prepareRefresh().execute().actionGet();
            assertTrue(pendingIndexResponses.stream().allMatch(ActionFuture::isDone));
            assertTrue(pendingSearchResponse.stream().allMatch(ActionFuture::isDone));
        }, 1, TimeUnit.MINUTES);
        verifyStoreContent();
        waitForSearchableDocs(INDEX_NAME, 2 * searchCount, List.of(primary, replica));
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testMultipleShards() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, indexSettings);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(1, 10);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 10);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);
            verifyStoreContent();
        }
    }

    public void testReplicationAfterForceMerge() throws Exception {
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(0, 10);
        final int additionalDocCount = scaledRandomIntBetween(0, 10);
        final int expectedHitCount = initialDocCount + additionalDocCount;
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);

            flush(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            // Index a second set of docs so we can merge into one segment.
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
            client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
            refresh(INDEX_NAME);
            verifyStoreContent();
        }
    }

    /**
     * This test verifies that segment replication does not fail for closed indices
     */
    public void testClosedIndices() {
        List<String> nodes = new ArrayList<>();
        // start 1st node so that it contains the primary
        nodes.add(internalCluster().startDataOnlyNode());
        createIndex(INDEX_NAME, super.indexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        // start 2nd node so that it contains the replica
        nodes.add(internalCluster().startDataOnlyNode());
        ensureGreen(INDEX_NAME);

        logger.info("--> Close index");
        assertAcked(client().admin().indices().prepareClose(INDEX_NAME));

        logger.info("--> waiting for allocation to have shards assigned");
        waitForRelocation(ClusterHealthStatus.GREEN);
    }

    /**
     * This test validates the primary node drop does not result in shard failure on replica.
     * @throws Exception when issue is encountered
     */
    public void testNodeDropWithOngoingReplication() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put("index.refresh_interval", -1)
                .build()
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);
        ClusterState state = client().admin().cluster().prepareState().execute().actionGet().getState();
        // Get replica allocation id
        final String replicaAllocationId = state.routingTable()
            .index(INDEX_NAME)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(routing -> routing.primary() == false)
            .findFirst()
            .get()
            .allocationId()
            .getId();
        DiscoveryNode primaryDiscovery = state.nodes().resolveNode(primaryNode);

        CountDownLatch blockFileCopy = new CountDownLatch(1);
        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));
        primaryTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    FileChunkRequest req = (FileChunkRequest) request;
                    logger.debug("file chunk [{}] lastChunk: {}", req, req.lastChunk());
                    if (req.name().endsWith("cfs") && req.lastChunk()) {
                        try {
                            blockFileCopy.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        throw new NodeClosedException(primaryDiscovery);
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );
        final int docCount = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
        }
        // Refresh, this should trigger round of segment replication
        refresh(INDEX_NAME);
        blockFileCopy.countDown();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        ensureYellow(INDEX_NAME);
        assertBusy(() -> { assertDocCounts(docCount, replicaNode); });
        state = client().admin().cluster().prepareState().execute().actionGet().getState();
        // replica now promoted as primary should have same allocation id
        final String currentAllocationID = state.routingTable()
            .index(INDEX_NAME)
            .shardsWithState(ShardRoutingState.STARTED)
            .stream()
            .filter(routing -> routing.primary())
            .findFirst()
            .get()
            .allocationId()
            .getId();
        assertEquals(currentAllocationID, replicaAllocationId);
    }

    public void testCancellation() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(INDEX_NAME);

        final String replicaNode = internalCluster().startDataOnlyNode();

        final SegmentReplicationSourceService segmentReplicationSourceService = internalCluster().getInstance(
            SegmentReplicationSourceService.class,
            primaryNode
        );
        final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);

        CountDownLatch latch = new CountDownLatch(1);

        MockTransportService mockTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primaryNode
        ));
        mockTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replicaNode),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    FileChunkRequest req = (FileChunkRequest) request;
                    logger.debug("file chunk [{}] lastChunk: {}", req, req.lastChunk());
                    if (req.name().endsWith("cfs") && req.lastChunk()) {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        final int docCount = scaledRandomIntBetween(0, 10);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(docCount);
            waitForDocs(docCount, indexer);

            flush(INDEX_NAME);
        }
        segmentReplicationSourceService.beforeIndexShardClosed(primaryShard.shardId(), primaryShard, indexSettings());
        latch.countDown();
        assertDocCounts(docCount, primaryNode);
    }

    public void testStartReplicaAfterPrimaryIndexesDocs() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(INDEX_NAME);

        // Index a doc to create the first set of segments. _s1.si
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();
        // Flush segments to disk and create a new commit point (Primary: segments_3, _s1.si)
        flushAndRefresh(INDEX_NAME);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 1);

        // Index to create another segment
        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo", "bar").get();

        // Force a merge here so that the in memory SegmentInfos does not reference old segments on disk.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();
        refresh(INDEX_NAME);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("foo", "bar").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(3, primaryNode, replicaNode);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        verifyStoreContent();
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testDeleteOperations() throws Exception {
        final String nodeA = internalCluster().startDataOnlyNode();
        final String nodeB = internalCluster().startDataOnlyNode();

        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(1, 5);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, nodeA, nodeB);

            final int additionalDocCount = scaledRandomIntBetween(0, 2);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, nodeA, nodeB);

            ensureGreen(INDEX_NAME);

            Set<String> ids = indexer.getIds();
            assertFalse(ids.isEmpty());
            String id = ids.toArray()[0].toString();
            client(nodeA).prepareDelete(INDEX_NAME, id).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

            refresh(INDEX_NAME);
            waitForSearchableDocs(expectedHitCount - 1, nodeA, nodeB);
            verifyStoreContent();
        }
    }

    /**
     * This tests that the max seqNo we send to replicas is accurate and that after failover
     * the new primary starts indexing from the correct maxSeqNo and replays the correct count of docs
     * from xlog.
     */
    public void testReplicationPostDeleteAndForceMerge() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);
        final int initialDocCount = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < initialDocCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
        }
        refresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, primary, replica);

        final int deletedDocCount = randomIntBetween(1, initialDocCount);
        for (int i = 0; i < deletedDocCount; i++) {
            client(primary).prepareDelete(INDEX_NAME, String.valueOf(i)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        }
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(false).get();

        // randomly flush here after the force merge to wipe any old segments.
        if (randomBoolean()) {
            flush(INDEX_NAME);
        }

        final IndexShard primaryShard = getIndexShard(primary, INDEX_NAME);
        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);
        assertBusy(
            () -> assertEquals(
                primaryShard.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                replicaShard.getLatestReplicationCheckpoint().getSegmentInfosVersion()
            )
        );

        // add some docs to the xlog and drop primary.
        final int additionalDocs = randomIntBetween(1, 5);
        for (int i = initialDocCount; i < initialDocCount + additionalDocs; i++) {
            client().prepareIndex(INDEX_NAME).setId(String.valueOf(i)).setSource("foo", "bar").get();
        }
        // Drop the primary and wait until replica is promoted.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());
        refresh(INDEX_NAME);
        final long expectedHitCount = initialDocCount + additionalDocs - deletedDocCount;
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

        int expectedMaxSeqNo = initialDocCount + deletedDocCount + additionalDocs - 1;
        assertEquals(expectedMaxSeqNo, replicaShard.seqNoStats().getMaxSeqNo());

        // index another doc.
        client().prepareIndex(INDEX_NAME).setId(String.valueOf(expectedMaxSeqNo + 1)).setSource("another", "doc").get();
        refresh(INDEX_NAME);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount + 1);
    }

    public void testUpdateOperations() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = scaledRandomIntBetween(1, 5);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, asList(primary, replica));

            final int additionalDocCount = scaledRandomIntBetween(0, 5);
            final int expectedHitCount = initialDocCount + additionalDocCount;
            indexer.start(additionalDocCount);
            waitForDocs(expectedHitCount, indexer);
            waitForSearchableDocs(expectedHitCount, asList(primary, replica));

            Set<String> ids = indexer.getIds();
            String id = ids.toArray()[0].toString();
            UpdateResponse updateResponse = client(primary).prepareUpdate(INDEX_NAME, id)
                .setDoc(Requests.INDEX_CONTENT_TYPE, "foo", "baz")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL)
                .get();
            assertFalse("request shouldn't have forced a refresh", updateResponse.forcedRefresh());
            assertEquals(2, updateResponse.getVersion());

            refresh(INDEX_NAME);

            verifyStoreContent();
            assertSearchHits(client(primary).prepareSearch(INDEX_NAME).setQuery(matchQuery("foo", "baz")).get(), id);
            assertSearchHits(client(replica).prepareSearch(INDEX_NAME).setQuery(matchQuery("foo", "baz")).get(), id);
        }
    }

    public void testDropPrimaryDuringReplication() throws Exception {
        final int replica_count = 6;
        final Settings settings = Settings.builder()
            .put(indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replica_count)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, settings);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(6);
        ensureGreen(INDEX_NAME);

        int initialDocCount = scaledRandomIntBetween(5, 10);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);
            // don't wait for replication to complete, stop the primary immediately.
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
            ensureYellow(INDEX_NAME);

            // start another replica.
            dataNodes.add(internalCluster().startDataOnlyNode());
            ensureGreen(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, dataNodes);

            // index another doc and refresh - without this the new replica won't catch up.
            String docId = String.valueOf(initialDocCount + 1);
            client().prepareIndex(INDEX_NAME).setId(docId).setSource("foo", "bar").get();

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount + 1, dataNodes);
            verifyStoreContent();
        }
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testReplicaHasDiffFilesThanPrimary() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);
        IndexWriterConfig iwc = newIndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.APPEND);

        // create a doc to index
        int numDocs = 2 + random().nextInt(10);

        List<Document> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(new StringField("id", "" + i, random().nextBoolean() ? Field.Store.YES : Field.Store.NO));
            doc.add(
                new TextField(
                    "body",
                    TestUtil.randomRealisticUnicodeString(random()),
                    random().nextBoolean() ? Field.Store.YES : Field.Store.NO
                )
            );
            doc.add(new SortedDocValuesField("dv", new BytesRef(TestUtil.randomRealisticUnicodeString(random()))));
            docs.add(doc);
        }
        // create some segments on the replica before copy.
        try (IndexWriter writer = new IndexWriter(replicaShard.store().directory(), iwc)) {
            for (Document d : docs) {
                writer.addDocument(d);
            }
            writer.flush();
            writer.commit();
        }

        final SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(replicaShard.store().directory());
        replicaShard.finalizeReplication(segmentInfos);
        ensureYellow(INDEX_NAME);

        final int docCount = scaledRandomIntBetween(10, 20);
        for (int i = 0; i < docCount; i++) {
            client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i).execute().get();
            // Refresh, this should trigger round of segment replication
            refresh(INDEX_NAME);
        }
        ensureGreen(INDEX_NAME);
        waitForSearchableDocs(docCount, primaryNode, replicaNode);
        verifyStoreContent();
        final IndexShard replicaAfterFailure = getIndexShard(replicaNode, INDEX_NAME);
        assertNotEquals(replicaAfterFailure.routingEntry().allocationId().getId(), replicaShard.routingEntry().allocationId().getId());
    }

    public void testPressureServiceStats() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        int initialDocCount = scaledRandomIntBetween(10, 20);
        try (
            BackgroundIndexer indexer = new BackgroundIndexer(
                INDEX_NAME,
                "_doc",
                client(),
                -1,
                RandomizedTest.scaledRandomIntBetween(2, 5),
                false,
                random()
            )
        ) {
            indexer.start(initialDocCount);
            waitForDocs(initialDocCount, indexer);
            refresh(INDEX_NAME);

            // get shard references.
            final IndexShard primaryShard = getIndexShard(primaryNode, INDEX_NAME);
            final IndexShard replicaShard = getIndexShard(replicaNode, INDEX_NAME);
            logger.info("Replica aid {}", replicaShard.routingEntry().allocationId());
            logger.info("former primary aid {}", primaryShard.routingEntry().allocationId());

            // fetch pressure stats from the Primary's Node.
            SegmentReplicationPressureService pressureService = internalCluster().getInstance(
                SegmentReplicationPressureService.class,
                primaryNode
            );

            // Fetch pressure stats from the Replica's Node we will assert replica node returns nothing until it is promoted.
            SegmentReplicationPressureService replicaNode_service = internalCluster().getInstance(
                SegmentReplicationPressureService.class,
                replicaNode
            );

            final Map<ShardId, SegmentReplicationPerGroupStats> shardStats = pressureService.nodeStats().getShardStats();
            assertEquals("We should have stats returned for the replication group", 1, shardStats.size());

            SegmentReplicationPerGroupStats groupStats = shardStats.get(primaryShard.shardId());
            Set<SegmentReplicationShardStats> replicaStats = groupStats.getReplicaStats();
            assertAllocationIdsInReplicaShardStats(Set.of(replicaShard.routingEntry().allocationId().getId()), replicaStats);

            assertTrue(replicaNode_service.nodeStats().getShardStats().isEmpty());

            // drop the primary, this won't hand off pressure stats between old/new primary.
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
            ensureYellowAndNoInitializingShards(INDEX_NAME);

            assertTrue("replica should be promoted as a primary", replicaShard.routingEntry().primary());
            assertEquals(
                "We should have stats returned for the replication group",
                1,
                replicaNode_service.nodeStats().getShardStats().size()
            );
            // after the primary is dropped and replica is promoted we won't have a replica assigned yet, so stats per replica should return
            // empty.
            replicaStats = replicaNode_service.nodeStats().getShardStats().get(primaryShard.shardId()).getReplicaStats();
            assertTrue(replicaStats.isEmpty());

            // start another replica.
            String replicaNode_2 = internalCluster().startDataOnlyNode();
            ensureGreen(INDEX_NAME);
            final IndexShard secondReplicaShard = getIndexShard(replicaNode_2, INDEX_NAME);
            final String second_replica_aid = secondReplicaShard.routingEntry().allocationId().getId();
            waitForSearchableDocs(initialDocCount, replicaNode_2);

            assertEquals(
                "We should have stats returned for the replication group",
                1,
                replicaNode_service.nodeStats().getShardStats().size()
            );
            replicaStats = replicaNode_service.nodeStats().getShardStats().get(replicaShard.shardId()).getReplicaStats();
            assertAllocationIdsInReplicaShardStats(Set.of(second_replica_aid), replicaStats);
            final SegmentReplicationShardStats replica_entry = replicaStats.stream().findFirst().get();
            assertEquals(replica_entry.getCheckpointsBehindCount(), 0);

            // test a checkpoint without any new segments
            flush(INDEX_NAME);
            assertBusy(() -> {
                assertEquals(1, replicaNode_service.nodeStats().getShardStats().size());
                final Set<SegmentReplicationShardStats> shardStatsSet = replicaNode_service.nodeStats()
                    .getShardStats()
                    .get(replicaShard.shardId())
                    .getReplicaStats();
                assertAllocationIdsInReplicaShardStats(Set.of(second_replica_aid), shardStatsSet);
                final SegmentReplicationShardStats stats = shardStatsSet.stream().findFirst().get();
                assertEquals(0, stats.getCheckpointsBehindCount());
            });
        }
    }

    private void assertAllocationIdsInReplicaShardStats(Set<String> expected, Set<SegmentReplicationShardStats> replicaStats) {
        assertEquals(expected, replicaStats.stream().map(SegmentReplicationShardStats::getAllocationId).collect(Collectors.toSet()));
    }

    /**
     * Tests a scroll query on the replica
     * @throws Exception when issue is encountered
     */
    public void testScrollCreatedOnReplica() throws Exception {
        // create the cluster with one primary node containing primary shard and replica node containing replica shard
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        // index 10 docs
        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            refresh(INDEX_NAME);
        }
        assertBusy(
            () -> assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            )
        );
        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);
        final Tuple<GatedCloseable<SegmentInfos>, ReplicationCheckpoint> tuple = replicaShard.getLatestSegmentInfosAndCheckpoint();
        final Collection<String> snapshottedSegments;
        try (final GatedCloseable<SegmentInfos> closeable = tuple.v1()) {
            snapshottedSegments = closeable.get().files(false);
        }
        // opens a scrolled query before a flush is called.
        // this is for testing scroll segment consistency between refresh and flush
        SearchResponse searchResponse = client(replica).prepareSearch()
            .setQuery(matchAllQuery())
            .setIndices(INDEX_NAME)
            .setRequestCache(false)
            .setPreference("_only_local")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .addSort("field", SortOrder.ASC)
            .setSize(10)
            .setScroll(TimeValue.timeValueDays(1))
            .get();

        // force call flush
        flush(INDEX_NAME);

        for (int i = 3; i < 5; i++) {
            client().prepareDelete(INDEX_NAME, String.valueOf(i)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
            refresh(INDEX_NAME);
            if (randomBoolean()) {
                client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
                flush(INDEX_NAME);
            }
        }
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });

        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });
        // Test stats
        logger.info("--> Collect all scroll query hits");
        long scrollHits = 0;
        do {
            scrollHits += searchResponse.getHits().getHits().length;
            searchResponse = client(replica).prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueDays(1)).get();
            assertAllSuccessful(searchResponse);
        } while (searchResponse.getHits().getHits().length > 0);

        List<String> currentFiles = List.of(replicaShard.store().directory().listAll());
        assertTrue("Files should be preserved", currentFiles.containsAll(snapshottedSegments));

        client(replica).prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();

        assertBusy(
            () -> assertFalse(
                "Files should be cleaned up post scroll clear request",
                List.of(replicaShard.store().directory().listAll()).containsAll(snapshottedSegments)
            )
        );
        assertEquals(10, scrollHits);

    }

    /**
     * Tests that when scroll query is cleared, it does not delete the temporary replication files, which are part of
     * ongoing round of segment replication
     *
     * @throws Exception when issue is encountered
     */
    public void testScrollWithOngoingSegmentReplication() throws Exception {
        // this test stubs transport calls specific to node-node replication.
        assumeFalse(
            "Skipping the test as its not compatible with segment replication with remote store.",
            segmentReplicationWithRemoteEnabled()
        );

        // create the cluster with one primary node containing primary shard and replica node containing replica shard
        final String primary = internalCluster().startDataOnlyNode();
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final int initialDocCount = 10;
        final int finalDocCount = 20;
        for (int i = 0; i < initialDocCount; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
        }
        // catch up replica with primary
        refresh(INDEX_NAME);
        assertBusy(
            () -> assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            )
        );
        logger.info("--> Create scroll query");
        // opens a scrolled query before a flush is called.
        SearchResponse searchResponse = client(replica).prepareSearch()
            .setQuery(matchAllQuery())
            .setIndices(INDEX_NAME)
            .setRequestCache(false)
            .setPreference("_only_local")
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .addSort("field", SortOrder.ASC)
            .setSize(10)
            .setScroll(TimeValue.timeValueDays(1))
            .get();

        // force call flush
        flush(INDEX_NAME);

        // Index more documents
        for (int i = initialDocCount; i < finalDocCount; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
        }
        // Block file copy operation to ensure replica has few temporary replication files
        CountDownLatch blockFileCopy = new CountDownLatch(1);
        CountDownLatch waitForFileCopy = new CountDownLatch(1);
        MockTransportService primaryTransportService = ((MockTransportService) internalCluster().getInstance(
            TransportService.class,
            primary
        ));
        primaryTransportService.addSendBehavior(
            internalCluster().getInstance(TransportService.class, replica),
            (connection, requestId, action, request, options) -> {
                if (action.equals(SegmentReplicationTargetService.Actions.FILE_CHUNK)) {
                    FileChunkRequest req = (FileChunkRequest) request;
                    logger.debug("file chunk [{}] lastChunk: {}", req, req.lastChunk());
                    if (req.name().endsWith("cfs") && req.lastChunk()) {
                        try {
                            waitForFileCopy.countDown();
                            logger.info("--> Waiting for file copy");
                            blockFileCopy.await();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            }
        );

        // perform refresh to start round of segment replication
        refresh(INDEX_NAME);

        // wait for segrep to start and copy temporary files
        waitForFileCopy.await();

        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);
        // Wait until replica has written a tmp file to disk.
        List<String> temporaryFiles = new ArrayList<>();
        assertBusy(() -> {
            // verify replica contains temporary files
            temporaryFiles.addAll(
                Arrays.stream(replicaShard.store().directory().listAll())
                    .filter(fileName -> fileName.startsWith(REPLICATION_PREFIX))
                    .collect(Collectors.toList())
            );
            logger.info("--> temporaryFiles {}", temporaryFiles);
            assertTrue(temporaryFiles.size() > 0);
        });

        // Clear scroll query, this should clean up files on replica
        client(replica).prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();

        // verify temporary files still exist
        List<String> temporaryFilesPostClear = Arrays.stream(replicaShard.store().directory().listAll())
            .filter(fileName -> fileName.startsWith(REPLICATION_PREFIX))
            .collect(Collectors.toList());
        logger.info("--> temporaryFilesPostClear {}", temporaryFilesPostClear);

        // Unblock segment replication
        blockFileCopy.countDown();

        assertTrue(temporaryFilesPostClear.containsAll(temporaryFiles));

        // wait for replica to catch up and verify doc count
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });
        verifyStoreContent();
        waitForSearchableDocs(finalDocCount, primary, replica);
    }

    public void testPitCreatedOnReplica() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        for (int i = 0; i < 10; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo", randomInt())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            refresh(INDEX_NAME);
        }
        // wait until replication finishes, then make the pit request.
        assertBusy(
            () -> assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            )
        );
        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), false);
        request.setPreference("_only_local");
        request.setIndices(new String[] { INDEX_NAME });
        ActionFuture<CreatePitResponse> execute = client(replica).execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        SearchResponse searchResponse = client(replica).prepareSearch(INDEX_NAME)
            .setSize(10)
            .setPreference("_only_local")
            .setRequestCache(false)
            .addSort("foo", SortOrder.ASC)
            .searchAfter(new Object[] { 30 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .get();
        assertEquals(1, searchResponse.getSuccessfulShards());
        assertEquals(1, searchResponse.getTotalShards());
        FlushRequest flushRequest = Requests.flushRequest(INDEX_NAME);
        client().admin().indices().flush(flushRequest).get();
        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);

        // fetch the segments snapshotted when the reader context was created.
        Collection<String> snapshottedSegments;
        SearchService searchService = internalCluster().getInstance(SearchService.class, replica);
        NamedWriteableRegistry registry = internalCluster().getInstance(NamedWriteableRegistry.class, replica);
        final PitReaderContext pitReaderContext = searchService.getPitReaderContext(
            decode(registry, pitResponse.getId()).shards().get(replicaShard.routingEntry().shardId()).getSearchContextId()
        );
        try (final Engine.Searcher searcher = pitReaderContext.acquireSearcher("test")) {
            final StandardDirectoryReader standardDirectoryReader = NRTReplicationReaderManager.unwrapStandardReader(
                (OpenSearchDirectoryReader) searcher.getDirectoryReader()
            );
            final SegmentInfos infos = standardDirectoryReader.getSegmentInfos();
            snapshottedSegments = infos.files(false);
        }

        flush(INDEX_NAME);
        for (int i = 11; i < 20; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource("foo", randomInt())
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .get();
            refresh(INDEX_NAME);
            if (randomBoolean()) {
                client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
                flush(INDEX_NAME);
            }
        }
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });

        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });
        // Test stats
        IndicesStatsRequest indicesStatsRequest = new IndicesStatsRequest();
        indicesStatsRequest.indices(INDEX_NAME);
        indicesStatsRequest.all();
        IndicesStatsResponse indicesStatsResponse = client().admin().indices().stats(indicesStatsRequest).get();
        long pitCurrent = indicesStatsResponse.getIndex(INDEX_NAME).getTotal().search.getTotal().getPitCurrent();
        long openContexts = indicesStatsResponse.getIndex(INDEX_NAME).getTotal().search.getOpenContexts();
        assertEquals(1, pitCurrent);
        assertEquals(1, openContexts);
        SearchResponse resp = client(replica).prepareSearch(INDEX_NAME)
            .setSize(10)
            .setPreference("_only_local")
            .addSort("foo", SortOrder.ASC)
            .searchAfter(new Object[] { 30 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()).setKeepAlive(TimeValue.timeValueDays(1)))
            .setRequestCache(false)
            .get();
        PitTestsUtil.assertUsingGetAllPits(client(replica), pitResponse.getId(), pitResponse.getCreationTime());
        assertSegments(false, INDEX_NAME, 1, client(replica), pitResponse.getId());

        List<String> currentFiles = List.of(replicaShard.store().directory().listAll());
        assertTrue("Files should be preserved", currentFiles.containsAll(snapshottedSegments));

        // delete the PIT
        DeletePitRequest deletePITRequest = new DeletePitRequest(pitResponse.getId());
        client().execute(DeletePitAction.INSTANCE, deletePITRequest).actionGet();
        assertBusy(
            () -> assertFalse(
                "Files should be cleaned up",
                List.of(replicaShard.store().directory().listAll()).containsAll(snapshottedSegments)
            )
        );
    }

    /**
     * This tests that if a primary receives docs while a replica is performing round of segrep during recovery
     * the replica will catch up to latest checkpoint once recovery completes without requiring an additional primary refresh/flush.
     */
    public void testPrimaryReceivesDocsDuringReplicaRecovery() throws Exception {
        final List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startDataOnlyNode();
        nodes.add(primaryNode);
        final Settings settings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(INDEX_NAME, settings);
        ensureGreen(INDEX_NAME);
        // start a replica node, initially will be empty with no shard assignment.
        final String replicaNode = internalCluster().startDataOnlyNode();
        nodes.add(replicaNode);

        // index a doc.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", randomInt()).get();
        refresh(INDEX_NAME);

        CountDownLatch latch = new CountDownLatch(1);
        // block replication
        try (final Releasable ignored = blockReplication(List.of(replicaNode), latch)) {
            // update to add replica, initiating recovery, this will get stuck at last step
            assertAcked(
                client().admin()
                    .indices()
                    .prepareUpdateSettings(INDEX_NAME)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
            );
            ensureYellow(INDEX_NAME);
            // index another doc while blocked, this would not get replicated to replica.
            client().prepareIndex(INDEX_NAME).setId("2").setSource("foo2", randomInt()).get();
            refresh(INDEX_NAME);
        }
        ensureGreen(INDEX_NAME);
        waitForSearchableDocs(2, nodes);
    }

    public void testIndexWhileRecoveringReplica() throws Exception {
        final String primaryNode = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(
                jsonBuilder().startObject()
                    .startObject("_routing")
                    .field("required", true)
                    .endObject()
                    .startObject("properties")
                    .startObject("online")
                    .field("type", "boolean")
                    .endObject()
                    .startObject("ts")
                    .field("type", "date")
                    .field("ignore_malformed", false)
                    .field("format", "epoch_millis")
                    .endObject()
                    .startObject("bs")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataOnlyNode();

        client().prepareIndex(INDEX_NAME)
            .setId("1")
            .setRouting("Y")
            .setSource("online", false, "bs", "Y", "ts", System.currentTimeMillis() - 100, "type", "s")
            .get();
        client().prepareIndex(INDEX_NAME)
            .setId("2")
            .setRouting("X")
            .setSource("online", true, "bs", "X", "ts", System.currentTimeMillis() - 10000000, "type", "s")
            .get();
        client().prepareIndex(INDEX_NAME)
            .setId("3")
            .setRouting(randomAlphaOfLength(2))
            .setSource("online", false, "ts", System.currentTimeMillis() - 100, "type", "bs")
            .get();
        client().prepareIndex(INDEX_NAME)
            .setId("4")
            .setRouting(randomAlphaOfLength(2))
            .setSource("online", true, "ts", System.currentTimeMillis() - 123123, "type", "bs")
            .get();
        refresh();
        ensureGreen(INDEX_NAME);
        waitForSearchableDocs(4, primaryNode, replicaNode);

        SearchResponse response = client().prepareSearch(INDEX_NAME)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(
                boolQuery().must(termQuery("online", true))
                    .must(
                        boolQuery().should(
                            boolQuery().must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000))).must(termQuery("type", "bs"))
                        )
                            .should(
                                boolQuery().must(rangeQuery("ts").lt(System.currentTimeMillis() - (15 * 1000))).must(termQuery("type", "s"))
                            )
                    )
            )
            .setVersion(true)
            .setFrom(0)
            .setSize(100)
            .setExplain(true)
            .get();
        assertNoFailures(response);
    }

    public void testRestartPrimary_NoReplicas() throws Exception {
        final String primary = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), primary);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (randomBoolean()) {
            flush(INDEX_NAME);
        } else {
            refresh(INDEX_NAME);
        }

        internalCluster().restartNode(primary);
        ensureYellow(INDEX_NAME);
        assertDocCounts(1, primary);
    }

    /**
     * Tests whether segment replication supports realtime get requests and reads and parses source from the translog to serve strong reads.
     */
    public void testRealtimeGetRequestsSuccessful() {
        final String primary = internalCluster().startDataOnlyNode();
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime get
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(Settings.builder().put("index.refresh_interval", -1).put(indexSettings()))
                .addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final String id = routingKeyForShard(INDEX_NAME, 0);

        GetResponse response = client(replica).prepareGet(indexOrAlias(), "1").get();
        assertFalse(response.isExists());

        // index doc 1
        client().prepareIndex(indexOrAlias()).setId("1").setSource("foo", "bar").get();

        // non realtime get 1
        response = client().prepareGet(indexOrAlias(), "1").setRealtime(false).get();
        assertFalse(response.isExists());

        // realtime get 1
        response = client(replica).prepareGet(indexOrAlias(), "1").get();
        assertTrue(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));
        assertThat(response.getSourceAsMap().get("foo").toString(), equalTo("bar"));

        // index doc 2
        client().prepareIndex(indexOrAlias()).setId("2").setSource("foo2", "bar2").setRouting(id).get();

        // realtime get 2 (with routing)
        response = client(replica).prepareGet(indexOrAlias(), "2").setRouting(id).get();
        assertTrue(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));
        assertThat(response.getSourceAsMap().get("foo2").toString(), equalTo("bar2"));
    }

    public void testRealtimeGetRequestsUnsuccessful() {
        final String primary = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final String id = routingKeyForShard(INDEX_NAME, 0);
        final String routingOtherShard = routingKeyForShard(INDEX_NAME, 1);

        // index doc 1
        client().prepareIndex(indexOrAlias()).setId("1").setSource("foo", "bar").setRouting(id).get();

        // non realtime get 1
        GetResponse response = client().prepareGet(indexOrAlias(), "1").setRealtime(false).get();
        assertFalse(response.isExists());

        // realtime get 1 (preference = _replica)
        response = client(replica).prepareGet(indexOrAlias(), "1").setPreference(Preference.REPLICA.type()).get();
        assertFalse(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));

        // realtime get 1 (with routing set)
        response = client(replica).prepareGet(INDEX_NAME, "1").setRouting(routingOtherShard).get();
        assertFalse(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));
    }

    /**
     * Tests whether segment replication supports realtime MultiGet requests and reads and parses source from the translog to serve strong reads.
     */
    public void testRealtimeMultiGetRequestsSuccessful() {
        final String primary = internalCluster().startDataOnlyNode();
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime multi get
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final String id = routingKeyForShard(INDEX_NAME, 0);

        // index doc 1
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").get();

        // index doc 2
        client().prepareIndex(INDEX_NAME).setId("2").setSource("foo2", "bar2").setRouting(id).get();

        // multi get non realtime 1
        MultiGetResponse mgetResponse = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(INDEX_NAME, "1"))
            .add(new MultiGetRequest.Item("nonExistingIndex", "1"))
            .setRealtime(false)
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));

        assertThat(mgetResponse.getResponses()[0].getIndex(), is(INDEX_NAME));
        assertFalse(mgetResponse.getResponses()[0].isFailed());
        assertFalse(mgetResponse.getResponses()[0].getResponse().isExists());

        // multi get realtime 1
        mgetResponse = client(replica).prepareMultiGet()
            .add(new MultiGetRequest.Item(INDEX_NAME, "1"))
            .add(new MultiGetRequest.Item(INDEX_NAME, "2").routing(id))
            .add(new MultiGetRequest.Item("nonExistingIndex", "1"))
            .get();

        assertThat(mgetResponse.getResponses().length, is(3));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is(INDEX_NAME));
        assertFalse(mgetResponse.getResponses()[0].isFailed());
        assertThat(mgetResponse.getResponses()[0].getResponse().getSourceAsMap().get("foo").toString(), equalTo("bar"));

        assertThat(mgetResponse.getResponses()[1].getIndex(), is(INDEX_NAME));
        assertFalse(mgetResponse.getResponses()[1].isFailed());
        assertThat(mgetResponse.getResponses()[1].getResponse().getSourceAsMap().get("foo2").toString(), equalTo("bar2"));

        assertThat(mgetResponse.getResponses()[2].getIndex(), is("nonExistingIndex"));
        assertTrue(mgetResponse.getResponses()[2].isFailed());
        assertThat(mgetResponse.getResponses()[2].getFailure().getMessage(), is("no such index [nonExistingIndex]"));
    }

    public void testRealtimeMultiGetRequestsUnsuccessful() {
        final String primary = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);

        final String id = routingKeyForShard(INDEX_NAME, 0);
        final String routingOtherShard = routingKeyForShard(INDEX_NAME, 1);

        // index doc 1
        client().prepareIndex(indexOrAlias()).setId("1").setSource("foo", "bar").setRouting(id).get();

        // realtime multi get 1 (preference = _replica)
        MultiGetResponse mgetResponse = client(replica).prepareMultiGet()
            .add(new MultiGetRequest.Item(INDEX_NAME, "1"))
            .setPreference(Preference.REPLICA.type())
            .add(new MultiGetRequest.Item("nonExistingIndex", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is(INDEX_NAME));
        assertFalse(mgetResponse.getResponses()[0].getResponse().isExists());

        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertTrue(mgetResponse.getResponses()[1].isFailed());

        // realtime multi get 1 (routing set)
        mgetResponse = client(replica).prepareMultiGet()
            .add(new MultiGetRequest.Item(INDEX_NAME, "1").routing(routingOtherShard))
            .add(new MultiGetRequest.Item("nonExistingIndex", "1"))
            .get();
        assertThat(mgetResponse.getResponses().length, is(2));
        assertThat(mgetResponse.getResponses()[0].getIndex(), is(INDEX_NAME));
        // expecting failure since we explicitly route request to a shard on which it doesn't exist
        assertFalse(mgetResponse.getResponses()[0].getResponse().isExists());
        assertThat(mgetResponse.getResponses()[1].getIndex(), is("nonExistingIndex"));
        assertTrue(mgetResponse.getResponses()[1].isFailed());

    }

    /**
     * Tests whether segment replication supports realtime termvector requests and reads and parses source from the translog to serve strong reads.
     */
    public void testRealtimeTermVectorRequestsSuccessful() throws IOException {
        final String primary = internalCluster().startDataOnlyNode();
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .field("analyzer", "tv_test")
            .endObject()
            .endObject()
            .endObject();
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime termvectors
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(mapping)
                .addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
                        .put("index.refresh_interval", -1)
                        .putList("index.analysis.analyzer.tv_test.filter", "lowercase")
                )
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);
        final String id = routingKeyForShard(INDEX_NAME, 0);

        TermVectorsResponse response = client(replica).prepareTermVectors(indexOrAlias(), "1").get();
        assertFalse(response.isExists());

        // index doc 1
        client().prepareIndex(INDEX_NAME)
            .setId(Integer.toString(1))
            .setSource(jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog").endObject())
            .execute()
            .actionGet();

        // non realtime termvectors 1
        response = client().prepareTermVectors(indexOrAlias(), Integer.toString(1)).setRealtime(false).get();
        assertFalse(response.isExists());

        // realtime termvectors 1
        TermVectorsRequestBuilder resp = client().prepareTermVectors(indexOrAlias(), Integer.toString(1))
            .setPayloads(true)
            .setOffsets(true)
            .setPositions(true)
            .setRealtime(true)
            .setSelectedFields();
        response = resp.execute().actionGet();
        assertThat(response.getIndex(), equalTo(INDEX_NAME));
        assertThat("doc id: " + 1 + " doesn't exists but should", response.isExists(), equalTo(true));
        Fields fields = response.getFields();
        assertThat(fields.size(), equalTo(1));

        // index doc 2 with routing
        client().prepareIndex(INDEX_NAME)
            .setId(Integer.toString(2))
            .setRouting(id)
            .setSource(jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog").endObject())
            .execute()
            .actionGet();

        // realtime termvectors 2 with routing
        resp = client().prepareTermVectors(indexOrAlias(), Integer.toString(2))
            .setPayloads(true)
            .setOffsets(true)
            .setPositions(true)
            .setRouting(id)
            .setSelectedFields();
        response = resp.execute().actionGet();
        assertThat(response.getIndex(), equalTo(INDEX_NAME));
        assertThat("doc id: " + 1 + " doesn't exists but should", response.isExists(), equalTo(true));
        fields = response.getFields();
        assertThat(fields.size(), equalTo(1));

    }

    public void testRealtimeTermVectorRequestsUnSuccessful() throws IOException {
        final String primary = internalCluster().startDataOnlyNode();
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .field("analyzer", "tv_test")
            .endObject()
            .endObject()
            .endObject();
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime termvectors
        assertAcked(
            prepareCreate(INDEX_NAME).setMapping(mapping)
                .addAlias(new Alias("alias"))
                .setSettings(
                    Settings.builder()
                        .put(indexSettings())
                        .put("index.analysis.analyzer.tv_test.tokenizer", "standard")
                        .put("index.refresh_interval", -1)
                        .putList("index.analysis.analyzer.tv_test.filter", "lowercase")
                        .put(indexSettings())
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                )
        );
        final String replica = internalCluster().startDataOnlyNode();
        ensureGreen(INDEX_NAME);
        final String id = routingKeyForShard(INDEX_NAME, 0);
        final String routingOtherShard = routingKeyForShard(INDEX_NAME, 1);

        // index doc 1
        client().prepareIndex(INDEX_NAME)
            .setId(Integer.toString(1))
            .setSource(jsonBuilder().startObject().field("field", "the quick brown fox jumps over the lazy dog").endObject())
            .setRouting(id)
            .execute()
            .actionGet();

        // non realtime termvectors 1
        TermVectorsResponse response = client().prepareTermVectors(indexOrAlias(), Integer.toString(1)).setRealtime(false).get();
        assertFalse(response.isExists());

        // realtime termvectors (preference = _replica)
        TermVectorsRequestBuilder resp = client(replica).prepareTermVectors(indexOrAlias(), Integer.toString(1))
            .setPayloads(true)
            .setOffsets(true)
            .setPositions(true)
            .setPreference(Preference.REPLICA.type())
            .setRealtime(true)
            .setSelectedFields();
        response = resp.execute().actionGet();

        assertFalse(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));

        // realtime termvectors (with routing set)
        resp = client(replica).prepareTermVectors(indexOrAlias(), Integer.toString(1))
            .setPayloads(true)
            .setOffsets(true)
            .setPositions(true)
            .setRouting(routingOtherShard)
            .setSelectedFields();
        response = resp.execute().actionGet();

        assertFalse(response.isExists());
        assertThat(response.getIndex(), equalTo(INDEX_NAME));

    }

    public void testReplicaAlreadyAtCheckpoint() throws Exception {
        final List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startDataOnlyNode();
        nodes.add(primaryNode);
        final Settings settings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(INDEX_NAME, settings);
        ensureGreen(INDEX_NAME);
        // start a replica node, initially will be empty with no shard assignment.
        final String replicaNode = internalCluster().startDataOnlyNode();
        nodes.add(replicaNode);
        final String replicaNode2 = internalCluster().startDataOnlyNode();
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(INDEX_NAME)
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2))
        );
        ensureGreen(INDEX_NAME);

        // index a doc.
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", randomInt()).get();
        refresh(INDEX_NAME);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        IndexShard replica_1 = getIndexShard(replicaNode, INDEX_NAME);
        IndexShard replica_2 = getIndexShard(replicaNode2, INDEX_NAME);
        // wait until a replica is promoted & finishes engine flip, we don't care which one
        AtomicReference<IndexShard> primary = new AtomicReference<>();
        assertBusy(() -> {
            assertTrue("replica should be promoted as a primary", replica_1.routingEntry().primary() || replica_2.routingEntry().primary());
            primary.set(replica_1.routingEntry().primary() ? replica_1 : replica_2);
        });

        FlushRequest request = new FlushRequest(INDEX_NAME);
        request.force(true);
        primary.get().flush(request);

        assertBusy(() -> {
            assertEquals(
                replica_1.getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                replica_2.getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });

        assertBusy(() -> {
            ClusterStatsResponse clusterStatsResponse = client().admin().cluster().prepareClusterStats().get();
            ReplicationStats replicationStats = clusterStatsResponse.getIndicesStats().getSegments().getReplicationStats();
            assertEquals(0L, replicationStats.maxBytesBehind);
            assertEquals(0L, replicationStats.maxReplicationLag);
            assertEquals(0L, replicationStats.totalBytesBehind);
        });
    }
}
