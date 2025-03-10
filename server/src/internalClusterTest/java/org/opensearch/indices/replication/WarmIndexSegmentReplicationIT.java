/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.termvectors.TermVectorsRequestBuilder;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexModule;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.Node;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Requests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAllSuccessful;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * This class runs Segment Replication Integ test suite with partial locality indices (warm indices).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class WarmIndexSegmentReplicationIT extends SegmentReplicationBaseIT {

    protected static final String REPOSITORY_NAME = "test-remote-store-repo";
    protected Path absolutePath;

    @Before
    private void setup() {
        internalCluster().startClusterManagerOnlyNode();
    }

    private static String indexOrAlias() {
        return randomBoolean() ? INDEX_NAME : "alias";
    }

    @Override
    public Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexModule.INDEX_STORE_LOCALITY_SETTING.getKey(), IndexModule.DataLocalityType.PARTIAL.name())
            .build();
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (absolutePath == null) {
            absolutePath = randomRepoPath().toAbsolutePath();
        }
        ByteSizeValue cacheSize = new ByteSizeValue(16, ByteSizeUnit.GB);
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(remoteStoreClusterSettings(REPOSITORY_NAME, absolutePath))
            .put(Node.NODE_SEARCH_CACHE_SIZE_SETTING.getKey(), cacheSize.toString())
            .build();
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        featureSettings.put(FeatureFlags.TIERED_REMOTE_INDEX, true);
        return featureSettings.build();
    }

    @Override
    protected boolean addMockIndexStorePlugin() {
        return false;
    }

    protected boolean warmIndexSegmentReplicationEnabled() {
        return true;
    }

    @After
    public void teardown() throws Exception {
        assertAcked(client().admin().indices().delete(new DeleteIndexRequest(INDEX_NAME)).get());
        for (String nodeName : internalCluster().getNodeNames()) {
            FileCache fileCache = internalCluster().getInstance(Node.class, nodeName).fileCache();
            if (fileCache != null) {
                fileCache.clear();
            }
        }
        clusterAdmin().prepareCleanupRepository(REPOSITORY_NAME).get();
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/17526")
    public void testRestartPrimary_NoReplicas() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), primary);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        if (randomBoolean()) {
            flush(INDEX_NAME);
        } else {
            refresh(INDEX_NAME);
        }
        FileCache fileCache = internalCluster().getInstance(Node.class, primary).fileCache();
        internalCluster().restartNode(primary);
        ensureYellow(INDEX_NAME);
        assertDocCounts(1, primary);
        fileCache.prune();
    }

    public void testPrimaryStopped_ReplicaPromoted() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        waitForSearchableDocs(1, primary, replica);

        // index another doc but don't refresh, we will ensure this is searchable once replica is promoted.
        client().prepareIndex(INDEX_NAME).setId("2").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();

        FileCache fileCache1 = internalCluster().getInstance(Node.class, primary).fileCache();
        // stop the primary node - we only have one shard on here.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());
        // new primary should have at least the doc count from the first set of segments.
        assertBusy(() -> {
            final SearchResponse response = client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get();
            assertTrue(response.getHits().getTotalHits().value() >= 1);
        }, 1, TimeUnit.MINUTES);

        // assert we can index into the new primary.
        client().prepareIndex(INDEX_NAME).setId("3").setSource("bar", "baz").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);

        // start another node, index another doc and replicate.
        String nodeC = internalCluster().startDataAndSearchNodes(1).get(0);
        ensureGreen(INDEX_NAME);
        client().prepareIndex(INDEX_NAME).setId("4").setSource("baz", "baz").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(4, nodeC, replica);
        verifyStoreContent();
        fileCache1.prune();
    }

    public void testRestartPrimary() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), primary);

        final int initialDocCount = 1;
        client().prepareIndex(INDEX_NAME).setId("1").setSource("foo", "bar").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE).get();
        refresh(INDEX_NAME);

        FileCache fileCache = internalCluster().getInstance(Node.class, primary).fileCache();
        waitForSearchableDocs(initialDocCount, replica, primary);
        internalCluster().restartNode(primary);
        ensureGreen(INDEX_NAME);

        assertEquals(getNodeContainingPrimaryShard().getName(), replica);

        flushAndRefresh(INDEX_NAME);
        waitForSearchableDocs(initialDocCount, replica, primary);
        verifyStoreContent();
        fileCache.prune();
    }

    public void testCancelPrimaryAllocation() throws Exception {
        // this test cancels allocation on the primary - promoting the new replica and recreating the former primary as a replica.
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String nodeA = internalCluster().startDataAndSearchNodes(1).get(0);
        final String nodeB = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/17526")
    public void testStartReplicaAfterPrimaryIndexesDocs() throws Exception {
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
        ensureGreen(INDEX_NAME);

        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 2);

        client().prepareIndex(INDEX_NAME).setId("3").setSource("foo", "bar").get();
        refresh(INDEX_NAME);
        waitForSearchableDocs(3, primaryNode, replicaNode);
        assertHitCount(client(primaryNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        assertHitCount(client(replicaNode).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), 3);
        if (!warmIndexSegmentReplicationEnabled()) {
            verifyStoreContent();
        }
    }

    /**
     * This tests that the max seqNo we send to replicas is accurate and that after failover
     * the new primary starts indexing from the correct maxSeqNo and replays the correct count of docs
     * from xlog.
     */
    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/17527")
    public void testReplicationPostDeleteAndForceMerge() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        FileCache fileCache1 = internalCluster().getInstance(Node.class, primary).fileCache();
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primary));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        final ShardRouting replicaShardRouting = getShardRoutingForNodeName(replica);
        assertNotNull(replicaShardRouting);
        assertTrue(replicaShardRouting + " should be promoted as a primary", replicaShardRouting.primary());
        refresh(INDEX_NAME);
        final long expectedHitCount = initialDocCount + additionalDocs - deletedDocCount;
        // waitForSearchableDocs(initialDocCount, replica, primary);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount);

        int expectedMaxSeqNo = initialDocCount + deletedDocCount + additionalDocs - 1;
        assertEquals(expectedMaxSeqNo, replicaShard.seqNoStats().getMaxSeqNo());

        // index another doc.
        client().prepareIndex(INDEX_NAME).setId(String.valueOf(expectedMaxSeqNo + 1)).setSource("another", "doc").get();
        refresh(INDEX_NAME);
        assertHitCount(client(replica).prepareSearch(INDEX_NAME).setSize(0).setPreference("_only_local").get(), expectedHitCount + 1);
        fileCache1.clear();
    }

    public void testScrollWithConcurrentIndexAndSearch() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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

    public void testMultipleShards() throws Exception {
        Settings indexSettings = Settings.builder()
            .put(super.indexSettings())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .build();
        final String nodeA = internalCluster().startDataAndSearchNodes(1).get(0);
        final String nodeB = internalCluster().startDataAndSearchNodes(1).get(0);
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
            if (!warmIndexSegmentReplicationEnabled()) {
                verifyStoreContent();
            }
        }
    }

    public void testReplicationAfterForceMerge() throws Exception {
        performReplicationAfterForceMerge(false, SHARD_COUNT * (1 + REPLICA_COUNT));
    }

    public void testReplicationAfterForceMergeOnPrimaryShardsOnly() throws Exception {
        performReplicationAfterForceMerge(true, SHARD_COUNT);
    }

    private void performReplicationAfterForceMerge(boolean primaryOnly, int expectedSuccessfulShards) throws Exception {
        final String nodeA = internalCluster().startDataAndSearchNodes(1).get(0);
        final String nodeB = internalCluster().startDataAndSearchNodes(1).get(0);
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

            // Perform force merge only on the primary shards.
            final ForceMergeResponse forceMergeResponse = client().admin()
                .indices()
                .prepareForceMerge(INDEX_NAME)
                .setPrimaryOnly(primaryOnly)
                .setMaxNumSegments(1)
                .setFlush(false)
                .get();
            assertThat(forceMergeResponse.getFailedShards(), is(0));
            assertThat(forceMergeResponse.getSuccessfulShards(), is(expectedSuccessfulShards));
            refresh(INDEX_NAME);
            if (!warmIndexSegmentReplicationEnabled()) {
                verifyStoreContent();
            }
        }
    }

    /**
     * This test verifies that segment replication does not fail for closed indices
     */
    public void testClosedIndices() {
        List<String> nodes = new ArrayList<>();
        // start 1st node so that it contains the primary
        nodes.add(internalCluster().startDataAndSearchNodes(1).get(0));
        createIndex(INDEX_NAME, super.indexSettings());
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        // start 2nd node so that it contains the replica
        nodes.add(internalCluster().startDataAndSearchNodes(1).get(0));
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
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
                    fail("File copy should not happen for warm index replica shards");
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
        FileCache fileCache = internalCluster().getInstance(Node.class, primaryNode).fileCache();
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
        fileCache.prune();
    }

    public void testCancellation() throws Exception {
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(INDEX_NAME);

        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);

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
                    fail("File copy should not happen for warm index replica shards");
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

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testDeleteOperations() throws Exception {
        final String nodeA = internalCluster().startDataAndSearchNodes(1).get(0);
        final String nodeB = internalCluster().startDataAndSearchNodes(1).get(0);

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

    public void testUpdateOperations() throws Exception {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME, settings);
        final List<String> dataNodes = internalCluster().startDataAndSearchNodes(6);
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
            FileCache fileCache = internalCluster().getInstance(Node.class, primaryNode).fileCache();
            internalCluster().stopRandomNode(InternalTestCluster.nameFilter(primaryNode));
            ensureYellow(INDEX_NAME);

            // start another replica.
            dataNodes.add(internalCluster().startDataAndSearchNodes(1).get(0));
            ensureGreen(INDEX_NAME);
            waitForSearchableDocs(initialDocCount, dataNodes);

            // index another doc and refresh - without this the new replica won't catch up.
            String docId = String.valueOf(initialDocCount + 1);
            client().prepareIndex(INDEX_NAME).setId(docId).setSource("foo", "bar").get();

            flushAndRefresh(INDEX_NAME);
            waitForSearchableDocs(initialDocCount + 1, dataNodes);
            verifyStoreContent();
            fileCache.prune();
        }
    }

    @TestLogging(reason = "Getting trace logs from replication package", value = "org.opensearch.indices.replication:TRACE")
    public void testReplicaHasDiffFilesThanPrimary() throws Exception {
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME, Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
        // ToDo: verifyStoreContent() needs to be fixed for warm indices
        if (!warmIndexSegmentReplicationEnabled()) {
            verifyStoreContent();
        }
        final IndexShard replicaAfterFailure = getIndexShard(replicaNode, INDEX_NAME);
        assertNotEquals(replicaAfterFailure.routingEntry().allocationId().getId(), replicaShard.routingEntry().allocationId().getId());
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/17527")
    public void testPressureServiceStats() throws Exception {
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        createIndex(INDEX_NAME);
        ensureYellow(INDEX_NAME);
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
            FileCache fileCache = internalCluster().getInstance(Node.class, primaryNode).fileCache();
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
            String replicaNode_2 = internalCluster().startDataAndSearchNodes(1).get(0);
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
            fileCache.prune();
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        prepareCreate(
            INDEX_NAME,
            Settings.builder()
                .put(indexSettings())
                // we want to control refreshes
                .put("index.refresh_interval", -1)
        ).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
        ensureGreen(INDEX_NAME);

        client().prepareIndex(INDEX_NAME)
            .setId(String.valueOf(0))
            .setSource(jsonBuilder().startObject().field("field", 0).endObject())
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        refresh(INDEX_NAME);

        assertBusy(
            () -> assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            )
        );

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

        final IndexShard replicaShard = getIndexShard(replica, INDEX_NAME);
        SegmentInfos latestSegmentInfos = getLatestSegmentInfos(replicaShard);
        final Set<String> snapshottedSegments = new HashSet<>(latestSegmentInfos.files(false));
        logger.info("Segments {}", snapshottedSegments);

        // index more docs and force merge down to 1 segment
        for (int i = 1; i < 5; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setSource(jsonBuilder().startObject().field("field", i).endObject())
                .get();
            refresh(INDEX_NAME);
        }
        // create new on-disk segments and copy them out.
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });

        // force merge and flush.
        client().admin().indices().prepareForceMerge(INDEX_NAME).setMaxNumSegments(1).setFlush(true).get();
        // wait for replication to complete
        assertBusy(() -> {
            assertEquals(
                getIndexShard(primary, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion(),
                getIndexShard(replica, INDEX_NAME).getLatestReplicationCheckpoint().getSegmentInfosVersion()
            );
        });
        logger.info("Local segments after force merge and commit {}", getLatestSegmentInfos(replicaShard).files(false));

        // Test stats
        logger.info("--> Collect all scroll query hits");
        long scrollHits = 0;
        do {
            scrollHits += searchResponse.getHits().getHits().length;
            searchResponse = client(replica).prepareSearchScroll(searchResponse.getScrollId()).setScroll(TimeValue.timeValueDays(1)).get();
            assertAllSuccessful(searchResponse);
        } while (searchResponse.getHits().getHits().length > 0);
        assertEquals(1, scrollHits);

        client(replica).prepareClearScroll().addScrollId(searchResponse.getScrollId()).get();
        final Set<String> filesAfterClearScroll = Arrays.stream(replicaShard.store().directory().listAll()).collect(Collectors.toSet());
        // there should be no active readers, snapshots, or on-disk commits containing the snapshotted files, check that they have been
        // deleted.
        Set<String> latestCommitSegments = new HashSet<>(replicaShard.store().readLastCommittedSegmentsInfo().files(false));
        assertEquals(
            "Snapshotted files are no longer part of the latest commit",
            Collections.emptySet(),
            Sets.intersection(latestCommitSegments, snapshottedSegments)
        );
        assertEquals(
            "All snapshotted files should be deleted",
            Collections.emptySet(),
            Sets.intersection(filesAfterClearScroll, snapshottedSegments)
        );
    }

    /**
     * This tests that if a primary receives docs while a replica is performing round of segrep during recovery
     * the replica will catch up to latest checkpoint once recovery completes without requiring an additional primary refresh/flush.
     */
    public void testPrimaryReceivesDocsDuringReplicaRecovery() throws Exception {
        final List<String> nodes = new ArrayList<>();
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        nodes.add(primaryNode);
        final Settings settings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(INDEX_NAME, settings);
        ensureGreen(INDEX_NAME);
        // start a replica node, initially will be empty with no shard assignment.
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);

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

    /**
     * Tests whether segment replication supports realtime get requests and reads and parses source from the translog to serve strong reads.
     */
    public void testRealtimeGetRequestsSuccessful() {
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime get
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(Settings.builder().put("index.refresh_interval", -1).put(indexSettings()))
                .addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        // refresh interval disabled to ensure refresh rate of index (when data is ready for search) doesn't affect realtime multi get
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put("index.refresh_interval", -1).put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
            ).addAlias(new Alias("alias"))
        );
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primary = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String replica = internalCluster().startDataAndSearchNodes(1).get(0);
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
        final String primaryNode = internalCluster().startDataAndSearchNodes(1).get(0);
        nodes.add(primaryNode);
        final Settings settings = Settings.builder().put(indexSettings()).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build();
        createIndex(INDEX_NAME, settings);
        ensureGreen(INDEX_NAME);
        // start a replica node, initially will be empty with no shard assignment.
        final String replicaNode = internalCluster().startDataAndSearchNodes(1).get(0);
        nodes.add(replicaNode);
        final String replicaNode2 = internalCluster().startDataAndSearchNodes(1).get(0);
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
        waitForSearchableDocs(1, primaryNode, replicaNode, replicaNode2);

        FileCache fileCache = internalCluster().getInstance(Node.class, primaryNode).fileCache();
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
        fileCache.prune();
    }

}
