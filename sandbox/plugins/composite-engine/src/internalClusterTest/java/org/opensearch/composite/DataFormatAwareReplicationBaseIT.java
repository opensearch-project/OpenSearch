/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.BackgroundIndexer;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract base class for DFA replication integration tests. Centralizes boilerplate
 * shared across promotion and peer-recovery ITs.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public abstract class DataFormatAwareReplicationBaseIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "dfa-replication-base-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class)
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    protected Settings dfaIndexSettings(int replicaCount) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of())
            .build();
    }

    protected void createDfaIndex(int replicaCount) throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(replicaCount)).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }

    /** Index N docs with RefreshPolicy.NONE. */
    protected void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }

    /** Primary's node name. */
    protected String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return nodeNameFromId(nodeId);
    }

    /** Replica node names. */
    protected List<String> replicaNodeNames() {
        return getClusterState().routingTable()
            .index(INDEX_NAME)
            .shard(0)
            .replicaShards()
            .stream()
            .filter(s -> s.started())
            .map(s -> nodeNameFromId(s.currentNodeId()))
            .collect(Collectors.toList());
    }

    private String nodeNameFromId(String nodeId) {
        // Use the authoritative cluster-state node map. During a failover the old primary's node id
        // can linger in the routing table for a moment after the node leaves; throw an AssertionError
        // so that callers under assertBusy() retry rather than fail hard.
        org.opensearch.cluster.node.DiscoveryNode node = getClusterState().nodes().get(nodeId);
        if (node == null) {
            throw new AssertionError("node with id " + nodeId + " not present in cluster state yet");
        }
        return node.getName();
    }

    /** Wait for the given shard's primary term to reach expected. */
    protected void waitForPrimaryTerm(String index, int shardId, long expectedTerm, TimeValue timeout) throws Exception {
        assertBusy(() -> {
            String currentPrimaryNode = primaryNodeName();
            IndexShard shard = getIndexShard(currentPrimaryNode, index);
            assertEquals("primary term did not reach expected value", expectedTerm, shard.getOperationPrimaryTerm());
        }, timeout.seconds(), TimeUnit.SECONDS);
    }

    /** Read the current catalog generation of a specific node's copy of the shard. */
    protected long readCatalogGeneration(String nodeName, String index) throws Exception {
        IndexShard shard = getIndexShard(nodeName, index);
        try (GatedCloseable<CatalogSnapshot> closeable = shard.getCatalogSnapshot()) {
            return closeable.get().getGeneration();
        }
    }

    /** Assert post-test convergence: primary and replica have equal catalog files (excluding segments_N). */
    protected void assertCatalogSnapshotsConverged(String index) throws Exception {
        assertBusy(() -> {
            try {
                IndexShard primary = getIndexShard(primaryNodeName(), index);
                Set<String> primaryFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
                for (String replicaNode : replicaNodeNames()) {
                    IndexShard replica = getIndexShard(replicaNode, index);
                    Set<String> replicaFiles = DataFormatAwareITUtils.catalogFilesExcludingSegments(replica);
                    assertEquals("primary/replica catalog files must converge on node " + replicaNode, primaryFiles, replicaFiles);
                }
                DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
            } catch (org.apache.lucene.store.AlreadyClosedException e) {
                // Engine can be transiently closed while a shard is being re-assigned after a
                // promotion (old primary → new replica). Treat as assertBusy-retryable.
                throw new AssertionError("engine transiently closed during reassignment; will retry", e);
            }
        }, 60, TimeUnit.SECONDS);
    }

    /** Assert the new generation is strictly greater than the reference. */
    protected void assertGenerationMonotonic(long reference, long current) {
        assertTrue("catalog generation must advance: reference=" + reference + ", current=" + current, current > reference);
    }

    /**
     * Wait for the BackgroundIndexer's acknowledged doc count to reach {@code numDocs}.
     * Replaces {@code waitForDocs(long, BackgroundIndexer)} which issues a Lucene search
     * that DFA engines reject (see {@link #assertNoDataLoss}).
     */
    protected void waitForIndexerDocs(long numDocs, BackgroundIndexer indexer) throws Exception {
        assertBusy(
            () -> assertTrue(
                "expected at least " + numDocs + " acked docs, got " + indexer.totalIndexedDocs(),
                indexer.totalIndexedDocs() >= numDocs
            ),
            60,
            TimeUnit.SECONDS
        );
    }

    /**
     * Verify that indexing acknowledged writes and that primary/replica catalogs converge.
     *
     * <p>We cannot rely on per-doc counting on DFA indices: {@code DataformatAwareCatalogSnapshot.getNumDocs()}
     * currently returns {@code 0L} (stub), and the Lucene search path is rejected for DFA engines via
     * {@code applyOnEngine}. The two signals available are:
     * <ul>
     *   <li>The BackgroundIndexer's own accounting of acknowledged writes.</li>
     *   <li>Convergence of the catalog file set between primary and replica.</li>
     * </ul>
     * Together these catch the common correctness regressions this test suite targets
     * (promotion losing commits, recovery dropping files, orphan files on replica).
     */
    protected void assertNoDataLoss(BackgroundIndexer indexer, String index) throws Exception {
        indexer.assertNoFailures();
        assertTrue("BackgroundIndexer acknowledged zero docs — indexing never made progress", indexer.totalIndexedDocs() > 0);
        // Flush so every ack'd write lives in a persisted catalog snapshot.
        client().admin().indices().prepareFlush(index).get();
        assertCatalogSnapshotsConverged(index);
    }

    /** Hard-kill a node with no graceful handshake. */
    protected void crashNode(String nodeName) throws Exception {
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));
    }

    /** Graceful stop with handshake. */
    protected void gracefulStopNode(String nodeName) throws Exception {
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(nodeName));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
    }

    /**
     * Trigger primary promotion WITHOUT stopping the host node, by cancelling the primary's
     * allocation via {@code _cluster/reroute}. The replica is promoted to primary while both
     * nodes remain alive. Avoids tearing down node-scoped native resources (e.g. the shared
     * DataFusion Tokio runtime manager in test clusters), which is why we prefer this over
     * {@link #gracefulStopNode}/{@link #crashNode} for promotion scenarios.
     */
    protected void cancelPrimaryAllocation(String index, int shardId, String nodeName) throws Exception {
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand(index, shardId, nodeName, true))
            .execute()
            .actionGet();
    }
}
