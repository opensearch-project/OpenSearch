/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.CancelAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Control variant of {@link CompositeEngineLuceneFailureCheckpointIT}: unmodified
 * {@link LucenePlugin}, with failover triggered by {@link CancelAllocationCommand} so both
 * nodes stay alive and the node-scoped DataFusion runtime survives.
 *
 * <p>Cluster: 2 data nodes, 1 shard, 1 replica, segment replication, remote-store-backed.
 * After cancel-reroute → 3 docs → flush + explicit GCP sync, asserts the same invariants as
 * the fault-injection variant: primary maxSeqNo, processed/persisted LCP, and lastSyncedGCP
 * converge to 2 and the replica's processed LCP catches up.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeEnginePrimaryFailoverCheckpointIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-failover-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                LucenePlugin.class,
                DataFusionPlugin.class,
                InternalSettingsPlugin.class
            )
        ).collect(Collectors.toList());
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    private Settings dfaIndexSettings() {
        return Settings.builder()
            .put(remoteStoreIndexSettings(1, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            // Tighten the periodic GCP sync so the post-failover assertion doesn't race a
            // 30s default timer waiting for the replica to catch up.
            .put("index.global_checkpoint_sync.interval", "1s")
            .build();
    }

    private String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return getClusterState().nodes().get(nodeId).getName();
    }

    private String replicaNodeName() {
        String nodeId = getClusterState().routingTable()
            .index(INDEX_NAME)
            .shard(0)
            .replicaShards()
            .stream()
            .filter(s -> s.started())
            .findFirst()
            .orElseThrow()
            .currentNodeId();
        return getClusterState().nodes().get(nodeId).getName();
    }

    public void testCheckpointsConvergeAfterPrimaryFailover() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings()).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        BulkItemResponse doc1 = indexOne("1", "value-1");
        assertFalse("doc 1 must succeed: " + doc1.getFailureMessage(), doc1.isFailed());

        // Trigger a graceful primary handover: cancel the primary's allocation via cluster
        // reroute. Both nodes stay alive, the replica is promoted, and we avoid tearing down
        // the node-scoped DataFusion Tokio runtime manager (which dies on hard node stop).
        String oldPrimary = primaryNodeName();
        client().admin().cluster().prepareReroute().add(new CancelAllocationCommand(INDEX_NAME, 0, oldPrimary, true)).execute().actionGet();
        waitForPrimaryTerm(2L, TimeValue.timeValueSeconds(30));
        ensureGreen(INDEX_NAME);
        logger.info("Current Allocation: {}", getClusterState().routingTable().index(INDEX_NAME));
        BulkItemResponse doc2 = indexOne("2", "value-2");
        assertFalse("doc 2 must succeed on new primary: " + doc2.getFailureMessage(), doc2.isFailed());

        BulkItemResponse doc3 = indexOne("3", "value-3");
        assertFalse("doc 3 must succeed: " + doc3.getFailureMessage(), doc3.isFailed());

        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        getIndexShard(primaryNodeName(), INDEX_NAME).maybeSyncGlobalCheckpoint("post-failover-test-sync");

        assertBusy(() -> {
            IndexShard p = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard r = getIndexShard(replicaNodeName(), INDEX_NAME);
            assert p.isActive() : "Primary is not active";
            assert r.isActive() : "Replica is not active";
            assertEquals("primary maxSeqNo", 2L, p.seqNoStats().getMaxSeqNo());
            assertEquals("primary processedLCP", 2L, p.getProcessedLocalCheckpoint());
            assertEquals("primary persistedLCP", 2L, p.getLocalCheckpoint());
            assertEquals("primary lastSyncedGCP catches up to LCP", p.getLocalCheckpoint(), p.getLastSyncedGlobalCheckpoint());
            assertEquals("replica lastProcessedLCP matches primary", p.getProcessedLocalCheckpoint(), r.getProcessedLocalCheckpoint());
        }, 30, TimeUnit.SECONDS);
    }

    /** Waits until the primary's operationPrimaryTerm reaches {@code expectedTerm}. */
    private void waitForPrimaryTerm(long expectedTerm, TimeValue timeout) throws Exception {
        assertBusy(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            assertEquals("primary term did not reach expected value", expectedTerm, shard.getOperationPrimaryTerm());
        }, timeout.seconds(), TimeUnit.SECONDS);
    }

    private BulkItemResponse indexOne(String id, String value) {
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setSource("field", value)).get();
        assertEquals(1, bulk.getItems().length);
        return bulk.getItems()[0];
    }
}
