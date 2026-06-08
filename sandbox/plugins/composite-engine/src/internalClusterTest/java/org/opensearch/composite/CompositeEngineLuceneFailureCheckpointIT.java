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
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
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
 * Verifies seqNo / checkpoint correctness on the composite engine when a Lucene secondary
 * write hits a {@link org.apache.lucene.store.Directory}-level I/O fault. The IOException
 * is a tragic event: IndexWriter self-closes, DFAE fails the engine, the cluster fails the
 * primary, the replica is promoted, and the bulk client retries the failed doc on the new
 * primary.
 *
 * <p>Cluster: 2 data nodes, 1 shard, 1 replica, segment replication, remote-store-backed,
 * with Lucene wrapped by {@link FailableLuceneDataFormatPlugin}. After flush + explicit GCP
 * sync, primary {@code maxSeqNo}, processed/persisted LCP, and lastSyncedGCP converge to 2
 * and the replica's processed LCP catches up.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeEngineLuceneFailureCheckpointIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-lucene-failure-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                ParquetDataFormatPlugin.class,
                CompositeDataFormatPlugin.class,
                FailableLuceneDataFormatPlugin.class,
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FailableLuceneDataFormatPlugin.clearFailure();
    }

    @Override
    public void tearDown() throws Exception {
        FailableLuceneDataFormatPlugin.clearFailure();
        super.tearDown();
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

    public void testCheckpointsConvergeAfterLuceneDirectoryFault() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings()).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        BulkItemResponse doc1 = indexOne("1", "value-1");
        assertFalse("doc 1 must succeed: " + doc1.getFailureMessage(), doc1.isFailed());

        // Arm a Directory write fault on the next op. Lucene's flush triggered by doc 2's
        // addDoc hits the fault, IndexWriter goes tragic, engine fails, primary fails over
        // to the replica, and the bulk client retries doc 2 on the new primary.
        FailableLuceneDataFormatPlugin.failOnNthWrite(0);
        BulkItemResponse doc2 = indexOne("2", "value-2");
        FailableLuceneDataFormatPlugin.clearFailure();
        assertFalse("doc 2 must ultimately succeed via failover retry: " + doc2.getFailureMessage(), doc2.isFailed());
        ensureGreen(INDEX_NAME);

        BulkItemResponse doc3 = indexOne("3", "value-3");
        assertFalse("doc 3 must succeed: " + doc3.getFailureMessage(), doc3.isFailed());

        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // Force the GCP sync immediately instead of waiting for the periodic timer (default
        // 30s) — the new primary's lastKnownGCP has advanced past what it propagated to the
        // replica during failover and we want the replica to catch up before assertions.
        getIndexShard(primaryNodeName(), INDEX_NAME).maybeSyncGlobalCheckpoint("post-failover-test-sync");

        assertBusy(() -> {
            IndexShard p = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard r = getIndexShard(replicaNodeName(), INDEX_NAME);
            assertEquals("primary maxSeqNo", 2L, p.seqNoStats().getMaxSeqNo());
            assertEquals("primary processedLCP", 2L, p.getProcessedLocalCheckpoint());
            assertEquals("primary persistedLCP", 2L, p.getLocalCheckpoint());
            assertEquals("primary lastSyncedGCP catches up to LCP", p.getLocalCheckpoint(), p.getLastSyncedGlobalCheckpoint());
            assertEquals("replica lastProcessedLCP matches primary", p.getProcessedLocalCheckpoint(), r.getProcessedLocalCheckpoint());
        }, 30, TimeUnit.SECONDS);
    }

    private BulkItemResponse indexOne(String id, String value) {
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setId(id).setSource("field", value)).get();
        assertEquals(1, bulk.getItems().length);
        return bulk.getItems()[0];
    }
}
