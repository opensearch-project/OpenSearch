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
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Exercises the parquet-primary self-rollback path end-to-end. Sequence:
 * <ol>
 *   <li>Doc 1 with field {@code f1} succeeds.</li>
 *   <li>Arm {@link FailableParquetDataFormatPlugin#armSchemaSuppression()} and add a new
 *       field {@code f2} via put-mapping. The parquet writer's mapping-version update is
 *       dropped, so its schema stays at v1.</li>
 *   <li>Doc 2 carries {@code f2}. {@code VSRManager.addDocument} raises
 *       {@link org.opensearch.parquet.writer.MismatchedInputException} (no {@code f2} vector
 *       on the active VSR). {@code ParquetWriter.addDoc} catches it (state PENDING_ROLLBACK → returns
 *       {@code WriteResult.Failure}), the composite drives {@code rollbackLastDoc} which
 *       no-ops in the VSR (since the doc never landed) and restores ACTIVE.</li>
 *   <li>Flush — retires the v1 writer, drains pending segments, mints a fresh writer.</li>
 *   <li>Clear suppression. Doc 3 with {@code f1} succeeds on the new writer (no schema
 *       mismatch since {@code f1} is in every schema version).</li>
 * </ol>
 *
 * <p>Asserts: engine stays open, primary maxSeqNo, processed/persisted LCP, and lastSyncedGCP
 * converge to 2 (failed doc backfilled by Translog.NoOp), replica processedLCP catches up,
 * and parquet/lucene each hold the 2 successful docs.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeEngineParquetFailureCheckpointIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-parquet-failure-idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(
            super.nodePlugins().stream(),
            Stream.of(
                ArrowBasePlugin.class,
                FailableParquetDataFormatPlugin.class,
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        FailableParquetDataFormatPlugin.clearFailure();
    }

    @Override
    public void tearDown() throws Exception {
        FailableParquetDataFormatPlugin.clearFailure();
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
            // Tighten the periodic GCP sync so the post-failure assertion doesn't race a
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

    public void testParquetAddDocFailureSelfRollsAndCheckpointsConverge() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings()).setMapping("f1", "type=keyword").get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Doc 1 with the original mapping (just f1) — succeeds.
        IndexResponse ok1 = client().prepareIndex(INDEX_NAME).setId("1").setSource("f1", "value-1").get();
        assertEquals(RestStatus.CREATED, ok1.status());

        // Suppress the parquet writer's mapping-version updates. Then add a new field f2
        // to the index mapping — the writer's schema stays at v1, so a doc carrying f2 will
        // hit VSRManager.addDocument with no ParquetField mapping and the production code
        // raises MismatchedInputException. ParquetWriter.addDoc catches it, briefly
        // transitions to PENDING_ROLLBACK, runs rollbackLastDoc (no-op against the VSR since the doc
        // never landed), restores ACTIVE, and returns WriteResult.Failure.
        FailableParquetDataFormatPlugin.armSchemaSuppression();
        client().admin().indices().preparePutMapping(INDEX_NAME).setSource("f2", "type=keyword").get();

        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setId("2").setSource("f2", "value-2")).get();
        assertEquals(1, bulk.getItems().length);
        BulkItemResponse failed = bulk.getItems()[0];
        assertTrue("doc 2 must surface as a per-item failure: " + failed.getFailureMessage(), failed.isFailed());
        assertNotNull("failure message should be present", failed.getFailureMessage());

        // Engine must still be open: MismatchedInputException is per-doc, not tragic.
        // commitStats() throws AlreadyClosedException if the engine has failed/closed.
        DataFormatAwareEngine engineAfterFailure = CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
        assertNotNull("engine must still hold a valid commit after parquet per-doc failure", engineAfterFailure.commitStats());

        // Flush the v1-schema writer out so doc 3 mints a fresh writer that reconciles to
        // the live mapping cleanly. Then drop suppression so future mapping updates propagate.
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        FailableParquetDataFormatPlugin.clearFailure();

        // Doc 3 with f1 (already in every schema version) succeeds on the fresh writer —
        // proves the engine survives a per-doc parquet failure and continues serving writes.
        IndexResponse ok3 = client().prepareIndex(INDEX_NAME).setId("3").setSource("f1", "value-3").get();
        assertEquals(RestStatus.CREATED, ok3.status());

        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        getIndexShard(primaryNodeName(), INDEX_NAME).maybeSyncGlobalCheckpoint("post-failure-test-sync");

        assertBusy(() -> {
            IndexShard p = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard r = getIndexShard(replicaNodeName(), INDEX_NAME);
            assert p.isActive() : "Primary is not active";
            assert r.isActive() : "Replica is not active";
            assertEquals("primary maxSeqNo", 2L, p.seqNoStats().getMaxSeqNo());
            assertEquals("primary processedLCP", 2L, p.getProcessedLocalCheckpoint());
            assertEquals("primary persistedLCP", 2L, p.getLocalCheckpoint());
            assertEquals("primary lastSyncedGCP catches up to LCP", p.getLocalCheckpoint(), p.getLastSyncedGlobalCheckpoint());
            assertEquals("replica processedLCP matches primary", p.getProcessedLocalCheckpoint(), r.getProcessedLocalCheckpoint());
        }, 30, TimeUnit.SECONDS);

        long parquetRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "parquet");
        long luceneRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "lucene");
        assertEquals("parquet must hold exactly 2 successful docs", 2L, parquetRows);
        assertEquals("lucene must hold exactly 2 successful docs", 2L, luceneRows);
    }

    /**
     * After a parquet self-rollback (Failure → state PENDING_ROLLBACK → composite drives rollback →
     * state ACTIVE), the writer should remain in the pool and the next doc must land on the
     * <b>same writer instance</b>. This validates that ACTIVE post-rollback prevents
     * unnecessary writer churn for fully-reversible failure modes.
     */
    public void testParquetSelfRollbackKeepsSameWriterInPool() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings()).setMapping("f1", "type=keyword").get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);

        // Doc 1 mints writer-1 in the pool.
        assertEquals(RestStatus.CREATED, client().prepareIndex(INDEX_NAME).setId("1").setSource("f1", "v1").get().status());

        DataFormatAwareEngine engine = CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
        Writer<?> writerBefore = singleWriter(engine);

        // Force a parquet self-rollback: suppression + new field → MismatchedInputException
        // → composite rollback → state restored to ACTIVE (no flush, no retire).
        FailableParquetDataFormatPlugin.armSchemaSuppression();
        client().admin().indices().preparePutMapping(INDEX_NAME).setSource("f2", "type=keyword").get();
        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setId("fail").setSource("f2", "x")).get();
        assertTrue("must surface per-item failure", bulk.getItems()[0].isFailed());
        FailableParquetDataFormatPlugin.clearFailure();

        // Doc 3 (f1, schema v1-compatible) must land on the same writer instance.
        assertEquals(RestStatus.CREATED, client().prepareIndex(INDEX_NAME).setId("3").setSource("f1", "v3").get().status());
        Writer<?> writerAfter = singleWriter(engine);

        assertSame("parquet self-rollback must keep the same writer instance in the pool", writerBefore, writerAfter);
        assertEquals("writer must still be ACTIVE after self-rollback", WriterState.ACTIVE, writerAfter.state());
    }

    private static Writer<?> singleWriter(DataFormatAwareEngine engine) {
        List<Writer<?>> writers = new ArrayList<>();
        engine.getWriterPool().forEach(writers::add);
        assertEquals("expected exactly one writer in pool", 1, writers.size());
        return writers.get(0);
    }
}
