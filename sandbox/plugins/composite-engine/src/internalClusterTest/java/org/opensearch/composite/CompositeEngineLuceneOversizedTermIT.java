/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.IndexWriter;
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
import org.opensearch.parquet.ParquetDataFormatPlugin;
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
 * Exercises the per-doc failure path on a parquet-primary / lucene-secondary composite index
 * when Lucene's {@link IndexWriter#MAX_TERM_LENGTH} (32766 UTF-8 bytes) rejects a doc with
 * {@link IllegalArgumentException}. No fault injection — a 33000-char {@code keyword} value
 * trips the limit naturally.
 *
 * <p>Two variants cover both branches of {@code LuceneWriter#flush}: without index sort
 * (plain force-merge) and with index sort (RowIdMapping-driven reorder merge).
 *
 * <p>Asserts: engine stays open, primary maxSeqNo, processed/persisted LCP, and lastSyncedGCP
 * all converge to 2 (the failed doc's seqNo backfilled by a Translog.NoOp), replica
 * processedLCP catches up, and parquet/lucene each hold the 2 successful docs.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeEngineLuceneOversizedTermIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-lucene-oversized-term-idx";
    /** 33000 chars > MAX_TERM_LENGTH (32766 bytes); ASCII so byte-length == char-length. */
    private static final int OVERSIZED_TERM_CHARS = 33000;

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

    private Settings dfaIndexSettings(boolean withIndexSort) {
        Settings.Builder sb = Settings.builder()
            .put(remoteStoreIndexSettings(1, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            // TODO: Remove the merge_on_refresh_max_size=0b override once the thread leak in the
            // merge-on-refresh code path is fixed.
            .put("index.composite.merge_on_refresh_max_size", "0b")
            // Tighten the periodic GCP sync so the post-failover assertion doesn't race a
            // 30s default timer waiting for the replica to catch up.
            .put("index.global_checkpoint_sync.interval", "1s");
        if (withIndexSort) {
            // Forces parquet to compute a RowIdMapping on flush, which drives the secondary
            // Lucene flush down the ReorderingMergePolicy branch.
            sb.putList("index.sort.field", "sort_key").putList("index.sort.order", "asc");
        }
        return sb.build();
    }

    private void createIndex(boolean withIndexSort) {
        // keyword field with default ignore_above forwards the entire string as one token,
        // so an oversized value reaches Lucene's per-term length check verbatim.
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(dfaIndexSettings(withIndexSort))
            .setMapping("field", "type=keyword", "sort_key", "type=long")
            .get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
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

    public void testOversizedTermProducesNoOpAndEngineStaysHealthy() throws Exception {
        runOversizedTermScenario(/* withIndexSort= */ false);
    }

    public void testOversizedTermWithIndexSortProducesNoOpAndEngineStaysHealthy() throws Exception {
        runOversizedTermScenario(/* withIndexSort= */ true);
    }

    private void runOversizedTermScenario(boolean withIndexSort) throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(withIndexSort);

        // Doc 1: succeeds.
        IndexResponse ok1 = client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "value-1", "sort_key", 10L).get();
        assertEquals(RestStatus.CREATED, ok1.status());

        // Doc 2: a single oversized keyword token. Lucene's IndexingChain throws
        // IllegalArgumentException from termsHashPerField.add — no tragic event, IndexWriter
        // remains open, the bulk item surfaces as a per-doc failure.
        String oversized = "x".repeat(OVERSIZED_TERM_CHARS);
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex(INDEX_NAME).setId("2").setSource("field", oversized, "sort_key", 5L))
            .get();
        assertEquals(1, bulk.getItems().length);
        BulkItemResponse failed = bulk.getItems()[0];
        assertTrue("oversized term doc must fail: " + failed.getFailureMessage(), failed.isFailed());
        assertNotNull("failure message should be present", failed.getFailureMessage());
        assertTrue(
            "failure should mention immense term: " + failed.getFailureMessage(),
            failed.getFailureMessage().contains("immense term") || failed.getFailureMessage().contains("max length")
        );

        // Engine must still be open: an oversized-term IAE is a per-doc rejection, not a
        // tragic event. The Lucene secondary writer transitions ACTIVE→PENDING_ROLLBACK, the composite
        // rolls back the parquet primary, and DFAE retires the writer to RETIRED_FLUSHABLE
        // (its buffered N-1 docs flushed into pendingSegments). The engine remains usable —
        // commitStats() throws AlreadyClosedException if the engine has failed/closed.
        DataFormatAwareEngine engineAfterFailure = CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
        assertNotNull("engine must still hold a valid commit after per-doc IAE", engineAfterFailure.commitStats());

        // Doc 3: succeeds on the same primary — proves the engine and writer pool recovered.
        // sort_key=1 is less than doc 1's sort_key=10, so the index-sort variant exercises
        // a non-trivial RowIdMapping permutation rather than the identity case.
        IndexResponse ok3 = client().prepareIndex(INDEX_NAME).setId("3").setSource("field", "value-3", "sort_key", 1L).get();
        assertEquals(RestStatus.CREATED, ok3.status());

        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        // Force the GCP sync immediately instead of waiting for the periodic timer — after
        // the NoOp + flush there are no further indexing ops to piggyback the new GCP onto a
        // replication request, and we want to assert state quickly.
        getIndexShard(primaryNodeName(), INDEX_NAME).maybeSyncGlobalCheckpoint("post-noop-test-sync");

        assertBusy(() -> {
            IndexShard p = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard r = getIndexShard(replicaNodeName(), INDEX_NAME);
            assert p.isActive() : "Primary is not active";
            assert r.isActive() : "Replica is not active";
            // Sequence-number bookkeeping: 3 ops attempted (seq 0, 1, 2). The failed doc 2 was
            // backfilled by a Translog.NoOp at DataFormatAwareEngine.indexIntoEngine, so the
            // LCP covers the gap and processed/persisted both equal maxSeqNo.
            assertEquals("primary maxSeqNo", 2L, p.seqNoStats().getMaxSeqNo());
            assertEquals("primary processedLCP", 2L, p.getProcessedLocalCheckpoint());
            assertEquals("primary persistedLCP", 2L, p.getLocalCheckpoint());
            assertEquals("primary lastSyncedGCP catches up to LCP", p.getLocalCheckpoint(), p.getLastSyncedGlobalCheckpoint());
            assertEquals("replica processedLCP matches primary", p.getProcessedLocalCheckpoint(), r.getProcessedLocalCheckpoint());
        }, 30, TimeUnit.SECONDS);

        // Cross-format parity on the primary: 2 successful docs in parquet AND in lucene —
        // the failed doc contributed neither a parquet row nor a lucene live doc.
        long parquetRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "parquet");
        long luceneRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "lucene");
        assertEquals("parquet must hold exactly 2 successful docs", 2L, parquetRows);
        assertEquals("lucene must hold exactly 2 successful docs", 2L, luceneRows);
    }

    /**
     * Lucene's docId allocator can't be reversed: after a per-doc IAE the secondary
     * transitions to RETIRED_FLUSHABLE post-rollback, DFAE retires it (flushes the buffered
     * N-1 docs, closes the writer, queues the segment in pendingSegments). The pool then
     * mints a fresh writer for the next doc.
     *
     * <p>This test asserts both halves: the writer instance held by the pool after the
     * failure is <b>different</b> from the one before, and the original writer is
     * <b>closed</b> (state == CLOSED — its IndexWriter, Directory, and temp dir released).
     */
    public void testLuceneFailureReplacesWriterAndClosesOldOne() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(/* withIndexSort= */ false);

        // Doc 1 mints writer-1 in the pool.
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(INDEX_NAME).setId("1").setSource("field", "v1", "sort_key", 10L).get().status()
        );

        DataFormatAwareEngine engine = CompositeEngineHelper.getEngine(clusterService(), internalCluster(), INDEX_NAME);
        Writer<?> writerBefore = singleWriter(engine);

        // Doc 2: oversized term → Lucene IAE → RETIRED_FLUSHABLE → DFAE retires + closes.
        String oversized = "x".repeat(OVERSIZED_TERM_CHARS);
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex(INDEX_NAME).setId("2").setSource("field", oversized, "sort_key", 5L))
            .get();
        assertTrue("oversized term doc must fail", bulk.getItems()[0].isFailed());

        // Doc 3 forces the pool to mint a fresh writer (the old one is gone).
        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex(INDEX_NAME).setId("3").setSource("field", "v3", "sort_key", 1L).get().status()
        );

        Writer<?> writerAfter = singleWriter(engine);
        assertNotSame("Lucene retirement must replace the writer instance in the pool", writerBefore, writerAfter);
        assertEquals("the retired writer must be CLOSED — resources released", WriterState.CLOSED, writerBefore.state());
        assertEquals("the new writer must be ACTIVE", WriterState.ACTIVE, writerAfter.state());
    }

    private static Writer<?> singleWriter(DataFormatAwareEngine engine) {
        List<Writer<?>> writers = new ArrayList<>();
        engine.getWriterPool().forEach(writers::add);
        assertEquals("expected exactly one writer in pool", 1, writers.size());
        return writers.get(0);
    }
}
