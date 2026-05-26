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
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Validates durability of the parquet-primary / lucene-secondary composite engine across a
 * full cluster restart, when a writer is retired mid-stream because of a per-doc failure.
 *
 * <p>Three variants exercise different durability paths:
 * <ul>
 *   <li>{@code testRetiredWriterDataSurvivesRestart_translogReplayOnly} — no refresh, no
 *       flush. Restart relies entirely on translog replay rebuilding the segments.</li>
 *   <li>{@code testRetiredWriterDataSurvivesRestart_refreshOnly} — refresh (segments
 *       searchable, but no commit). Restart still has to replay the translog from the last
 *       persisted commit.</li>
 *   <li>{@code testRetiredWriterDataSurvivesRestart_refreshAndFlush} — refresh + flush
 *       (catalog snapshot committed). Restart restores from the committed snapshot;
 *       translog replay is a no-op past the commit point.</li>
 * </ul>
 *
 * <p>Each variant indexes N docs into writer-1, induces a per-doc parquet failure that
 * retires writer-1, indexes M more docs into writer-2, then full-restarts. After restart,
 * both formats must report exactly {@code N + M} live docs.
 *
 * <p>Cluster: 1 shard, 0 replicas (durability-only test, no replication assertions).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeEngineParquetFailureDurabilityIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "composite-parquet-durability-idx";

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
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", List.of("lucene"))
            .build();
    }

    /** Restart relies entirely on translog replay — no segments committed pre-restart. */
    public void testRetiredWriterDataSurvivesRestart_translogReplayOnly() throws Exception {
        runDurabilityScenario(/* refreshBeforeRestart= */ false, /* flushBeforeRestart= */ false);
    }

    /** Refresh makes segments searchable but doesn't commit; translog replay still required. */
    public void testRetiredWriterDataSurvivesRestart_refreshOnly() throws Exception {
        runDurabilityScenario(/* refreshBeforeRestart= */ true, /* flushBeforeRestart= */ false);
    }

    /** Refresh + flush commits the catalog snapshot; restart restores from commit. */
    public void testRetiredWriterDataSurvivesRestart_refreshAndFlush() throws Exception {
        runDurabilityScenario(/* refreshBeforeRestart= */ true, /* flushBeforeRestart= */ true);
    }

    private void runDurabilityScenario(boolean refreshBeforeRestart, boolean flushBeforeRestart) throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings()).setMapping("f1", "type=keyword").get();
        ensureGreen(INDEX_NAME);

        // Phase 1: index N docs with f1 into writer-1.
        final int initialDocs = 5;
        for (int i = 0; i < initialDocs; i++) {
            IndexResponse r = client().prepareIndex(INDEX_NAME).setId("d" + i).setSource("f1", "v" + i).get();
            assertEquals(RestStatus.CREATED, r.status());
        }

        // Phase 2: suppress the writer's mapping-version updates and add f2 to the live
        // mapping. The writer's schema stays at v1, so a doc carrying f2 trips
        // MismatchedInputException on the parquet primary. Composite rolls back; DFAE
        // retires writer-1 and queues its flushed segment in pendingSegments.
        FailableParquetDataFormatPlugin.armSchemaSuppression();
        client().admin().indices().preparePutMapping(INDEX_NAME).setSource("f2", "type=keyword").get();

        BulkResponse bulk = client().prepareBulk().add(client().prepareIndex(INDEX_NAME).setId("fail").setSource("f2", "value-fail")).get();
        assertEquals(1, bulk.getItems().length);
        BulkItemResponse failed = bulk.getItems()[0];
        assertTrue("oversized-schema doc must surface as a per-item failure: " + failed.getFailureMessage(), failed.isFailed());

        // Phase 3: drop suppression so a fresh writer reconciles to v2 cleanly. Index M
        // more docs with f1 — these go into writer-2.
        FailableParquetDataFormatPlugin.clearFailure();
        final int postRetirementDocs = 3;
        for (int i = 0; i < postRetirementDocs; i++) {
            IndexResponse r = client().prepareIndex(INDEX_NAME).setId("p" + i).setSource("f1", "p" + i).get();
            assertEquals(RestStatus.CREATED, r.status());
        }

        // Phase 4: optional refresh / flush before restart, per scenario.
        if (refreshBeforeRestart) {
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        if (flushBeforeRestart) {
            client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).setWaitIfOngoing(true).get();
        }

        // Phase 5: full cluster restart.
        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        // Phase 6: every successful doc must have survived. Both formats must agree —
        // whether durability came from the committed catalog snapshot, an in-flight
        // segment recovered via translog replay, or pure translog replay.
        long expected = initialDocs + postRetirementDocs;
        long parquetRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "parquet");
        long luceneRows = CompositeEngineHelper.getRowCount(clusterService(), internalCluster(), INDEX_NAME, "lucene");
        assertEquals("parquet must hold exactly " + expected + " live docs after restart", expected, parquetRows);
        assertEquals("lucene must hold exactly " + expected + " live docs after restart", expected, luceneRows);
    }
}
