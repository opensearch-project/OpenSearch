/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.DataFormatAwareEngine;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardTestCase;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * POC integration tests for WRITABLE warm parquet (composite) indices.
 *
 * <p>Enabled via the experimental {@code index.warm.writable.enabled} setting: a warm
 * primary gets the full writable {@link DataFormatAwareEngine} instead of the read-only
 * engine. Lifecycle exercised on a warm node: write (native parquet, local), commit
 * (fsync via TieredSubdirectoryAwareDirectory.sync), upload to remote, afterSyncToRemote
 * flip (registry REMOTE + local delete), reads served from remote through the block cache.
 *
 * <p>Search is not yet supported on DFA indices, so validation uses stats doc counts,
 * get-by-id (exercises the native parquet read path), the remote upload map, and local
 * parquet directory contents.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, supportsDedicatedMasters = false)
public class WritableWarmParquetIT extends DataFormatAwareReadonlyEngineBaseIT {

    private static final int WARM_DOC_COUNT = 25;

    /** Create hot DFA index, seed docs, flush, then tier to warm WITH the writable POC setting. */
    private List<String> createHotIndexAndTierToWritableWarm(int replicaCount) throws Exception {
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(replicaCount)).get();
        ensureGreen(INDEX_NAME);

        List<String> docIds = new ArrayList<>();
        for (int i = 0; i < DOC_COUNT; i++) {
            IndexResponse indexResponse = client().prepareIndex(INDEX_NAME)
                .setSource("field_text", "value_" + i, "field_number", (long) i)
                .get();
            assertEquals(RestStatus.CREATED, indexResponse.status());
            docIds.add(indexResponse.getId());
        }
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Tier to warm: close, mark warm + writable POC, open.
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .indices()
            .prepareUpdateSettings(INDEX_NAME)
            .setSettings(
                Settings.builder()
                    .put(IndexModule.IS_WARM_INDEX_SETTING.getKey(), true)
                    .put(IndexModule.WARM_WRITABLE_SETTING.getKey(), true)
            )
            .get();
        client().admin().indices().prepareOpen(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);
        return docIds;
    }

    /** Index docs on the (already warm) index, values offset to distinguish from hot-phase docs. */
    private List<String> indexDocsOnWarm(int offset, int count) {
        List<String> ids = new ArrayList<>();
        for (int i = offset; i < offset + count; i++) {
            IndexResponse resp = client().prepareIndex(INDEX_NAME)
                .setSource("field_text", "warm_value_" + i, "field_number", (long) i)
                .get();
            assertEquals(RestStatus.CREATED, resp.status());
            ids.add(resp.getId());
        }
        return ids;
    }

    private long primaryDocCount() {
        return client().admin().indices().prepareStats(INDEX_NAME).get().getPrimaries().getDocs().getCount();
    }

    private Set<String> localParquetFiles(IndexShard shard) throws Exception {
        Path dir = shard.shardPath().getDataPath().resolve("parquet");
        if (Files.isDirectory(dir) == false) {
            return Set.of();
        }
        try (var stream = Files.list(dir)) {
            return stream.map(p -> p.getFileName().toString()).collect(Collectors.toSet());
        }
    }

    private Set<String> uploadedParquetFiles(IndexShard shard) {
        Map<String, UploadedSegmentMetadata> uploadMap = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
        return uploadMap.keySet().stream().filter(f -> f.startsWith("parquet/")).collect(Collectors.toSet());
    }

    /**
     * Core POC scenario: with the POC setting, the warm primary runs the full writable
     * engine and accepts new documents.
     */
    public void testWritableEngineAcceptsWritesOnWarm() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWritableWarm(0);

        // Engine must be the full writable engine, on a warm-role node.
        IndexShard primaryShard = getIndexShard(primaryNodeName());
        Indexer indexer = IndexShardTestCase.getIndexer(primaryShard);
        assertTrue(
            "warm primary with POC setting must use DataFormatAwareEngine, got: " + indexer.getClass().getSimpleName(),
            indexer instanceof DataFormatAwareEngine
        );
        assertTrue(
            "primary must be on a warm-role node",
            getClusterState().nodes().resolveNode(primaryNodeName()).getRoles().contains(DiscoveryNodeRole.WARM_ROLE)
        );

        // Writes must be accepted (read-only warm rejects these).
        List<String> warmIds = indexDocsOnWarm(DOC_COUNT, WARM_DOC_COUNT);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertBusy(() -> assertEquals("all docs must be visible in stats", DOC_COUNT + WARM_DOC_COUNT, primaryDocCount()));

        // Get-by-id must resolve warm-written docs (exercises the native parquet read path).
        for (String id : warmIds) {
            GetResponse resp = client().prepareGet(INDEX_NAME, id).setRealtime(false).get();
            assertTrue("warm-written doc [" + id + "] must be resolvable by id", resp.isExists());
            assertNotNull(resp.getSourceAsMap().get("field_text"));
        }
    }

    /**
     * Upload + flip scenario: warm-written parquet files are uploaded to remote, the local
     * copies are deleted by afterSyncToRemote, and reads keep working afterwards (served
     * from remote via the tiered store / block cache).
     */
    public void testWarmWritesUploadFlipAndRemainReadable() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWritableWarm(0);

        IndexShard primaryShard = getIndexShard(primaryNodeName());
        Set<String> uploadedBefore = uploadedParquetFiles(primaryShard);
        // Hot-phase leftovers are deleted by constructor reconciliation at warm open
        // (remote wins). Snapshot whatever remains so the flip assertion below is scoped
        // strictly to files written during the warm phase.
        Set<String> localBeforeWarmWrites = localParquetFiles(primaryShard);

        List<String> warmIds = indexDocsOnWarm(DOC_COUNT, WARM_DOC_COUNT);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // New parquet blob(s) must appear in the remote upload map.
        assertBusy(() -> {
            Set<String> uploadedNow = uploadedParquetFiles(primaryShard);
            assertTrue(
                "expected new parquet uploads beyond " + uploadedBefore + ", got " + uploadedNow,
                uploadedNow.size() > uploadedBefore.size()
            );
        });

        // afterSyncToRemote must delete local copies of files uploaded during the WARM phase.
        // Files local before the warm writes are excluded from the assertion (reconciliation
        // handles those independently at open).
        assertBusy(() -> {
            Set<String> local = localParquetFiles(primaryShard);
            Set<String> uploaded = uploadedParquetFiles(primaryShard).stream()
                .map(f -> f.substring("parquet/".length()))
                .collect(Collectors.toSet());
            Set<String> uploadedStillLocal = local.stream()
                .filter(uploaded::contains)
                .filter(f -> localBeforeWarmWrites.contains(f) == false)
                .collect(Collectors.toSet());
            assertTrue(
                "warm-written parquet files must be deleted locally after upload, still present: " + uploadedStillLocal,
                uploadedStillLocal.isEmpty()
            );
        });

        // Reads must keep working after the local delete (remote + block cache path).
        for (String id : warmIds) {
            GetResponse resp = client().prepareGet(INDEX_NAME, id).setRealtime(false).get();
            assertTrue("doc [" + id + "] must remain readable after upload flip", resp.isExists());
        }
        assertEquals(DOC_COUNT + WARM_DOC_COUNT, primaryDocCount());
    }

    /**
     * Merge scenario: background merges on a writable warm shard read their parquet inputs
     * through the tiered object store. By the time the merge policy fires, earlier writer
     * files have been uploaded and flipped to REMOTE (local copies deleted) - including the
     * hot-phase file removed at warm-open by reconciliation - so the native merge must
     * fetch them from the remote store. The merged output then goes through the normal
     * write -> upload -> flip lifecycle.
     */
    public void testBackgroundMergeOnWarmReadsRemoteInputs() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWritableWarm(0);

        IndexShard primaryShard = getIndexShard(primaryNodeName());

        // Several warm write+refresh cycles: each refresh creates a new parquet writer file
        // and triggers upload; earlier files flip to REMOTE while later cycles keep writing.
        int docsPerCycle = 5;
        int cycles = 15;
        for (int c = 0; c < cycles; c++) {
            indexDocsOnWarm(DOC_COUNT + c * docsPerCycle, docsPerCycle);
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        long expectedDocs = DOC_COUNT + (long) cycles * docsPerCycle;

        // The background merge must produce a merged parquet file, and that file must be
        // uploaded to remote (proving the full merge -> upload -> flip lifecycle on warm).
        assertBusy(() -> {
            Set<String> uploaded = uploadedParquetFiles(primaryShard);
            assertTrue(
                "expected a merged parquet file in remote uploads, got " + uploaded,
                uploaded.stream().anyMatch(f -> f.contains("merged"))
            );
        }, 120, java.util.concurrent.TimeUnit.SECONDS);

        // No docs lost across the merge, and reads still work.
        assertBusy(() -> assertEquals("doc count must be intact after warm merge", expectedDocs, primaryDocCount()));
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(expectedDocs, primaryDocCount());
    }

    /**
     * Restart scenario: after warm writes are uploaded, a full restart recovers the shard
     * from remote and all docs (hot-phase and warm-phase) remain present and readable.
     */
    public void testRestartAfterWarmWrites() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataAndWarmNodes(2);
        createHotIndexAndTierToWritableWarm(0);

        List<String> warmIds = indexDocsOnWarm(DOC_COUNT, WARM_DOC_COUNT);
        client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();

        // Wait for uploads to complete so restart recovers everything from remote.
        IndexShard primaryShard = getIndexShard(primaryNodeName());
        assertBusy(() -> {
            Set<String> local = localParquetFiles(primaryShard);
            Set<String> uploaded = uploadedParquetFiles(primaryShard).stream()
                .map(f -> f.substring("parquet/".length()))
                .collect(Collectors.toSet());
            assertTrue("all local parquet files must be uploaded before restart", uploaded.containsAll(local));
        });

        internalCluster().fullRestart();
        ensureGreen(INDEX_NAME);

        assertBusy(() -> assertEquals("doc count must survive restart", DOC_COUNT + WARM_DOC_COUNT, primaryDocCount()));
        for (String id : warmIds) {
            GetResponse resp = client().prepareGet(INDEX_NAME, id).setRealtime(false).get();
            assertTrue("warm-written doc [" + id + "] must survive restart", resp.isExists());
        }
    }
}
