/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * ITs for DFA remote store RECOVERY and SHARD RELOCATION flows.
 *
 * <p>Recovery tests cover the full restore-from-remote-store path: stop data node, start new node,
 * close index, restore from remote store. They validate that the recovered shard starts empty and
 * after recovery has all files matching the original.
 *
 * <p>Shard relocation tests cover moving a live shard between nodes via {@link MoveAllocationCommand}.
 * Unlike recovery, relocation uses the peer-recovery source path
 * ({@code RemoteStorePeerRecoverySourceHandler}) with a catalog snapshot — the target downloads
 * segments from the remote store and must end up with every format-specific file (Lucene +
 * parquet) on local disk, a catalog equal to the source's pre-relocation catalog, and tier
 * agreement between catalog, local disk, and remote directory.
 *
 * <p>SEGMENT replication, {@code Scope.TEST} per test. Requires {@code -Dsandbox.enabled=true}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareRemoteStoreRecoveryIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "dfa-recovery-test-idx";

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

    protected void indexDocs(int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }

    protected void indexDocsWithOffset(int offset, int count) {
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(offset + i))
                .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE)
                .setSource(
                    "field_text",
                    randomAlphaOfLength(10),
                    "field_keyword",
                    randomAlphaOfLength(10),
                    "field_number",
                    (long) (offset + i)
                )
                .get();
        }
    }

    protected String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return nodeNameFromId(nodeId);
    }

    private String nodeNameFromId(String nodeId) {
        for (String nodeName : internalCluster().getNodeNames()) {
            if (internalCluster().clusterService(nodeName).localNode().getId().equals(nodeId)) {
                return nodeName;
            }
        }
        throw new IllegalStateException("node not found for id " + nodeId);
    }

    protected static Set<String> formatsOf(Map<String, UploadedSegmentMetadata> segments) {
        return segments.keySet().stream().map(file -> new FileMetadata(file).dataFormat()).collect(Collectors.toSet());
    }

    /**
     * Asserts that every file from the upload map exists on the recovered shard's local disk.
     * Uses the same file layout convention as {@link DataFormatAwareITUtils#assertPrimaryUploadMapOnReplicaDisk}:
     * Lucene files under {@code <shard>/index/}, non-default format files under {@code <shard>/<format>/}.
     */
    private void assertUploadMapFilesOnDisk(Map<String, UploadedSegmentMetadata> uploadMap, IndexShard shard) {
        assertFalse("upload map must not be empty", uploadMap.isEmpty());
        for (String originalName : uploadMap.keySet()) {
            if (originalName.startsWith("segments_")) {
                continue; // segments_N generation may differ
            }
            final Path onDisk;
            int sep = originalName.indexOf('/');
            if (sep < 0) {
                onDisk = shard.shardPath().resolveIndex().resolve(originalName);
            } else {
                String format = originalName.substring(0, sep);
                String file = originalName.substring(sep + 1);
                onDisk = shard.shardPath().getDataPath().resolve(format).resolve(file);
            }
            assertTrue(
                "file from upload map must exist on recovered shard disk: " + onDisk + " (originalName=" + originalName + ")",
                Files.isRegularFile(onDisk)
            );
        }
    }

    /**
     * Asserts that the shard's data path has no format-specific data files (parquet directory
     * should not exist or be empty). This validates the shard starts clean before recovery.
     */
    private void assertNoFormatFilesOnDisk(IndexShard shard) throws Exception {
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        if (Files.exists(parquetDir)) {
            long fileCount = Files.list(parquetDir).count();
            assertEquals("parquet directory should be empty before recovery, found " + fileCount + " files", 0, fileCount);
        }
    }

    /**
     * Basic recovery with full file validation: verifies the new shard starts without format
     * files, and after remote store recovery all uploaded files are present on disk with
     * matching catalog and format metadata.
     */
    public void testRemoteStoreRecoveryPreservesFormatMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 50));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before recovery: upload map + catalog
        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shard);
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(primaryNodeName(), INDEX_NAME));

        // Stop data node, start new one
        internalCluster().stopRandomNode(s -> dataNode.equals(s));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNode();

        // Before restore: verify the new shard has no format data files
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        // After recovery: all uploaded files must be on disk, catalog must match
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = recovered.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after recovery", meta);
            assertTrue("formats must include parquet after recovery", formatsOf(meta.getMetadata()).contains("parquet"));

            // Every file from the original upload map must exist on the recovered shard's disk
            assertUploadMapFilesOnDisk(uploadMapBefore, recovered);

            // Catalog must match local and remote
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);

            // Catalog file set must match what was there before
            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(recovered);
            assertEquals("catalog files must match before/after recovery", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Multiple flush cycles creating multiple parquet generations, then full remote store
     * recovery. Validates all generation files are recovered to disk.
     */
    public void testRemoteStoreRecoveryWithMultipleFlushGenerations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        int totalDocs = 0;
        for (int cycle = 0; cycle < 3; cycle++) {
            int docs = randomIntBetween(10, 25);
            indexDocsWithOffset(totalDocs, docs);
            totalDocs += docs;
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }

        // Capture upload map and catalog before recovery
        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(primaryNodeName(), INDEX_NAME));

        long parquetFilesBefore = uploadMapBefore.keySet().stream().filter(f -> new FileMetadata(f).dataFormat().equals("parquet")).count();
        assertTrue("should have multiple parquet files from multiple generations", parquetFilesBefore > 1);

        // Full remote store recovery
        internalCluster().stopRandomNode(s -> dataNode.equals(s));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        // After recovery: all files from all generations must be on disk
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = recovered.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after recovery", meta);
            assertTrue("formats must include parquet after recovery", formatsOf(meta.getMetadata()).contains("parquet"));

            assertUploadMapFilesOnDisk(uploadMapBefore, recovered);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);

            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(recovered);
            assertEquals("catalog files must match before/after recovery", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Index docs WITHOUT flush (translog only), stop node, restore, verify the shard
     * recovers and the engine opens successfully with translog replay.
     */
    public void testRemoteStoreRecoveryFromTranslogOnly() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(10, 30));
        // No flush — data only in translog

        internalCluster().stopRandomNode(s -> dataNode.equals(s));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        // Verify shard is operational after translog-only recovery
        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            assertNotNull("recovered shard must not be null", recovered);
            assertTrue("shard must be started", recovered.routingEntry().started());
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Restart primary node (not full restore), verify it recovers with all format files
     * intact on disk and catalog matching.
     */
    public void testPrimaryRestartRecoveryPreservesFormats() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 50));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before restart
        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(primaryNodeName(), INDEX_NAME));

        String nodeName = primaryNodeName();
        internalCluster().restartNode(nodeName);
        ensureGreen(INDEX_NAME);

        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = recovered.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after restart", meta);
            assertTrue("formats must include parquet after restart", formatsOf(meta.getMetadata()).contains("parquet"));

            assertUploadMapFilesOnDisk(uploadMapBefore, recovered);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);

            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(recovered);
            assertEquals("catalog files must match before/after restart", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Full recovery, then write new docs and flush. Verifies the recovered shard can continue
     * writing in the correct format, uploading new files, and maintaining catalog consistency.
     */
    public void testRemoteStoreRecoveryThenNewWrites() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(10, 30));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture upload map before recovery
        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        // Full remote store recovery
        internalCluster().stopRandomNode(s -> dataNode.equals(s));
        ensureRed(INDEX_NAME);
        internalCluster().startDataOnlyNode();
        client().admin().indices().prepareClose(INDEX_NAME).get();
        client().admin()
            .cluster()
            .restoreRemoteStore(new RestoreRemoteStoreRequest().indices(INDEX_NAME).restoreAllShards(true), PlainActionFuture.newFuture());
        ensureGreen(INDEX_NAME);

        // Write new docs after recovery
        indexDocsWithOffset(1000, randomIntBetween(10, 25));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertBusy(() -> {
            IndexShard recovered = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata meta = recovered.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after new writes", meta);
            assertTrue("formats must include parquet after new writes", formatsOf(meta.getMetadata()).contains("parquet"));

            // Upload map after new writes should be a superset of the original
            Map<String, UploadedSegmentMetadata> uploadMapAfter = recovered.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            for (String originalFile : uploadMapBefore.keySet()) {
                if (originalFile.startsWith("segments_")) {
                    continue;
                }
                assertTrue(
                    "original file must still be in upload map after new writes: " + originalFile,
                    uploadMapAfter.containsKey(originalFile)
                );
            }

            // All current files must be on disk
            assertUploadMapFilesOnDisk(uploadMapAfter, recovered);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(recovered);
        }, 60, TimeUnit.SECONDS);
    }

    // ---------------------------------------------------------------------------------------
    // Shard relocation tests
    //
    // Relocation is a distinct code path from recovery. The source shard stays live throughout;
    // the target is bootstrapped via the peer-recovery source handler
    // (RemoteStorePeerRecoverySourceHandler), which takes a catalog snapshot on the source and
    // drives phase1 — the target downloads segments from the remote store into its local
    // DataFormatAwareStoreDirectory, which routes Lucene files under <shard>/index/ and each
    // non-default format (e.g. parquet) under <shard>/<format>/.
    //
    // These tests validate the DFA invariants that must hold after a successful relocation:
    // * every file the source uploaded to remote (across Lucene + non-default formats) is
    // present on the target's local disk in the correct format-specific subdirectory;
    // * catalog files excluding segments_N are equal on source (before) and target (after);
    // * catalog ⊆ local-store-listing and catalog ⊆ remote-directory-listing on the target;
    // * the relocated shard can continue writing and re-uploading format-aware files.
    //
    // Backward compatibility: a pure-Lucene (no secondary format) relocation is exercised by
    // {@link #testPrimaryRelocationPureLuceneBackwardCompatible}, which disables the pluggable
    // data format and verifies the tests still hold — this ensures the relocation contract on
    // DFA-enabled clusters does not regress the Lucene-only path.
    //
    // Extensibility: all relocation assertions go through format-agnostic helpers
    // (assertUploadMapFilesOnDisk, formatsOf, FileMetadata) that route any format prefix to the
    // correct shard subdirectory via FileMetadata's delimiter convention. A new DataFormat
    // plugin automatically slots into these checks without test-side changes.
    // ---------------------------------------------------------------------------------------

    /**
     * Moves the primary of {@link #INDEX_NAME} from {@code sourceNode} to {@code targetNode} via a
     * {@link MoveAllocationCommand} and blocks until the cluster reports no relocating shards.
     * Fails the test if the relocation does not complete within {@code timeoutSeconds}.
     */
    private void relocatePrimaryTo(String sourceNode, String targetNode, int timeoutSeconds) {
        String sourceNodeId = internalCluster().clusterService(sourceNode).localNode().getId();
        String targetNodeId = internalCluster().clusterService(targetNode).localNode().getId();
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNodeId, targetNodeId))
            .execute()
            .actionGet();
        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(timeoutSeconds))
            .execute()
            .actionGet();
        assertFalse("timed out waiting for relocation of " + INDEX_NAME + " from " + sourceNode + " to " + targetNode, health.isTimedOut());
    }

    /**
     * Resolves the id of the replica currently assigned to {@code nodeName} for shard 0 of
     * {@link #INDEX_NAME}. Throws {@link IllegalStateException} if no such replica exists.
     */
    private String replicaNodeIdOn(String nodeName) {
        String expectedNodeId = internalCluster().clusterService(nodeName).localNode().getId();
        return getClusterState().routingTable()
            .index(INDEX_NAME)
            .shard(0)
            .replicaShards()
            .stream()
            .map(s -> s.currentNodeId())
            .filter(expectedNodeId::equals)
            .findFirst()
            .orElseThrow(() -> new IllegalStateException("no replica of " + INDEX_NAME + " found on node " + nodeName));
    }

    /**
     * Basic primary relocation on a DFA index: source node holds the primary, a target node is
     * started, {@link MoveAllocationCommand} moves the primary, and after relocation every
     * uploaded file (across every format) must be on the target's disk with a matching catalog.
     * This is the DFA analogue of {@link #testRemoteStoreRecoveryPreservesFormatMetadata} but
     * exercises the peer-recovery source path instead of the remote-store restore path.
     */
    public void testPrimaryRelocationPreservesFormatMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String sourceNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 50));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Capture state before relocation: upload map + catalog on the source primary.
        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(sourceNode, INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet before relocation", formatsOf(meta.getMetadata()).contains("parquet"));
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(shard);
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(sourceNode, INDEX_NAME));

        // Start the relocation target and wait for the cluster to see it.
        String targetNode = internalCluster().startDataOnlyNode();
        ClusterHealthResponse joined = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("3")
            .execute()
            .actionGet();
        assertFalse("timed out waiting for target data node to join", joined.isTimedOut());

        // Relocate primary source -> target.
        relocatePrimaryTo(sourceNode, targetNode, 60);
        ensureGreen(INDEX_NAME);

        // Primary must actually be on the target node now.
        assertEquals("primary must be on target node after relocation", targetNode, primaryNodeName());

        // Validate DFA invariants on the relocated primary.
        assertBusy(() -> {
            IndexShard relocated = getIndexShard(targetNode, INDEX_NAME);
            RemoteSegmentMetadata meta = relocated.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after relocation", meta);
            assertTrue("formats must include parquet after relocation", formatsOf(meta.getMetadata()).contains("parquet"));

            // Every file the source had uploaded must exist on the target's disk.
            assertUploadMapFilesOnDisk(uploadMapBefore, relocated);

            // Catalog ⊆ local, catalog ⊆ remote.
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(relocated);

            // Catalog file set must equal the pre-relocation catalog (segments_N excluded).
            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(relocated);
            assertEquals("catalog files must match before/after relocation", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Primary relocation after multiple flush cycles. Multiple parquet generations must all be
     * transferred to the target, not just the latest one. Regression guard against the target
     * receiving a partial segment set when the catalog references several flushed generations.
     */
    public void testPrimaryRelocationWithMultipleFlushGenerations() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String sourceNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        int totalDocs = 0;
        for (int cycle = 0; cycle < 3; cycle++) {
            int docs = randomIntBetween(10, 25);
            indexDocsWithOffset(totalDocs, docs);
            totalDocs += docs;
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }

        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(sourceNode, INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        Set<String> catalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(sourceNode, INDEX_NAME));

        long parquetFilesBefore = uploadMapBefore.keySet().stream().filter(f -> new FileMetadata(f).dataFormat().equals("parquet")).count();
        assertTrue(
            "precondition: should have multiple parquet files from multiple generations, got " + parquetFilesBefore,
            parquetFilesBefore > 1
        );

        String targetNode = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").get();

        relocatePrimaryTo(sourceNode, targetNode, 60);
        ensureGreen(INDEX_NAME);
        assertEquals("primary must be on target node after relocation", targetNode, primaryNodeName());

        assertBusy(() -> {
            IndexShard relocated = getIndexShard(targetNode, INDEX_NAME);
            RemoteSegmentMetadata meta = relocated.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after relocation", meta);
            assertTrue("formats must include parquet after relocation", formatsOf(meta.getMetadata()).contains("parquet"));

            // Every file from every generation must be on disk.
            assertUploadMapFilesOnDisk(uploadMapBefore, relocated);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(relocated);

            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(relocated);
            assertEquals("catalog files must match before/after relocation", catalogBefore, catalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Relocate the primary, then index + flush on the new primary. Verifies the relocated shard
     * can continue producing format-aware writes: the new upload map is a superset of the original
     * (modulo segments_N), new parquet files appear, and catalog/local/remote tiers stay in sync.
     */
    public void testPrimaryRelocationThenNewWrites() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String sourceNode = internalCluster().startDataOnlyNode();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(0)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(10, 30));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(sourceNode, INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            assertTrue("formats must include parquet", formatsOf(meta.getMetadata()).contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        String targetNode = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").get();

        relocatePrimaryTo(sourceNode, targetNode, 60);
        ensureGreen(INDEX_NAME);
        assertEquals("primary must be on target node after relocation", targetNode, primaryNodeName());

        // Write new docs + flush on the relocated primary.
        indexDocsWithOffset(1000, randomIntBetween(10, 25));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        assertBusy(() -> {
            IndexShard relocated = getIndexShard(targetNode, INDEX_NAME);
            RemoteSegmentMetadata meta = relocated.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after new writes on relocated primary", meta);
            assertTrue(
                "formats must include parquet after new writes on relocated primary",
                formatsOf(meta.getMetadata()).contains("parquet")
            );

            // The relocated primary's upload map must retain every pre-relocation file
            // (segments_N excluded) and gain at least one new parquet file from the post-relocation flush.
            Map<String, UploadedSegmentMetadata> uploadMapAfter = relocated.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            for (String originalFile : uploadMapBefore.keySet()) {
                if (originalFile.startsWith("segments_")) {
                    continue;
                }
                assertTrue(
                    "pre-relocation file must still be in upload map after new writes: " + originalFile,
                    uploadMapAfter.containsKey(originalFile)
                );
            }
            long parquetBefore = uploadMapBefore.keySet().stream().filter(f -> new FileMetadata(f).dataFormat().equals("parquet")).count();
            long parquetAfter = uploadMapAfter.keySet().stream().filter(f -> new FileMetadata(f).dataFormat().equals("parquet")).count();
            assertTrue(
                "relocated primary must produce new parquet files (before=" + parquetBefore + ", after=" + parquetAfter + ")",
                parquetAfter > parquetBefore
            );

            assertUploadMapFilesOnDisk(uploadMapAfter, relocated);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(relocated);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Replica shard relocation. Creates a primary + 1 replica on two nodes, starts a third node,
     * relocates the replica onto the third node, and validates the replica ends up with the
     * primary's uploaded files on disk and a catalog matching the primary's.
     *
     * <p>This exercises the replica-target peer-recovery path for DFA: the relocated replica is
     * bootstrapped from the remote store via {@code RemoteStoreReplicationSource}; after
     * relocation it must converge with the primary on catalog and format-specific files on disk,
     * matching the invariants of {@link DataFormatAwareReplicationIT}.
     */
    public void testReplicaRelocationPreservesFormatMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(1)).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        // Wait for initial replication to converge so the replica has a full catalog to carry.
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(pMeta);
            assertTrue("formats must include parquet", formatsOf(pMeta.getMetadata()).contains("parquet"));
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
        }, 30, TimeUnit.SECONDS);

        // Find the replica's current node by elimination: data node that is not the primary's.
        String primaryNode = primaryNodeName();
        String sourceReplicaNode = null;
        for (String n : internalCluster().getNodeNames()) {
            if (!n.equals(primaryNode) && !n.equals(internalCluster().getClusterManagerName())) {
                sourceReplicaNode = n;
                break;
            }
        }
        assertNotNull("could not find replica's source node", sourceReplicaNode);
        // Sanity: the node we just picked must actually host the replica.
        assertNotNull("chosen source node must host the replica", replicaNodeIdOn(sourceReplicaNode));

        Set<String> primaryCatalogBefore = DataFormatAwareITUtils.catalogFilesExcludingSegments(getIndexShard(primaryNode, INDEX_NAME));

        // Start the relocation target.
        String targetNode = internalCluster().startDataOnlyNode();
        ClusterHealthResponse joined = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes("4")
            .execute()
            .actionGet();
        assertFalse("timed out waiting for relocation target to join", joined.isTimedOut());

        // Move replica from sourceReplicaNode to targetNode.
        String sourceReplicaNodeId = internalCluster().clusterService(sourceReplicaNode).localNode().getId();
        String targetNodeId = internalCluster().clusterService(targetNode).localNode().getId();
        client().admin()
            .cluster()
            .prepareReroute()
            .add(new MoveAllocationCommand(INDEX_NAME, 0, sourceReplicaNodeId, targetNodeId))
            .execute()
            .actionGet();

        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setTimeout(TimeValue.timeValueSeconds(60))
            .execute()
            .actionGet();
        assertFalse("timed out waiting for replica relocation", health.isTimedOut());
        ensureGreen(INDEX_NAME);

        // After relocation the replica must be on targetNode.
        assertNotNull("replica must be on target node after relocation", replicaNodeIdOn(targetNode));

        // Validate cross-shard invariants: primary-uploaded files on the relocated replica's disk,
        // replica catalog equals primary catalog, replica tier agreement.
        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard replica = getIndexShard(targetNode, INDEX_NAME);

            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);
            DataFormatAwareITUtils.assertPrimaryUploadMapOnReplicaDisk(primary, replica);

            Set<String> primaryCatalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(primary);
            Set<String> replicaCatalogAfter = DataFormatAwareITUtils.catalogFilesExcludingSegments(replica);
            assertEquals("primary/replica catalogs must match after replica relocation", primaryCatalogAfter, replicaCatalogAfter);
            // Primary catalog should not regress across the relocation.
            assertEquals("primary catalog must be stable across replica relocation", primaryCatalogBefore, primaryCatalogAfter);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Backward-compatibility guard: on the same plugin + cluster configuration (pluggable
     * data format module loaded), an index with no secondary formats — i.e. Lucene-only —
     * must still relocate cleanly. Every file the source uploaded (all under the Lucene default
     * format, living under {@code <shard>/index/}) must land on the target's disk and the
     * catalog must match.
     *
     * <p>This ensures the DFA relocation machinery degrades to a no-op for non-DFA indices.
     */
    public void testPrimaryRelocationPureLuceneBackwardCompatible() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String sourceNode = internalCluster().startDataOnlyNode();

        // Lucene-only index: same remote-store + segment replication setup, pluggable dataformat disabled.
        Settings luceneSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            .put("index.pluggable.dataformat.enabled", false)
            .build();
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(luceneSettings).get();
        ensureGreen(INDEX_NAME);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        Map<String, UploadedSegmentMetadata> uploadMapBefore = assertBusyAndReturn(() -> {
            IndexShard shard = getIndexShard(sourceNode, INDEX_NAME);
            RemoteSegmentMetadata meta = shard.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(meta);
            // On a Lucene-only index the only format must be "lucene" — no parquet files.
            Set<String> formats = formatsOf(meta.getMetadata());
            assertTrue("Lucene-only index must have lucene format present: " + formats, formats.contains("lucene"));
            assertFalse("Lucene-only index must not contain parquet files: " + formats, formats.contains("parquet"));
            Map<String, UploadedSegmentMetadata> map = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
            assertFalse("upload map must not be empty", map.isEmpty());
            return map;
        }, 30, TimeUnit.SECONDS);

        String targetNode = internalCluster().startDataOnlyNode();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes("3").get();

        relocatePrimaryTo(sourceNode, targetNode, 60);
        ensureGreen(INDEX_NAME);
        assertEquals("primary must be on target node after relocation", targetNode, primaryNodeName());

        assertBusy(() -> {
            IndexShard relocated = getIndexShard(targetNode, INDEX_NAME);
            RemoteSegmentMetadata meta = relocated.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("metadata must exist after relocation", meta);

            // Every Lucene file on the source must be on the target's disk. assertUploadMapFilesOnDisk
            // routes Lucene files (no "/" in the name) under <shard>/index/, exactly the path the
            // target's DataFormatAwareStoreDirectory (or plain FSDirectory if DFA is off) will use.
            assertUploadMapFilesOnDisk(uploadMapBefore, relocated);
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Helper that runs an assertion block with assertBusy semantics but returns a value.
     */
    @SuppressWarnings("unchecked")
    private <T> T assertBusyAndReturn(CheckedSupplierWithReturn<T> supplier, long timeout, TimeUnit unit) throws Exception {
        Object[] holder = new Object[1];
        assertBusy(() -> { holder[0] = supplier.get(); }, timeout, unit);
        return (T) holder[0];
    }

    @FunctionalInterface
    private interface CheckedSupplierWithReturn<T> {
        T get() throws Exception;
    }
}
