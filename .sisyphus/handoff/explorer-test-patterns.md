# DFA Integration Test Patterns — Explorer Handoff

## 1. DataFormatAwareReplicationBaseIT.java (Abstract Base Class — FULL)

```java
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
     */
    protected void assertNoDataLoss(BackgroundIndexer indexer, String index) throws Exception {
        indexer.assertNoFailures();
        assertTrue("BackgroundIndexer acknowledged zero docs — indexing never made progress", indexer.totalIndexedDocs() > 0);
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
     * allocation via _cluster/reroute.
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
```

## 2. DataFormatAwareITUtils.java (Shared Utilities — FULL)

```java
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.RemoteSegmentStoreDirectory.UploadedSegmentMetadata;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.test.OpenSearchTestCase.getShardLevelBlobPath;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Shared helpers for DFA integration tests.
 *
 * <p>Two assertions serve different purposes:
 * <ul>
 *   <li>{@link #assertCatalogMatchesLocalAndRemote}: replication-focused. The catalog snapshot
 *       must be a subset of both the local store directory and the remote directory listing.
 *   <li>{@link #assertCatalogMatchesUploadedBlobs}: upload-focused. Each catalog file must be
 *       present in the upload map AND physically on disk under the shard's remote blob path.
 * </ul>
 */
final class DataFormatAwareITUtils {

    private DataFormatAwareITUtils() {}

    /**
     * Replication invariant: every file in the catalog must exist locally on the shard and be
     * listed by the remote segment directory.
     */
    static void assertCatalogMatchesLocalAndRemote(IndexShard shard) throws IOException {
        Set<String> catalog = catalogFilesExcludingSegments(shard);
        Set<String> local = localFiles(shard);
        Set<String> remote = new HashSet<>(Arrays.asList(shard.getRemoteDirectory().listAll()));

        assertFalse("catalog snapshot has no files on " + shard.routingEntry(), catalog.isEmpty());
        assertSubset("local store directory", catalog, local, shard);
        assertSubset("remote directory listing", catalog, remote, shard);
    }

    /**
     * Upload invariant: each catalog file has a corresponding upload-map entry AND the uploaded
     * blob physically exists under the shard's remote blob path.
     */
    static void assertCatalogMatchesUploadedBlobs(IndexShard shard, Client client, Settings nodeSettings, Path segmentRepoPath)
        throws IOException {
        Set<String> catalog = catalogFilesExcludingSegments(shard);
        Map<String, UploadedSegmentMetadata> uploadMap = shard.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
        assertFalse("catalog snapshot has no files on " + shard.routingEntry(), catalog.isEmpty());

        Path shardDiskPath = segmentRepoPath.resolve(shardBlobPath(shard, client, nodeSettings).buildAsString());
        for (String originalName : catalog) {
            UploadedSegmentMetadata md = uploadMap.get(originalName);
            assertFalse(
                "no upload-map entry for " + originalName + " on " + shard.routingEntry() + "; keys=" + uploadMap.keySet(),
                md == null
            );
            int sep = originalName.indexOf('/');
            Path blobDir = sep < 0 ? shardDiskPath : shardDiskPath.resolve(originalName.substring(0, sep));
            Path blobFile = blobDir.resolve(md.getUploadedFilename());
            assertTrue(
                "remote blob missing on disk: " + blobFile + " for catalog file " + originalName + " on " + shard.routingEntry(),
                Files.isRegularFile(blobFile)
            );
        }
    }

    /**
     * Cross-shard invariant: every data file the primary uploaded to remote must exist on the
     * replica's local disk.
     */
    static void assertPrimaryUploadMapOnReplicaDisk(IndexShard primary, IndexShard replica) {
        Map<String, UploadedSegmentMetadata> uploadMap = primary.getRemoteDirectory().getSegmentsUploadedToRemoteStore();
        assertFalse("primary upload map is empty on " + primary.routingEntry(), uploadMap.isEmpty());

        for (String originalName : uploadMap.keySet()) {
            if (originalName.startsWith("segments_")) {
                continue;
            }
            final Path onDisk;
            int sep = originalName.indexOf('/');
            if (sep < 0) {
                onDisk = replica.shardPath().resolveIndex().resolve(originalName);
            } else {
                String format = originalName.substring(0, sep);
                String file = originalName.substring(sep + 1);
                onDisk = replica.shardPath().getDataPath().resolve(format).resolve(file);
            }
            assertTrue(
                "primary-uploaded file missing on replica disk: " + onDisk + " (originalName=" + originalName + ")",
                Files.isRegularFile(onDisk)
            );
        }
    }

    static Set<String> catalogFiles(IndexShard shard) throws IOException {
        try (GatedCloseable<CatalogSnapshot> ref = shard.getCatalogSnapshot()) {
            return new HashSet<>(ref.get().getFiles(true));
        }
    }

    /**
     * Returns catalog files excluding the Lucene segments_N file.
     */
    static Set<String> catalogFilesExcludingSegments(IndexShard shard) throws IOException {
        Set<String> files = catalogFiles(shard);
        files.removeIf(f -> f.startsWith("segments_"));
        return files;
    }

    static Set<String> localFiles(IndexShard shard) throws IOException {
        return new HashSet<>(Arrays.asList(shard.store().directory().listAll()));
    }

    private static void assertSubset(String rhsName, Set<String> catalog, Set<String> rhs, IndexShard shard) {
        Set<String> missing = new HashSet<>(catalog);
        missing.removeAll(rhs);
        assertTrue(
            "catalog files missing from " + rhsName + " on " + shard.routingEntry() + ": " + missing + "; " + rhsName + "=" + rhs,
            missing.isEmpty()
        );
    }

    private static BlobPath shardBlobPath(IndexShard shard, Client client, Settings nodeSettings) {
        String prefix = RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.get(nodeSettings);
        return getShardLevelBlobPath(
            client,
            shard.shardId().getIndexName(),
            new BlobPath(),
            String.valueOf(shard.shardId().id()),
            DataCategory.SEGMENTS,
            DataType.DATA,
            prefix
        );
    }
}
```

## 3. DataFormatAwareRemoteStoreRecoveryIT.java (First 100 lines — class setup + pattern)

```java
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
 * close index, restore from remote store.
 *
 * <p>Shard relocation tests cover moving a live shard between nodes via MoveAllocationCommand.
 *
 * <p>SEGMENT replication, Scope.TEST per test. Requires -Dsandbox.enabled=true.
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
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) (offset + i))
                .get();
        }
    }
```

**Key pattern — first test method (`testRemoteStoreRecoveryPreservesFormatMetadata`):**

```java
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

        // Close + restore from remote store
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
            // ... validates upload map files on disk, catalog convergence, format metadata
        }, 60, TimeUnit.SECONDS);
    }
```

## 4. DataFormatAwareReplicationIT.java — Class Declaration + `testReplicaRecoveryWithRemoteStore`

**Class declaration:**

```java
/**
 * ITs for DFA segment replication between a DFA primary and replica(s) on remote store.
 * SEGMENT replication, Scope.TEST per test. Requires -Dsandbox.enabled=true.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicationIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "dfa-replication-test-idx";

    // ... nodePlugins(), nodeSettings(), dfaIndexSettings(), indexDocs(), primaryNodeName(),
    //     replicaNodeNames(), nodeNameFromId(), formatsOf() — same pattern as base class
}
```

**`testReplicaRecoveryWithRemoteStore` method (closest to block-fetch recovery pattern):**

```java
    /**
     * After the replica data node is restarted, it must rehydrate from the remote store via
     * segment replication and converge to the primary's latest checkpoint with matching catalog.
     */
    public void testReplicaRecoveryWithRemoteStore() throws Exception {
        startClusterAndCreateIndex(2, 1);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        client().admin().indices().prepareRefresh(INDEX_NAME).get();

        String replicaNode = replicaNodeNames().get(0);
        IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);

        // Wait for initial replication to settle so the replica has state to lose + recover.
        assertBusy(() -> {
            IndexShard replicaBefore = getIndexShard(replicaNode, INDEX_NAME);
            RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
            RemoteSegmentMetadata rMeta = replicaBefore.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(pMeta);
            assertNotNull(rMeta);
            assertEquals(
                pMeta.getReplicationCheckpoint().getSegmentInfosVersion(),
                rMeta.getReplicationCheckpoint().getSegmentInfosVersion()
            );
        }, 60, TimeUnit.SECONDS);

        internalCluster().restartNode(replicaNode);
        ensureGreen(INDEX_NAME);

        IndexShard recoveredReplica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);
        assertBusy(() -> {
            RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
            RemoteSegmentMetadata rMeta = recoveredReplica.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull(pMeta);
            assertNotNull(rMeta);
            assertEquals(
                "replica version must match primary after recovery",
                pMeta.getReplicationCheckpoint().getSegmentInfosVersion(),
                rMeta.getReplicationCheckpoint().getSegmentInfosVersion()
            );
            assertEquals(
                "primary/replica catalog files must agree after recovery",
                DataFormatAwareITUtils.catalogFilesExcludingSegments(primary),
                DataFormatAwareITUtils.catalogFilesExcludingSegments(recoveredReplica)
            );
            DataFormatAwareITUtils.assertPrimaryUploadMapOnReplicaDisk(primary, recoveredReplica);
        }, 90, TimeUnit.SECONDS);
    }
```

---

## Key Patterns Summary

| Pattern | Usage |
|---------|-------|
| **Base class** (`DataFormatAwareReplicationBaseIT`) | Extends `RemoteStoreBaseIntegTestCase`, provides `createDfaIndex()`, `indexDocs()`, `primaryNodeName()`, `replicaNodeNames()`, `crashNode()`, `gracefulStopNode()`, `cancelPrimaryAllocation()`, `assertCatalogSnapshotsConverged()`, `assertNoDataLoss()`, `waitForIndexerDocs()` |
| **Utilities** (`DataFormatAwareITUtils`) | Static helpers: `assertCatalogMatchesLocalAndRemote()`, `assertCatalogMatchesUploadedBlobs()`, `assertPrimaryUploadMapOnReplicaDisk()`, `catalogFiles()`, `catalogFilesExcludingSegments()` |
| **Index settings** | Always: `SEGMENT` replication, `pluggable.dataformat.enabled=true`, `composite` format, `parquet` primary format |
| **Cluster setup** | `@ClusterScope(Scope.TEST, numDataNodes=0)` — each test starts its own cluster |
| **Assertion pattern** | `assertBusy(() -> { ... }, timeout, TimeUnit.SECONDS)` wrapping convergence checks |
| **Recovery test pattern** | Index → flush → wait for replication → restart/stop node → ensureGreen → assertBusy convergence |
| **Promotion test pattern** | Index → flush → `cancelPrimaryAllocation()` → wait for new primary term → assert catalog convergence |
