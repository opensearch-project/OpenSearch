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
import org.opensearch.common.settings.Settings;
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
 * ITs for DFA segment replication between a DFA primary and replica(s) on remote store.
 * SEGMENT replication, {@code Scope.TEST} per test. Requires {@code -Dsandbox.enabled=true}.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicationIT extends RemoteStoreBaseIntegTestCase {

    protected static final String INDEX_NAME = "dfa-replication-test-idx";

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

    /** Starts 1 cluster-manager + {@code dataNodes} data nodes and creates the DFA index. */
    protected void startClusterAndCreateIndex(int dataNodes, int replicaCount) throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(dataNodes);
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(dfaIndexSettings(replicaCount)).get();
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        ensureGreen(INDEX_NAME);
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
        // RefreshPolicy.NONE so per-doc background refreshes don't fire; a single explicit
        // refresh/flush at the end gives deterministic replication rounds.
        for (int i = 0; i < count; i++) {
            client().prepareIndex(INDEX_NAME)
                .setId(String.valueOf(i))
                .setRefreshPolicy(org.opensearch.action.support.WriteRequest.RefreshPolicy.NONE)
                .setSource("field_text", randomAlphaOfLength(10), "field_keyword", randomAlphaOfLength(10), "field_number", (long) i)
                .get();
        }
    }

    protected String primaryNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return nodeNameFromId(nodeId);
    }

    protected List<String> replicaNodeNames() {
        return getClusterState().routingTable()
            .index(INDEX_NAME)
            .shard(0)
            .replicaShards()
            .stream()
            .map(s -> nodeNameFromId(s.currentNodeId()))
            .collect(Collectors.toList());
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
     * After a single flush, primary and each replica agree on every observable invariant:
     * checkpoint version, uploaded SegmentInfos bytes (non-empty), the set of data formats,
     * catalog file set, and the catalog↔local↔remote tier agreement on each node.
     * Consolidates what were individual single-invariant tests.
     */
    public void testReplicationConvergesAcrossAllInvariants() throws Exception {
        startClusterAndCreateIndex(2, 1);

        indexDocs(randomIntBetween(20, 50));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);

            RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
            RemoteSegmentMetadata rMeta = replica.getRemoteDirectory().readLatestMetadataFile();
            assertNotNull("primary metadata", pMeta);
            assertNotNull("replica metadata", rMeta);

            // Checkpoint version + non-empty SegmentInfos bytes.
            assertEquals(
                "primary/replica checkpoint versions must match",
                pMeta.getReplicationCheckpoint().getSegmentInfosVersion(),
                rMeta.getReplicationCheckpoint().getSegmentInfosVersion()
            );
            assertTrue(
                "primary SegmentInfos bytes non-empty",
                pMeta.getSegmentInfosBytes() != null && pMeta.getSegmentInfosBytes().length > 0
            );
            assertTrue(
                "replica SegmentInfos bytes non-empty",
                rMeta.getSegmentInfosBytes() != null && rMeta.getSegmentInfosBytes().length > 0
            );

            // Data-format set must agree and include the primary format (parquet).
            Set<String> pFormats = formatsOf(pMeta.getMetadata());
            Set<String> rFormats = formatsOf(rMeta.getMetadata());
            assertEquals("primary/replica formats must agree", pFormats, rFormats);
            assertTrue("formats must include parquet, got " + pFormats, pFormats.contains("parquet"));

            // Catalog↔local↔remote tier agreement on each node.
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
            DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);

            // Catalog file sets agree across nodes.
            assertEquals(
                "primary/replica catalog files must agree",
                DataFormatAwareITUtils.catalogFiles(primary),
                DataFormatAwareITUtils.catalogFiles(replica)
            );
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * Multiple index + flush cycles. After each cycle the replica must converge to the primary's
     * checkpoint with strictly-advancing version and matching catalog file set.
     */
    public void testMultipleRefreshCyclesWithReplication() throws Exception {
        startClusterAndCreateIndex(2, 1);

        long previousVersion = -1L;
        for (int i = 0; i < 3; i++) {
            indexDocs(randomIntBetween(10, 25));
            client().admin().indices().prepareFlush(INDEX_NAME).get();
            ensureGreen(INDEX_NAME);

            final long capturedPrev = previousVersion;
            final int iteration = i;
            assertBusy(() -> {
                IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
                IndexShard replica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);
                RemoteSegmentMetadata pMeta = primary.getRemoteDirectory().readLatestMetadataFile();
                RemoteSegmentMetadata rMeta = replica.getRemoteDirectory().readLatestMetadataFile();
                assertNotNull("primary metadata cycle " + iteration, pMeta);
                assertNotNull("replica metadata cycle " + iteration, rMeta);
                long primaryVersion = pMeta.getReplicationCheckpoint().getSegmentInfosVersion();
                assertEquals(
                    "primary/replica versions must match at cycle " + iteration,
                    primaryVersion,
                    rMeta.getReplicationCheckpoint().getSegmentInfosVersion()
                );
                assertTrue(
                    "version must strictly advance at cycle " + iteration + ": previous=" + capturedPrev + ", current=" + primaryVersion,
                    primaryVersion > capturedPrev
                );
                DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(primary);
                DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);
                assertEquals(
                    "primary/replica catalog files must agree at cycle " + iteration,
                    DataFormatAwareITUtils.catalogFiles(primary),
                    DataFormatAwareITUtils.catalogFiles(replica)
                );
            }, 60, TimeUnit.SECONDS);

            previousVersion = getIndexShard(primaryNodeName(), INDEX_NAME).getRemoteDirectory()
                .readLatestMetadataFile()
                .getReplicationCheckpoint()
                .getSegmentInfosVersion();
        }
    }

    /**
     * After the replica data node is restarted, it must rehydrate from the remote store via
     * segment replication and converge to the primary's latest checkpoint with matching catalog.
     */
    public void testReplicaRecoveryWithRemoteStore() throws Exception {
        startClusterAndCreateIndex(2, 1);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();

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
                DataFormatAwareITUtils.catalogFiles(primary),
                DataFormatAwareITUtils.catalogFiles(recoveredReplica)
            );
        }, 90, TimeUnit.SECONDS);
    }

    /**
     * On replica restart, any non-Lucene files sitting in the shard's format subdirectory that
     * are <em>not</em> referenced by the restored catalog snapshot must be physically removed by
     * the startup orphan sweep in {@link org.opensearch.index.engine.exec.coord.IndexFileDeleter}
     * (wired through {@code DataFormatAwareNRTReplicationEngine}).
     * <p>
     * This exercises the remote-store path end-to-end: the primary uploads a new catalog, the
     * replica restarts, a planted orphan file in the parquet directory must be gone after
     * recovery while every catalog-referenced file survives.
     */
    public void testReplicaStartupCleansOrphanFiles() throws Exception {
        startClusterAndCreateIndex(2, 1);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        String replicaNode = replicaNodeNames().get(0);
        IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);

        // Wait for replication to converge so the replica has a committed snapshot on disk.
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

        // Plant an orphan file in the replica's parquet directory. The filename must not match
        // any catalog-referenced file — use a name unlike anything the writer produces.
        IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);
        Path parquetDir = replica.shardPath().getDataPath().resolve("parquet");
        Path orphan = parquetDir.resolve("orphan-from-previous-session.parquet");
        Files.createDirectories(parquetDir);
        Files.write(orphan, new byte[] { 0x50, 0x41, 0x52, 0x31 });  // "PAR1" magic — irrelevant to the test
        assertTrue("planted orphan must exist before restart", Files.exists(orphan));

        internalCluster().restartNode(replicaNode);
        ensureGreen(INDEX_NAME);

        IndexShard recoveredReplica = getIndexShard(replicaNodeNames().get(0), INDEX_NAME);
        assertBusy(() -> {
            Path parquetDirAfter = recoveredReplica.shardPath().getDataPath().resolve("parquet");
            Path orphanAfter = parquetDirAfter.resolve("orphan-from-previous-session.parquet");
            assertFalse("startup orphan sweep must delete " + orphanAfter, Files.exists(orphanAfter));

            // Every catalog-referenced file must still be on disk (Lucene secondary files live
            // in <shard>/index/; non-default formats in <shard>/<format>/).
            Set<String> catalogAfter = DataFormatAwareITUtils.catalogFiles(recoveredReplica);
            for (String catalogFile : catalogAfter) {
                FileMetadata fm = new FileMetadata(catalogFile);
                Path dir = "lucene".equals(fm.dataFormat())
                    ? recoveredReplica.shardPath().resolveIndex()
                    : recoveredReplica.shardPath().getDataPath().resolve(fm.dataFormat());
                Path onDisk = dir.resolve(fm.file());
                assertTrue("catalog-referenced file must survive sweep: " + onDisk, Files.exists(onDisk));
            }
            assertEquals(
                "primary/replica catalog files must agree after recovery",
                DataFormatAwareITUtils.catalogFiles(primary),
                catalogAfter
            );
        }, 90, TimeUnit.SECONDS);
    }

    /**
     * With multiple replicas, all replicas must converge to the primary's checkpoint and expose
     * the same catalog file set — validating fan-out segment replication on DFA shards.
     */
    public void testReplicationWithMultipleReplicas() throws Exception {
        startClusterAndCreateIndex(3, 2);

        indexDocs(randomIntBetween(20, 40));
        client().admin().indices().prepareFlush(INDEX_NAME).get();
        ensureGreen(INDEX_NAME);

        assertBusy(() -> {
            IndexShard primary = getIndexShard(primaryNodeName(), INDEX_NAME);
            long primaryVersion = primary.getRemoteDirectory().readLatestMetadataFile().getReplicationCheckpoint().getSegmentInfosVersion();
            Set<String> primaryCatalog = DataFormatAwareITUtils.catalogFiles(primary);
            List<String> replicaNodes = replicaNodeNames();
            assertEquals("expected 2 replicas, got " + replicaNodes, 2, replicaNodes.size());
            for (String replicaNode : replicaNodes) {
                IndexShard replica = getIndexShard(replicaNode, INDEX_NAME);
                RemoteSegmentMetadata rMeta = replica.getRemoteDirectory().readLatestMetadataFile();
                assertNotNull("replica " + replicaNode + " metadata", rMeta);
                assertEquals(
                    "replica " + replicaNode + " version must match primary",
                    primaryVersion,
                    rMeta.getReplicationCheckpoint().getSegmentInfosVersion()
                );
                assertEquals(
                    "replica " + replicaNode + " catalog must match primary",
                    primaryCatalog,
                    DataFormatAwareITUtils.catalogFiles(replica)
                );
                DataFormatAwareITUtils.assertCatalogMatchesLocalAndRemote(replica);
            }
        }, 90, TimeUnit.SECONDS);
    }
}
