/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.shrink.ResizeType;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.transfer.RemoteStoreFence;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.translog.transfer.RemoteStoreFence.FENCE_BLOB_PREFIX;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_PREFIX;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies that the object-store fencing token (see {@link RemoteStoreFence}) is actually exercised on the
 * acknowledgement path, both for zero-replica indices — where no replica exists to act as a primary term validation
 * witness — and for indices with one or more replicas, where fencing must coexist with segment replication.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreFencingIT extends RemoteStoreBaseIntegTestCase {

    private static final String INDEX_NAME = "remote-store-fencing-idx";

    /**
     * {@code IndexShard}'s seal step. Asserted as a stack frame because the point being verified is <i>where</i> on the
     * recovery path the failure happens, which no exception type or message can distinguish.
     */
    private static final String SEAL_METHOD_NAME = "sealRemoteStoreFenceBeforeRestore";

    /** {@code IndexShard}'s shared seal step, the frame present on both the recovery and the promotion seal paths. */
    private static final String PROMOTION_SEAL_METHOD_NAME = "sealRemoteStoreFence";

    /** This suite manages fencing explicitly per index (and per cluster in the baking test); ignore the suite-wide flag. */
    @Override
    protected boolean remoteStoreFencingForAllIndices() {
        return false;
    }

    private Settings fencedIndexSettings(int replicaCount, Translog.Durability durability) {
        return Settings.builder()
            .put(remoteStoreIndexSettings(replicaCount, 1))
            .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), durability.name())
            .build();
    }

    /** The {@link BlobPath} of the translog metadata directory, which is where the fence blob lives. */
    private BlobPath translogMetadataBlobPath(String indexName) {
        return getShardLevelBlobPath(
            client(),
            indexName,
            BlobPath.cleanPath(),
            "0",
            TRANSLOG,
            METADATA,
            RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.get(getNodeSettings())
        );
    }

    /** The on-disk translog metadata directory backing {@link #translogMetadataBlobPath}. */
    private Path translogMetadataDirectory(String indexName) {
        return Path.of(translogRepoPath.toString(), translogMetadataBlobPath(indexName).buildAsString());
    }

    /**
     * Parses the fence blob independently of {@link RemoteStoreFence}, so that the on-disk layout documented as
     * {@code v1|<indexUUID>|<shardId>|<term>|<allocationId>|<nodeId>|<seq>} is asserted rather than assumed.
     */
    private static Fence readFence(Path metadataDirectory) throws IOException {
        String raw = new String(Files.readAllBytes(currentFenceBlob(metadataDirectory)), StandardCharsets.UTF_8);
        String[] tokens = raw.split("\\|");
        assertEquals("unexpected fence blob layout [" + raw + "]", 7, tokens.length);
        assertEquals("unexpected fence codec version [" + raw + "]", "v1", tokens[0]);
        return new Fence(
            tokens[1],
            Integer.parseInt(tokens[2]),
            Long.parseLong(tokens[3]),
            tokens[4],
            tokens[5],
            Long.parseLong(tokens[6])
        );
    }

    /**
     * The acknowledgement-path object of the highest term present. Names embed the inverted term, so the highest term
     * sorts first - the same ordering the fence itself relies on to compare cluster-manager-issued grants.
     */
    private static Path currentFenceBlob(Path metadataDirectory) throws IOException {
        try (Stream<Path> listing = Files.list(metadataDirectory)) {
            return listing.filter(path -> path.getFileName().toString().startsWith(FENCE_BLOB_PREFIX))
                .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("no fence object under " + metadataDirectory));
        }
    }

    private static Set<String> fenceBlobNames(Path metadataDirectory) throws IOException {
        try (Stream<Path> listing = Files.list(metadataDirectory)) {
            return listing.map(path -> path.getFileName().toString())
                .filter(name -> name.startsWith(FENCE_BLOB_PREFIX))
                .collect(Collectors.toSet());
        }
    }

    private static boolean fenceExists(Path metadataDirectory) throws IOException {
        if (Files.isDirectory(metadataDirectory) == false) {
            return false;
        }
        try (Stream<Path> listing = Files.list(metadataDirectory)) {
            return listing.anyMatch(path -> path.getFileName().toString().startsWith(FENCE_BLOB_PREFIX));
        }
    }

    private static Fence awaitFence(Path metadataDirectory) throws Exception {
        assertBusy(() -> assertTrue("no fence object was ever created under " + metadataDirectory, fenceExists(metadataDirectory)));
        return readFence(metadataDirectory);
    }

    private static final class Fence {
        private final String indexUUID;
        private final int shardId;
        private final long term;
        private final String allocationId;
        private final String nodeId;
        private final long seq;

        private Fence(String indexUUID, int shardId, long term, String allocationId, String nodeId, long seq) {
            this.indexUUID = indexUUID;
            this.shardId = shardId;
            this.term = term;
            this.allocationId = allocationId;
            this.nodeId = nodeId;
            this.seq = seq;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "index [%s] shard [%d] term [%d] allocation [%s] node [%s] seq [%d]",
                indexUUID,
                shardId,
                term,
                allocationId,
                nodeId,
                seq
            );
        }
    }

    private String nodeId(String nodeName) {
        return internalCluster().getInstance(ClusterService.class, nodeName).localNode().getId();
    }

    /**
     * The fencing gate is a <b>dynamic cluster setting</b> baked into a <b>final index setting</b> at creation time:
     * operators can turn fencing on or off for indices created from that point on, but an existing index never
     * switches its write witness mid-flight — toggling the fence on a live index would leave a window in which a stale
     * primary is checked against neither the fence nor the replicas.
     */
    public void testClusterFencingDefaultIsBakedIntoAFinalIndexSetting() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        Settings unfencedSettings = Settings.builder()
            .put(remoteStoreIndexSettings(0, 1))
            .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST.name())
            .build();

        try {
            // Cluster default off: the index is created unfenced. The key stays absent — the final index setting
            // resolves to false forever, so there is no explicit false to materialize.
            String unfencedIndex = INDEX_NAME + "-default-off";
            createIndex(unfencedIndex, unfencedSettings);
            ensureGreen(unfencedIndex);
            assertNull(indexSetting(unfencedIndex, IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED));
            indexSingleDoc(unfencedIndex);
            assertFalse("an unfenced index must not create a fence object", fenceExists(translogMetadataDirectory(unfencedIndex)));

            // Flip the cluster default on, dynamically. Only indices created afterwards are fenced.
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_FENCING_ENABLED.getKey(), true))
            );
            String fencedIndex = INDEX_NAME + "-default-on";
            createIndex(fencedIndex, unfencedSettings);
            ensureGreen(fencedIndex);
            assertEquals("true", indexSetting(fencedIndex, IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED));
            indexSingleDoc(fencedIndex);
            awaitFence(translogMetadataDirectory(fencedIndex));

            // An explicit per-index value always wins over the cluster default.
            String optedOutIndex = INDEX_NAME + "-opt-out";
            createIndex(
                optedOutIndex,
                Settings.builder().put(unfencedSettings).put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, false).build()
            );
            ensureGreen(optedOutIndex);
            assertEquals("false", indexSetting(optedOutIndex, IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED));

            // The baked-in index setting is final: the witness of a live index cannot be switched.
            IllegalArgumentException rejection = expectThrows(
                IllegalArgumentException.class,
                () -> client().admin()
                    .indices()
                    .prepareUpdateSettings(fencedIndex)
                    .setSettings(Settings.builder().put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, false))
                    .get()
            );
            assertTrue(rejection.getMessage(), rejection.getMessage().contains(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED));
            // And the pre-existing index did not become fenced by the cluster-level flip.
            assertNull(indexSetting(unfencedIndex, IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED));
        } finally {
            assertAcked(
                client().admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setTransientSettings(Settings.builder().putNull(RemoteStoreSettings.CLUSTER_REMOTE_STORE_FENCING_ENABLED.getKey()))
            );
        }
    }

    private String indexSetting(String indexName, String settingKey) {
        return client().admin().indices().prepareGetSettings(indexName).get().getIndexToSettings().get(indexName).get(settingKey);
    }

    /**
     * Zero replicas: the object store is the only witness, so every acknowledged write must advance the CAS chain.
     */
    public void testFenceIsExercisedWithZeroReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        Path fenceDir = translogMetadataDirectory(INDEX_NAME);
        IndexShard primary = getIndexShard(dataNode, INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence bootstrapped = awaitFence(fenceDir);
        assertEquals("fence owner should be the primary's node", nodeId(dataNode), bootstrapped.nodeId);
        assertEquals("fence term should be the primary term", primary.getOperationPrimaryTerm(), bootstrapped.term);
        assertThat(bootstrapped.seq, greaterThanOrEqualTo(0L));

        // Each acknowledged write advances the chain. The exact delta is not asserted: background syncs may also
        // upload, and the guarantee being verified is that the chain moves forward, never that it moves by one.
        Fence previous = bootstrapped;
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            indexSingleDoc(INDEX_NAME);
            Fence current = readFence(fenceDir);
            assertThat("fence did not advance after an acknowledged write", current.seq, greaterThan(previous.seq));
            assertEquals(previous.nodeId, current.nodeId);
            assertEquals(previous.allocationId, current.allocationId);
            assertEquals(previous.term, current.term);
            previous = current;
        }
    }

    /**
     * The fence is on the control flow only: it shares a directory with the translog metadata files but must never be
     * mistaken for one, since metadata listings drive both restore lineage and garbage collection.
     */
    public void testFenceBlobIsNotVisibleAsTranslogMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        Path metadataDirectory = translogMetadataDirectory(INDEX_NAME);
        int numDocs = randomIntBetween(2, 5);
        for (int i = 0; i < numDocs; i++) {
            indexSingleDoc(INDEX_NAME);
        }
        awaitFence(metadataDirectory);

        Set<String> blobs;
        try (Stream<Path> listing = Files.list(metadataDirectory)) {
            blobs = listing.map(path -> path.getFileName().toString()).collect(Collectors.toSet());
        }
        Set<String> fenceBlobs = blobs.stream().filter(name -> name.startsWith(FENCE_BLOB_PREFIX)).collect(Collectors.toSet());
        assertFalse("fence objects missing from " + blobs, fenceBlobs.isEmpty());
        assertFalse("the fence prefix must not match the translog metadata prefix", FENCE_BLOB_PREFIX.startsWith(METADATA_PREFIX));
        assertFalse("the translog metadata prefix must not match the fence prefix", METADATA_PREFIX.startsWith(FENCE_BLOB_PREFIX));
        Set<String> metadataFiles = blobs.stream().filter(name -> name.startsWith(METADATA_PREFIX)).collect(Collectors.toSet());
        assertFalse("no translog metadata files were written: " + blobs, metadataFiles.isEmpty());
        assertTrue("fence objects leaked into the metadata listing", Collections.disjoint(metadataFiles, fenceBlobs));

        // The translog metadata lineage is still readable with the fence sitting next to it: recovering the shard from
        // the remote store replays every acknowledged write.
        refresh(INDEX_NAME);
        assertEquals(numDocs, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
        internalCluster().restartNode(dataNode);
        ensureGreen(INDEX_NAME);
        refresh(INDEX_NAME);
        assertEquals(numDocs, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
    }

    /**
     * One or more replicas: fencing takes replicas off the primary term validation path, but segment replication and
     * replica bookkeeping must be unaffected, and the fence must still advance on every acknowledged write.
     */
    public void testFenceIsExercisedWithReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(3);
        int replicaCount = randomIntBetween(1, 2);
        // Both durabilities keep the fence on the upload path; only the replication mode of the write path differs.
        Translog.Durability durability = randomFrom(Translog.Durability.REQUEST, Translog.Durability.ASYNC);
        createIndex(INDEX_NAME, fencedIndexSettings(replicaCount, durability));
        ensureGreen(INDEX_NAME);

        String primaryNode = primaryNodeName(INDEX_NAME);
        Path fenceDir = translogMetadataDirectory(INDEX_NAME);
        IndexShard primary = getIndexShard(primaryNode, INDEX_NAME);

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            indexSingleDoc(INDEX_NAME);
        }
        Fence fence = awaitFence(fenceDir);
        assertEquals("fence owner should be the primary's node", nodeId(primaryNode), fence.nodeId);
        assertEquals("fence term should be the primary term", primary.getOperationPrimaryTerm(), fence.term);

        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            indexSingleDoc(INDEX_NAME);
            numDocs++;
        }
        assertBusy(() -> assertThat("fence did not advance with replicas present", readFence(fenceDir).seq, greaterThan(fence.seq)));

        // Replicas are still fed by segment replication and converge on the same doc count.
        refresh(INDEX_NAME);
        ensureGreen(INDEX_NAME);
        final int expectedDocs = numDocs;
        assertBusy(() -> {
            ShardStats[] shards = client().admin().indices().prepareStats(INDEX_NAME).setDocs(true).get().getShards();
            assertEquals(replicaCount + 1, shards.length);
            for (ShardStats shard : shards) {
                assertEquals(
                    "shard copy " + shard.getShardRouting() + " is out of sync",
                    expectedDocs,
                    shard.getStats().getDocs().getCount()
                );
            }
        }, 60, TimeUnit.SECONDS);
        assertEquals(expectedDocs, client().prepareSearch(INDEX_NAME).setSize(0).get().getHits().getTotalHits().value());
    }

    /**
     * Deletion is the fencing act. Acknowledgement paths are keyed by term, so a takeover claims its own object and
     * then deletes every lower-term one outright - which is what makes a higher-term takeover deterministic rather
     * than a race for a shared object. Observably: after failover exactly one path remains, at the higher term, and
     * the superseded term's path is gone.
     */
    public void testSupersededTermPathsAreDeletedOnTakeover() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, fencedIndexSettings(1, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        String originalPrimaryNode = primaryNodeName(INDEX_NAME);
        Path fenceDir = translogMetadataDirectory(INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence before = awaitFence(fenceDir);
        assertEquals(nodeId(originalPrimaryNode), before.nodeId);
        assertEquals("one acknowledgement path per shard at a time", 1, fenceBlobNames(fenceDir).size());
        String supersededPath = RemoteStoreFence.fenceBlobName(before.term);
        assertTrue(fenceBlobNames(fenceDir).contains(supersededPath));

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(originalPrimaryNode));
        ensureYellowAndNoInitializingShards(INDEX_NAME);
        indexSingleDoc(INDEX_NAME);

        assertBusy(() -> {
            Fence after = readFence(fenceDir);
            assertThat("the promoted copy did not claim a higher term", after.term, greaterThan(before.term));
            Set<String> paths = fenceBlobNames(fenceDir);
            assertFalse("the superseded term's path was not deleted: " + paths, paths.contains(supersededPath));
            assertEquals("exactly one acknowledgement path must survive a takeover: " + paths, 1, paths.size());
            assertTrue(paths.contains(RemoteStoreFence.fenceBlobName(after.term)));
        }, 60, TimeUnit.SECONDS);
    }

    /**
     * On failover the fence seals over to the promoted copy: a new owner at a higher term takes the chain, which is
     * what makes the old primary's next upload fail its CAS.
     */
    public void testFenceSealsOverToThePromotedPrimary() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, fencedIndexSettings(1, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        String originalPrimaryNode = primaryNodeName(INDEX_NAME);
        Path fenceDir = translogMetadataDirectory(INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence beforeFailover = awaitFence(fenceDir);
        assertEquals(nodeId(originalPrimaryNode), beforeFailover.nodeId);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(originalPrimaryNode));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        assertBusy(() -> {
            Fence afterFailover = readFence(fenceDir);
            assertThat(
                "fence term did not advance on failover: " + beforeFailover + " -> " + afterFailover,
                afterFailover.term,
                greaterThan(beforeFailover.term)
            );
            assertNotEquals("fence ownership did not move to the promoted copy", beforeFailover.nodeId, afterFailover.nodeId);
        });
        assertEquals(nodeId(primaryNodeName(INDEX_NAME)), readFence(fenceDir).nodeId);
    }

    /**
     * Seal-on-promotion, the failover twin of {@link #testASupersededCopyFailsRecoveryAtTheSeal}. A replica being
     * promoted to primary claims the fence before it reads its translog restore point, inside the primary term
     * transition. A promoted copy that has been superseded — the chain is owned by another writer at a strictly higher
     * term — must therefore fail the promotion itself, never activate, and never claim the chain. Without the seal the
     * promotion succeeds, and the fenced translog upload in {@code postActivatePrimaryMode} is logged and swallowed,
     * leaving a superseded copy serving as a started primary.
     */
    public void testASupersededReplicaFailsPromotionAtTheSeal() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, fencedIndexSettings(1, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        String originalPrimaryNode = primaryNodeName(INDEX_NAME);
        Path fenceDir = translogMetadataDirectory(INDEX_NAME);
        BlobPath metadataBlobPath = translogMetadataBlobPath(INDEX_NAME);
        ShardId shardId = getIndexShard(originalPrimaryNode, INDEX_NAME).shardId();

        indexSingleDoc(INDEX_NAME);
        Fence before = awaitFence(fenceDir);
        assertEquals(nodeId(originalPrimaryNode), before.nodeId);

        // Stand in for a copy hydrated elsewhere that has already moved the shard well past this cluster's term. The
        // gap is deliberately wider than the number of allocation retries, so neither the promotion nor any subsequent
        // reassignment can lift the primary term above the fence.
        String clusterManager = internalCluster().getClusterManagerName();
        BlobStoreRepository translogRepository = (BlobStoreRepository) internalCluster().getInstance(
            RepositoriesService.class,
            clusterManager
        ).repository(REPOSITORY_2_NAME);
        BlobContainer fenceContainer = translogRepository.blobStore().blobContainer(metadataBlobPath);
        new RemoteStoreFence(
            fenceContainer,
            "superseding-allocation",
            "superseding-node",
            shardId,
            internalCluster().getInstance(ThreadPool.class, clusterManager)
        ).validateAndAdvance(before.term + 50);
        Fence superseding = readFence(fenceDir);
        assertEquals("superseding-node", superseding.nodeId);

        // Failover: the replica is promoted in place. No write is issued against the promoted copy.
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(originalPrimaryNode));

        // The promotion must abort in the seal, so the primary ends up unassigned with that failure recorded rather
        // than a superseded copy serving as a started primary.
        assertBusy(() -> {
            UnassignedInfo unassignedInfo = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .routingTable()
                .index(INDEX_NAME)
                .shard(0)
                .primaryShard()
                .unassignedInfo();
            assertNotNull("primary was assigned despite being superseded", unassignedInfo);
            assertNotNull("no allocation failure recorded yet: " + unassignedInfo, unassignedInfo.getFailure());
            String stackTrace = ExceptionsHelper.stackTrace(unassignedInfo.getFailure());
            assertTrue(
                "the copy was not refused at the fence seal, but with:\n" + stackTrace,
                stackTrace.contains(PROMOTION_SEAL_METHOD_NAME + "(")
            );
        }, 60, TimeUnit.SECONDS);

        // The superseded copy neither claimed nor advanced the chain, so it cannot have read a restore point it would
        // then serve from.
        Fence after = readFence(fenceDir);
        assertEquals("superseded copy claimed the fence: " + after, superseding.nodeId, after.nodeId);
        assertEquals("superseded copy advanced the fence: " + superseding + " -> " + after, superseding.term, after.term);
        assertEquals("superseded copy advanced the fence: " + superseding + " -> " + after, superseding.seq, after.seq);

        // The index is unrecoverable by construction; drop it rather than leaving a red index for the test teardown.
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
    }

    /**
     * Seal-before-restore. A recovering primary claims the fence <b>before</b> it reads its translog restore point:
     * otherwise a previous primary that is still alive - and still holding a valid CAS token - could keep acknowledging
     * writes that land after the restore point the new copy read, and those acknowledged writes would be lost.
     * <p>
     * The race window itself is not observable from an integration test, so what is asserted here is that the seal is a
     * real, blocking step on the recovery path rather than something the first post-recovery upload happens to do: a
     * copy that has been superseded - the chain is owned by another writer at a strictly higher term - must fail
     * recovery <em>at the seal</em>, never reach the restore point, and never claim the chain. Without the seal the
     * shard opens its engine, reads its restore point, and only trips later on the upload path, so the recorded
     * allocation failure comes from a different call site and this test fails.
     */
    public void testASupersededCopyFailsRecoveryAtTheSeal() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        Path fenceDir = translogMetadataDirectory(INDEX_NAME);
        BlobPath metadataBlobPath = translogMetadataBlobPath(INDEX_NAME);
        ShardId shardId = getIndexShard(dataNode, INDEX_NAME).shardId();

        indexSingleDoc(INDEX_NAME);
        Fence before = awaitFence(fenceDir);
        assertEquals(nodeId(dataNode), before.nodeId);

        // Stand in for a copy hydrated elsewhere that has already moved the shard well past this node's term. The gap
        // is deliberately wider than the number of allocation retries, so no reassignment can lift this node's primary
        // term above the fence and turn the takeover into a legitimate one.
        String clusterManager = internalCluster().getClusterManagerName();
        BlobStoreRepository translogRepository = (BlobStoreRepository) internalCluster().getInstance(
            RepositoriesService.class,
            clusterManager
        ).repository(REPOSITORY_2_NAME);
        BlobContainer fenceContainer = translogRepository.blobStore().blobContainer(metadataBlobPath);
        new RemoteStoreFence(
            fenceContainer,
            "superseding-allocation",
            "superseding-node",
            shardId,
            internalCluster().getInstance(ThreadPool.class, clusterManager)
        ).validateAndAdvance(before.term + 50);
        Fence superseding = readFence(fenceDir);
        assertEquals("superseding-node", superseding.nodeId);

        internalCluster().restartNode(dataNode);

        // Every allocation attempt must abort in the seal, so the shard stays unassigned with that failure recorded.
        assertBusy(() -> {
            UnassignedInfo unassignedInfo = client().admin()
                .cluster()
                .prepareState()
                .get()
                .getState()
                .routingTable()
                .index(INDEX_NAME)
                .shard(0)
                .primaryShard()
                .unassignedInfo();
            assertNotNull("primary was assigned despite being superseded", unassignedInfo);
            assertNotNull("no allocation failure recorded yet: " + unassignedInfo, unassignedInfo.getFailure());
            String stackTrace = ExceptionsHelper.stackTrace(unassignedInfo.getFailure());
            assertTrue("recovery did not fail in the fence seal, but with:\n" + stackTrace, stackTrace.contains(SEAL_METHOD_NAME));
            assertThat("recovery was not retried to exhaustion", unassignedInfo.getNumFailedAllocations(), greaterThan(0));
        }, 60, TimeUnit.SECONDS);

        // The superseded copy neither claimed nor advanced the chain, so it cannot have read a restore point it would
        // then serve from.
        Fence after = readFence(fenceDir);
        assertEquals("superseded copy claimed the fence: " + after, superseding.nodeId, after.nodeId);
        assertEquals("superseded copy advanced the fence: " + superseding + " -> " + after, superseding.term, after.term);
        assertEquals("superseded copy advanced the fence: " + superseding + " -> " + after, superseding.seq, after.seq);

        // The index is unrecoverable by construction; drop it rather than leaving a red index for the test teardown.
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
    }

    /**
     * The point of the fence: once another writer owns the chain, the incumbent's next upload must fail before the
     * write is acknowledged, rather than silently writing behind the new owner. This is the zero-replica case, where
     * replica-based primary term validation cannot help.
     */
    public void testIncumbentIsFencedByACompetingOwner() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        BlobPath metadataBlobPath = translogMetadataBlobPath(INDEX_NAME);
        Path fenceDir = translogMetadataDirectory(INDEX_NAME);
        IndexShard primary = getIndexShard(dataNode, INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence incumbent = awaitFence(fenceDir);
        assertEquals(nodeId(dataNode), incumbent.nodeId);

        // Stand in for a copy hydrated on another node: take ownership of the chain at a higher term, out of band.
        BlobStoreRepository translogRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNode)
            .repository(REPOSITORY_2_NAME);
        BlobContainer fenceContainer = translogRepository.blobStore().blobContainer(metadataBlobPath);
        new RemoteStoreFence(
            fenceContainer,
            "competing-allocation",
            "competing-node",
            primary.shardId(),
            internalCluster().getInstance(ThreadPool.class, dataNode)
        ).validateAndAdvance(incumbent.term + 1);
        Fence competing = readFence(fenceDir);
        assertEquals("competing-node", competing.nodeId);

        // The incumbent holds a stale token, so its next upload loses the CAS and the write is never acknowledged.
        Exception failure = expectThrows(Exception.class, () -> indexSingleDoc(INDEX_NAME));
        String stackTrace = ExceptionsHelper.stackTrace(failure);
        assertTrue("expected a fencing failure but got:\n" + stackTrace, stackTrace.contains("primary fenced by remote store"));

        // A legitimate new incarnation of the shard reclaims the chain: the shard failed, is reassigned at a higher
        // primary term, and its bootstrap seals the fence back over to this node. Doc counts are deliberately not
        // asserted here - a fenced upload can leave an orphan metadata file, which is a documented follow-up.
        ensureGreen(INDEX_NAME);
        indexSingleDoc(INDEX_NAME);
        assertBusy(() -> {
            Fence reclaimed = readFence(fenceDir);
            assertEquals("fence was not reclaimed by the new incarnation", nodeId(dataNode), reclaimed.nodeId);
            assertThat(reclaimed.term, greaterThanOrEqualTo(competing.term));
        });
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
    }

    /**
     * A resize recovers from LOCAL_SHARDS, which reaches the seal like every non-PEER recovery source. The seal must be
     * harmless there rather than merely untested: the fence path is keyed by index UUID and a resize target is a new
     * index with its own UUID, so the target claims a fresh key namespace uncontested and has no lower-term path to
     * sweep. Asserted on the target's own fence blob, and on the source remaining serviceable afterwards, so that a
     * seal which contended with the write-blocked source would fail here.
     */
    public void testResizeTargetSealsItsOwnFreshChain() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);
        indexSingleDoc(INDEX_NAME);

        Fence sourceFence = readFence(translogMetadataDirectory(INDEX_NAME));

        // A resize requires the source to stop accepting writes; the source keeps its own fence throughout.
        assertAcked(
            client().admin().indices().prepareUpdateSettings(INDEX_NAME).setSettings(Settings.builder().put("index.blocks.write", true))
        );

        final String targetIndex = INDEX_NAME + "-resized";
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(INDEX_NAME, targetIndex)
                .setResizeType(ResizeType.CLONE)
                .setSettings(
                    Settings.builder()
                        .put("index.number_of_replicas", 0)
                        .put(IndexMetadata.SETTING_REMOTE_STORE_FENCING_ENABLED, true)
                        .putNull("index.blocks.write")
                        .build()
                )
                .get()
        );
        ensureGreen(targetIndex);

        // The target sealed a fence of its own, under its own index UUID, and it is not the source's.
        Fence targetFence = readFence(translogMetadataDirectory(targetIndex));
        assertNotEquals("resize target must not share the source's fence object", sourceFence.indexUUID, targetFence.indexUUID);
        assertThat("resize target should claim a fresh chain", targetFence.seq, greaterThanOrEqualTo(0L));

        // The target acknowledges writes through its own chain, and the source is still intact behind it.
        indexSingleDoc(targetIndex);
        assertBusy(() -> assertThat(readFence(translogMetadataDirectory(targetIndex)).seq, greaterThan(targetFence.seq)));
        assertEquals(sourceFence.indexUUID, readFence(translogMetadataDirectory(INDEX_NAME)).indexUUID);

        assertAcked(client().admin().indices().prepareDelete(targetIndex, INDEX_NAME));
    }
}
