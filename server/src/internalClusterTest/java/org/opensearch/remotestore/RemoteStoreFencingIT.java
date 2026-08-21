/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.ExceptionsHelper;
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
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.METADATA;
import static org.opensearch.index.translog.transfer.RemoteStoreFence.FENCE_BLOB_NAME;
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
     * {@code v1|<term>|<ownerNodeId>|<seq>} is asserted rather than assumed.
     */
    private static Fence readFence(Path fenceBlob) throws IOException {
        String raw = new String(Files.readAllBytes(fenceBlob), StandardCharsets.UTF_8);
        String[] tokens = raw.split("\\|");
        assertEquals("unexpected fence blob layout [" + raw + "]", 4, tokens.length);
        assertEquals("unexpected fence codec version [" + raw + "]", "v1", tokens[0]);
        return new Fence(Long.parseLong(tokens[1]), tokens[2], Long.parseLong(tokens[3]));
    }

    private static Fence awaitFence(Path fenceBlob) throws Exception {
        assertBusy(() -> assertTrue("fence blob [" + fenceBlob + "] was never created", Files.exists(fenceBlob)));
        return readFence(fenceBlob);
    }

    private static final class Fence {
        private final long term;
        private final String owner;
        private final long seq;

        private Fence(long term, String owner, long seq) {
            this.term = term;
            this.owner = owner;
            this.seq = seq;
        }

        @Override
        public String toString() {
            return String.format(Locale.ROOT, "term [%d] owner [%s] seq [%d]", term, owner, seq);
        }
    }

    private String nodeId(String nodeName) {
        return internalCluster().getInstance(ClusterService.class, nodeName).localNode().getId();
    }

    /**
     * Zero replicas: the object store is the only witness, so every acknowledged write must advance the CAS chain.
     */
    public void testFenceIsExercisedWithZeroReplicas() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        Path fenceBlob = translogMetadataDirectory(INDEX_NAME).resolve(FENCE_BLOB_NAME);
        IndexShard primary = getIndexShard(dataNode, INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence bootstrapped = awaitFence(fenceBlob);
        assertEquals("fence owner should be the primary's node", nodeId(dataNode), bootstrapped.owner);
        assertEquals("fence term should be the primary term", primary.getOperationPrimaryTerm(), bootstrapped.term);
        assertThat(bootstrapped.seq, greaterThanOrEqualTo(0L));

        // Each acknowledged write advances the chain. The exact delta is not asserted: background syncs may also
        // upload, and the guarantee being verified is that the chain moves forward, never that it moves by one.
        Fence previous = bootstrapped;
        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            indexSingleDoc(INDEX_NAME);
            Fence current = readFence(fenceBlob);
            assertThat("fence did not advance after an acknowledged write", current.seq, greaterThan(previous.seq));
            assertEquals(previous.owner, current.owner);
            assertEquals(previous.term, current.term);
            previous = current;
        }
    }

    /**
     * The fence is control plane only: it shares a directory with the translog metadata files but must never be
     * mistaken for one, since metadata listings drive both restore lineage and garbage collection.
     */
    public void testFenceBlobIsNotVisibleAsTranslogMetadata() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        String dataNode = internalCluster().startDataOnlyNode();
        createIndex(INDEX_NAME, fencedIndexSettings(0, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        Path metadataDirectory = translogMetadataDirectory(INDEX_NAME);
        Path fenceBlob = metadataDirectory.resolve(FENCE_BLOB_NAME);
        int numDocs = randomIntBetween(2, 5);
        for (int i = 0; i < numDocs; i++) {
            indexSingleDoc(INDEX_NAME);
        }
        awaitFence(fenceBlob);

        Set<String> blobs;
        try (Stream<Path> listing = Files.list(metadataDirectory)) {
            blobs = listing.map(path -> path.getFileName().toString()).collect(Collectors.toSet());
        }
        assertTrue("fence blob missing from " + blobs, blobs.contains(FENCE_BLOB_NAME));
        assertFalse("fence blob must not match the translog metadata prefix", FENCE_BLOB_NAME.startsWith(METADATA_PREFIX));
        Set<String> metadataFiles = blobs.stream().filter(name -> name.startsWith(METADATA_PREFIX)).collect(Collectors.toSet());
        assertFalse("no translog metadata files were written: " + blobs, metadataFiles.isEmpty());
        assertFalse("fence blob leaked into the metadata listing", metadataFiles.contains(FENCE_BLOB_NAME));

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
        Path fenceBlob = translogMetadataDirectory(INDEX_NAME).resolve(FENCE_BLOB_NAME);
        IndexShard primary = getIndexShard(primaryNode, INDEX_NAME);

        int numDocs = randomIntBetween(5, 10);
        for (int i = 0; i < numDocs; i++) {
            indexSingleDoc(INDEX_NAME);
        }
        Fence fence = awaitFence(fenceBlob);
        assertEquals("fence owner should be the primary's node", nodeId(primaryNode), fence.owner);
        assertEquals("fence term should be the primary term", primary.getOperationPrimaryTerm(), fence.term);

        for (int i = 0; i < randomIntBetween(2, 5); i++) {
            indexSingleDoc(INDEX_NAME);
            numDocs++;
        }
        assertBusy(() -> assertThat("fence did not advance with replicas present", readFence(fenceBlob).seq, greaterThan(fence.seq)));

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
     * On failover the fence seals over to the promoted copy: a new owner at a higher term takes the chain, which is
     * what makes the old primary's next upload fail its CAS.
     */
    public void testFenceSealsOverToThePromotedPrimary() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNodes(2);
        createIndex(INDEX_NAME, fencedIndexSettings(1, Translog.Durability.REQUEST));
        ensureGreen(INDEX_NAME);

        String originalPrimaryNode = primaryNodeName(INDEX_NAME);
        Path fenceBlob = translogMetadataDirectory(INDEX_NAME).resolve(FENCE_BLOB_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence beforeFailover = awaitFence(fenceBlob);
        assertEquals(nodeId(originalPrimaryNode), beforeFailover.owner);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(originalPrimaryNode));
        ensureYellowAndNoInitializingShards(INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        assertBusy(() -> {
            Fence afterFailover = readFence(fenceBlob);
            assertThat(
                "fence term did not advance on failover: " + beforeFailover + " -> " + afterFailover,
                afterFailover.term,
                greaterThan(beforeFailover.term)
            );
            assertNotEquals("fence ownership did not move to the promoted copy", beforeFailover.owner, afterFailover.owner);
        });
        assertEquals(nodeId(primaryNodeName(INDEX_NAME)), readFence(fenceBlob).owner);
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

        Path fenceBlob = translogMetadataDirectory(INDEX_NAME).resolve(FENCE_BLOB_NAME);
        BlobPath metadataBlobPath = translogMetadataBlobPath(INDEX_NAME);
        ShardId shardId = getIndexShard(dataNode, INDEX_NAME).shardId();

        indexSingleDoc(INDEX_NAME);
        Fence before = awaitFence(fenceBlob);
        assertEquals(nodeId(dataNode), before.owner);

        // Stand in for a copy hydrated elsewhere that has already moved the shard well past this node's term. The gap
        // is deliberately wider than the number of allocation retries, so no reassignment can lift this node's primary
        // term above the fence and turn the takeover into a legitimate one.
        String clusterManager = internalCluster().getClusterManagerName();
        BlobStoreRepository translogRepository = (BlobStoreRepository) internalCluster().getInstance(
            RepositoriesService.class,
            clusterManager
        ).repository(REPOSITORY_2_NAME);
        BlobContainer fenceContainer = translogRepository.blobStore().blobContainer(metadataBlobPath);
        new RemoteStoreFence(fenceContainer, "superseding-node", shardId, internalCluster().getInstance(ThreadPool.class, clusterManager))
            .validateAndAdvance(before.term + 50);
        Fence superseding = readFence(fenceBlob);
        assertEquals("superseding-node", superseding.owner);

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
        Fence after = readFence(fenceBlob);
        assertEquals("superseded copy claimed the fence: " + after, superseding.owner, after.owner);
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
        Path fenceBlob = translogMetadataDirectory(INDEX_NAME).resolve(FENCE_BLOB_NAME);
        IndexShard primary = getIndexShard(dataNode, INDEX_NAME);

        indexSingleDoc(INDEX_NAME);
        Fence incumbent = awaitFence(fenceBlob);
        assertEquals(nodeId(dataNode), incumbent.owner);

        // Stand in for a copy hydrated on another node: take ownership of the chain at a higher term, out of band.
        BlobStoreRepository translogRepository = (BlobStoreRepository) internalCluster().getInstance(RepositoriesService.class, dataNode)
            .repository(REPOSITORY_2_NAME);
        BlobContainer fenceContainer = translogRepository.blobStore().blobContainer(metadataBlobPath);
        new RemoteStoreFence(fenceContainer, "competing-node", primary.shardId(), internalCluster().getInstance(ThreadPool.class, dataNode))
            .validateAndAdvance(incumbent.term + 1);
        Fence competing = readFence(fenceBlob);
        assertEquals("competing-node", competing.owner);

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
            Fence reclaimed = readFence(fenceBlob);
            assertEquals("fence was not reclaimed by the new incarnation", nodeId(dataNode), reclaimed.owner);
            assertThat(reclaimed.term, greaterThanOrEqualTo(competing.term));
        });
        assertAcked(client().admin().indices().prepareDelete(INDEX_NAME));
    }
}
