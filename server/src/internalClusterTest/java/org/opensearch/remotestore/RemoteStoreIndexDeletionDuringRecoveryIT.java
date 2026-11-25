/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexSettings;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Deleting an index while a remote-store recovery of one of its shards is still downloading segments onto a node must
 * not leave that index's data on the node's disk.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreIndexDeletionDuringRecoveryIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    /**
     * Reproduces an index deletion that lands while one shard copy on the node is still being downloaded from the remote
     * store and therefore holds its shard lock, and while copies of other shards of the same index - left behind by
     * earlier, failed recovery attempts - sit on the node's disk with no owner. The copies whose lock is free must be
     * removed right away, the one still downloading stays until it is released, and nothing of the index remains once
     * the download is let go.
     */
    public void testDeletedIndexDataIsRemovedWhileAShardIsStillRecovering() throws Exception {
        final Path repositoryLocation = randomRepoPath().toAbsolutePath();
        final Settings nodeSettings = Settings.builder()
            .put(buildRemoteStoreNodeAttributes(repositoryLocation, 0d, "metadata", Long.MAX_VALUE))
            .build();
        disableRepoConsistencyCheck("Remote Store Creates System Repository");

        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        final String sourceNode = internalCluster().startDataOnlyNode(nodeSettings);
        createIndex(INDEX_NAME, Settings.builder().put(remoteStoreIndexSettings(0)).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).build());
        ensureGreen(INDEX_NAME);
        indexData(5, true);
        final Index index = resolveIndex(INDEX_NAME);
        // Relocate a shard that has segments to download; the other two play the role of the stale copies.
        final int recoveringShard = Arrays.stream(client().admin().indices().prepareStats(INDEX_NAME).get().getShards())
            .filter(shardStats -> shardStats.getStats().getDocs().getCount() > 0)
            .map(shardStats -> shardStats.getShardRouting().id())
            .findFirst()
            .orElseThrow(() -> new AssertionError("no shard has documents"));

        // Keep all shards on sourceNode until the explicit relocation below.
        assertAcked(
            client(clusterManagerNode).admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.rebalance.enable", "none"))
        );
        final String targetNode = internalCluster().startDataOnlyNode(nodeSettings);
        ensureStableCluster(3);
        final NodeEnvironment targetEnv = internalCluster().getInstance(NodeEnvironment.class, targetNode);
        final ShardId recovering = new ShardId(index, recoveringShard);
        final ShardId[] stale = IntStream.range(0, 3)
            .filter(id -> id != recoveringShard)
            .mapToObj(id -> new ShardId(index, id))
            .toArray(ShardId[]::new);

        // The test framework may give the index a custom data path; the shard directories live wherever the node resolves
        // them for this index, so look them up the same way the node does.
        final IndexSettings indexSettings = new IndexSettings(
            client().admin().cluster().prepareState().get().getState().metadata().index(INDEX_NAME),
            Settings.EMPTY
        );
        final Function<ShardId, Path[]> shardPaths = shardId -> indexSettings.hasCustomDataPath()
            ? new Path[] { targetEnv.resolveCustomLocation(indexSettings.customDataPath(), shardId) }
            : targetEnv.availableShardPaths(shardId);
        final Path[] indexPaths = indexSettings.hasCustomDataPath()
            ? new Path[] { targetEnv.resolveCustomLocation(indexSettings.customDataPath(), recovering).getParent() }
            : targetEnv.indexPaths(index);

        // The segment download of the relocating shard onto targetNode blocks inside the repository and keeps the shard
        // lock of that copy held.
        blockNodeOnAnySegmentFile(REPOSITORY_NAME, targetNode);
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger(IndicesClusterStateService.class))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "first pending-delete attempt fails on the held lock and is retried",
                    IndicesClusterStateService.class.getCanonicalName(),
                    Level.WARN,
                    "*failed to lock all shards for index*retrying in*"
                )
            );
            assertAcked(
                client(clusterManagerNode).admin()
                    .cluster()
                    .prepareReroute()
                    .add(new MoveAllocationCommand(INDEX_NAME, recovering.id(), sourceNode, targetNode))
            );
            waitForBlock(targetNode, REPOSITORY_NAME, TimeValue.timeValueSeconds(30));
            assertTrue("recovering copy should be on disk", FileSystemUtils.exists(shardPaths.apply(recovering)));

            // Copies of the other shards that an earlier recovery attempt left behind: complete on disk, not allocated
            // here, nobody holding their lock.
            for (ShardId shardId : stale) {
                for (Path shardPath : shardPaths.apply(shardId)) {
                    Files.createDirectories(shardPath.resolve("index"));
                    Files.write(shardPath.resolve("index").resolve("_0.cfs"), new byte[] { 1, 2, 3 });
                }
            }

            assertAcked(client(clusterManagerNode).admin().indices().prepareDelete(INDEX_NAME).setTimeout("10s"));

            // The unowned copies are removed as soon as the deletion is applied, even though the recovering copy of the
            // same index still holds its lock.
            assertBusy(() -> {
                for (ShardId shardId : stale) {
                    assertFalse(shardId + " should be removed immediately", FileSystemUtils.exists(shardPaths.apply(shardId)));
                }
            }, 10, TimeUnit.SECONDS);
            assertTrue("the copy still downloading must not be touched", FileSystemUtils.exists(shardPaths.apply(recovering)));

            // Keep the download blocked until the first pending-deletion attempt has given up waiting for the lock, so that
            // completing the deletion depends on the retry.
            assertBusy(appender::assertAllExpectationsMatched, 30, TimeUnit.SECONDS);
            assertTrue("the copy still downloading must not be touched", FileSystemUtils.exists(shardPaths.apply(recovering)));
            assertTrue("index directory must remain while a copy is locked", FileSystemUtils.exists(indexPaths));
        } finally {
            unblockNode(REPOSITORY_NAME, targetNode);
        }

        // Once the download is released the copy closes and removes its own files, and the retried pending deletion of
        // the index removes what is left. The first retry is scheduled a minute after the failed attempt.
        assertBusy(
            () -> assertFalse("no data of the deleted index should remain", FileSystemUtils.exists(indexPaths)),
            3,
            TimeUnit.MINUTES
        );
    }
}
