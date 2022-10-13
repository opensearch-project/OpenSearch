/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.snapshots;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.hamcrest.MatcherAssert;
import org.junit.BeforeClass;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.Index;
import org.opensearch.monitor.fs.FsInfo;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest.Metric.FS;
import static org.opensearch.common.util.CollectionUtils.iterableAsArrayList;

public final class SearchableSnapshotIT extends AbstractSnapshotIntegTestCase {

    @BeforeClass
    public static void assumeFeatureFlag() {
        assumeTrue(
            "Searchable snapshot feature flag is enabled",
            Boolean.parseBoolean(System.getProperty(FeatureFlags.SEARCHABLE_SNAPSHOT))
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testCreateSearchableSnapshot() throws Exception {
        final Client client = client();
        createRepository("test-repo", "fs");
        createIndex(
            "test-idx-1",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1").build()
        );
        createIndex(
            "test-idx-2",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, "0").put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, "1").build()
        );
        ensureGreen();
        indexRandomDocs("test-idx-1", 100);
        indexRandomDocs("test-idx-2", 100);

        logger.info("--> snapshot");
        final CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot("test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx-1", "test-idx-2")
            .get();
        MatcherAssert.assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        MatcherAssert.assertThat(
            createSnapshotResponse.getSnapshotInfo().successfulShards(),
            equalTo(createSnapshotResponse.getSnapshotInfo().totalShards())
        );

        assertTrue(client.admin().indices().prepareDelete("test-idx-1", "test-idx-2").get().isAcknowledged());

        logger.info("--> restore indices as 'remote_snapshot'");
        client.admin()
            .cluster()
            .prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("(.+)")
            .setRenameReplacement("$1-copy")
            .setStorageType(RestoreSnapshotRequest.StorageType.REMOTE_SNAPSHOT)
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        ensureGreen();

        assertDocCount("test-idx-1-copy", 100L);
        assertDocCount("test-idx-2-copy", 100L);
        assertIndexDirectoryDoesNotExist("test-idx-1-copy", "test-idx-2-copy");
    }

    /**
     * Picks a shard out of the cluster state for each given index and asserts
     * that the 'index' directory does not exist in the node's file system.
     * This assertion is digging a bit into the implementation details to
     * verify that the Lucene segment files are not copied from the snapshot
     * repository to the node's local disk for a remote snapshot index.
     */
    private void assertIndexDirectoryDoesNotExist(String... indexNames) {
        final ClusterState state = client().admin().cluster().prepareState().get().getState();
        for (String indexName : indexNames) {
            final Index index = state.metadata().index(indexName).getIndex();
            // Get the primary shards for the given index
            final GroupShardsIterator<ShardIterator> shardIterators = state.getRoutingTable()
                .activePrimaryShardsGrouped(new String[] { indexName }, false);
            // Randomly pick one of the shards
            final List<ShardIterator> iterators = iterableAsArrayList(shardIterators);
            final ShardIterator shardIterator = RandomPicks.randomFrom(random(), iterators);
            final ShardRouting shardRouting = shardIterator.nextOrNull();
            assertNotNull(shardRouting);
            assertTrue(shardRouting.primary());
            assertTrue(shardRouting.assignedToNode());
            // Get the file system stats for the assigned node
            final String nodeId = shardRouting.currentNodeId();
            final NodesStatsResponse nodeStats = client().admin().cluster().prepareNodesStats(nodeId).addMetric(FS.metricName()).get();
            for (FsInfo.Path info : nodeStats.getNodes().get(0).getFs()) {
                // Build the expected path for the index data for a "normal"
                // index and assert it does not exist
                final String path = info.getPath();
                final Path file = PathUtils.get(path)
                    .resolve("indices")
                    .resolve(index.getUUID())
                    .resolve(Integer.toString(shardRouting.getId()))
                    .resolve("index");
                MatcherAssert.assertThat("Expect file not to exist: " + file, Files.exists(file), is(false));
            }
        }
    }
}
