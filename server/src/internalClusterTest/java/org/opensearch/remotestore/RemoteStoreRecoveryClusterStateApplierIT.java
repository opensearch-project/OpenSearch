/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.file.Path;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Verifies that remote-store recovery does not block the cluster-state applier while hydrating a shard.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRecoveryClusterStateApplierIT extends AbstractRemoteStoreMockRepositoryIntegTestCase {

    public void testIndexDeletionIsAppliedWhileRemoteHydrationIsBlocked() throws Exception {
        final Path repositoryLocation = randomRepoPath().toAbsolutePath();
        final Settings nodeSettings = Settings.builder()
            .put(buildRemoteStoreNodeAttributes(repositoryLocation, 0d, "metadata", Long.MAX_VALUE))
            .build();
        disableRepoConsistencyCheck("Remote Store Creates System Repository");

        final String clusterManagerNode = internalCluster().startClusterManagerOnlyNode(nodeSettings);
        final String sourceNode = internalCluster().startDataOnlyNode(nodeSettings);
        createIndex(INDEX_NAME, remoteStoreIndexSettings(0));
        ensureGreen(INDEX_NAME);
        indexData(3, true);

        // Keep the primary on sourceNode until the explicit relocation below.
        assertAcked(
            client(clusterManagerNode).admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("cluster.routing.rebalance.enable", "none"))
        );
        final String targetNode = internalCluster().startDataOnlyNode(nodeSettings);
        ensureStableCluster(3);
        assertEquals(sourceNode, primaryNodeName(INDEX_NAME));

        blockNodeOnAnyFiles(TRANSLOG_REPOSITORY_NAME, targetNode);
        try {
            assertAcked(
                client(clusterManagerNode).admin()
                    .cluster()
                    .prepareReroute()
                    .add(new MoveAllocationCommand(INDEX_NAME, 0, sourceNode, targetNode))
            );
            waitForBlock(targetNode, TRANSLOG_REPOSITORY_NAME, TimeValue.timeValueSeconds(30));

            // Applying this state closes the recovering target shard while its engine-open translog hydration is blocked.
            // The acknowledgement includes the target node, so it proves its cluster-state applier completed without
            // waiting for the repository read.
            assertAcked(client(clusterManagerNode).admin().indices().prepareDelete(INDEX_NAME).setTimeout("10s"));

            final ClusterState targetState = internalCluster().getInstance(ClusterService.class, targetNode).state();
            assertFalse("target node did not apply the index deletion", targetState.metadata().hasIndex(INDEX_NAME));
        } finally {
            unblockNode(TRANSLOG_REPOSITORY_NAME, targetNode);
        }
    }
}
