/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.autoforcemerge.ForceMergeManagerSettings;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.node.NodeService;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Verifies the node-duress guardrail of the {@code _forcemerge} API on a real cluster: a force merge on a node whose
 * resource usage is over the force merge thresholds is rejected with 429 unless {@code ignore_duress} is set.
 * <p>
 * Duress is induced deterministically by setting the disk threshold to its minimum (10%) on a node whose disk is more
 * used than that; the tests are skipped on a host with an almost empty disk. Nodes that must stay healthy get thresholds
 * of 100% so that load on the CI host cannot trip them.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ForceMergeDuressIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final double DURESS_DISK_THRESHOLD = 10;

    private static final Settings HEALTHY_THRESHOLDS = Settings.builder()
        .put(ForceMergeManagerSettings.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 100)
        .put(ForceMergeManagerSettings.JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 100)
        .put(ForceMergeManagerSettings.DISK_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 100)
        .build();

    private static final Settings DISK_DURESS_THRESHOLDS = Settings.builder()
        .put(HEALTHY_THRESHOLDS)
        .put(ForceMergeManagerSettings.DISK_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), DURESS_DISK_THRESHOLD)
        .build();

    public void testForceMergeRejectedWhenNodeInDuress() {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode(DISK_DURESS_THRESHOLDS);
        assumeDiskUsedAboveDuressThreshold(dataNode);
        // auto force merge is disabled by default; the duress check must not depend on it
        assertFalse(
            internalCluster().clusterService(dataNode).getClusterSettings().get(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING)
        );
        createIndexWithSegments(1);
        final Map<Integer, Integer> before = segmentsPerShard();

        final Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get()
        );

        assertRejectedForDuress(e, 1, 1);
        assertThat(segmentsPerShard(), equalTo(before));
    }

    public void testIgnoreDuressBypassesTheGuardrail() {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode(DISK_DURESS_THRESHOLDS);
        assumeDiskUsedAboveDuressThreshold(dataNode);
        createIndexWithSegments(1);

        final ForceMergeResponse response = client().admin()
            .indices()
            .prepareForceMerge(INDEX)
            .setMaxNumSegments(1)
            .setIgnoreDuress(true)
            .get();

        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(segmentsPerShard().get(0), equalTo(1));
    }

    public void testForceMergeRunsWhenNodeHealthy() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode(HEALTHY_THRESHOLDS);
        createIndexWithSegments(1);

        final ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();

        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(segmentsPerShard().get(0), equalTo(1));
    }

    public void testRejectionFollowsDynamicThresholdUpdate() {
        internalCluster().startClusterManagerOnlyNode();
        final String dataNode = internalCluster().startDataOnlyNode(HEALTHY_THRESHOLDS);
        assumeDiskUsedAboveDuressThreshold(dataNode);
        createIndexWithSegments(1);

        // lowering the threshold through the cluster settings API puts the node in duress without a restart
        updateDiskThreshold(DURESS_DISK_THRESHOLD);
        final Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get()
        );
        assertRejectedForDuress(e, 1, 1);

        // and raising it again lets the force merge through
        updateDiskThreshold(100);
        final ForceMergeResponse response = client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
        assertThat(response.getFailedShards(), equalTo(0));
        assertThat(segmentsPerShard().get(0), equalTo(1));
    }

    /**
     * Only one of the two data nodes is in duress. The request as a whole is rejected with 429, while the shard on the
     * healthy node is still merged, since the coordinating node cannot know the remote node health up front.
     */
    public void testPartialDuressRejectsRequestButMergesHealthyShards() {
        internalCluster().startClusterManagerOnlyNode();
        final String stressedNode = internalCluster().startDataOnlyNode(DISK_DURESS_THRESHOLDS);
        final String healthyNode = internalCluster().startDataOnlyNode(HEALTHY_THRESHOLDS);
        assumeDiskUsedAboveDuressThreshold(stressedNode);
        createIndexWithSegments(2);
        final Map<Integer, Integer> before = segmentsPerShard();

        final Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get()
        );

        assertRejectedForDuress(e, 1, 2);
        final Map<Integer, Integer> after = segmentsPerShard();
        final int stressedShard = shardOn(stressedNode);
        final int healthyShard = shardOn(healthyNode);
        assertThat(after.get(stressedShard), equalTo(before.get(stressedShard)));
        assertThat(after.get(healthyShard), equalTo(1));
    }

    private void assertRejectedForDuress(Exception e, int rejectedShards, int totalShards) {
        final Throwable rejection = ExceptionsHelper.unwrap(e, OpenSearchRejectedExecutionException.class);
        assertNotNull("expected a duress rejection but got " + e, rejection);
        assertThat(ExceptionsHelper.status(rejection), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertThat(rejection.getMessage(), containsString("[" + rejectedShards + "] of [" + totalShards + "] shards"));
        assertThat(rejection.getMessage(), containsString("ignore_duress=true"));
    }

    private void updateDiskThreshold(double threshold) {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(
                    Settings.builder().put(ForceMergeManagerSettings.DISK_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), threshold)
                )
        );
    }

    /**
     * Creates the index with one shard per data node and no replicas, and indexes until every shard has more than one
     * segment so that a successful force merge is observable.
     */
    private void createIndexWithSegments(int shards) {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.total_shards_per_node", 1)
            )
        );
        ensureGreen(INDEX);
        for (int i = 0; i < 200; i++) {
            client().prepareIndex(INDEX).setSource("field", "value" + i).get();
            refresh(INDEX);
            final Map<Integer, Integer> segments = segmentsPerShard();
            if (segments.size() == shards && segments.values().stream().allMatch(count -> count > 1)) {
                return;
            }
        }
        fail("could not create more than one segment on every shard: " + segmentsPerShard());
    }

    private Map<Integer, Integer> segmentsPerShard() {
        refresh(INDEX);
        final Map<Integer, Integer> segments = new HashMap<>();
        for (IndexShardSegments shard : client().admin().indices().prepareSegments(INDEX).get().getIndices().get(INDEX)) {
            for (ShardSegments copy : shard) {
                if (copy.getShardRouting().primary()) {
                    segments.put(shard.getShardId().id(), copy.getSegments().size());
                }
            }
        }
        return segments;
    }

    private int shardOn(String nodeName) {
        final ClusterState state = clusterService().state();
        final String nodeId = state.nodes().resolveNode(nodeName).getId();
        for (ShardRouting shard : state.routingTable().index(INDEX).shardsWithState(ShardRoutingState.STARTED)) {
            if (shard.currentNodeId().equals(nodeId)) {
                return shard.id();
            }
        }
        throw new AssertionError("no shard of [" + INDEX + "] on " + nodeName);
    }

    private void assumeDiskUsedAboveDuressThreshold(String nodeName) {
        final FsInfo.Path total = internalCluster().getInstance(NodeService.class, nodeName)
            .getMonitorService()
            .fsService()
            .stats()
            .getTotal();
        final double usedPercent = 100.0 * (total.getTotal().getBytes() - total.getAvailable().getBytes()) / total.getTotal().getBytes();
        assumeTrue(
            "disk is only " + usedPercent + "% used, cannot induce duress with a " + DURESS_DISK_THRESHOLD + "% threshold",
            usedPercent > DURESS_DISK_THRESHOLD + 1
        );
    }
}
