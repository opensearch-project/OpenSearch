/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.ResourceUsageCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlMode;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.ThreadPoolStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE;
import static org.opensearch.ratelimitting.admissioncontrol.settings.CpuBasedAdmissionControllerSettings.FORCE_MERGE_CPU_USAGE_LIMIT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration tests for admission control of the force merge API. Admission is evaluated per shard on the data node
 * that executes the merge. Resource pressure is simulated by stopping each node's {@link ResourceUsageCollectorService}
 * and injecting a fixed CPU utilization, so the decision is deterministic. Only the CPU controller is used because it is
 * registered on every platform; IO and native memory controllers share the same integration point.
 * <p>
 * The force merge UUID is written to the commit user data only by a merge that actually ran, so it is used as proof of
 * whether a shard copy was merged (independent of background merges and segment topology).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ForceMergeAdmissionControlIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test-index";
    private static final double HIGH_CPU = 90;
    private static final double LOW_CPU = 10;
    private static final long FORCE_MERGE_LIMIT = 50;

    public void testForceMergeRejectedOnEveryShardWhenAllNodesUnderPressure() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        enableCpuAdmissionControl(AdmissionControlMode.ENFORCED);
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        ForceMergeResponse response = forceMergeToOneSegment();

        assertEquals(4, response.getTotalShards());
        assertEquals(0, response.getSuccessfulShards());
        assertEquals(4, response.getFailedShards());
        assertEquals(RestStatus.TOO_MANY_REQUESTS, response.getStatus());
        for (DefaultShardOperationFailedException failure : response.getShardFailures()) {
            assertEquals(RestStatus.TOO_MANY_REQUESTS, failure.status());
            assertThat(failure.reason(), containsString("OpenSearchRejectedExecutionException"));
            assertThat(failure.reason(), containsString(AdmissionControlActionType.FORCE_MERGE.name()));
        }
        assertForceMergeUUIDs(nodeId -> false);
        assertEquals(4, totalForceMergeRejections(nodes));
        assertForceMergePoolIdle();

        // pressure drops: the next request is admitted everywhere, nothing was left behind by the rejections
        nodes.forEach(node -> setCpu(node, LOW_CPU));
        response = forceMergeToOneSegment();
        assertEquals(4, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());
        assertForceMergeUUIDs(nodeId -> true);
        assertEquals(4, totalForceMergeRejections(nodes));
    }

    public void testPartialRejectionAcrossNodes() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(4, 1);
        enableCpuAdmissionControl(AdmissionControlMode.ENFORCED);
        String constrainedNode = nodes.get(0);
        String healthyNode = nodes.get(1);
        setCpu(constrainedNode, HIGH_CPU);
        setCpu(healthyNode, LOW_CPU);
        String constrainedNodeId = nodeId(constrainedNode);
        int copiesOnConstrainedNode = shardCopiesOn(constrainedNodeId);
        assertThat(copiesOnConstrainedNode, greaterThan(0));

        ForceMergeResponse response = forceMergeToOneSegment();

        assertEquals(8, response.getTotalShards());
        assertEquals(copiesOnConstrainedNode, response.getFailedShards());
        assertEquals(8 - copiesOnConstrainedNode, response.getSuccessfulShards());
        for (DefaultShardOperationFailedException failure : response.getShardFailures()) {
            assertEquals(RestStatus.TOO_MANY_REQUESTS, failure.status());
        }
        // only copies on the healthy node were merged
        assertForceMergeUUIDs(nodeId -> nodeId.equals(constrainedNodeId) == false);
        assertEquals(copiesOnConstrainedNode, forceMergeRejections(constrainedNode));
        assertEquals(0, forceMergeRejections(healthyNode));
    }

    public void testMonitorModeAdmitsAndCountsRejections() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        enableCpuAdmissionControl(AdmissionControlMode.MONITOR);
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        ForceMergeResponse response = forceMergeToOneSegment();

        assertEquals(4, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());
        assertForceMergeUUIDs(nodeId -> true);
        assertEquals(4, totalForceMergeRejections(nodes));
    }

    public void testDisabledAdmissionControlPreservesForceMergeBehavior() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        // only the limit is configured; the controller mode stays at its default (disabled)
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put(FORCE_MERGE_CPU_USAGE_LIMIT.getKey(), 0))
        );
        nodes.forEach(node -> setCpu(node, 100));

        ForceMergeResponse response = forceMergeToOneSegment();

        assertEquals(4, response.getSuccessfulShards());
        assertEquals(0, response.getFailedShards());
        assertEquals(RestStatus.OK, response.getStatus());
        assertForceMergeUUIDs(nodeId -> true);
        assertEquals(0, totalForceMergeRejections(nodes));
    }

    public void testConcurrentForceMergeRequests() throws Exception {
        List<String> nodes = startDataNodes(2);
        createIndexWithDocs(2, 1);
        enableCpuAdmissionControl(AdmissionControlMode.ENFORCED);
        nodes.forEach(node -> setCpu(node, HIGH_CPU));
        int requests = randomIntBetween(3, 6);

        for (ForceMergeResponse response : forceMergeConcurrently(requests)) {
            assertEquals(4, response.getFailedShards());
            assertEquals(0, response.getSuccessfulShards());
        }
        assertEquals(4L * requests, totalForceMergeRejections(nodes));
        assertForceMergePoolIdle();

        nodes.forEach(node -> setCpu(node, LOW_CPU));
        for (ForceMergeResponse response : forceMergeConcurrently(requests)) {
            assertEquals(4, response.getSuccessfulShards());
            assertEquals(0, response.getFailedShards());
        }
        assertEquals(4L * requests, totalForceMergeRejections(nodes));
        assertForceMergePoolIdle();
    }

    public void testSegmentReplicationReplicasSkipAdmissionControl() throws Exception {
        List<String> nodes = startDataNodes(2);
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
            )
        );
        ensureGreen(INDEX);
        enableCpuAdmissionControl(AdmissionControlMode.ENFORCED);
        nodes.forEach(node -> setCpu(node, HIGH_CPU));

        ForceMergeResponse response = forceMergeToOneSegment();

        // primaries merge locally and are rejected; segment replication replicas do not merge locally and are not gated
        assertEquals(4, response.getTotalShards());
        assertEquals(2, response.getFailedShards());
        assertEquals(2, response.getSuccessfulShards());
        for (DefaultShardOperationFailedException failure : response.getShardFailures()) {
            assertEquals(RestStatus.TOO_MANY_REQUESTS, failure.status());
        }
        assertEquals(2, totalForceMergeRejections(nodes));
    }

    private List<String> startDataNodes(int count) {
        List<String> nodes = internalCluster().startNodes(count);
        ensureStableCluster(count);
        return nodes;
    }

    private void createIndexWithDocs(int shards, int replicas) {
        assertAcked(
            prepareCreate(INDEX).setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicas)
            )
        );
        ensureGreen(INDEX);
        for (int batch = 0; batch < 3; batch++) {
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < 10; i++) {
                bulk.add(client().prepareIndex(INDEX).setSource("field", "value-" + batch + "-" + i));
            }
            assertFalse(bulk.get().hasFailures());
            client().admin().indices().prepareRefresh(INDEX).get();
        }
    }

    private void enableCpuAdmissionControl(AdmissionControlMode mode) {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(
                    Settings.builder()
                        .put(CPU_BASED_ADMISSION_CONTROLLER_TRANSPORT_LAYER_MODE.getKey(), mode.getMode())
                        .put(FORCE_MERGE_CPU_USAGE_LIMIT.getKey(), FORCE_MERGE_LIMIT)
                )
        );
    }

    /**
     * Freezes the node's resource usage collector and injects the given CPU utilization for the local node.
     */
    private void setCpu(String node, double cpuPercent) {
        ResourceUsageCollectorService collector = internalCluster().getInstance(ResourceUsageCollectorService.class, node);
        collector.stop();
        String nodeId = nodeId(node);
        collector.collectNodeResourceUsageStats(nodeId, System.currentTimeMillis(), 0, cpuPercent, new IoUsageStats(0), 0);
        assertEquals(cpuPercent, collector.getNodeStatistics(nodeId).get().getCpuUtilizationPercent(), 0.0);
    }

    private ForceMergeResponse forceMergeToOneSegment() {
        return client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).get();
    }

    private List<ForceMergeResponse> forceMergeConcurrently(int requests) {
        List<ActionFuture<ForceMergeResponse>> futures = new ArrayList<>();
        for (int i = 0; i < requests; i++) {
            futures.add(client().admin().indices().prepareForceMerge(INDEX).setMaxNumSegments(1).execute());
        }
        List<ForceMergeResponse> responses = new ArrayList<>();
        for (ActionFuture<ForceMergeResponse> future : futures) {
            responses.add(future.actionGet(30, TimeUnit.SECONDS));
        }
        return responses;
    }

    private String nodeId(String node) {
        return internalCluster().clusterService(node).localNode().getId();
    }

    private int shardCopiesOn(String nodeId) {
        int copies = 0;
        for (ShardRouting shardRouting : clusterService().state().routingTable().allShards(INDEX)) {
            if (nodeId.equals(shardRouting.currentNodeId())) {
                copies++;
            }
        }
        return copies;
    }

    private CpuBasedAdmissionController cpuController(String node) {
        return (CpuBasedAdmissionController) internalCluster().getInstance(AdmissionControlService.class, node)
            .getAdmissionController(CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER);
    }

    private long forceMergeRejections(String node) {
        return cpuController(node).getRejectionCount(AdmissionControlActionType.FORCE_MERGE.getType());
    }

    private long totalForceMergeRejections(List<String> nodes) {
        return nodes.stream().mapToLong(this::forceMergeRejections).sum();
    }

    /**
     * Asserts, for every shard copy, whether a force merge ran on it. A forced flush is issued first so that the force
     * merge UUID of a completed merge is part of the last commit.
     */
    private void assertForceMergeUUIDs(Predicate<String> mergedOnNode) throws IOException {
        assertNoFailures(client().admin().indices().prepareFlush(INDEX).setForce(true).get());
        for (ShardRouting shardRouting : clusterService().state().routingTable().allShards(INDEX)) {
            String nodeId = shardRouting.currentNodeId();
            String nodeName = clusterService().state().nodes().get(nodeId).getName();
            IndexShard shard = internalCluster().getInstance(IndicesService.class, nodeName)
                .indexServiceSafe(shardRouting.index())
                .getShard(shardRouting.id());
            String uuid;
            try (GatedCloseable<IndexCommit> commit = shard.acquireLastIndexCommit(true)) {
                uuid = commit.get().getUserData().get(Engine.FORCE_MERGE_UUID_KEY);
            }
            if (mergedOnNode.test(nodeId)) {
                assertThat("expected " + shardRouting + " to be force merged", uuid, notNullValue());
            } else {
                assertThat("expected " + shardRouting + " not to be force merged", uuid, nullValue());
            }
        }
    }

    /**
     * Rejections happen on the force merge pool thread; nothing must remain active or queued afterwards.
     */
    private void assertForceMergePoolIdle() throws Exception {
        assertBusy(() -> {
            NodesStatsResponse stats = client().admin()
                .cluster()
                .prepareNodesStats()
                .clear()
                .addMetric(NodesStatsRequest.Metric.THREAD_POOL.metricName())
                .get();
            for (NodeStats nodeStats : stats.getNodes()) {
                for (ThreadPoolStats.Stats pool : nodeStats.getThreadPool()) {
                    if (ThreadPool.Names.FORCE_MERGE.equals(pool.getName())) {
                        assertEquals(0, pool.getActive());
                        assertEquals(0, pool.getQueue());
                    }
                }
            }
        });
    }
}
