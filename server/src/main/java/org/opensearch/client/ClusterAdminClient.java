/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainRequestBuilder;
import org.opensearch.action.admin.cluster.allocation.ClusterAllocationExplainResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequestBuilder;
import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequestBuilder;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateResponse;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequestBuilder;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequestBuilder;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequestBuilder;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.reload.NodesReloadSecureSettingsRequestBuilder;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequestBuilder;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequestBuilder;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksResponse;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequestBuilder;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequestBuilder;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageRequestBuilder;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageResponse;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreRequest;
import org.opensearch.action.admin.cluster.remotestore.restore.RestoreRemoteStoreResponse;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryResponse;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryResponse;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequestBuilder;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequestBuilder;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequestBuilder;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequestBuilder;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequestBuilder;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingResponse;
import org.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.clone.CloneSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequestBuilder;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequestBuilder;
import org.opensearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequestBuilder;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptRequestBuilder;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequest;
import org.opensearch.action.admin.cluster.storedscripts.PutStoredScriptRequestBuilder;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequest;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksRequestBuilder;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.opensearch.action.admin.indices.dangling.delete.DeleteDanglingIndexRequest;
import org.opensearch.action.admin.indices.dangling.import_index.ImportDanglingIndexRequest;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesRequest;
import org.opensearch.action.admin.indices.dangling.list.ListDanglingIndicesResponse;
import org.opensearch.action.ingest.DeletePipelineRequest;
import org.opensearch.action.ingest.DeletePipelineRequestBuilder;
import org.opensearch.action.ingest.GetPipelineRequest;
import org.opensearch.action.ingest.GetPipelineRequestBuilder;
import org.opensearch.action.ingest.GetPipelineResponse;
import org.opensearch.action.ingest.PutPipelineRequest;
import org.opensearch.action.ingest.PutPipelineRequestBuilder;
import org.opensearch.action.ingest.SimulatePipelineRequest;
import org.opensearch.action.ingest.SimulatePipelineRequestBuilder;
import org.opensearch.action.ingest.SimulatePipelineResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.tasks.TaskId;

/**
 * Administrative actions/operations against indices.
 *
 * @see AdminClient#cluster()
 *
 * @opensearch.internal
 */
public interface ClusterAdminClient extends OpenSearchClient {

    /**
     * The health of the cluster.
     *
     * @param request The cluster state request
     * @return The result future
     * @see Requests#clusterHealthRequest(String...)
     */
    ActionFuture<ClusterHealthResponse> health(ClusterHealthRequest request);

    /**
     * The health of the cluster.
     *
     * @param request  The cluster state request
     * @param listener A listener to be notified with a result
     * @see Requests#clusterHealthRequest(String...)
     */
    void health(ClusterHealthRequest request, ActionListener<ClusterHealthResponse> listener);

    /**
     * The health of the cluster.
     */
    ClusterHealthRequestBuilder prepareHealth(String... indices);

    /**
     * The state of the cluster.
     *
     * @param request The cluster state request.
     * @return The result future
     * @see Requests#clusterStateRequest()
     */
    ActionFuture<ClusterStateResponse> state(ClusterStateRequest request);

    /**
     * The state of the cluster.
     *
     * @param request  The cluster state request.
     * @param listener A listener to be notified with a result
     * @see Requests#clusterStateRequest()
     */
    void state(ClusterStateRequest request, ActionListener<ClusterStateResponse> listener);

    /**
     * The state of the cluster.
     */
    ClusterStateRequestBuilder prepareState();

    /**
     * Updates settings in the cluster.
     */
    ActionFuture<ClusterUpdateSettingsResponse> updateSettings(ClusterUpdateSettingsRequest request);

    /**
     * Update settings in the cluster.
     */
    void updateSettings(ClusterUpdateSettingsRequest request, ActionListener<ClusterUpdateSettingsResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterUpdateSettingsRequestBuilder prepareUpdateSettings();

    /**
     * Re initialize each cluster node and pass them the secret store password.
     */
    NodesReloadSecureSettingsRequestBuilder prepareReloadSecureSettings();

    /**
     * Reroutes allocation of shards. Advance API.
     */
    ActionFuture<ClusterRerouteResponse> reroute(ClusterRerouteRequest request);

    /**
     * Reroutes allocation of shards. Advance API.
     */
    void reroute(ClusterRerouteRequest request, ActionListener<ClusterRerouteResponse> listener);

    /**
     * Update settings in the cluster.
     */
    ClusterRerouteRequestBuilder prepareReroute();

    /**
     * Nodes info of the cluster.
     *
     * @param request The nodes info request
     * @return The result future
     * @see org.opensearch.client.Requests#nodesInfoRequest(String...)
     */
    ActionFuture<NodesInfoResponse> nodesInfo(NodesInfoRequest request);

    /**
     * Nodes info of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.opensearch.client.Requests#nodesInfoRequest(String...)
     */
    void nodesInfo(NodesInfoRequest request, ActionListener<NodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
     */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    /**
     * Cluster wide aggregated stats.
     *
     * @param request The cluster stats request
     * @return The result future
     * @see org.opensearch.client.Requests#clusterStatsRequest
     */
    ActionFuture<ClusterStatsResponse> clusterStats(ClusterStatsRequest request);

    /**
     * Cluster wide aggregated stats
     *
     * @param request  The cluster stats request
     * @param listener A listener to be notified with a result
     * @see org.opensearch.client.Requests#clusterStatsRequest()
     */
    void clusterStats(ClusterStatsRequest request, ActionListener<ClusterStatsResponse> listener);

    ClusterStatsRequestBuilder prepareClusterStats();

    /**
     * Nodes stats of the cluster.
     *
     * @param request The nodes stats request
     * @return The result future
     * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
     */
    ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
     *
     * @param request  The nodes info request
     * @param listener A listener to be notified with a result
     * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
     */
    void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    /**
     * Nodes stats of the cluster.
     */
    NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request. Nodes usage of the
     * cluster.
     *
     * @param request
     *            The nodes usage request
     * @return The result future
     * @see org.opensearch.client.Requests#nodesUsageRequest(String...)
     */
    ActionFuture<NodesUsageResponse> nodesUsage(NodesUsageRequest request);

    /**
     * Nodes usage of the cluster.
     *
     * @param request
     *            The nodes usage request
     * @param listener
     *            A listener to be notified with a result
     * @see org.opensearch.client.Requests#nodesUsageRequest(String...)
     */
    void nodesUsage(NodesUsageRequest request, ActionListener<NodesUsageResponse> listener);

    /**
     * Nodes usage of the cluster.
     */
    NodesUsageRequestBuilder prepareNodesUsage(String... nodesIds);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only
     * sampled for the node ids specified in the request.
     *
     */
    ActionFuture<NodesHotThreadsResponse> nodesHotThreads(NodesHotThreadsRequest request);

    /**
     * Returns top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids specified in the request.
     */
    void nodesHotThreads(NodesHotThreadsRequest request, ActionListener<NodesHotThreadsResponse> listener);

    /**
     * Returns a request builder to fetch top N hot-threads samples per node. The hot-threads are only sampled
     * for the node ids provided. Note: Use {@code *} to fetch samples for all nodes
     */
    NodesHotThreadsRequestBuilder prepareNodesHotThreads(String... nodesIds);

    /**
     * List tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see org.opensearch.client.Requests#listTasksRequest()
     */
    ActionFuture<ListTasksResponse> listTasks(ListTasksRequest request);

    /**
     * List active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see org.opensearch.client.Requests#listTasksRequest()
     */
    void listTasks(ListTasksRequest request, ActionListener<ListTasksResponse> listener);

    /**
     * List active tasks
     */
    ListTasksRequestBuilder prepareListTasks(String... nodesIds);

    /**
     * Get a task.
     *
     * @param request the request
     * @return the result future
     * @see org.opensearch.client.Requests#getTaskRequest()
     */
    ActionFuture<GetTaskResponse> getTask(GetTaskRequest request);

    /**
     * Get a task.
     *
     * @param request the request
     * @param listener A listener to be notified with the result
     * @see org.opensearch.client.Requests#getTaskRequest()
     */
    void getTask(GetTaskRequest request, ActionListener<GetTaskResponse> listener);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(String taskId);

    /**
     * Fetch a task by id.
     */
    GetTaskRequestBuilder prepareGetTask(TaskId taskId);

    /**
     * Cancel tasks
     *
     * @param request The nodes tasks request
     * @return The result future
     * @see org.opensearch.client.Requests#cancelTasksRequest()
     */
    ActionFuture<CancelTasksResponse> cancelTasks(CancelTasksRequest request);

    /**
     * Cancel active tasks
     *
     * @param request  The nodes tasks request
     * @param listener A listener to be notified with a result
     * @see org.opensearch.client.Requests#cancelTasksRequest()
     */
    void cancelTasks(CancelTasksRequest request, ActionListener<CancelTasksResponse> listener);

    /**
     * Cancel active tasks
     */
    CancelTasksRequestBuilder prepareCancelTasks(String... nodesIds);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ActionFuture<ClusterSearchShardsResponse> searchShards(ClusterSearchShardsRequest request);

    /**
     * Returns list of shards the given search would be executed on.
     */
    void searchShards(ClusterSearchShardsRequest request, ActionListener<ClusterSearchShardsResponse> listener);

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards();

    /**
     * Returns list of shards the given search would be executed on.
     */
    ClusterSearchShardsRequestBuilder prepareSearchShards(String... indices);

    /**
     * Registers a snapshot repository.
     */
    ActionFuture<AcknowledgedResponse> putRepository(PutRepositoryRequest request);

    /**
     * Registers a snapshot repository.
     */
    void putRepository(PutRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Registers a snapshot repository.
     */
    PutRepositoryRequestBuilder preparePutRepository(String name);

    /**
     * Unregisters a repository.
     */
    ActionFuture<AcknowledgedResponse> deleteRepository(DeleteRepositoryRequest request);

    /**
     * Unregisters a repository.
     */
    void deleteRepository(DeleteRepositoryRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Unregisters a repository.
     */
    DeleteRepositoryRequestBuilder prepareDeleteRepository(String name);

    /**
     * Gets repositories.
     */
    ActionFuture<GetRepositoriesResponse> getRepositories(GetRepositoriesRequest request);

    /**
     * Gets repositories.
     */
    void getRepositories(GetRepositoriesRequest request, ActionListener<GetRepositoriesResponse> listener);

    /**
     * Gets repositories.
     */
    GetRepositoriesRequestBuilder prepareGetRepositories(String... name);

    /**
     * Cleans up repository.
     */
    CleanupRepositoryRequestBuilder prepareCleanupRepository(String repository);

    /**
     * Cleans up repository.
     */
    ActionFuture<CleanupRepositoryResponse> cleanupRepository(CleanupRepositoryRequest repository);

    /**
     * Cleans up repository.
     */
    void cleanupRepository(CleanupRepositoryRequest repository, ActionListener<CleanupRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    ActionFuture<VerifyRepositoryResponse> verifyRepository(VerifyRepositoryRequest request);

    /**
     * Verifies a repository.
     */
    void verifyRepository(VerifyRepositoryRequest request, ActionListener<VerifyRepositoryResponse> listener);

    /**
     * Verifies a repository.
     */
    VerifyRepositoryRequestBuilder prepareVerifyRepository(String name);

    /**
     * Creates a new snapshot.
     */
    ActionFuture<CreateSnapshotResponse> createSnapshot(CreateSnapshotRequest request);

    /**
     * Creates a new snapshot.
     */
    void createSnapshot(CreateSnapshotRequest request, ActionListener<CreateSnapshotResponse> listener);

    /**
     * Creates a new snapshot.
     */
    CreateSnapshotRequestBuilder prepareCreateSnapshot(String repository, String name);

    /**
     * Clones a snapshot.
     */
    CloneSnapshotRequestBuilder prepareCloneSnapshot(String repository, String source, String target);

    /**
     * Clones a snapshot.
     */
    ActionFuture<AcknowledgedResponse> cloneSnapshot(CloneSnapshotRequest request);

    /**
     * Clones a snapshot.
     */
    void cloneSnapshot(CloneSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Get snapshots.
     */
    ActionFuture<GetSnapshotsResponse> getSnapshots(GetSnapshotsRequest request);

    /**
     * Get snapshot.
     */
    void getSnapshots(GetSnapshotsRequest request, ActionListener<GetSnapshotsResponse> listener);

    /**
     * Get snapshot.
     */
    GetSnapshotsRequestBuilder prepareGetSnapshots(String repository);

    /**
     * Delete snapshot.
     */
    ActionFuture<AcknowledgedResponse> deleteSnapshot(DeleteSnapshotRequest request);

    /**
     * Delete snapshot.
     */
    void deleteSnapshot(DeleteSnapshotRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete snapshot.
     */
    DeleteSnapshotRequestBuilder prepareDeleteSnapshot(String repository, String... snapshot);

    /**
     * Restores a snapshot.
     */
    ActionFuture<RestoreSnapshotResponse> restoreSnapshot(RestoreSnapshotRequest request);

    /**
     * Restores a snapshot.
     */
    void restoreSnapshot(RestoreSnapshotRequest request, ActionListener<RestoreSnapshotResponse> listener);

    /**
     * Restores from remote store.
     */
    void restoreRemoteStore(RestoreRemoteStoreRequest request, ActionListener<RestoreRemoteStoreResponse> listener);

    /**
     * Restores a snapshot.
     */
    RestoreSnapshotRequestBuilder prepareRestoreSnapshot(String repository, String snapshot);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    void pendingClusterTasks(PendingClusterTasksRequest request, ActionListener<PendingClusterTasksResponse> listener);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    ActionFuture<PendingClusterTasksResponse> pendingClusterTasks(PendingClusterTasksRequest request);

    /**
     * Returns a list of the pending cluster tasks, that are scheduled to be executed. This includes operations
     * that update the cluster state (for example, a create index operation)
     */
    PendingClusterTasksRequestBuilder preparePendingClusterTasks();

    /**
     * Get snapshot status.
     */
    ActionFuture<SnapshotsStatusResponse> snapshotsStatus(SnapshotsStatusRequest request);

    /**
     * Get snapshot status.
     */
    void snapshotsStatus(SnapshotsStatusRequest request, ActionListener<SnapshotsStatusResponse> listener);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus(String repository);

    /**
     * Get snapshot status.
     */
    SnapshotsStatusRequestBuilder prepareSnapshotStatus();

    /**
     * Stores an ingest pipeline
     */
    void putPipeline(PutPipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Stores an ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> putPipeline(PutPipelineRequest request);

    /**
     * Stores an ingest pipeline
     */
    PutPipelineRequestBuilder preparePutPipeline(String id, BytesReference source, XContentType xContentType);

    /**
     * Deletes a stored ingest pipeline
     */
    void deletePipeline(DeletePipelineRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Deletes a stored ingest pipeline
     */
    ActionFuture<AcknowledgedResponse> deletePipeline(DeletePipelineRequest request);

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline();

    /**
     * Deletes a stored ingest pipeline
     */
    DeletePipelineRequestBuilder prepareDeletePipeline(String id);

    /**
     * Returns a stored ingest pipeline
     */
    void getPipeline(GetPipelineRequest request, ActionListener<GetPipelineResponse> listener);

    /**
     * Returns a stored ingest pipeline
     */
    ActionFuture<GetPipelineResponse> getPipeline(GetPipelineRequest request);

    /**
     * Returns a stored ingest pipeline
     */
    GetPipelineRequestBuilder prepareGetPipeline(String... ids);

    /**
     * Simulates an ingest pipeline
     */
    void simulatePipeline(SimulatePipelineRequest request, ActionListener<SimulatePipelineResponse> listener);

    /**
     * Simulates an ingest pipeline
     */
    ActionFuture<SimulatePipelineResponse> simulatePipeline(SimulatePipelineRequest request);

    /**
     * Simulates an ingest pipeline
     */
    SimulatePipelineRequestBuilder prepareSimulatePipeline(BytesReference source, XContentType xContentType);

    /**
     * Explain the allocation of a shard
     */
    void allocationExplain(ClusterAllocationExplainRequest request, ActionListener<ClusterAllocationExplainResponse> listener);

    /**
     * Explain the allocation of a shard
     */
    ActionFuture<ClusterAllocationExplainResponse> allocationExplain(ClusterAllocationExplainRequest request);

    /**
     * Explain the allocation of a shard
     */
    ClusterAllocationExplainRequestBuilder prepareAllocationExplain();

    /**
     * Store a script in the cluster state
     */
    PutStoredScriptRequestBuilder preparePutStoredScript();

    /**
     * Delete a script from the cluster state
     */
    void deleteStoredScript(DeleteStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete a script from the cluster state
     */
    ActionFuture<AcknowledgedResponse> deleteStoredScript(DeleteStoredScriptRequest request);

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript();

    /**
     * Delete a script from the cluster state
     */
    DeleteStoredScriptRequestBuilder prepareDeleteStoredScript(String id);

    /**
     * Store a script in the cluster state
     */
    void putStoredScript(PutStoredScriptRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Store a script in the cluster state
     */
    ActionFuture<AcknowledgedResponse> putStoredScript(PutStoredScriptRequest request);

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript();

    /**
     * Get a script from the cluster state
     */
    GetStoredScriptRequestBuilder prepareGetStoredScript(String id);

    /**
     * Get a script from the cluster state
     */
    void getStoredScript(GetStoredScriptRequest request, ActionListener<GetStoredScriptResponse> listener);

    /**
     * Get a script from the cluster state
     */
    ActionFuture<GetStoredScriptResponse> getStoredScript(GetStoredScriptRequest request);

    /**
     * List dangling indices on all nodes.
     */
    void listDanglingIndices(ListDanglingIndicesRequest request, ActionListener<ListDanglingIndicesResponse> listener);

    /**
     * List dangling indices on all nodes.
     */
    ActionFuture<ListDanglingIndicesResponse> listDanglingIndices(ListDanglingIndicesRequest request);

    /**
     * Restore specified dangling indices.
     */
    void importDanglingIndex(ImportDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Restore specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> importDanglingIndex(ImportDanglingIndexRequest request);

    /**
     * Delete specified dangling indices.
     */
    void deleteDanglingIndex(DeleteDanglingIndexRequest request, ActionListener<AcknowledgedResponse> listener);

    /**
     * Delete specified dangling indices.
     */
    ActionFuture<AcknowledgedResponse> deleteDanglingIndex(DeleteDanglingIndexRequest request);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterPutWeightedRoutingResponse> putWeightedRouting(ClusterPutWeightedRoutingRequest request);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    void putWeightedRouting(ClusterPutWeightedRoutingRequest request, ActionListener<ClusterPutWeightedRoutingResponse> listener);

    /**
     * Updates weights for weighted round-robin search routing policy.
     */
    ClusterPutWeightedRoutingRequestBuilder prepareWeightedRouting();

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterGetWeightedRoutingResponse> getWeightedRouting(ClusterGetWeightedRoutingRequest request);

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    void getWeightedRouting(ClusterGetWeightedRoutingRequest request, ActionListener<ClusterGetWeightedRoutingResponse> listener);

    /**
     * Gets weights for weighted round-robin search routing policy.
     */
    ClusterGetWeightedRoutingRequestBuilder prepareGetWeightedRouting();

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    ActionFuture<ClusterDeleteWeightedRoutingResponse> deleteWeightedRouting(ClusterDeleteWeightedRoutingRequest request);

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    void deleteWeightedRouting(ClusterDeleteWeightedRoutingRequest request, ActionListener<ClusterDeleteWeightedRoutingResponse> listener);

    /**
     * Deletes weights for weighted round-robin search routing policy.
     */
    ClusterDeleteWeightedRoutingRequestBuilder prepareDeleteWeightedRouting();

    /**
     * Decommission awareness attribute
     */
    ActionFuture<DecommissionResponse> decommission(DecommissionRequest request);

    /**
     * Decommission awareness attribute
     */
    void decommission(DecommissionRequest request, ActionListener<DecommissionResponse> listener);

    /**
     * Decommission awareness attribute
     */
    DecommissionRequestBuilder prepareDecommission(DecommissionRequest request);

    /**
     * Get Decommissioned attribute
     */
    ActionFuture<GetDecommissionStateResponse> getDecommissionState(GetDecommissionStateRequest request);

    /**
     * Get Decommissioned attribute
     */
    void getDecommissionState(GetDecommissionStateRequest request, ActionListener<GetDecommissionStateResponse> listener);

    /**
     * Get Decommissioned attribute
     */
    GetDecommissionStateRequestBuilder prepareGetDecommissionState();

    /**
     * Deletes the decommission metadata.
     */
    ActionFuture<DeleteDecommissionStateResponse> deleteDecommissionState(DeleteDecommissionStateRequest request);

    /**
     * Deletes the decommission metadata.
     */
    void deleteDecommissionState(DeleteDecommissionStateRequest request, ActionListener<DeleteDecommissionStateResponse> listener);

    /**
     * Deletes the decommission metadata.
     */
    DeleteDecommissionStateRequestBuilder prepareDeleteDecommissionRequest();
}
