/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
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
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoResponse;
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
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequestBuilder;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;
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
import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineRequest;
import org.opensearch.action.search.GetSearchPipelineResponse;
import org.opensearch.action.search.PutSearchPipelineRequest;
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
public interface ProtobufClusterAdminClient extends ProtobufOpenSearchClient {

    /**
     * The state of the cluster.
    *
    * @param request The cluster state request.
    * @return The result future
    * @see Requests#clusterStateRequest()
    */
    ActionFuture<ProtobufClusterStateResponse> state(ProtobufClusterStateRequest request);

    /**
     * The state of the cluster.
    *
    * @param request  The cluster state request.
    * @param listener A listener to be notified with a result
    * @see Requests#clusterStateRequest()
    */
    void state(ProtobufClusterStateRequest request, ActionListener<ProtobufClusterStateResponse> listener);

    /**
     * The state of the cluster.
    */
    ProtobufClusterStateRequestBuilder prepareState();

    /**
     * Nodes info of the cluster.
    *
    * @param request The nodes info request
    * @return The result future
    * @see org.opensearch.client.Requests#nodesInfoRequest(String...)
    */
    ActionFuture<ProtobufNodesInfoResponse> nodesInfo(ProtobufNodesInfoRequest request);

    /**
     * Nodes info of the cluster.
    *
    * @param request  The nodes info request
    * @param listener A listener to be notified with a result
    * @see org.opensearch.client.Requests#nodesInfoRequest(String...)
    */
    void nodesInfo(ProtobufNodesInfoRequest request, ActionListener<ProtobufNodesInfoResponse> listener);

    /**
     * Nodes info of the cluster.
    */
    NodesInfoRequestBuilder prepareNodesInfo(String... nodesIds);

    // /**
    //  * Nodes stats of the cluster.
    // *
    // * @param request The nodes stats request
    // * @return The result future
    // * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
    // */
    // ActionFuture<NodesStatsResponse> nodesStats(NodesStatsRequest request);

    // /**
    //  * Nodes stats of the cluster.
    // *
    // * @param request  The nodes info request
    // * @param listener A listener to be notified with a result
    // * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
    // */
    // void nodesStats(NodesStatsRequest request, ActionListener<NodesStatsResponse> listener);

    // /**
    //  * Nodes stats of the cluster.
    // */
    // NodesStatsRequestBuilder prepareNodesStats(String... nodesIds);
}
