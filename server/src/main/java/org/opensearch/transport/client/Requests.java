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

package org.opensearch.transport.client;

import org.opensearch.action.admin.cluster.decommission.awareness.delete.DeleteDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.get.GetDecommissionStateRequest;
import org.opensearch.action.admin.cluster.decommission.awareness.put.DecommissionRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.node.info.NodesInfoRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.opensearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.opensearch.action.admin.cluster.node.usage.NodesUsageRequest;
import org.opensearch.action.admin.cluster.repositories.cleanup.CleanupRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.delete.DeleteRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.get.GetRepositoriesRequest;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.action.admin.cluster.repositories.verify.VerifyRepositoryRequest;
import org.opensearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.delete.ClusterDeleteWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.get.ClusterGetWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.shards.routing.weighted.put.ClusterPutWeightedRoutingRequest;
import org.opensearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.delete.DeleteSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.opensearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.opensearch.action.admin.cluster.snapshots.status.SnapshotsStatusRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.stats.ClusterStatsRequest;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.opensearch.action.admin.indices.close.CloseIndexRequest;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.opensearch.action.admin.indices.flush.FlushRequest;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.admin.indices.open.OpenIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.segments.IndicesSegmentsRequest;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateRequest;
import org.opensearch.action.admin.indices.upgrade.post.UpgradeRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;

/**
 * A handy one stop shop for creating requests (make sure to import static this class).
 *
 * @opensearch.internal
 */
public class Requests {

    /**
     * The content type used to generate request builders (query / search).
     */
    public static XContentType CONTENT_TYPE = XContentType.SMILE;

    /**
     * The default content type to use to generate source documents when indexing.
     */
    public static MediaType INDEX_CONTENT_TYPE = MediaTypeRegistry.JSON;

    public static IndexRequest indexRequest() {
        return new IndexRequest();
    }

    /**
     * Create an index request against a specific index.
     * Note that setting {@link IndexRequest#id(String)} is optional.
     *
     * @param index The index name to index the request against
     * @return The index request
     * @see Client#index(org.opensearch.action.index.IndexRequest)
     */
    public static IndexRequest indexRequest(String index) {
        return new IndexRequest(index);
    }

    /**
     * Creates a delete request against a specific index.
     * Note that  {@link DeleteRequest#id(String)} must be set.
     *
     * @param index The index name to delete from
     * @return The delete request
     * @see Client#delete(org.opensearch.action.delete.DeleteRequest)
     */
    public static DeleteRequest deleteRequest(String index) {
        return new DeleteRequest(index);
    }

    /**
     * Creates a new bulk request.
     */
    public static BulkRequest bulkRequest() {
        return new BulkRequest();
    }

    /**
     * Creates a get request to get the JSON source from an index based on a type and id. Note, the
     * {@link GetRequest#id(String)} must be set.
     *
     * @param index The index to get the JSON source from
     * @return The get request
     * @see Client#get(org.opensearch.action.get.GetRequest)
     */
    public static GetRequest getRequest(String index) {
        return new GetRequest(index);
    }

    /**
     * Creates a search request against one or more indices. Note, the search source must be set either using the
     * actual JSON search source, or the {@link org.opensearch.search.builder.SearchSourceBuilder}.
     *
     * @param indices The indices to search against. Use {@code null} or {@code _all} to execute against all indices
     * @return The search request
     * @see Client#search(org.opensearch.action.search.SearchRequest)
     */
    public static SearchRequest searchRequest(String... indices) {
        return new SearchRequest(indices);
    }

    /**
     * Creates a search scroll request allowing to continue searching a previous search request.
     *
     * @param scrollId The scroll id representing the scrollable search
     * @return The search scroll request
     * @see Client#searchScroll(org.opensearch.action.search.SearchScrollRequest)
     */
    public static SearchScrollRequest searchScrollRequest(String scrollId) {
        return new SearchScrollRequest(scrollId);
    }

    public static IndicesSegmentsRequest indicesSegmentsRequest(String... indices) {
        return new IndicesSegmentsRequest(indices);
    }

    /**
     * Creates an indices shard stores info request.
     * @param indices The indices to get shard store information on
     * @return The indices shard stores request
     * @see IndicesAdminClient#shardStores(IndicesShardStoresRequest)
     */
    public static IndicesShardStoresRequest indicesShardStoresRequest(String... indices) {
        return new IndicesShardStoresRequest(indices);
    }

    /**
     * Creates an indices exists request.
     *
     * @param indices The indices to check if they exists or not.
     * @return The indices exists request
     * @see IndicesAdminClient#exists(IndicesExistsRequest)
     */
    public static IndicesExistsRequest indicesExistsRequest(String... indices) {
        return new IndicesExistsRequest(indices);
    }

    /**
     * Creates a create index request.
     *
     * @param index The index to create
     * @return The index create request
     * @see IndicesAdminClient#create(CreateIndexRequest)
     */
    public static CreateIndexRequest createIndexRequest(String index) {
        return new CreateIndexRequest(index);
    }

    /**
     * Creates a delete index request.
     *
     * @param index The index to delete
     * @return The delete index request
     * @see IndicesAdminClient#delete(DeleteIndexRequest)
     */
    public static DeleteIndexRequest deleteIndexRequest(String index) {
        return new DeleteIndexRequest(index);
    }

    /**
     * Creates a close index request.
     *
     * @param index The index to close
     * @return The delete index request
     * @see IndicesAdminClient#close(CloseIndexRequest)
     */
    public static CloseIndexRequest closeIndexRequest(String index) {
        return new CloseIndexRequest(index);
    }

    /**
     * Creates an open index request.
     *
     * @param index The index to open
     * @return The delete index request
     * @see IndicesAdminClient#open(OpenIndexRequest)
     */
    public static OpenIndexRequest openIndexRequest(String index) {
        return new OpenIndexRequest(index);
    }

    /**
     * Create a create mapping request against one or more indices.
     *
     * @param indices The indices to create mapping. Use {@code null} or {@code _all} to execute against all indices
     * @return The create mapping request
     * @see IndicesAdminClient#putMapping(PutMappingRequest)
     */
    public static PutMappingRequest putMappingRequest(String... indices) {
        return new PutMappingRequest(indices);
    }

    /**
     * Creates an index aliases request allowing to add and remove aliases.
     *
     * @return The index aliases request
     */
    public static IndicesAliasesRequest indexAliasesRequest() {
        return new IndicesAliasesRequest();
    }

    /**
     * Creates a refresh indices request.
     *
     * @param indices The indices to refresh. Use {@code null} or {@code _all} to execute against all indices
     * @return The refresh request
     * @see IndicesAdminClient#refresh(RefreshRequest)
     */
    public static RefreshRequest refreshRequest(String... indices) {
        return new RefreshRequest(indices);
    }

    /**
     * Creates a flush indices request.
     *
     * @param indices The indices to flush. Use {@code null} or {@code _all} to execute against all indices
     * @return The flush request
     * @see IndicesAdminClient#flush(FlushRequest)
     */
    public static FlushRequest flushRequest(String... indices) {
        return new FlushRequest(indices);
    }

    /**
     * Creates a force merge request.
     *
     * @param indices The indices to force merge. Use {@code null} or {@code _all} to execute against all indices
     * @return The force merge request
     * @see IndicesAdminClient#forceMerge(ForceMergeRequest)
     */
    public static ForceMergeRequest forceMergeRequest(String... indices) {
        return new ForceMergeRequest(indices);
    }

    /**
     * Creates an upgrade request.
     *
     * @param indices The indices to upgrade. Use {@code null} or {@code _all} to execute against all indices
     * @return The upgrade request
     * @see IndicesAdminClient#upgrade(UpgradeRequest)
     */
    public static UpgradeRequest upgradeRequest(String... indices) {
        return new UpgradeRequest(indices);
    }

    /**
     * Creates a clean indices cache request.
     *
     * @param indices The indices to clean their caches. Use {@code null} or {@code _all} to execute against all indices
     * @return The request
     */
    public static ClearIndicesCacheRequest clearIndicesCacheRequest(String... indices) {
        return new ClearIndicesCacheRequest(indices);
    }

    /**
     * A request to update indices settings.
     *
     * @param indices The indices to update the settings for. Use {@code null} or {@code _all} to executed against all indices.
     * @return The request
     */
    public static UpdateSettingsRequest updateSettingsRequest(String... indices) {
        return new UpdateSettingsRequest(indices);
    }

    /**
     * Creates a cluster state request.
     *
     * @return The cluster state request.
     * @see ClusterAdminClient#state(ClusterStateRequest)
     */
    public static ClusterStateRequest clusterStateRequest() {
        return new ClusterStateRequest();
    }

    public static ClusterRerouteRequest clusterRerouteRequest() {
        return new ClusterRerouteRequest();
    }

    public static ClusterUpdateSettingsRequest clusterUpdateSettingsRequest() {
        return new ClusterUpdateSettingsRequest();
    }

    /**
     * Creates a cluster health request.
     *
     * @param indices The indices to provide additional cluster health information for.
     *                Use {@code null} or {@code _all} to execute against all indices
     * @return The cluster health request
     * @see ClusterAdminClient#health(ClusterHealthRequest)
     */
    public static ClusterHealthRequest clusterHealthRequest(String... indices) {
        return new ClusterHealthRequest(indices);
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest() {
        return new ClusterSearchShardsRequest();
    }

    /**
     * List all shards for the give search
     */
    public static ClusterSearchShardsRequest clusterSearchShardsRequest(String... indices) {
        return new ClusterSearchShardsRequest(indices);
    }

    /**
     * Creates a nodes info request against all the nodes.
     *
     * @return The nodes info request
     * @see ClusterAdminClient#nodesInfo(NodesInfoRequest)
     */
    public static NodesInfoRequest nodesInfoRequest() {
        return new NodesInfoRequest();
    }

    /**
     * Creates a nodes info request against one or more nodes. Pass {@code null} or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the status for
     * @return The nodes info request
     * @see ClusterAdminClient#nodesStats(NodesStatsRequest)
     */
    public static NodesInfoRequest nodesInfoRequest(String... nodesIds) {
        return new NodesInfoRequest(nodesIds);
    }

    /**
     * Creates a nodes stats request against one or more nodes. Pass {@code null} or an empty array for all nodes.
     *
     * @param nodesIds The nodes ids to get the stats for
     * @return The nodes info request
     * @see ClusterAdminClient#nodesStats(NodesStatsRequest)
     */
    public static NodesStatsRequest nodesStatsRequest(String... nodesIds) {
        return new NodesStatsRequest(nodesIds);
    }

    /**
     * Creates a nodes usage request against one or more nodes. Pass
     * {@code null} or an empty array for all nodes.
     *
     * @param nodesIds
     *            The nodes ids to get the usage for
     * @return The nodes usage request
     * @see ClusterAdminClient#nodesUsage(NodesUsageRequest)
     */
    public static NodesUsageRequest nodesUsageRequest(String... nodesIds) {
        return new NodesUsageRequest(nodesIds);
    }

    /**
     * Creates a cluster stats request.
     *
     * @return The cluster stats request
     * @see ClusterAdminClient#clusterStats(ClusterStatsRequest)
     */
    public static ClusterStatsRequest clusterStatsRequest() {
        return new ClusterStatsRequest();
    }

    /**
     * Creates a nodes tasks request against all the nodes.
     *
     * @return The nodes tasks request
     * @see ClusterAdminClient#listTasks(ListTasksRequest)
     */
    public static ListTasksRequest listTasksRequest() {
        return new ListTasksRequest();
    }

    /**
     * Creates a get task request.
     *
     * @return The nodes tasks request
     * @see ClusterAdminClient#getTask(GetTaskRequest)
     */
    public static GetTaskRequest getTaskRequest() {
        return new GetTaskRequest();
    }

    /**
     * Creates a nodes tasks request against one or more nodes. Pass {@code null} or an empty array for all nodes.
     *
     * @return The nodes tasks request
     * @see ClusterAdminClient#cancelTasks(CancelTasksRequest)
     */
    public static CancelTasksRequest cancelTasksRequest() {
        return new CancelTasksRequest();
    }

    /**
     * Registers snapshot repository
     *
     * @param name repository name
     * @return repository registration request
     */
    public static PutRepositoryRequest putRepositoryRequest(String name) {
        return new PutRepositoryRequest(name);
    }

    /**
     * Gets snapshot repository
     *
     * @param repositories names of repositories
     * @return get repository request
     */
    public static GetRepositoriesRequest getRepositoryRequest(String... repositories) {
        return new GetRepositoriesRequest(repositories);
    }

    /**
     * Deletes registration for snapshot repository
     *
     * @param name repository name
     * @return delete repository request
     */
    public static DeleteRepositoryRequest deleteRepositoryRequest(String name) {
        return new DeleteRepositoryRequest(name);
    }

    /**
     * Cleanup repository
     *
     * @param name repository name
     * @return cleanup repository request
     */
    public static CleanupRepositoryRequest cleanupRepositoryRequest(String name) {
        return new CleanupRepositoryRequest(name);
    }

    /**
     * Verifies snapshot repository
     *
     * @param name repository name
     * @return repository verification request
     */
    public static VerifyRepositoryRequest verifyRepositoryRequest(String name) {
        return new VerifyRepositoryRequest(name);
    }

    /**
     * Creates new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return create snapshot request
     */
    public static CreateSnapshotRequest createSnapshotRequest(String repository, String snapshot) {
        return new CreateSnapshotRequest(repository, snapshot);
    }

    /**
     * Gets snapshots from repository
     *
     * @param repository repository name
     * @return get snapshot  request
     */
    public static GetSnapshotsRequest getSnapshotsRequest(String repository) {
        return new GetSnapshotsRequest(repository);
    }

    /**
     * Restores new snapshot
     *
     * @param repository repository name
     * @param snapshot   snapshot name
     * @return snapshot creation request
     */
    public static RestoreSnapshotRequest restoreSnapshotRequest(String repository, String snapshot) {
        return new RestoreSnapshotRequest(repository, snapshot);
    }

    /**
     * Deletes snapshots
     *
     * @param snapshots  snapshot names
     * @param repository repository name
     * @return delete snapshot request
     */
    public static DeleteSnapshotRequest deleteSnapshotRequest(String repository, String... snapshots) {
        return new DeleteSnapshotRequest(repository, snapshots);
    }

    /**
     *  Get status of snapshots
     *
     * @param repository repository name
     * @return snapshot status request
     */
    public static SnapshotsStatusRequest snapshotsStatusRequest(String repository) {
        return new SnapshotsStatusRequest(repository);
    }

    /**
     * Updates weights for weighted round-robin search routing policy
     *
     * @return update weight request
     */
    public static ClusterPutWeightedRoutingRequest putWeightedRoutingRequest(String attributeName) {
        return new ClusterPutWeightedRoutingRequest(attributeName);
    }

    /**
     * Gets weights for weighted round-robin search routing policy
     *
     * @return get weight request
     */
    public static ClusterGetWeightedRoutingRequest getWeightedRoutingRequest(String attributeName) {
        return new ClusterGetWeightedRoutingRequest(attributeName);
    }

    /**
     * Deletes weights for weighted round-robin search routing policy
     *
     * @return delete weight request
     */
    public static ClusterDeleteWeightedRoutingRequest deleteWeightedRoutingRequest(String attributeName) {
        return new ClusterDeleteWeightedRoutingRequest(attributeName);
    }

    /**
     * Creates a new decommission request.
     *
     * @return returns put decommission request
     */
    public static DecommissionRequest decommissionRequest() {
        return new DecommissionRequest();
    }

    /**
     * Get decommissioned attribute from metadata
     *
     * @return returns get decommission request
     */
    public static GetDecommissionStateRequest getDecommissionStateRequest() {
        return new GetDecommissionStateRequest();
    }

    /**
     * Creates a new delete decommission request.
     */
    public static DeleteDecommissionStateRequest deleteDecommissionStateRequest() {
        return new DeleteDecommissionStateRequest();
    }

    /**
     * Creates a pause ingestion request given list of index names.
     */
    public static PauseIngestionRequest pauseIngestionRequest(String... index) {
        return new PauseIngestionRequest(index);
    }

    /**
     * Creates a resume ingestion request given list of index names.
     */
    public static ResumeIngestionRequest resumeIngestionRequest(String... index) {
        return new ResumeIngestionRequest(index);
    }

    /**
     * Creates a get ingestion state request given an index.
     */
    public static GetIngestionStateRequest getIngestionStateRequest(String index) {
        return new GetIngestionStateRequest(new String[] { index });
    }

    /**
     * Creates a get ingestion state request given list of indices.
     */
    public static GetIngestionStateRequest getIngestionStateRequest(String[] indices, int[] shards, PageParams pageParams) {
        GetIngestionStateRequest request = new GetIngestionStateRequest(indices);
        request.setShards(shards);
        request.setPageParams(pageParams);
        return request;
    }
}
