/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.nodes.TransportNodesAction;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
<<<<<<< HEAD
<<<<<<< HEAD
import org.opensearch.threadpool.ThreadPool;
=======
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequest;
>>>>>>> b5cbfa4de9e (changelog)
=======
import org.opensearch.threadpool.ThreadPool;
>>>>>>> 3a7ac33beb6 (modify based on comments)
import org.opensearch.transport.TransportService;
import org.opensearch.wlm.QueryGroupService;
import org.opensearch.wlm.stats.QueryGroupStats;

import java.io.IOException;
import java.util.List;

/**
 * Transport action for obtaining QueryGroupStats
 *
 * @opensearch.experimental
 */
<<<<<<< HEAD:server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportQueryGroupStatsAction.java
public class TransportQueryGroupStatsAction extends TransportNodesAction<
    QueryGroupStatsRequest,
    QueryGroupStatsResponse,
<<<<<<< HEAD
<<<<<<< HEAD
    QueryGroupStatsRequest,
    QueryGroupStats> {
=======
public class TransportWlmStatsAction extends TransportNodesAction<WlmStatsRequest, WlmStatsResponse, WlmStatsRequest, QueryGroupStats> {
>>>>>>> bb4288b3eba (modify based on comments):server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportWlmStatsAction.java

    final QueryGroupService queryGroupService;
=======
    TransportQueryGroupStatsAction.NodeQueryGroupStatsRequest,
=======
    QueryGroupStatsRequest,
>>>>>>> 3a7ac33beb6 (modify based on comments)
    QueryGroupStats> {

<<<<<<< HEAD
    QueryGroupService queryGroupService;
>>>>>>> b5cbfa4de9e (changelog)
=======
    final QueryGroupService queryGroupService;
>>>>>>> fb30e9af3d4 (revise)

    @Inject
    public TransportWlmStatsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        QueryGroupService queryGroupService,
        ActionFilters actionFilters
    ) {
        super(
            WlmStatsAction.NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
<<<<<<< HEAD:server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportQueryGroupStatsAction.java
            QueryGroupStatsRequest::new,
<<<<<<< HEAD
<<<<<<< HEAD
            QueryGroupStatsRequest::new,
=======
            NodeQueryGroupStatsRequest::new,
>>>>>>> b5cbfa4de9e (changelog)
=======
            QueryGroupStatsRequest::new,
>>>>>>> 3a7ac33beb6 (modify based on comments)
=======
            WlmStatsRequest::new,
            WlmStatsRequest::new,
>>>>>>> bb4288b3eba (modify based on comments):server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportWlmStatsAction.java
            ThreadPool.Names.MANAGEMENT,
            QueryGroupStats.class
        );
        this.queryGroupService = queryGroupService;
    }

    @Override
    protected WlmStatsResponse newResponse(
        WlmStatsRequest request,
        List<QueryGroupStats> queryGroupStats,
        List<FailedNodeException> failures
    ) {
        return new WlmStatsResponse(clusterService.getClusterName(), queryGroupStats, failures);
    }

    @Override
<<<<<<< HEAD:server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportQueryGroupStatsAction.java
<<<<<<< HEAD
<<<<<<< HEAD
    protected QueryGroupStatsRequest newNodeRequest(QueryGroupStatsRequest request) {
=======
    protected WlmStatsRequest newNodeRequest(WlmStatsRequest request) {
>>>>>>> bb4288b3eba (modify based on comments):server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportWlmStatsAction.java
        return request;
=======
    protected NodeQueryGroupStatsRequest newNodeRequest(QueryGroupStatsRequest request) {
        return new NodeQueryGroupStatsRequest(request);
>>>>>>> b5cbfa4de9e (changelog)
=======
    protected QueryGroupStatsRequest newNodeRequest(QueryGroupStatsRequest request) {
        return request;
>>>>>>> 3a7ac33beb6 (modify based on comments)
    }

    @Override
    protected QueryGroupStats newNodeResponse(StreamInput in) throws IOException {
        return new QueryGroupStats(in);
    }

    @Override
<<<<<<< HEAD:server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportQueryGroupStatsAction.java
<<<<<<< HEAD
<<<<<<< HEAD
    protected QueryGroupStats nodeOperation(QueryGroupStatsRequest queryGroupStatsRequest) {
        return queryGroupService.nodeStats(queryGroupStatsRequest.getQueryGroupIds(), queryGroupStatsRequest.isBreach());
=======
    protected QueryGroupStats nodeOperation(NodeQueryGroupStatsRequest nodeQueryGroupStatsRequest) {
        QueryGroupStatsRequest request = nodeQueryGroupStatsRequest.request;
        return queryGroupService.nodeStats(request.getQueryGroupIds(), request.isBreach());
    }

    /**
     * Inner QueryGroupStatsRequest
     *
     * @opensearch.experimental
     */
    public static class NodeQueryGroupStatsRequest extends TransportRequest {

        protected QueryGroupStatsRequest request;

        public NodeQueryGroupStatsRequest(StreamInput in) throws IOException {
            super(in);
            request = new QueryGroupStatsRequest(in);
        }

        NodeQueryGroupStatsRequest(QueryGroupStatsRequest request) {
            this.request = request;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
<<<<<<< HEAD
>>>>>>> b5cbfa4de9e (changelog)
=======

        public DiscoveryNode[] getDiscoveryNodes() {
            return this.request.concreteNodes();
        }
>>>>>>> ffe0d7fa2cd (address comments)
=======
    protected QueryGroupStats nodeOperation(QueryGroupStatsRequest queryGroupStatsRequest) {
        return queryGroupService.nodeStats(queryGroupStatsRequest.getQueryGroupIds(), queryGroupStatsRequest.isBreach());
>>>>>>> 3a7ac33beb6 (modify based on comments)
=======
    protected QueryGroupStats nodeOperation(WlmStatsRequest wlmStatsRequest) {
        return queryGroupService.nodeStats(wlmStatsRequest.getQueryGroupIds(), wlmStatsRequest.isBreach());
>>>>>>> bb4288b3eba (modify based on comments):server/src/main/java/org/opensearch/action/admin/cluster/wlm/TransportWlmStatsAction.java
    }
}
