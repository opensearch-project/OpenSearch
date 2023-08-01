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
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoRequest;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ProtobufClusterStateResponse;

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
     * Nodes stats of the cluster.
    *
    * @param request The nodes stats request
    * @return The result future
    * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
    */
    ActionFuture<ProtobufNodesStatsResponse> nodesStats(ProtobufNodesStatsRequest request);

    /**
     * Nodes stats of the cluster.
    *
    * @param request  The nodes info request
    * @param listener A listener to be notified with a result
    * @see org.opensearch.client.Requests#nodesStatsRequest(String...)
    */
    void nodesStats(ProtobufNodesStatsRequest request, ActionListener<ProtobufNodesStatsResponse> listener);
}
