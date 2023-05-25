/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ProtobufClusterManagerNodeReadOperationRequestBuilder;
import org.opensearch.client.ProtobufOpenSearchClient;
import org.opensearch.common.unit.TimeValue;

/**
 * Transport request builder for obtaining cluster state
*
* @opensearch.internal
*/
public class ProtobufClusterStateRequestBuilder extends ProtobufClusterManagerNodeReadOperationRequestBuilder<
    ProtobufClusterStateRequest,
    ProtobufClusterStateResponse,
    ProtobufClusterStateRequestBuilder> {

    public ProtobufClusterStateRequestBuilder(ProtobufOpenSearchClient client, ProtobufClusterStateAction action) {
        super(client, action, new ProtobufClusterStateRequest());
    }

    /**
     * Include all data
    */
    public ProtobufClusterStateRequestBuilder all() {
        request.all();
        return this;
    }

    /**
     * Do not include any data
    */
    public ProtobufClusterStateRequestBuilder clear() {
        request.clear();
        return this;
    }

    public ProtobufClusterStateRequestBuilder setBlocks(boolean filter) {
        request.blocks(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.opensearch.cluster.metadata.Metadata}. Defaults
    * to {@code true}.
    */
    public ProtobufClusterStateRequestBuilder setMetadata(boolean filter) {
        request.metadata(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.opensearch.cluster.node.DiscoveryNodes}. Defaults
    * to {@code true}.
    */
    public ProtobufClusterStateRequestBuilder setNodes(boolean filter) {
        request.nodes(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.opensearch.cluster.ClusterState.Custom}. Defaults
    * to {@code true}.
    */
    public ProtobufClusterStateRequestBuilder setCustoms(boolean filter) {
        request.customs(filter);
        return this;
    }

    /**
     * Should the cluster state result include the {@link org.opensearch.cluster.routing.RoutingTable}. Defaults
    * to {@code true}.
    */
    public ProtobufClusterStateRequestBuilder setRoutingTable(boolean filter) {
        request.routingTable(filter);
        return this;
    }

    /**
     * When {@link #setMetadata(boolean)} is set, which indices to return the {@link org.opensearch.cluster.metadata.IndexMetadata}
    * for. Defaults to all indices.
    */
    public ProtobufClusterStateRequestBuilder setIndices(String... indices) {
        request.indices(indices);
        return this;
    }

    public ProtobufClusterStateRequestBuilder setIndicesOptions(IndicesOptions indicesOptions) {
        request.indicesOptions(indicesOptions);
        return this;
    }

    /**
     * Causes the request to wait for the metadata version to advance to at least the given version.
    * @param waitForMetadataVersion The metadata version for which to wait
    */
    public ProtobufClusterStateRequestBuilder setWaitForMetadataVersion(long waitForMetadataVersion) {
        request.waitForMetadataVersion(waitForMetadataVersion);
        return this;
    }

    /**
     * If {@link ProtobufClusterStateRequest#waitForMetadataVersion()} is set then this determines how long to wait
    */
    public ProtobufClusterStateRequestBuilder setWaitForTimeOut(TimeValue waitForTimeout) {
        request.waitForTimeout(waitForTimeout);
        return this;
    }
}
