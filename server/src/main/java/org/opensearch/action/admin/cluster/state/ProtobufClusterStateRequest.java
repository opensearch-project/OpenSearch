/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.ProtobufClusterManagerNodeReadRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.server.proto.ClusterStateRequestProto;
import org.opensearch.server.proto.ClusterStateRequestProto.ClusterStateRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Transport request for obtaining cluster state
*
* @opensearch.internal
*/
public class ProtobufClusterStateRequest extends ProtobufClusterManagerNodeReadRequest<ProtobufClusterStateRequest>
    implements
        IndicesRequest.Replaceable {

    public static final TimeValue DEFAULT_WAIT_FOR_NODE_TIMEOUT = TimeValue.timeValueMinutes(1);
    private ClusterStateRequestProto.ClusterStateRequest clusterStateRequest;

    public ProtobufClusterStateRequest() {}

    public ProtobufClusterStateRequest(
        boolean routingTable,
        boolean nodes,
        boolean metadata,
        boolean blocks,
        boolean customs,
        long waitForMetadataVersion,
        TimeValue waitForTimeout,
        List<String> indices
    ) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setRoutingTable(routingTable)
            .setNodes(nodes)
            .setMetadata(metadata)
            .setBlocks(blocks)
            .setCustoms(customs)
            .setWaitForMetadataVersion(waitForMetadataVersion)
            .setWaitForTimeout(waitForTimeout.toString())
            .addAllIndices(indices)
            .build();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    public ProtobufClusterStateRequest all() {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setRoutingTable(true)
            .setNodes(true)
            .setMetadata(true)
            .setBlocks(true)
            .setCustoms(true)
            .addAllIndices(Arrays.asList(Strings.EMPTY_ARRAY))
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public ProtobufClusterStateRequest clear() {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setRoutingTable(false)
            .setNodes(false)
            .setMetadata(false)
            .setBlocks(false)
            .setCustoms(false)
            .addAllIndices(Arrays.asList(Strings.EMPTY_ARRAY))
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public boolean routingTable() {
        return this.clusterStateRequest.getRoutingTable();
    }

    public ProtobufClusterStateRequest routingTable(boolean routingTable) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setRoutingTable(routingTable)
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public boolean nodes() {
        return this.clusterStateRequest.getNodes();
    }

    public ProtobufClusterStateRequest nodes(boolean nodes) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setNodes(nodes)
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public boolean metadata() {
        return this.clusterStateRequest.getMetadata();
    }

    public ProtobufClusterStateRequest metadata(boolean metadata) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setMetadata(metadata)
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public boolean blocks() {
        return this.clusterStateRequest.getBlocks();
    }

    public ProtobufClusterStateRequest blocks(boolean blocks) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .setBlocks(blocks)
            .build();
        return this;
    }

    @Override
    public String[] indices() {
        return this.clusterStateRequest.getIndicesList().toArray(new String[0]);
    }

    @Override
    public ProtobufClusterStateRequest indices(String... indices) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .addAllIndices(Arrays.asList(indices))
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    @Override
    public boolean includeDataStreams() {
        return true;
    }

    public ProtobufClusterStateRequest customs(boolean customs) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setCustoms(customs)
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public boolean customs() {
        return this.clusterStateRequest.getCustoms();
    }

    public TimeValue waitForTimeout() {
        return TimeValue.parseTimeValue(
            this.clusterStateRequest.getWaitForTimeout(),
            getClass().getSimpleName() + ".clusterManagerNodeTimeout"
        );
    }

    public ProtobufClusterStateRequest waitForTimeout(TimeValue waitForTimeout) {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setWaitForTimeout(waitForTimeout.toString())
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    public Long waitForMetadataVersion() {
        return this.clusterStateRequest.getWaitForMetadataVersion();
    }

    public ProtobufClusterStateRequest waitForMetadataVersion(long waitForMetadataVersion) {
        if (waitForMetadataVersion < 1) {
            throw new IllegalArgumentException(
                "provided waitForMetadataVersion should be >= 1, but instead is [" + waitForMetadataVersion + "]"
            );
        }
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.newBuilder()
            .setWaitForMetadataVersion(waitForMetadataVersion)
            .setWaitForTimeout(DEFAULT_WAIT_FOR_NODE_TIMEOUT.toString())
            .build();
        return this;
    }

    @Override
    public IndicesOptions indicesOptions() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'indicesOptions'");
    }

    public ProtobufClusterStateRequest(byte[] data) throws IOException {
        this.clusterStateRequest = ClusterStateRequestProto.ClusterStateRequest.parseFrom(data);
    }

    public ProtobufClusterStateRequest(ClusterStateRequestProto.ClusterStateRequest clusterStateRequest) {
        this.clusterStateRequest = clusterStateRequest;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.clusterStateRequest.toByteArray());
    }

    public ClusterStateRequest request() {
        return this.clusterStateRequest;
    }
}
