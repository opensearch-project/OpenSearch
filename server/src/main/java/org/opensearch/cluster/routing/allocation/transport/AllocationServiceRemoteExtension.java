/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.allocation.transport;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.MetadataCreateIndexService;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.StaleShard;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.extensions.ExtensionsManager;
import org.opensearch.extensions.action.ExtensionActionRequest;
import org.opensearch.extensions.action.ExtensionActionResponse;
import org.opensearch.sdk.sample.helloworld.transport.SampleRequest;

public class AllocationServiceRemoteExtension implements AllocationServiceInterface {

    private ExtensionsManager extensionsManager;
    private static final Logger logger = LogManager.getLogger(AllocationServiceRemoteExtension.class);

    final byte UNIT_SEPARATOR = (byte) '\u001F';

    public AllocationServiceRemoteExtension(ExtensionsManager extensionsManager) {
        this.extensionsManager = extensionsManager;
    }

    @Override
    public ClusterState applyStartedShards(ClusterState clusterState, List<ShardRouting> startedShards) {
        return null;
    }

    @Override
    public ClusterState applyFailedShard(ClusterState clusterState, ShardRouting failedShard, boolean markAsStale) {
        return null;
    }

    @Override
    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards) {
        return null;
    }

    @Override
    public ClusterState applyFailedShards(ClusterState clusterState, List<FailedShard> failedShards, List<StaleShard> staleShards) {
        return null;
    }

    @Override
    public ClusterState disassociateDeadNodes(ClusterState clusterState, boolean reroute, String reason) {
        return null;
    }

    @Override
    public AllocationService.CommandsResult reroute(
        ClusterState clusterState, AllocationCommands commands, boolean explain, boolean retryFailed
    ) {
        return null;
    }

    @Override
    public ClusterState reroute(ClusterState clusterState, String reason) {
        logger.info("send request");
        try {
            String requestClass = RerouteActionRequest.class.getName();
            RerouteActionRequest request = new RerouteActionRequest(clusterState, reason);
            BytesStreamOutput output = new BytesStreamOutput();
            request.writeTo(output);
            byte[] response = BytesReference.toBytes(output.bytes());
            byte[] requestClassBytes = requestClass.getBytes(StandardCharsets.UTF_8);
            byte[] proxyRequestBytes = ByteBuffer.allocate(requestClassBytes.length + 1 + response.length)
                .put(requestClassBytes)
                .put(UNIT_SEPARATOR)
                .put(response)
                .array();
            ExtensionActionRequest extensionRequest = new ExtensionActionRequest(AllocationServiceAction.NAME, ByteString.copyFrom(proxyRequestBytes));
            ExtensionActionResponse extensionActionResponse = extensionsManager.handleTransportRequest(extensionRequest);
            BytesStreamInput input = new BytesStreamInput(extensionActionResponse.getResponseBytes());

            ClusterState updatedState = ClusterState.readFrom(input, null);
            logger.info("extensionActionResponse {}", input.readString());
            return updatedState;
        } catch (IOException e) {
            throw  new RuntimeException(e.getMessage(), e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
