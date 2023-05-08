/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * A helper class to utilize the compressed stream.
 */
public class CompressionHelper {
    private static final Logger logger = LogManager.getLogger(CompressionHelper.class);

    /**
     * Serialize the given cluster state or diff. It'll always use compression before writing on a newly created output
     * stream.
     *
     * @param writer             Object which is going to write the content
     * @param node               version of cluster node
     * @param isFullClusterState flag used at receiver end to make intelligent decisions. For example, ClusterState
     *                           assumes full state of diff of the states based on this flag.
     * @return reference to serialized bytes
     * @throws IOException if writing on the compressed stream is failed.
     */
    public static BytesReference serializeClusterState(Writeable writer, DiscoveryNode node, boolean isFullClusterState)
        throws IOException {
        try {
            final BytesStreamOutput bStream = new BytesStreamOutput();
            try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
                stream.setVersion(node.getVersion());
                stream.writeBoolean(isFullClusterState);
                writer.writeTo(stream);
            }
            final BytesReference serializedByteRef = bStream.bytes();
            logger.trace("serialized writable object for node version [{}] with size [{}]", node.getVersion(), serializedByteRef.length());
            return serializedByteRef;
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("failed to serialize cluster state during validateJoin" + " {}", node), e);
            throw e;
        }
    }

    public static StreamInput decompressClusterState(BytesTransportRequest request, NamedWriteableRegistry namedWriteableRegistry)
        throws IOException {
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        StreamInput in = request.bytes().streamInput();
        if (compressor != null) {
            in = new InputStreamStreamInput(compressor.threadLocalInputStream(in));
        }
        in = new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
        in.setVersion(request.version());
        return in;
    }

    public static ClusterState deserializeFullClusterState(StreamInput in, TransportService transportService) throws IOException {
        final ClusterState incomingState;
        try (StreamInput input = in) {
            incomingState = ClusterState.readFrom(input, transportService.getLocalNode());
        } catch (Exception e) {
            logger.warn("unexpected error while deserializing an incoming cluster state", e);
            throw e;
        }
        return incomingState;
    }

    public static Diff<ClusterState> deserializeClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
        final Diff<ClusterState> diff;
        // Close stream early to release resources used by the de-compression as early as possible
        try (StreamInput input = in) {
            diff = ClusterState.readDiffFrom(input, localNode);
        } catch (Exception e) {
            logger.warn("unexpected error while deserializing an incoming cluster state diff", e);
            throw e;
        }
        return diff;
    }
}
