/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.Compressor;
import org.opensearch.common.compress.CompressorFactory;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.transport.BytesTransportRequest;

import java.io.IOException;

/**
 * A helper class to utilize the compressed stream.
 *
 * @opensearch.internal
 */
public final class ClusterStateUtils {
    private static final Logger logger = LogManager.getLogger(ClusterStateUtils.class);

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
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
            stream.setVersion(node.getVersion());
            stream.writeBoolean(isFullClusterState);
            writer.writeTo(stream);
        }
        final BytesReference serializedByteRef = bStream.bytes();
        logger.trace("serialized writable object for node version [{}] with size [{}]", node.getVersion(), serializedByteRef.length());
        return serializedByteRef;
    }

    /**
     * Decompress the incoming compressed BytesTransportRequest into StreamInput which can be deserialized.
     * @param request incoming compressed request in bytes form
     * @param namedWriteableRegistry existing registry of readers which contains ClusterState writable
     * @return StreamInput object containing uncompressed request sent by sender
     * @throws IOException if creating StreamInput object fails due to EOF
     */
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

    public static ClusterState deserializeFullClusterState(StreamInput in, DiscoveryNode localNode) throws IOException {
        final ClusterState incomingState;
        try (StreamInput input = in) {
            incomingState = ClusterState.readFrom(input, localNode);
        }
        return incomingState;
    }

    public static Diff<ClusterState> deserializeClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
        final Diff<ClusterState> incomingStateDiff;
        // Close stream early to release resources used by the de-compression as early as possible
        try (StreamInput input = in) {
            incomingStateDiff = ClusterState.readDiffFrom(input, localNode);
        }
        return incomingStateDiff;
    }
}
