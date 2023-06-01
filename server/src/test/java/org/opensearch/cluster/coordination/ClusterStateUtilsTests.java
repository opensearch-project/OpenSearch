/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.BytesTransportRequest;

import java.io.EOFException;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Contains tests for {@link ClusterStateUtils}
 */
public class ClusterStateUtilsTests extends OpenSearchTestCase {

    public void testSerializeClusterState() throws IOException {
        // serialization success with normal state
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        BytesReference bytes = ClusterStateUtils.serializeClusterState(localClusterState, localNode);
        assertNotNull(bytes);

        // Fail on write failure on mocked cluster state's writeTo exception
        ClusterState mockedState = mock(ClusterState.class);
        doThrow(IOException.class).when(mockedState).writeTo(any());
        assertThrows(IOException.class, () -> ClusterStateUtils.serializeClusterState(mockedState, localNode));
    }

    public void testDecompressClusterState() throws IOException {
        // Decompression works fine
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        BytesReference bytes = ClusterStateUtils.serializeClusterState(localClusterState, localNode);
        BytesTransportRequest request = new BytesTransportRequest(bytes, localNode.getVersion());
        StreamInput in = ClusterStateUtils.decompressClusterState(request, DEFAULT_NAMED_WRITABLE_REGISTRY);
        assertEquals(request.version(), in.getVersion());

        // Decompression fails with AssertionError on non-compressed request
        BytesTransportRequest mockedRequest = mock(BytesTransportRequest.class, RETURNS_DEEP_STUBS);
        when(mockedRequest.bytes().streamInput()).thenThrow(IOException.class);
        assertThrows(AssertionError.class, () -> ClusterStateUtils.decompressClusterState(mockedRequest, DEFAULT_NAMED_WRITABLE_REGISTRY));
    }

    public void testDeserializeFullClusterState() throws IOException {
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        BytesReference bytes = ClusterStateUtils.serializeClusterState(localClusterState, localNode);
        BytesTransportRequest request = new BytesTransportRequest(bytes, localNode.getVersion());
        StreamInput in = ClusterStateUtils.decompressClusterState(request, DEFAULT_NAMED_WRITABLE_REGISTRY);
        ClusterState decompressedState = null;
        // success when state data is correct
        if (in.readBoolean()) {
            decompressedState = ClusterStateUtils.deserializeFullClusterState(in, localNode);
        }
        assertEquals(localClusterState.getClusterName(), decompressedState.getClusterName());
        assertEquals(localClusterState.metadata().clusterUUID(), decompressedState.metadata().clusterUUID());

        // failure when mocked stream or null
        assertThrows(NullPointerException.class, () -> ClusterStateUtils.deserializeFullClusterState(null, localNode));
        StreamInput mockedStreamInput = mock(StreamInput.class, RETURNS_DEEP_STUBS);
        assertThrows(NullPointerException.class, () -> ClusterStateUtils.deserializeFullClusterState(mockedStreamInput, localNode));
    }

    public void testDeserializeDiffClusterState() throws IOException {
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        // fail with NPE if mocked stream is passed
        StreamInput mockedStreamInput = mock(StreamInput.class, RETURNS_DEEP_STUBS);
        assertThrows(NullPointerException.class, () -> ClusterStateUtils.deserializeClusterStateDiff(mockedStreamInput, localNode));

        // fail with EOF is full cluster state is passed
        BytesReference bytes = ClusterStateUtils.serializeClusterState(localClusterState, localNode);
        BytesTransportRequest request = new BytesTransportRequest(bytes, localNode.getVersion());
        try (StreamInput in = ClusterStateUtils.decompressClusterState(request, DEFAULT_NAMED_WRITABLE_REGISTRY)) {
            assertThrows(EOFException.class, () -> ClusterStateUtils.deserializeClusterStateDiff(in, localNode));
        }
    }
}
