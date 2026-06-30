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
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.BytesTransportRequest;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Contains tests for {@link CompressedStreamUtils}
 */
public class CompressedStreamUtilsTests extends OpenSearchTestCase {

    public void testCreateCompressedStream() throws IOException {
        // serialization success with normal state
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        BytesReference bytes = CompressedStreamUtils.createCompressedStream(localNode.getVersion(), localClusterState::writeTo);
        assertNotNull(bytes);

        // Fail on write failure on mocked cluster state's writeTo exception
        ClusterState mockedState = mock(ClusterState.class);
        doThrow(IOException.class).when(mockedState).writeTo(any());
        assertThrows(IOException.class, () -> CompressedStreamUtils.createCompressedStream(localNode.getVersion(), mockedState::writeTo));
    }

    public void testDecompressBytes() throws IOException {
        // Decompression works fine
        final ClusterState localClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().generateClusterUuidIfNeeded().clusterUUIDCommitted(true))
            .build();
        DiscoveryNode localNode = new DiscoveryNode("node0", buildNewFakeTransportAddress(), Version.CURRENT);
        BytesReference bytes = CompressedStreamUtils.createCompressedStream(localNode.getVersion(), localClusterState::writeTo);
        BytesTransportRequest request = new BytesTransportRequest(bytes, localNode.getVersion());
        StreamInput input = CompressedStreamUtils.decompressBytes(request, DEFAULT_NAMED_WRITABLE_REGISTRY);
        assertEquals(request.version(), input.getVersion());

        // Decompression fails with AssertionError on non-compressed request
        BytesTransportRequest mockedRequest = mock(BytesTransportRequest.class, RETURNS_DEEP_STUBS);
        when(mockedRequest.bytes().streamInput()).thenThrow(IOException.class);
        assertThrows(AssertionError.class, () -> CompressedStreamUtils.decompressBytes(mockedRequest, DEFAULT_NAMED_WRITABLE_REGISTRY));
    }
}
