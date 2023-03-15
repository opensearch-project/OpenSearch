/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.discovery;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamInput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.transport.TransportAddress;
import org.opensearch.extensions.DiscoveryExtensionNode;
import org.opensearch.extensions.ExtensionDependency;
import org.opensearch.test.OpenSearchTestCase;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;

public class InitializeExtensionRequestTests extends OpenSearchTestCase {

    public void testInitializeExtensionRequest() throws Exception {
        String expectedUniqueId = "test uniqueid";
        Version expectedVersion = Version.fromString("2.0.0");
        ExtensionDependency expectedDependency = new ExtensionDependency(expectedUniqueId, expectedVersion);
        DiscoveryExtensionNode expectedExtensionNode = new DiscoveryExtensionNode(
            "firstExtension",
            "uniqueid1",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 9300),
            new HashMap<>(),
            Version.CURRENT,
            Version.CURRENT,
            List.of(expectedDependency)
        );
        DiscoveryNode expectedSourceNode = new DiscoveryNode(
            "sourceNode",
            "uniqueid2",
            new TransportAddress(InetAddress.getByName("127.0.0.0"), 1000),
            new HashMap<>(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        InitializeExtensionRequest initializeExtensionRequest = new InitializeExtensionRequest(expectedSourceNode, expectedExtensionNode);
        assertEquals(expectedExtensionNode, initializeExtensionRequest.getExtension());
        assertEquals(expectedSourceNode, initializeExtensionRequest.getSourceNode());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            initializeExtensionRequest.writeTo(out);
            out.flush();
            try (BytesStreamInput in = new BytesStreamInput(BytesReference.toBytes(out.bytes()))) {
                initializeExtensionRequest = new InitializeExtensionRequest(in);

                assertEquals(expectedExtensionNode, initializeExtensionRequest.getExtension());
                assertEquals(expectedSourceNode, initializeExtensionRequest.getSourceNode());
            }
        }
    }
}
