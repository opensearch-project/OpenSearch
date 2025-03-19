/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.pagination;

import org.opensearch.core.common.transport.TransportAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashSet;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;

public class DiscoveryNodeMock {
    public static DiscoveryNode createDummyNode(int index) {
        try {
            return new DiscoveryNode(
                "node-" + index,
                new TransportAddress(new InetSocketAddress("127.0.0.1", 9300 + index)),
                Collections.emptyMap(),
                new HashSet<>(Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE)),
                Version.CURRENT
            );
        } catch (Exception e) {
            throw new RuntimeException("Error creating dummy DiscoveryNode", e);
        }
    }
}


