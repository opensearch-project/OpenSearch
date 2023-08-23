/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ClusterPluginTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(DummyClusterPlugin.class, DummyClusterPlugin2.class);
    }

    public void testOnNodeStarted_shouldContainLocalNodeInfo() {

        DiscoveryNode localNode = DummyClusterPlugin.getLocalNode();

        assertTrue(localNode != null);
        // TODO Figure out if there is a way to check ephemeralId
        assertTrue(localNode.getId().equals(node().getNodeEnvironment().nodeId()));
    }

    public void testOnNodeStarted_shouldCallDeprecatedMethod() {
        DummyClusterPlugin2 dummyClusterPlugin2 = mock(DummyClusterPlugin2.class);
        dummyClusterPlugin2.onNodeStarted();
        verify(dummyClusterPlugin2, times(1)).onNodeStarted();

        DiscoveryNode localNode = DummyClusterPlugin2.getLocalNode();
        assertTrue(localNode != null);
    }

}

final class DummyClusterPlugin extends Plugin implements ClusterPlugin {

    private static volatile DiscoveryNode localNode;

    public DummyClusterPlugin() {}

    @Override
    public void onNodeStarted(DiscoveryNode localNode) {
        DummyClusterPlugin.localNode = localNode;
    }

    public static DiscoveryNode getLocalNode() {
        return localNode;
    }
}

class DummyClusterPlugin2 extends Plugin implements ClusterPlugin {

    private static volatile DiscoveryNode localNode;

    public DummyClusterPlugin2() {}

    @Override
    public void onNodeStarted() {
        localNode = mock(DiscoveryNode.class);
    }

    public static DiscoveryNode getLocalNode() {
        return localNode;
    }

}
