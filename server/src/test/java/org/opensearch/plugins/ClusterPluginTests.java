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

public class ClusterPluginTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(DummyClusterPlugin.class);
    }

    public void testOnNodeStarted_shouldContainLocalNodeInfo() {

        DiscoveryNode localNode = DummyClusterPlugin.getLocalNode();

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
