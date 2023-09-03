/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.remotestore;

import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteStoreRepositoryRegistrationIT extends RemoteStoreBaseIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    public void testSingleNodeClusterRepositoryRegistration() throws Exception {
        internalCluster().startNode();
    }

    public void testMultiNodeClusterRepositoryRegistration() throws Exception {
        internalCluster().startNodes(3);
    }

    public void testMultiNodeClusterRepositoryRegistrationWithMultipleClusterManager() throws Exception {
        internalCluster().startClusterManagerOnlyNodes(3);
        internalCluster().startNodes(3);
    }

    public void testMultiNodeClusterActiveClusterManagerShutDown() throws Exception {
        internalCluster().startNodes(3);
        internalCluster().stopCurrentClusterManagerNode();
        ensureStableCluster(2);
    }

    public void testMultiNodeClusterActiveMClusterManagerRestart() throws Exception {
        internalCluster().startNodes(3);
        String clusterManagerNodeName = internalCluster().getClusterManagerName();
        internalCluster().restartNode(clusterManagerNodeName);
        ensureStableCluster(3);
    }

    public void testMultiNodeClusterRandomNodeRestart() throws Exception {
        internalCluster().startNodes(3);
        internalCluster().restartRandomDataNode();
        ensureStableCluster(3);
    }

    public void testMultiNodeClusterActiveClusterManagerRecoverNetworkIsolation() {
        internalCluster().startClusterManagerOnlyNodes(3);
        String dataNode = internalCluster().startNode();

        NetworkDisruption partition = isolateClusterManagerDisruption(NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);

        partition.startDisrupting();
        ensureStableCluster(3, dataNode);
        partition.stopDisrupting();

        ensureStableCluster(4);

        internalCluster().clearDisruptionScheme();
    }

    public void testMultiNodeClusterRandomNodeRecoverNetworkIsolation() {
        Set<String> nodesInOneSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        Set<String> nodesInAnotherSide = internalCluster().startNodes(3).stream().collect(Collectors.toCollection(HashSet::new));
        ensureStableCluster(6);

        NetworkDisruption networkDisruption = new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(nodesInOneSide, nodesInAnotherSide),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(networkDisruption);

        networkDisruption.startDisrupting();
        ensureStableCluster(3, nodesInOneSide.stream().findAny().get());
        networkDisruption.stopDisrupting();

        ensureStableCluster(6);

        internalCluster().clearDisruptionScheme();
    }
}
