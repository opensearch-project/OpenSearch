/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Priority;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class VotingConfigurationIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(MockTransportService.TestPlugin.class);
    }

    public void testAbdicateAfterVotingConfigExclusionAdded() throws ExecutionException, InterruptedException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        internalCluster().startNodes(2);
        final String originalClusterManager = internalCluster().getClusterManagerName();

        logger.info("--> excluding cluster-manager node {}", originalClusterManager);
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(originalClusterManager)).get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get();
        assertNotEquals(originalClusterManager, internalCluster().getClusterManagerName());
    }

    public void testElectsNodeNotInVotingConfiguration() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        final List<String> nodeNames = internalCluster().startNodes(4);

        // a 4-node cluster settles on a 3-node configuration; we then prevent the nodes in the configuration from winning an election
        // by failing at the pre-voting stage, so that the extra node must be elected instead when the cluster-manager shuts down. This
        // extra node
        // should then add itself into the voting configuration.

        assertFalse(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes("4")
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );

        String excludedNodeName = null;
        final ClusterState clusterState = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setNodes(true)
            .setMetadata(true)
            .get()
            .getState();
        final Set<String> votingConfiguration = clusterState.getLastCommittedConfiguration().getNodeIds();
        assertThat(votingConfiguration, hasSize(3));
        assertThat(clusterState.nodes().getSize(), equalTo(4));
        assertThat(votingConfiguration, hasItem(clusterState.nodes().getClusterManagerNodeId()));
        for (DiscoveryNode discoveryNode : clusterState.nodes()) {
            if (votingConfiguration.contains(discoveryNode.getId()) == false) {
                assertThat(excludedNodeName, nullValue());
                excludedNodeName = discoveryNode.getName();
            }
        }

        for (final String sender : nodeNames) {
            if (sender.equals(excludedNodeName)) {
                continue;
            }
            final MockTransportService senderTransportService = (MockTransportService) internalCluster().getInstance(
                TransportService.class,
                sender
            );
            for (final String receiver : nodeNames) {
                senderTransportService.addSendBehavior(
                    internalCluster().getInstance(TransportService.class, receiver),
                    (connection, requestId, action, request, options) -> {
                        if (action.equals(PreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)) {
                            throw new OpenSearchException("rejected");
                        }
                        connection.sendRequest(requestId, action, request, options);
                    }
                );
            }
        }

        internalCluster().stopCurrentClusterManagerNode();
        assertFalse(
            internalCluster().client()
                .admin()
                .cluster()
                .prepareHealth()
                .setWaitForNodes("3")
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );

        final ClusterState newClusterState = internalCluster().client()
            .admin()
            .cluster()
            .prepareState()
            .clear()
            .setNodes(true)
            .setMetadata(true)
            .get()
            .getState();
        assertThat(newClusterState.nodes().getClusterManagerNode().getName(), equalTo(excludedNodeName));
        assertThat(
            newClusterState.getLastCommittedConfiguration().getNodeIds(),
            hasItem(newClusterState.nodes().getClusterManagerNodeId())
        );
    }
}
