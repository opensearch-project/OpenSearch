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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateTaskExecutor;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.List;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.transport.TransportService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.opensearch.test.VersionUtils.maxCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomVersion;
import static org.opensearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoinTaskExecutorTests extends OpenSearchTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT), metadata)
        );
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.fromString("5.8.0")))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata));
    }

    public void testPreventJoinClusterWithUnsupportedNodeVersions() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final Version version = randomVersion(random());
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), version));
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), randomCompatibleVersion(random(), version)));
        DiscoveryNodes nodes = builder.build();

        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();
        if (maxNodeVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            final Version tooLow = LegacyESVersion.fromString("6.7.0");
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, nodes);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
                }
            });
        }

        if (minNodeVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            Version oldMajor = minNodeVersion.minimumCompatibilityVersion();
            expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));
        }

        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ?
        // we have to stick with the same major
            minNodeVersion : maxNodeVersion.minimumCompatibilityVersion();
        final Version justGood = randomVersionBetween(random(), minGoodVersion, maxCompatibleVersion(minNodeVersion));

        if (randomBoolean()) {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, nodes);
        } else {
            JoinTaskExecutor.ensureNodesCompatibility(justGood, minNodeVersion, maxNodeVersion);
        }
    }

    public void testSuccess() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                settings(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(
                settings(VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT))
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);
    }

    public void testUpdatesNodeWithNewRoles() throws Exception {
        // Node roles vary by version, and new roles are suppressed for BWC. This means we can receive a join from a node that's already
        // in the cluster but with a different set of roles: the node didn't change roles, but the cluster state came via an older master.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(Settings.EMPTY, allocationService, logger, rerouteService, null);

        final DiscoveryNode masterNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        final DiscoveryNode actualNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode bwcNode = new DiscoveryNode(
            actualNode.getName(),
            actualNode.getId(),
            actualNode.getEphemeralId(),
            actualNode.getHostName(),
            actualNode.getHostAddress(),
            actualNode.getAddress(),
            actualNode.getAttributes(),
            new HashSet<>(randomSubsetOf(actualNode.getRoles())),
            actualNode.getVersion()
        );
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()).add(bwcNode))
            .build();

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(new JoinTaskExecutor.Task(actualNode, "test"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());

        assertThat(result.resultingState.getNodes().get(actualNode.getId()).getRoles(), equalTo(actualNode.getRoles()));
    }

    public void testUpdatesNodeWithOpenSearchVersionForExistingAndNewNodes() throws Exception {
        // During the upgrade from Elasticsearch, OpenSearch node send their version as 7.10.2 to Elasticsearch master
        // in order to successfully join the cluster. But as soon as OpenSearch node becomes the master, cluster state
        // should show the OpenSearch nodes version as 1.x. As the cluster state was carry forwarded from ES master,
        // version in DiscoveryNode is stale 7.10.2.
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        when(allocationService.disassociateDeadNodes(any(), anyBoolean(), any())).then(
            invocationOnMock -> invocationOnMock.getArguments()[0]
        );
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        Map<String, Version> channelVersions = new HashMap<>();
        String node_1 = UUIDs.base64UUID();  // OpenSearch node running BWC version
        String node_2 = UUIDs.base64UUID();  // OpenSearch node running BWC version
        String node_3 = UUIDs.base64UUID();  // OpenSearch node running BWC version, sending new join request and no active channel
        String node_4 = UUIDs.base64UUID();  // ES node 7.10.2
        String node_5 = UUIDs.base64UUID();  // ES node 7.10.2 in cluster but missing channel version
        String node_6 = UUIDs.base64UUID();  // ES node 7.9.0
        String node_7 = UUIDs.base64UUID();  // ES node 7.9.0 in cluster but missing channel version
        channelVersions.put(node_1, Version.CURRENT);
        channelVersions.put(node_2, Version.CURRENT);
        channelVersions.put(node_4, LegacyESVersion.V_7_10_2);
        channelVersions.put(node_6, LegacyESVersion.V_7_10_0);

        final TransportService transportService = mock(TransportService.class);
        when(transportService.getChannelVersion(any())).thenReturn(channelVersions);
        DiscoveryNodes.Builder nodes = new DiscoveryNodes.Builder().localNodeId(node_1);
        nodes.add(new DiscoveryNode(node_1, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_2));
        nodes.add(new DiscoveryNode(node_2, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_2));
        nodes.add(new DiscoveryNode(node_3, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_2));
        nodes.add(new DiscoveryNode(node_4, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_2));
        nodes.add(new DiscoveryNode(node_5, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_2));
        nodes.add(new DiscoveryNode(node_6, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_1));
        nodes.add(new DiscoveryNode(node_7, buildNewFakeTransportAddress(), LegacyESVersion.V_7_10_0));
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            transportService
        );
        final DiscoveryNode existing_node_3 = clusterState.nodes().get(node_3);
        final DiscoveryNode node_3_new_join = new DiscoveryNode(
            existing_node_3.getName(),
            existing_node_3.getId(),
            existing_node_3.getEphemeralId(),
            existing_node_3.getHostName(),
            existing_node_3.getHostAddress(),
            existing_node_3.getAddress(),
            existing_node_3.getAttributes(),
            existing_node_3.getRoles(),
            Version.CURRENT
        );

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(
                new JoinTaskExecutor.Task(node_3_new_join, "test"),
                JoinTaskExecutor.newBecomeMasterTask(),
                JoinTaskExecutor.newFinishElectionTask()
            )
        );
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());
        DiscoveryNodes resultNodes = result.resultingState.getNodes();
        assertEquals(Version.CURRENT, resultNodes.get(node_1).getVersion());
        assertEquals(Version.CURRENT, resultNodes.get(node_2).getVersion());
        assertEquals(Version.CURRENT, resultNodes.get(node_3).getVersion()); // 7.10.2 in old state but sent new join and processed
        assertEquals(LegacyESVersion.V_7_10_2, resultNodes.get(node_4).getVersion());
        assertFalse(resultNodes.nodeExists(node_5));  // 7.10.2 node without active channel will be removed and should rejoin
        assertEquals(LegacyESVersion.V_7_10_0, resultNodes.get(node_6).getVersion());
        // 7.9.0 node without active channel but shouldn't get removed
        assertEquals(LegacyESVersion.V_7_10_0, resultNodes.get(node_7).getVersion());
    }
}
