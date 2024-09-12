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
import org.opensearch.cluster.decommission.DecommissionAttribute;
import org.opensearch.cluster.decommission.DecommissionAttributeMetadata;
import org.opensearch.cluster.decommission.DecommissionStatus;
import org.opensearch.cluster.decommission.NodeDecommissionedException;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.common.util.FeatureFlags.REMOTE_STORE_MIGRATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.VersionUtils.allOpenSearchVersions;
import static org.opensearch.test.VersionUtils.maxCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomVersion;
import static org.opensearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
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

        Metadata metadata = Metadata.EMPTY_METADATA;

        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();
        if (maxNodeVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            final DiscoveryNode tooLowJoiningNode = new DiscoveryNode(
                UUIDs.base64UUID(),
                buildNewFakeTransportAddress(),
                LegacyESVersion.fromString("6.7.0")
            );
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLowJoiningNode, nodes, metadata);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLowJoiningNode, nodes, metadata, minNodeVersion, maxNodeVersion);
                }
            });
        }

        if (minNodeVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            Version oldMajor = minNodeVersion.minimumCompatibilityVersion();
            expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));
        }

        final Version minGoodVersion = maxNodeVersion.compareMajor(minNodeVersion) == 0 ?
        // we have to stick with the same major
            minNodeVersion : maxNodeVersion.minimumCompatibilityVersion();
        final Version justGood = randomVersionBetween(random(), minGoodVersion, maxCompatibleVersion(minNodeVersion));
        final DiscoveryNode justGoodJoiningNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), justGood);
        if (randomBoolean()) {
            JoinTaskExecutor.ensureNodesCompatibility(justGoodJoiningNode, nodes, metadata);
        } else {
            JoinTaskExecutor.ensureNodesCompatibility(justGoodJoiningNode, nodes, metadata, minNodeVersion, maxNodeVersion);
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
        // Node roles vary by version, and new roles are suppressed for BWC.
        // This means we can receive a join from a node that's already in the cluster but with a different set of roles:
        // the node didn't change roles, but the cluster state came via an older cluster-manager.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = mock(RemoteStoreNodeService.class);
        when(remoteStoreNodeService.updateRepositoriesMetadata(any(), any())).thenReturn(new RepositoriesMetadata(Collections.emptyList()));

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

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
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
                    .add(bwcNode)
            )
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
        final RemoteStoreNodeService remoteStoreNodeService = mock(RemoteStoreNodeService.class);
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
            transportService,
            remoteStoreNodeService
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

    /**
     * Validate isBecomeClusterManagerTask() can identify "become cluster manager task" properly
     */
    public void testIsBecomeClusterManagerTask() {
        JoinTaskExecutor.Task joinTaskOfMaster = JoinTaskExecutor.newBecomeMasterTask();
        assertThat(joinTaskOfMaster.isBecomeClusterManagerTask(), is(true));
        JoinTaskExecutor.Task joinTaskOfClusterManager = JoinTaskExecutor.newBecomeClusterManagerTask();
        assertThat(joinTaskOfClusterManager.isBecomeClusterManagerTask(), is(true));
    }

    public void testJoinClusterWithNoDecommission() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        Metadata metadata = metaBuilder.build();
        DiscoveryNode discoveryNode = newDiscoveryNode(Collections.singletonMap("zone", "zone-2"));
        JoinTaskExecutor.ensureNodeCommissioned(discoveryNode, metadata);
    }

    public void testPreventJoinClusterWithDecommission() {
        Settings.builder().build();
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-1");
        DecommissionStatus decommissionStatus = randomFrom(
            DecommissionStatus.IN_PROGRESS,
            DecommissionStatus.SUCCESSFUL,
            DecommissionStatus.DRAINING
        );
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            decommissionStatus,
            randomAlphaOfLength(10)
        );
        Metadata metadata = Metadata.builder().decommissionAttributeMetadata(decommissionAttributeMetadata).build();
        DiscoveryNode discoveryNode = newDiscoveryNode(Collections.singletonMap("zone", "zone-1"));
        expectThrows(NodeDecommissionedException.class, () -> JoinTaskExecutor.ensureNodeCommissioned(discoveryNode, metadata));
    }

    public void testJoinClusterWithDifferentDecommission() {
        Settings.builder().build();
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-1");
        DecommissionStatus decommissionStatus = randomFrom(
            DecommissionStatus.INIT,
            DecommissionStatus.IN_PROGRESS,
            DecommissionStatus.SUCCESSFUL
        );
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            decommissionStatus,
            randomAlphaOfLength(10)
        );
        Metadata metadata = Metadata.builder().decommissionAttributeMetadata(decommissionAttributeMetadata).build();

        DiscoveryNode discoveryNode = newDiscoveryNode(Collections.singletonMap("zone", "zone-2"));
        JoinTaskExecutor.ensureNodeCommissioned(discoveryNode, metadata);
    }

    public void testJoinFailedForDecommissionedNode() throws Exception {
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = mock(RemoteStoreNodeService.class);

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone1");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.SUCCESSFUL,
            randomAlphaOfLength(10)
        );
        final ClusterState clusterManagerClusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
            )
            .metadata(Metadata.builder().decommissionAttributeMetadata(decommissionAttributeMetadata))
            .build();

        final DiscoveryNode decommissionedNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            Collections.singletonMap("zone", "zone1"),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        String decommissionedNodeID = decommissionedNode.getId();

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterManagerClusterState,
            List.of(new JoinTaskExecutor.Task(decommissionedNode, "test"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertFalse(taskResult.isSuccess());
        assertTrue(taskResult.getFailure() instanceof NodeDecommissionedException);
        assertFalse(result.resultingState.getNodes().nodeExists(decommissionedNodeID));
    }

    public void testJoinClusterWithDecommissionFailed() {
        Settings.builder().build();
        DecommissionAttribute decommissionAttribute = new DecommissionAttribute("zone", "zone-1");
        DecommissionAttributeMetadata decommissionAttributeMetadata = new DecommissionAttributeMetadata(
            decommissionAttribute,
            DecommissionStatus.FAILED,
            randomAlphaOfLength(10)
        );
        Metadata metadata = Metadata.builder().decommissionAttributeMetadata(decommissionAttributeMetadata).build();

        DiscoveryNode discoveryNode = newDiscoveryNode(Collections.singletonMap("zone", "zone-1"));
        JoinTaskExecutor.ensureNodeCommissioned(discoveryNode, metadata);
    }

    public void testJoinClusterWithNonRemoteStoreNodeJoining() {
        DiscoveryNode joiningNode = newDiscoveryNode(Collections.emptyMap());
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(joiningNode).build())
            .build();

        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinClusterWithRemoteStoreNodeJoining() {
        Map<String, String> map = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        DiscoveryNode joiningNode = newDiscoveryNode(map);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(joiningNode).build())
            .build();

        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinClusterWithNonRemoteStoreNodeJoiningNonRemoteStoreCluster() {
        final DiscoveryNode existingNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(Collections.emptyMap());

        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testPreventJoinClusterWithRemoteStoreNodeJoiningNonRemoteStoreCluster() {

        final DiscoveryNode existingNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertTrue(e.getMessage().equals("a remote store node [" + joiningNode + "] is trying to join a non remote " + "store cluster"));
    }

    public void testRemoteStoreNodeJoiningNonRemoteStoreClusterMixedMode() {
        final DiscoveryNode existingNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        final Settings settings = Settings.builder()
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
            .build();
        Metadata metadata = Metadata.builder().persistentSettings(settings).build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .metadata(metadata)
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testAllTypesNodeJoiningRemoteStoreClusterMixedMode() {
        final DiscoveryNode docrepNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        DiscoveryNode remoteNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        final Settings settings = Settings.builder()
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
            .build();
        Metadata metadata = Metadata.builder().persistentSettings(settings).build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(docrepNode)
                    .localNodeId(docrepNode.getId())
                    .add(remoteNode)
                    .localNodeId(remoteNode.getId())
                    .build()
            )
            .metadata(metadata)
            .build();

        // compatible remote node should not be able to join a mixed mode having a remote node
        DiscoveryNode goodRemoteNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        JoinTaskExecutor.ensureNodesCompatibility(goodRemoteNode, currentState.getNodes(), currentState.metadata());

        // incompatible node should not be able to join a mixed mode
        DiscoveryNode badRemoteNode = newDiscoveryNode(remoteStoreNodeAttributes(TRANSLOG_REPO, TRANSLOG_REPO));
        assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(badRemoteNode, currentState.getNodes(), currentState.metadata())
        );

        // DocRep node should be able to join a mixed mode
        DiscoveryNode docrepNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        JoinTaskExecutor.ensureNodesCompatibility(docrepNode2, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinClusterWithRemoteStoreNodeJoiningRemoteStoreCluster() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testPreventJoinClusterWithRemoteStoreNodeWithDifferentAttributesJoiningRemoteStoreCluster() {
        Map<String, String> existingNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        for (Map.Entry<String, String> nodeAttribute : existingNodeAttributes.entrySet()) {
            if (nodeAttribute.getKey() != REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY
                && nodeAttribute.getKey() != REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY
                && nodeAttribute.getKey() != REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY) {
                remoteStoreNodeAttributes.put(nodeAttribute.getKey(), nodeAttribute.getValue() + "-new");
                validateAttributes(remoteStoreNodeAttributes, currentState, existingNode);
                remoteStoreNodeAttributes.put(nodeAttribute.getKey(), nodeAttribute.getValue());
            }
        }
    }

    public void testPreventJoinClusterWithRemoteStoreNodeWithDifferentNameAttributesJoiningRemoteStoreCluster() {
        Map<String, String> existingNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        for (Map.Entry<String, String> nodeAttribute : existingNodeAttributes.entrySet()) {
            if (REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY.equals(nodeAttribute.getKey())) {
                Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO + "new", TRANSLOG_REPO);
                validateAttributes(remoteStoreNodeAttributes, currentState, existingNode);
            } else if (REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY.equals(nodeAttribute.getKey())) {
                Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO + "new");
                validateAttributes(remoteStoreNodeAttributes, currentState, existingNode);
            } else if (REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY.equals(nodeAttribute.getKey())) {
                Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(
                    SEGMENT_REPO,
                    TRANSLOG_REPO,
                    CLUSTER_STATE_REPO + "new"
                );
                validateAttributes(remoteStoreNodeAttributes, currentState, existingNode);
            }
        }
    }

    public void testPreventJoinClusterWithNonRemoteStoreNodeJoiningRemoteStoreCluster() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(Collections.emptyMap());
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertTrue(e.getMessage().equals("a non remote store node [" + joiningNode + "] is trying to join a remote " + "store cluster"));
    }

    public void testPreventJoinClusterWithRemoteStoreNodeWithPartialAttributesJoiningRemoteStoreCluster() {
        Map<String, String> existingNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        for (Map.Entry<String, String> nodeAttribute : existingNodeAttributes.entrySet()) {
            remoteStoreNodeAttributes.put(nodeAttribute.getKey(), null);
            DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes);
            Exception e = assertThrows(
                IllegalStateException.class,
                () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
            );
            assertTrue(
                e.getMessage().equals("joining node [" + joiningNode + "] doesn't have the node attribute [" + nodeAttribute.getKey() + "]")
                    || e.getMessage()
                        .equals(
                            "a remote store node ["
                                + joiningNode
                                + "] is trying to join a remote store cluster with incompatible node attributes in comparison with existing node ["
                                + currentState.getNodes().getNodes().values().stream().findFirst().get()
                                + "]"
                        )
            );

            remoteStoreNodeAttributes.put(nodeAttribute.getKey(), nodeAttribute.getValue());
        }
    }

    public void testJoinClusterWithRemoteStateNodeJoiningRemoteStateCluster() {
        Map<String, String> existingNodeAttributes = remoteStateNodeAttributes(CLUSTER_STATE_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();
        DiscoveryNode joiningNode = newDiscoveryNode(remoteStateNodeAttributes(CLUSTER_STATE_REPO));
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinRemotePublicationClusterWithNonRemoteNodes() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remotePublicationNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(new HashMap<>());
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinRemotePublicationCluster() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remotePublicationNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remotePublicationNodeAttributes());
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinRemotePubClusterWithRemoteStoreNodes() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remotePublicationNodeAttributes(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        Map<String, String> newNodeAttributes = new HashMap<>();
        newNodeAttributes.putAll(remoteStateNodeAttributes(CLUSTER_STATE_REPO));
        newNodeAttributes.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        newNodeAttributes.putAll(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));

        DiscoveryNode joiningNode = newDiscoveryNode(newNodeAttributes);
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertTrue(e.getMessage().equals("a remote store node [" + joiningNode + "] is trying to join a non remote store cluster"));
    }

    public void testPreventJoinRemotePublicationClusterWithIncompatibleAttributes() {
        Map<String, String> existingNodeAttributes = remotePublicationNodeAttributes();
        Map<String, String> remoteStoreNodeAttributes = remotePublicationNodeAttributes();
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        for (Map.Entry<String, String> nodeAttribute : existingNodeAttributes.entrySet()) {
            remoteStoreNodeAttributes.put(nodeAttribute.getKey(), null);
            DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes);
            Exception e = assertThrows(
                IllegalStateException.class,
                () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
            );
            assertTrue(
                e.getMessage().equals("joining node [" + joiningNode + "] doesn't have the node attribute [" + nodeAttribute.getKey() + "]")
                    || e.getMessage()
                        .equals(
                            "a remote store node ["
                                + joiningNode
                                + "] is trying to join a remote store cluster with incompatible node attributes in comparison with existing node ["
                                + currentState.getNodes().getNodes().values().stream().findFirst().get()
                                + "]"
                        )
            );

            remoteStoreNodeAttributes.put(nodeAttribute.getKey(), nodeAttribute.getValue());
        }
    }

    public void testPreventJoinClusterWithRemoteStateNodeJoiningRemoteStoreCluster() {
        Map<String, String> existingNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();
        DiscoveryNode joiningNode = newDiscoveryNode(remoteStateNodeAttributes(CLUSTER_STATE_REPO));
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertTrue(e.getMessage().equals("a non remote store node [" + joiningNode + "] is trying to join a remote store cluster"));
    }

    public void testPreventJoinClusterWithRemoteStoreNodeJoiningRemoteStateCluster() {
        Map<String, String> existingNodeAttributes = remoteStateNodeAttributes(CLUSTER_STATE_REPO);
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            existingNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();
        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertTrue(e.getMessage().equals("a remote store node [" + joiningNode + "] is trying to join a non remote store cluster"));
    }

    public void testUpdatesClusterStateWithSingleNodeCluster() throws Exception {
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = new RemoteStoreNodeService(
            new SetOnce<>(mock(RepositoriesService.class))::get,
            null
        );

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
            )
            .build();

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(new JoinTaskExecutor.Task(clusterManagerNode, "elect leader"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());
        validateRepositoryMetadata(result.resultingState, clusterManagerNode, 3);
    }

    public void testUpdatesClusterStateWithMultiNodeCluster() throws Exception {
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = new RemoteStoreNodeService(
            new SetOnce<>(mock(RepositoriesService.class))::get,
            null
        );

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final RepositoryMetadata segmentRepositoryMetadata = buildRepositoryMetadata(clusterManagerNode, SEGMENT_REPO);
        final RepositoryMetadata translogRepositoryMetadata = buildRepositoryMetadata(clusterManagerNode, TRANSLOG_REPO);
        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>() {
            {
                add(segmentRepositoryMetadata);
                add(translogRepositoryMetadata);
            }
        };

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
            )
            .metadata(Metadata.builder().putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositoriesMetadata)))
            .build();

        final DiscoveryNode joiningNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(new JoinTaskExecutor.Task(joiningNode, "test"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());
        validateRepositoryMetadata(result.resultingState, clusterManagerNode, 3);
    }

    public void testUpdatesClusterStateWithSingleNodeClusterAndSameRepository() throws Exception {
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(COMMON_REPO, COMMON_REPO);
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = new RemoteStoreNodeService(
            new SetOnce<>(mock(RepositoriesService.class))::get,
            null
        );

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
            )
            .build();

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(new JoinTaskExecutor.Task(clusterManagerNode, "elect leader"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());
        validateRepositoryMetadata(result.resultingState, clusterManagerNode, 2);
    }

    public void testUpdatesClusterStateWithMultiNodeClusterAndSameRepository() throws Exception {
        Map<String, String> remoteStoreNodeAttributes = remoteStoreNodeAttributes(COMMON_REPO, COMMON_REPO);
        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final RemoteStoreNodeService remoteStoreNodeService = new RemoteStoreNodeService(
            new SetOnce<>(mock(RepositoriesService.class))::get,
            null
        );

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(
            Settings.EMPTY,
            allocationService,
            logger,
            rerouteService,
            null,
            remoteStoreNodeService
        );

        final DiscoveryNode clusterManagerNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final RepositoryMetadata segmentRepositoryMetadata = buildRepositoryMetadata(clusterManagerNode, COMMON_REPO);
        List<RepositoryMetadata> repositoriesMetadata = new ArrayList<>() {
            {
                add(segmentRepositoryMetadata);
            }
        };

        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(clusterManagerNode)
                    .localNodeId(clusterManagerNode.getId())
                    .clusterManagerNodeId(clusterManagerNode.getId())
            )
            .metadata(Metadata.builder().putCustom(RepositoriesMetadata.TYPE, new RepositoriesMetadata(repositoriesMetadata)))
            .build();

        final DiscoveryNode joiningNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result = joinTaskExecutor.execute(
            clusterState,
            List.of(new JoinTaskExecutor.Task(joiningNode, "test"))
        );
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());
        validateRepositoryMetadata(result.resultingState, clusterManagerNode, 2);
    }

    public void testNodeJoinInMixedMode() {
        List<Version> versions = allOpenSearchVersions();
        assert versions.size() >= 2 : "test requires at least two open search versions";
        Version baseVersion = versions.get(versions.size() - 2);
        Version higherVersion = versions.get(versions.size() - 1);

        DiscoveryNode currentNode1 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), baseVersion);
        DiscoveryNode currentNode2 = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), baseVersion);
        DiscoveryNodes currentNodes = DiscoveryNodes.builder()
            .add(currentNode1)
            .localNodeId(currentNode1.getId())
            .add(currentNode2)
            .localNodeId(currentNode2.getId())
            .build();

        Settings mixedModeCompatibilitySettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .build();

        Metadata metadata = Metadata.builder().persistentSettings(mixedModeCompatibilitySettings).build();

        // joining node of a higher version than the current nodes
        DiscoveryNode joiningNode1 = new DiscoveryNode(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            higherVersion
        );
        final IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode1, currentNodes, metadata)
        );
        String reason = String.format(
            Locale.ROOT,
            "remote migration : a node [%s] of higher version [%s] is not allowed to join a cluster with maximum version [%s]",
            joiningNode1,
            joiningNode1.getVersion(),
            currentNodes.getMaxNodeVersion()
        );
        assertEquals(reason, exception.getMessage());

        // joining node of the same version as the current nodes
        DiscoveryNode joiningNode2 = new DiscoveryNode(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            baseVersion
        );
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode2, currentNodes, metadata);
    }

    public void testRemoteRoutingTableRepoAbsentNodeJoin() {

        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testRemoteRoutingTableNodeJoinRepoPresentInJoiningNode() {
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        Map<String, String> attr = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        attr.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        DiscoveryNode joiningNode = newDiscoveryNode(attr);
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testRemoteRoutingTableNodeJoinRepoPresentInExistingNode() {
        Map<String, String> attr = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        attr.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attr,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO));
        assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
    }

    public void testRemoteRoutingTableNodeJoinRepoPresentInBothNode() {
        Map<String, String> attr = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        attr.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attr,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(attr);
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testRemoteRoutingTableNodeJoinNodeWithRemoteAndRoutingRepoDifference() {
        Map<String, String> attr = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        attr.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attr,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final DiscoveryNode existingNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode2).add(existingNode).localNodeId(existingNode.getId()).build())
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(attr);
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testRemoteRoutingTableNodeJoinNodeWithRemoteAndRoutingRepoDifferenceMixedMode() {
        Map<String, String> attr = remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO);
        attr.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        final DiscoveryNode existingNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            attr,
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final DiscoveryNode existingNode2 = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final Settings settings = Settings.builder()
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
            .build();
        final Settings nodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        Metadata metadata = Metadata.builder().persistentSettings(settings).build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(existingNode2).add(existingNode).localNodeId(existingNode.getId()).build())
            .metadata(metadata)
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(attr);
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    public void testJoinRemoteStoreClusterWithRemotePublicationNodeInMixedMode() {
        final DiscoveryNode remoteStoreNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            remoteStoreNodeAttributes(SEGMENT_REPO, TRANSLOG_REPO),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final DiscoveryNode nonRemoteStoreNode = new DiscoveryNode(
            UUIDs.base64UUID(),
            buildNewFakeTransportAddress(),
            new HashMap<>(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            Version.CURRENT
        );

        final Settings settings = Settings.builder()
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), "mixed")
            .build();
        final Settings nodeSettings = Settings.builder().put(REMOTE_STORE_MIGRATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        Metadata metadata = Metadata.builder().persistentSettings(settings).build();
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(remoteStoreNode).add(nonRemoteStoreNode).localNodeId(remoteStoreNode.getId()).build())
            .metadata(metadata)
            .build();

        DiscoveryNode joiningNode = newDiscoveryNode(remotePublicationNodeAttributes());
        JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata());
    }

    private void validateRepositoryMetadata(ClusterState updatedState, DiscoveryNode existingNode, int expectedRepositories)
        throws Exception {

        final RepositoriesMetadata repositoriesMetadata = updatedState.metadata().custom(RepositoriesMetadata.TYPE);
        assertTrue(repositoriesMetadata.repositories().size() == expectedRepositories);
        if (repositoriesMetadata.repositories().size() == 2 || repositoriesMetadata.repositories().size() == 3) {
            final RepositoryMetadata segmentRepositoryMetadata = buildRepositoryMetadata(existingNode, SEGMENT_REPO);
            final RepositoryMetadata translogRepositoryMetadata = buildRepositoryMetadata(existingNode, TRANSLOG_REPO);
            for (RepositoryMetadata repositoryMetadata : repositoriesMetadata.repositories()) {
                if (repositoryMetadata.name().equals(segmentRepositoryMetadata.name())) {
                    assertTrue(segmentRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata));
                } else if (repositoryMetadata.name().equals(translogRepositoryMetadata.name())) {
                    assertTrue(translogRepositoryMetadata.equalsIgnoreGenerations(repositoryMetadata));
                } else if (repositoriesMetadata.repositories().size() == 3) {
                    final RepositoryMetadata clusterStateRepoMetadata = buildRepositoryMetadata(existingNode, CLUSTER_STATE_REPO);
                    assertTrue(clusterStateRepoMetadata.equalsIgnoreGenerations(repositoryMetadata));
                }
            }
        } else if (repositoriesMetadata.repositories().size() == 1) {
            final RepositoryMetadata repositoryMetadata = buildRepositoryMetadata(existingNode, COMMON_REPO);
            assertTrue(repositoryMetadata.equalsIgnoreGenerations(repositoriesMetadata.repositories().get(0)));
        } else {
            throw new Exception("Stack overflow example: checkedExceptionThrower");
        }
    }

    private DiscoveryNode newDiscoveryNode(Map<String, String> attributes) {
        return new DiscoveryNode(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            buildNewFakeTransportAddress(),
            attributes,
            Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
    }

    private static final String SEGMENT_REPO = "segment-repo";

    private static final String TRANSLOG_REPO = "translog-repo";
    private static final String CLUSTER_STATE_REPO = "cluster-state-repo";
    private static final String COMMON_REPO = "remote-repo";
    private static final String ROUTING_TABLE_REPO = "routing-table-repo";

    private Map<String, String> remoteStoreNodeAttributes(String segmentRepoName, String translogRepoName) {
        return remoteStoreNodeAttributes(segmentRepoName, translogRepoName, CLUSTER_STATE_REPO);
    }

    private Map<String, String> remoteStoreNodeAttributes(String segmentRepoName, String translogRepoName, String clusterStateRepo) {
        String segmentRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            segmentRepoName
        );
        String segmentRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            segmentRepoName
        );
        String translogRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            translogRepoName
        );
        String translogRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            translogRepoName
        );

        return new HashMap<>() {
            {
                put(REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, segmentRepoName);
                put(segmentRepositoryTypeAttributeKey, "s3");
                put(segmentRepositorySettingsAttributeKeyPrefix + "bucket", "segment_bucket");
                put(segmentRepositorySettingsAttributeKeyPrefix + "base_path", "/segment/path");
                put(REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, translogRepoName);
                putIfAbsent(translogRepositoryTypeAttributeKey, "s3");
                putIfAbsent(translogRepositorySettingsAttributeKeyPrefix + "bucket", "translog_bucket");
                putIfAbsent(translogRepositorySettingsAttributeKeyPrefix + "base_path", "/translog/path");
                putAll(remoteStateNodeAttributes(clusterStateRepo));
            }
        };
    }

    private Map<String, String> remotePublicationNodeAttributes() {
        Map<String, String> existingNodeAttributes = new HashMap<>();
        existingNodeAttributes.putAll(remoteStateNodeAttributes(CLUSTER_STATE_REPO));
        existingNodeAttributes.putAll(remoteRoutingTableAttributes(ROUTING_TABLE_REPO));
        return existingNodeAttributes;
    }

    private Map<String, String> remoteStateNodeAttributes(String clusterStateRepo) {
        String clusterStateRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            clusterStateRepo
        );
        String clusterStateRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            clusterStateRepo
        );

        return new HashMap<>() {
            {
                put(REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, clusterStateRepo);
                putIfAbsent(clusterStateRepositoryTypeAttributeKey, "s3");
                putIfAbsent(clusterStateRepositorySettingsAttributeKeyPrefix + "bucket", "state_bucket");
                putIfAbsent(clusterStateRepositorySettingsAttributeKeyPrefix + "base_path", "/state/path");
            }
        };
    }

    private Map<String, String> remoteRoutingTableAttributes(String repoName) {
        String routingTableRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            repoName
        );
        String routingTableRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            repoName
        );

        return new HashMap<>() {
            {
                put(REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, repoName);
                putIfAbsent(routingTableRepositoryTypeAttributeKey, "s3");
                putIfAbsent(routingTableRepositorySettingsAttributeKeyPrefix + "bucket", "state_bucket");
                putIfAbsent(routingTableRepositorySettingsAttributeKeyPrefix + "base_path", "/state/path");
            }
        };
    }

    private void validateAttributes(Map<String, String> remoteStoreNodeAttributes, ClusterState currentState, DiscoveryNode existingNode) {
        DiscoveryNode joiningNode = newDiscoveryNode(remoteStoreNodeAttributes);
        Exception e = assertThrows(
            IllegalStateException.class,
            () -> JoinTaskExecutor.ensureNodesCompatibility(joiningNode, currentState.getNodes(), currentState.metadata())
        );
        assertEquals(
            e.getMessage(),
            "a remote store node ["
                + joiningNode
                + "] is trying to join a remote store cluster with incompatible node attributes in "
                + "comparison with existing node ["
                + existingNode
                + "]"
        );
    }

    private RepositoryMetadata buildRepositoryMetadata(DiscoveryNode node, String name) {
        Map<String, String> nodeAttributes = node.getAttributes();
        String type = nodeAttributes.get(String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT, name));

        String settingsAttributeKeyPrefix = String.format(Locale.getDefault(), REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX, name);
        Map<String, String> settingsMap = node.getAttributes()
            .keySet()
            .stream()
            .filter(key -> key.startsWith(settingsAttributeKeyPrefix))
            .collect(Collectors.toMap(key -> key.replace(settingsAttributeKeyPrefix, ""), key -> node.getAttributes().get(key)));

        Settings.Builder settings = Settings.builder();
        settingsMap.entrySet().forEach(entry -> settings.put(entry.getKey(), entry.getValue()));

        settings.put(BlobStoreRepository.SYSTEM_REPOSITORY_SETTING.getKey(), true);

        return new RepositoryMetadata(name, type, settings.build());
    }
}
