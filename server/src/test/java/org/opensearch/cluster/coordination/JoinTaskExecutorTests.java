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
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.opensearch.test.VersionUtils.allVersions;
import static org.opensearch.test.VersionUtils.maxCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomOpenSearchVersion;
import static org.opensearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.any;
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
        final Version version = randomOpenSearchVersion(random());
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), version));
        builder.add(new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), randomCompatibleVersion(random(), version)));
        DiscoveryNodes nodes = builder.build();

        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();
        final Version tooLow = LegacyESVersion.fromString("6.7.0");
        expectThrows(IllegalStateException.class, () -> {
            if (randomBoolean()) {
                JoinTaskExecutor.ensureNodesCompatibility(tooLow, nodes);
            } else {
                JoinTaskExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
            }
        });

        if (minNodeVersion.before(Version.V_3_0_0)) {
            Version oldMajor = minNodeVersion.minimumCompatibilityVersion();
            expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));
        }

        final Version minGoodVersion;
        if (maxNodeVersion.compareMajor(minNodeVersion) == 0) {
            // we have to stick with the same major
            minGoodVersion = minNodeVersion;
        } else {
            Version minCompatVersion = maxNodeVersion.minimumCompatibilityVersion();
            minGoodVersion = minCompatVersion.before(allVersions().get(0)) ? allVersions().get(0) : minCompatVersion;
        }
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
        // Node roles vary by version, and new roles are suppressed for BWC.
        // This means we can receive a join from a node that's already in the cluster but with a different set of roles:
        // the node didn't change roles, but the cluster state came via an older cluster-manager.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(Settings.EMPTY, allocationService, logger, rerouteService);

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

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(Settings.EMPTY, allocationService, logger, rerouteService);

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
}
