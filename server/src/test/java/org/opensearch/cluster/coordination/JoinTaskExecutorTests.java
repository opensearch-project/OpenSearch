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

import java.util.HashSet;

import static org.opensearch.test.VersionUtils.getPreviousVersion;
import static org.opensearch.test.VersionUtils.incompatibleFutureVersion;
import static org.opensearch.test.VersionUtils.maxCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomCompatibleVersion;
import static org.opensearch.test.VersionUtils.randomVersion;
import static org.opensearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JoinTaskExecutorTests extends OpenSearchTestCase {

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(IllegalStateException.class, () ->
        JoinTaskExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT),
            metadata));
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(VersionUtils.getPreviousVersion(Version.CURRENT
                .minimumIndexCompatibilityVersion())))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        expectThrows(IllegalStateException.class, () ->
            JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT,
                metadata));
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
            final Version tooLow = getPreviousVersion(maxNodeVersion.minimumCompatibilityVersion());
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, nodes);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
                }
            });
        }

        if (minNodeVersion.before(LegacyESVersion.V_6_0_0)) {
            Version tooHigh = incompatibleFutureVersion(minNodeVersion);
            expectThrows(IllegalStateException.class, () -> {
                if (randomBoolean()) {
                    JoinTaskExecutor.ensureNodesCompatibility(tooHigh, nodes);
                } else {
                    JoinTaskExecutor.ensureNodesCompatibility(tooHigh, minNodeVersion, maxNodeVersion);
                }
            });
        }

        if (minNodeVersion.onOrAfter(LegacyESVersion.V_7_0_0)) {
            Version oldMajor = LegacyESVersion.V_6_4_0.minimumCompatibilityVersion();
            expectThrows(IllegalStateException.class, () -> JoinTaskExecutor.ensureMajorVersionBarrier(oldMajor, minNodeVersion));
        }

        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ?
            // we have to stick with the same major
            minNodeVersion :
            maxNodeVersion.minimumCompatibilityVersion();
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
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(settings(VersionUtils.randomVersionBetween(random(),
                Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT)))
            .numberOfShards(1)
            .numberOfReplicas(1).build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
            JoinTaskExecutor.ensureIndexCompatibility(Version.CURRENT,
                metadata);
    }

    public void testUpdatesNodeWithNewRoles() throws Exception {
        // Node roles vary by version, and new roles are suppressed for BWC. This means we can receive a join from a node that's already
        // in the cluster but with a different set of roles: the node didn't change roles, but the cluster state came via an older master.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final JoinTaskExecutor joinTaskExecutor = new JoinTaskExecutor(Settings.EMPTY, allocationService, logger, rerouteService);

        final DiscoveryNode masterNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        final DiscoveryNode actualNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);
        final DiscoveryNode bwcNode = new DiscoveryNode(actualNode.getName(), actualNode.getId(), actualNode.getEphemeralId(),
                actualNode.getHostName(), actualNode.getHostAddress(), actualNode.getAddress(), actualNode.getAttributes(),
                new HashSet<>(randomSubsetOf(actualNode.getRoles())), actualNode.getVersion());
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(DiscoveryNodes.builder()
                .add(masterNode)
                .localNodeId(masterNode.getId())
                .masterNodeId(masterNode.getId())
                .add(bwcNode)
        ).build();

        final ClusterStateTaskExecutor.ClusterTasksResult<JoinTaskExecutor.Task> result
                = joinTaskExecutor.execute(clusterState, List.of(new JoinTaskExecutor.Task(actualNode, "test")));
        assertThat(result.executionResults.entrySet(), hasSize(1));
        final ClusterStateTaskExecutor.TaskResult taskResult = result.executionResults.values().iterator().next();
        assertTrue(taskResult.isSuccess());

        assertThat(result.resultingState.getNodes().get(actualNode.getId()).getRoles(), equalTo(actualNode.getRoles()));
    }
}
