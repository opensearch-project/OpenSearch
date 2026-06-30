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

package org.opensearch.rest.action.cat;

import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodeRoleGenerator;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.transport.client.node.NodeClient;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestNodesActionTests extends OpenSearchTestCase {

    private RestNodesAction action;

    @Before
    public void setUpAction() {
        action = new RestNodesAction();
    }

    public void testBuildTableDoesNotThrowGivenNullNodeInfoAndStats() {
        testBuildTableWithRoles(emptySet(), (table) -> {
            Map<String, List<Table.Cell>> nodeInfoMap = table.getAsMap();
            List<Table.Cell> cells = nodeInfoMap.get("node.role");
            assertEquals(1, cells.size());
            assertEquals("-", cells.get(0).value);
            cells = nodeInfoMap.get("node.roles");
            assertEquals(1, cells.size());
            assertEquals("-", cells.get(0).value);
        });
    }

    public void testCatNodesWithLocalDeprecationWarning() {
        TestThreadPool threadPool = new TestThreadPool(RestNodesActionTests.class.getName());
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("local", randomFrom("", "true", "false"));

        action.doCatRequest(request, client);
        assertWarnings(RestNodesAction.LOCAL_DEPRECATED_MESSAGE);

        terminate(threadPool);
    }

    public void testBuildTableWithDynamicRoleOnly() {
        Set<DiscoveryNodeRole> roles = new HashSet<>();
        String roleName = "test_role";
        DiscoveryNodeRole testRole = DiscoveryNodeRoleGenerator.createDynamicRole(roleName);
        roles.add(testRole);

        testBuildTableWithRoles(roles, (table) -> {
            Map<String, List<Table.Cell>> nodeInfoMap = table.getAsMap();
            List<Table.Cell> cells = nodeInfoMap.get("node.roles");
            assertEquals(1, cells.size());
            assertEquals(roleName, cells.get(0).value);
        });
    }

    public void testBuildTableWithBothBuiltInAndDynamicRoles() {
        Set<DiscoveryNodeRole> roles = new HashSet<>();
        roles.add(DiscoveryNodeRole.DATA_ROLE);
        String roleName = "test_role";
        DiscoveryNodeRole testRole = DiscoveryNodeRoleGenerator.createDynamicRole(roleName);
        roles.add(testRole);

        testBuildTableWithRoles(roles, (table) -> {
            Map<String, List<Table.Cell>> nodeInfoMap = table.getAsMap();
            List<Table.Cell> cells = nodeInfoMap.get("node.roles");
            assertEquals(1, cells.size());
            assertEquals("data,test_role", cells.get(0).value);
        });
    }

    private void testBuildTableWithRoles(Set<DiscoveryNodeRole> roles, Consumer<Table> verificationFunction) {
        ClusterName clusterName = new ClusterName("cluster-1");
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();

        builder.add(new DiscoveryNode("node-1", buildNewFakeTransportAddress(), emptyMap(), roles, Version.CURRENT));
        DiscoveryNodes discoveryNodes = builder.build();
        ClusterState clusterState = mock(ClusterState.class);
        when(clusterState.nodes()).thenReturn(discoveryNodes);

        ClusterStateResponse clusterStateResponse = new ClusterStateResponse(clusterName, clusterState, false);
        NodesInfoResponse nodesInfoResponse = new NodesInfoResponse(clusterName, Collections.emptyList(), Collections.emptyList());
        NodesStatsResponse nodesStatsResponse = new NodesStatsResponse(clusterName, Collections.emptyList(), Collections.emptyList());

        Table table = action.buildTable(false, new FakeRestRequest(), clusterStateResponse, nodesInfoResponse, nodesStatsResponse);

        verificationFunction.accept(table);
    }
}
