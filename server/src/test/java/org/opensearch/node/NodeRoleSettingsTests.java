/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;

public class NodeRoleSettingsTests extends OpenSearchTestCase {

    /**
     * Validate cluster_manager role and master role can not coexist in a node.
     * Remove the test after removing MASTER_ROLE.
     */
    public void testClusterManagerAndMasterRoleCanNotCoexist() {
        DiscoveryNode.setDeprecatedMasterRole();
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "cluster_manager, master").build();
        Exception exception = expectThrows(IllegalArgumentException.class, () -> NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings));
        assertThat(exception.getMessage(), containsString("[master, cluster_manager] can not be assigned together to a node"));
    }

    /**
     * Validate cluster_manager role and data role can coexist in a node. The test is added along with validateRole().
     */
    public void testClusterManagerAndDataNodeRoles() {
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "cluster_manager, data").build();
        assertEquals(
            Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings)
        );
    }

    /**
     * Validate search role cannot coexist with any other role on a node.
     */
    public void testSearchRoleCannotCoExistWithAnyOtherRole() {
        DiscoveryNode.setDeprecatedMasterRole();
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "search, test_role").build();
        Exception exception = expectThrows(IllegalArgumentException.class, () -> NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings));
        assertThat(exception.getMessage(), containsString("search role cannot be combined with any other role on a node."));
    }

    /**
     * Validate setting master role will result a deprecation message.
     * Remove the test after removing MASTER_ROLE.
     */
    public void testMasterRoleDeprecationMessage() {
        DiscoveryNode.setDeprecatedMasterRole();
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "master").build();
        assertEquals(Collections.singletonList(DiscoveryNodeRole.MASTER_ROLE), NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings));
        assertWarnings(DiscoveryNodeRole.MASTER_ROLE_DEPRECATION_MESSAGE);
    }

    public void testUnknownNodeRoleAndBuiltInRoleCanCoexist() {
        String testRole = "test_role";
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "data, " + testRole).build();
        List<DiscoveryNodeRole> nodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings);
        assertEquals(2, nodeRoles.size());
        assertEquals(DiscoveryNodeRole.DATA_ROLE, nodeRoles.get(0));
        assertEquals(testRole, nodeRoles.get(1).roleName());
        assertEquals(testRole, nodeRoles.get(1).roleNameAbbreviation());
    }

    public void testUnknownNodeRoleOnly() {
        String testRole = "test_role";
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), testRole).build();
        List<DiscoveryNodeRole> nodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings);
        assertEquals(1, nodeRoles.size());
        assertEquals(testRole, nodeRoles.get(0).roleName());
        assertEquals(testRole, nodeRoles.get(0).roleNameAbbreviation());
    }

    public void testNodeRolesFromEnvironmentVariables() {
        Settings roleSettings = Settings.builder()
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "${node.roles.test}")
            .replacePropertyPlaceholders()
            .build();
        List<DiscoveryNodeRole> nodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings);
        assertThat(nodeRoles, empty());
    }
}
