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

public class NodeRoleSettingsTests extends OpenSearchTestCase {

    /**
     * Validate cluster_manager role and master role can not coexist in a node.
     * Remove the test after removing MASTER_ROLE.
     */
    public void testClusterManagerAndMasterRoleCanNotCoexist() {
        // It's used to add MASTER_ROLE into 'roleMap', because MASTER_ROLE is removed from DiscoveryNodeRole.BUILT_IN_ROLES in 2.0.
        DiscoveryNode.setAdditionalRoles(Collections.emptySet());
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "cluster_manager, master").build();
        Exception exception = expectThrows(IllegalArgumentException.class, () -> NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings));
        assertThat(exception.getMessage(), containsString("The two roles can not be assigned together to a node"));
    }

    /**
     * Validate cluster_manager role and data role can coexist in a node. The test is added along with validateRole().
     */
    public void testClusterManagerAndDataNodeRoles() {
        Settings roleSettings = Settings.builder().put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), "cluster_manager, data").build();
        List<DiscoveryNodeRole> actualNodeRoles = NodeRoleSettings.NODE_ROLES_SETTING.get(roleSettings);
        List<DiscoveryNodeRole> expectedNodeRoles = Arrays.asList(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE, DiscoveryNodeRole.DATA_ROLE);
        assertEquals(expectedNodeRoles, actualNodeRoles);
    }
}
