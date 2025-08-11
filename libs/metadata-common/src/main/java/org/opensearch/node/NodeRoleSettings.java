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

package org.opensearch.node;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Settings for a node role
 *
 * @opensearch.internal
 */
public class NodeRoleSettings {

    public static final Setting<List<DiscoveryNodeRole>> NODE_ROLES_SETTING = Setting.listSetting(
        "node.roles",
        null,
        DiscoveryNode::getRoleFromRoleName,
        settings -> DiscoveryNode.getPossibleRoles()
            .stream()
            .filter(role -> role.isEnabledByDefault(settings))
            .map(DiscoveryNodeRole::roleName)
            .collect(Collectors.toList()),
        roles -> {
            for (DiscoveryNodeRole role : roles) {
                role.validateRole(roles);
            }
        },
        Property.NodeScope
    );

}
