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

package org.opensearch.test;

import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.settings.Settings;
import org.opensearch.node.NodeRoleSettings;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Utility methods for creating {@link Settings} instances defining a set of {@link DiscoveryNodeRole}.
 */
public class NodeRoles {

    public static Settings onlyRole(final DiscoveryNodeRole role) {
        return onlyRole(Settings.EMPTY, role);
    }

    public static Settings onlyRole(final Settings settings, final DiscoveryNodeRole role) {
        return onlyRoles(settings, Collections.singleton(role));
    }

    public static Settings onlyRoles(final Set<DiscoveryNodeRole> roles) {
        return onlyRoles(Settings.EMPTY, roles);
    }

    public static Settings onlyRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        return Settings.builder()
            .put(settings)
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                Collections.unmodifiableList(roles.stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toList()))
            )
            .build();
    }

    public static Settings removeRoles(final Set<DiscoveryNodeRole> roles) {
        return removeRoles(Settings.EMPTY, roles);
    }

    public static Settings removeRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder().put(settings);
        builder.putList(
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            Collections.unmodifiableList(
                NodeRoleSettings.NODE_ROLES_SETTING.get(settings)
                    .stream()
                    .filter(r -> roles.contains(r) == false)
                    // TODO: Remove the below filter after removing MASTER_ROLE.
                    // It's used to remove both CLUSTER_MANAGER_ROLE and MASTER_ROLE, when requested to remove either.
                    .filter(
                        roles.contains(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE) || roles.contains(DiscoveryNodeRole.MASTER_ROLE)
                            ? r -> !r.isClusterManager()
                            : r -> true
                    )
                    .map(DiscoveryNodeRole::roleName)
                    .collect(Collectors.toList())
            )
        );
        return builder.build();
    }

    public static Settings addRoles(final Set<DiscoveryNodeRole> roles) {
        return addRoles(Settings.EMPTY, roles);
    }

    public static Settings addRoles(final Settings settings, final Set<DiscoveryNodeRole> roles) {
        final Settings.Builder builder = Settings.builder().put(settings);
        builder.putList(
            NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
            Collections.unmodifiableList(
                Stream.concat(NodeRoleSettings.NODE_ROLES_SETTING.get(settings).stream(), roles.stream())
                    .map(DiscoveryNodeRole::roleName)
                    .distinct()
                    .collect(Collectors.toList())
            )
        );
        return builder.build();
    }

    public static Settings noRoles() {
        return noRoles(Settings.EMPTY);
    }

    public static Settings noRoles(final Settings settings) {
        return Settings.builder().put(settings).putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), Collections.emptyList()).build();
    }

    public static Settings dataNode() {
        return dataNode(Settings.EMPTY);
    }

    public static Settings dataNode(final Settings settings) {
        return addRoles(settings, Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    }

    public static Settings dataOnlyNode() {
        return dataOnlyNode(Settings.EMPTY);
    }

    public static Settings dataOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.DATA_ROLE);
    }

    public static Settings nonDataNode() {
        return nonDataNode(Settings.EMPTY);
    }

    public static Settings nonDataNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    }

    public static Settings ingestNode() {
        return ingestNode(Settings.EMPTY);
    }

    public static Settings ingestNode(final Settings settings) {
        return addRoles(settings, Collections.singleton(DiscoveryNodeRole.INGEST_ROLE));
    }

    public static Settings ingestOnlyNode() {
        return ingestOnlyNode(Settings.EMPTY);
    }

    public static Settings ingestOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.INGEST_ROLE);
    }

    public static Settings nonIngestNode() {
        return nonIngestNode(Settings.EMPTY);
    }

    public static Settings nonIngestNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.INGEST_ROLE));
    }

    public static Settings clusterManagerNode() {
        return clusterManagerNode(Settings.EMPTY);
    }

    public static Settings clusterManagerNode(final Settings settings) {
        return addRoles(settings, Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE));
    }

    public static Settings clusterManagerOnlyNode() {
        return clusterManagerOnlyNode(Settings.EMPTY);
    }

    public static Settings clusterManagerOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
    }

    public static Settings nonClusterManagerNode() {
        return nonClusterManagerNode(Settings.EMPTY);
    }

    public static Settings nonClusterManagerNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE));
    }

    public static Settings remoteClusterClientNode() {
        return remoteClusterClientNode(Settings.EMPTY);
    }

    public static Settings remoteClusterClientNode(final Settings settings) {
        return addRoles(settings, Collections.singleton(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public static Settings nonRemoteClusterClientNode() {
        return nonRemoteClusterClientNode(Settings.EMPTY);
    }

    public static Settings nonRemoteClusterClientNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
    }

    public static Settings warmNode() {
        return warmNode(Settings.EMPTY);
    }

    public static Settings warmNode(final Settings settings) {
        return addRoles(settings, Collections.singleton(DiscoveryNodeRole.WARM_ROLE));
    }

    public static Settings nonWarmNode() {
        return nonWarmNode(Settings.EMPTY);
    }

    public static Settings nonWarmNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.WARM_ROLE));
    }

    public static Settings searchOnlyNode() {
        return searchOnlyNode(Settings.EMPTY);
    }

    public static Settings searchOnlyNode(final Settings settings) {
        return onlyRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    public static Settings nonSearchNode() {
        return nonSearchNode(Settings.EMPTY);
    }

    public static Settings nonSearchNode(final Settings settings) {
        return removeRoles(settings, Collections.singleton(DiscoveryNodeRole.SEARCH_ROLE));
    }
}
