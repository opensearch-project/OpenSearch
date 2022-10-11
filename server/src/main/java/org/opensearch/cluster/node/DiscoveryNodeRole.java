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

package org.opensearch.cluster.node;

import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.transport.RemoteClusterService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Represents a node role.
 *
 * @opensearch.internal
 */
public abstract class DiscoveryNodeRole implements Comparable<DiscoveryNodeRole> {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(DiscoveryNodeRole.class);
    public static final String MASTER_ROLE_DEPRECATION_MESSAGE =
        "Assigning [master] role in setting [node.roles] is deprecated. To promote inclusive language, please use [cluster_manager] role instead.";
    private final String roleName;

    /**
     * The name of the role.
     *
     * @return the role name
     */
    public final String roleName() {
        return roleName;
    }

    private final String roleNameAbbreviation;

    /**
     * The abbreviation of the name of the role. This is used in the cat nodes API to display an abbreviated version of the name of the
     * role.
     *
     * @return the role name abbreviation
     */
    public final String roleNameAbbreviation() {
        return roleNameAbbreviation;
    }

    private final boolean canContainData;

    /**
     * Indicates whether a node with this role can contain data.
     *
     * @return true if a node with this role can contain data, otherwise false
     */
    public final boolean canContainData() {
        return canContainData;
    }

    private final boolean isKnownRole;

    private final boolean isDynamicRole;

    /**
     * Whether this role is known by this node, or is an {@link DiscoveryNodeRole.UnknownRole}.
     */
    public final boolean isKnownRole() {
        return isKnownRole;
    }

    public final boolean isDynamicRole() {
        return isDynamicRole;
    }

    public boolean isEnabledByDefault(final Settings settings) {
        return legacySetting() != null && legacySetting().get(settings);
    }

    protected DiscoveryNodeRole(final String roleName, final String roleNameAbbreviation) {
        this(roleName, roleNameAbbreviation, false);
    }

    protected DiscoveryNodeRole(final String roleName, final String roleNameAbbreviation, final boolean canContainData) {
        this(true, false, roleName, roleNameAbbreviation, canContainData);
    }

    private DiscoveryNodeRole(
        final boolean isKnownRole,
        final boolean isDynamicRole,
        final String roleName,
        final String roleNameAbbreviation,
        final boolean canContainData
    ) {
        this.isKnownRole = isKnownRole;
        this.isDynamicRole = isDynamicRole;
        // As we are supporting dynamic role, should make role name case-insensitive to avoid confusion of role name like "Data"/"DATA"
        this.roleName = Objects.requireNonNull(roleName).toLowerCase(Locale.ROOT);
        this.roleNameAbbreviation = Objects.requireNonNull(roleNameAbbreviation).toLowerCase(Locale.ROOT);
        this.canContainData = canContainData;
    }

    public abstract Setting<Boolean> legacySetting();

    /**
     * When serializing a {@link DiscoveryNodeRole}, the role may not be available to nodes of
     * previous versions, where the role had not yet been added. This method allows overriding
     * the role that should be serialized when communicating to versions prior to the introduction
     * of the discovery node role.
     */
    public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
        return this;
    }

    /**
     * Validate the role is compatible with the other roles in the list, when assigning the list of roles to a node.
     * An {@link IllegalArgumentException} is expected to be thrown, if the role can't coexist with the other roles.
     * @param roles a {@link List} of {@link DiscoveryNodeRole} that a node is going to have
     */
    public void validateRole(List<DiscoveryNodeRole> roles) {};

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DiscoveryNodeRole that = (DiscoveryNodeRole) o;
        return roleName.equals(that.roleName)
            && roleNameAbbreviation.equals(that.roleNameAbbreviation)
            && canContainData == that.canContainData
            && isKnownRole == that.isKnownRole
            && isDynamicRole == that.isDynamicRole;
    }

    @Override
    public final int hashCode() {
        return Objects.hash(isKnownRole, isDynamicRole, roleName(), roleNameAbbreviation(), canContainData());
    }

    @Override
    public final int compareTo(final DiscoveryNodeRole o) {
        return roleName.compareTo(o.roleName);
    }

    @Override
    public final String toString() {
        return "DiscoveryNodeRole{"
            + "roleName='"
            + roleName
            + '\''
            + ", roleNameAbbreviation='"
            + roleNameAbbreviation
            + '\''
            + ", canContainData="
            + canContainData
            + (isKnownRole ? "" : ", isKnownRole=false")
            + (isDynamicRole ? "" : ", isDynamicRole=false")
            + '}';
    }

    /**
     * Represents the role for a data node.
     */
    public static final DiscoveryNodeRole DATA_ROLE = new DiscoveryNodeRole("data", "d", true) {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.opensearch.node.Node
            return Setting.boolSetting("node.data", true, Property.Deprecated, Property.NodeScope);
        }

    };

    /**
     * Represents the role for an ingest node.
     */
    public static final DiscoveryNodeRole INGEST_ROLE = new DiscoveryNodeRole("ingest", "i") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.opensearch.node.Node
            return Setting.boolSetting("node.ingest", true, Property.Deprecated, Property.NodeScope);
        }

    };

    /**
     * Represents the role for a cluster-manager-eligible node.
     * @deprecated As of 2.0, because promoting inclusive language, replaced by {@link #CLUSTER_MANAGER_ROLE}
     */
    @Deprecated
    public static final DiscoveryNodeRole MASTER_ROLE = new DiscoveryNodeRole("master", "m") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.opensearch.node.Node
            // As of 2.0, set the default value to 'false', so that MASTER_ROLE isn't added as a default value of NODE_ROLES_SETTING
            return Setting.boolSetting("node.master", false, Property.Deprecated, Property.NodeScope);
        }

        @Override
        public void validateRole(List<DiscoveryNodeRole> roles) {
            deprecationLogger.deprecate("node_role_master", MASTER_ROLE_DEPRECATION_MESSAGE);
        }

    };

    /**
     * Represents the role for a cluster-manager-eligible node.
     */
    public static final DiscoveryNodeRole CLUSTER_MANAGER_ROLE = new DiscoveryNodeRole("cluster_manager", "m") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.opensearch.node.Node
            return Setting.boolSetting("node.master", true, Property.Deprecated, Property.NodeScope);
        }

        @Override
        public DiscoveryNodeRole getCompatibilityRole(Version nodeVersion) {
            if (nodeVersion.onOrAfter(Version.V_2_0_0)) {
                return this;
            } else {
                return DiscoveryNodeRole.MASTER_ROLE;
            }
        }

        @Override
        public void validateRole(List<DiscoveryNodeRole> roles) {
            if (roles.contains(DiscoveryNodeRole.MASTER_ROLE)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "The two roles [%s, %s] can not be assigned together to a node. %s",
                        DiscoveryNodeRole.MASTER_ROLE.roleName(),
                        DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(),
                        MASTER_ROLE_DEPRECATION_MESSAGE
                    )
                );
            }
        }

    };

    public static final DiscoveryNodeRole REMOTE_CLUSTER_CLIENT_ROLE = new DiscoveryNodeRole("remote_cluster_client", "r") {

        @Override
        public Setting<Boolean> legacySetting() {
            // copy the setting here so we can mark it private in org.opensearch.node.Node
            return Setting.boolSetting(
                "node.remote_cluster_client",
                RemoteClusterService.ENABLE_REMOTE_CLUSTERS,
                Property.Deprecated,
                Property.NodeScope
            );
        }

    };

    /**
     * Represents the role for a search node, which is dedicated to provide search capability.
     */
    public static final DiscoveryNodeRole SEARCH_ROLE = new DiscoveryNodeRole("search", "s", true) {

        @Override
        public Setting<Boolean> legacySetting() {
            // search role is added in 2.4 so doesn't need to configure legacy setting
            return null;
        }

    };

    /**
     * The built-in node roles.
     */
    public static SortedSet<DiscoveryNodeRole> BUILT_IN_ROLES = Collections.unmodifiableSortedSet(
        new TreeSet<>(Arrays.asList(DATA_ROLE, INGEST_ROLE, CLUSTER_MANAGER_ROLE, REMOTE_CLUSTER_CLIENT_ROLE, SEARCH_ROLE))
    );

    /**
     * The version that {@link #REMOTE_CLUSTER_CLIENT_ROLE} is introduced. Nodes before this version do not have that role even
     * they can connect to remote clusters.
     */
    public static final Version REMOTE_CLUSTER_CLIENT_ROLE_VERSION = LegacyESVersion.fromString("7.8.0");

    static SortedSet<DiscoveryNodeRole> LEGACY_ROLES = Collections.unmodifiableSortedSet(
        new TreeSet<>(Arrays.asList(DATA_ROLE, INGEST_ROLE, MASTER_ROLE))
    );

    /**
     * Represents an unknown role. This can occur if a newer version adds a role that an older version does not know about, or a newer
     * version removes a role that an older version knows about.
     */
    static class UnknownRole extends DiscoveryNodeRole {

        /**
         * Construct an unknown role with the specified role name and role name abbreviation.
         *
         * @param roleName             the role name
         * @param roleNameAbbreviation the role name abbreviation
         * @param canContainData       whether or not nodes with the role can contain data
         */
        UnknownRole(final String roleName, final String roleNameAbbreviation, final boolean canContainData) {
            super(false, false, roleName, roleNameAbbreviation, canContainData);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // since this setting is not registered, it will always return false when testing if the local node has the role
            assert false;
            return Setting.boolSetting("node. " + roleName(), false, Setting.Property.NodeScope);
        }

    }

    /**
     * Represents a dynamic role. This can occur if a custom role that not in {@link DiscoveryNodeRole#BUILT_IN_ROLES} added for a node.
     * Some plugin can support extension function with dynamic roles. For example, ML plugin may run machine learning tasks on nodes
     * with "ml" dynamic role.
     */
    static class DynamicRole extends DiscoveryNodeRole {

        /**
         * Construct a dynamic role with the specified role name and role name abbreviation.
         *
         * @param roleName             the role name
         * @param roleNameAbbreviation the role name abbreviation
         * @param canContainData       whether or not nodes with the role can contain data
         */
        DynamicRole(final String roleName, final String roleNameAbbreviation, final boolean canContainData) {
            super(false, true, roleName, roleNameAbbreviation, canContainData);
        }

        @Override
        public Setting<Boolean> legacySetting() {
            // return null as dynamic role has no legacy setting
            return null;
        }

    }

    /**
     * Check if the role is {@link #CLUSTER_MANAGER_ROLE} or {@link #MASTER_ROLE}.
     * @deprecated As of 2.0, because promoting inclusive language. MASTER_ROLE is deprecated.
     * @return true if the node role is{@link #CLUSTER_MANAGER_ROLE} or {@link #MASTER_ROLE}
     */
    @Deprecated
    public boolean isClusterManager() {
        return this.equals(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE) || this.equals(DiscoveryNodeRole.MASTER_ROLE);
    }
}
