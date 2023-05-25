/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

/*
* Modifications Copyright OpenSearch Contributors. See
* GitHub history for details.
*/

package org.opensearch.cluster.node;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.transport.ProtobufTransportAddress;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.node.Node;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.node.NodeRoleSettings.NODE_ROLES_SETTING;

/**
 * A discovery node represents a node that is part of the cluster.
*
* @opensearch.internal
*/
public class ProtobufDiscoveryNode implements ProtobufWriteable, ToXContentFragment {

    static final String COORDINATING_ONLY = "coordinating_only";

    public static boolean nodeRequiresLocalStorage(Settings settings) {
        boolean localStorageEnable = Node.NODE_LOCAL_STORAGE_SETTING.get(settings);
        if (localStorageEnable == false && (isDataNode(settings) || isClusterManagerNode(settings))) {
            // TODO: make this a proper setting validation logic, requiring multi-settings validation
            throw new IllegalArgumentException("storage can not be disabled for cluster-manager and data nodes");
        }
        return localStorageEnable;
    }

    public static boolean hasRole(final Settings settings, final DiscoveryNodeRole role) {
        /*
        * This method can be called before the o.e.n.NodeRoleSettings.NODE_ROLES_SETTING is initialized. We do not want to trigger
        * initialization prematurely because that will bake the default roles before plugins have had a chance to register them. Therefore,
        * to avoid initializing this setting prematurely, we avoid using the actual node roles setting instance here.
        */
        if (settings.hasValue("node.roles")) {
            return settings.getAsList("node.roles").contains(role.roleName());
        } else if (role.legacySetting() != null && settings.hasValue(role.legacySetting().getKey())) {
            return role.legacySetting().get(settings);
        } else {
            return role.isEnabledByDefault(settings);
        }
    }

    public static boolean isClusterManagerNode(Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.MASTER_ROLE) || hasRole(settings, DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
    }

    /** @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #isClusterManagerNode(Settings)} */
    @Deprecated
    public static boolean isMasterNode(Settings settings) {
        return isClusterManagerNode(settings);
    }

    /**
     * Due to the way that plugins may not be available when settings are being initialized,
    * not all roles may be available from a static/initializing context such as a {@link Setting}
    * default value function. In that case, be warned that this may not include all plugin roles.
    */
    public static boolean isDataNode(final Settings settings) {
        return getRolesFromSettings(settings).stream().anyMatch(DiscoveryNodeRole::canContainData);
    }

    public static boolean isIngestNode(Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.INGEST_ROLE);
    }

    public static boolean isRemoteClusterClient(final Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    public static boolean isSearchNode(Settings settings) {
        return hasRole(settings, DiscoveryNodeRole.SEARCH_ROLE);
    }

    private final String nodeName;
    private final String nodeId;
    private final String ephemeralId;
    private final String hostName;
    private final String hostAddress;
    private final ProtobufTransportAddress address;
    private final Map<String, String> attributes;
    private final Version version;
    private final SortedSet<DiscoveryNodeRole> roles;

    /**
     * Creates a new {@link ProtobufDiscoveryNode}
    * <p>
    * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
    * version. it corresponds to the minimum version this opensearch version can communicate with. If a higher version is used
    * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
    * and updated.
    * </p>
    *
    * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
    * @param address          the nodes transport address
    * @param version          the version of the node
    */
    public ProtobufDiscoveryNode(final String id, ProtobufTransportAddress address, Version version) {
        this(id, address, Collections.emptyMap(), DiscoveryNodeRole.BUILT_IN_ROLES, version);
    }

    /**
     * Creates a new {@link ProtobufDiscoveryNode}
    * <p>
    * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
    * version. it corresponds to the minimum version this opensearch version can communicate with. If a higher version is used
    * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
    * and updated.
    * </p>
    *
    * @param id               the nodes unique (persistent) node id. This constructor will auto generate a random ephemeral id.
    * @param address          the nodes transport address
    * @param attributes       node attributes
    * @param roles            node roles
    * @param version          the version of the node
    */
    public ProtobufDiscoveryNode(
        String id,
        ProtobufTransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        this("", id, address, attributes, roles, version);
    }

    /**
     * Creates a new {@link ProtobufDiscoveryNode}
    * <p>
    * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
    * version. it corresponds to the minimum version this opensearch version can communicate with. If a higher version is used
    * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
    * and updated.
    * </p>
    *
    * @param nodeName         the nodes name
    * @param nodeId           the nodes unique persistent id. An ephemeral id will be auto generated.
    * @param address          the nodes transport address
    * @param attributes       node attributes
    * @param roles            node roles
    * @param version          the version of the node
    */
    public ProtobufDiscoveryNode(
        String nodeName,
        String nodeId,
        ProtobufTransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        this(
            nodeName,
            nodeId,
            UUIDs.randomBase64UUID(),
            address.address().getHostString(),
            address.getAddress(),
            address,
            attributes,
            roles,
            version
        );
    }

    /**
     * Creates a new {@link ProtobufDiscoveryNode}.
    * <p>
    * <b>Note:</b> if the version of the node is unknown {@link Version#minimumCompatibilityVersion()} should be used for the current
    * version. it corresponds to the minimum version this opensearch version can communicate with. If a higher version is used
    * the node might not be able to communicate with the remote node. After initial handshakes node versions will be discovered
    * and updated.
    * </p>
    *
    * @param nodeName         the nodes name
    * @param nodeId           the nodes unique persistent id
    * @param ephemeralId      the nodes unique ephemeral id
    * @param hostAddress      the nodes host address
    * @param address          the nodes transport address
    * @param attributes       node attributes
    * @param roles            node roles
    * @param version          the version of the node
    */
    public ProtobufDiscoveryNode(
        String nodeName,
        String nodeId,
        String ephemeralId,
        String hostName,
        String hostAddress,
        ProtobufTransportAddress address,
        Map<String, String> attributes,
        Set<DiscoveryNodeRole> roles,
        Version version
    ) {
        if (nodeName != null) {
            this.nodeName = nodeName.intern();
        } else {
            this.nodeName = "";
        }
        this.nodeId = nodeId.intern();
        this.ephemeralId = ephemeralId.intern();
        this.hostName = hostName.intern();
        this.hostAddress = hostAddress.intern();
        this.address = address;
        if (version == null) {
            this.version = Version.CURRENT;
        } else {
            this.version = version;
        }
        this.attributes = Collections.unmodifiableMap(attributes);
        // verify that no node roles are being provided as attributes
        Predicate<Map<String, String>> predicate = (attrs) -> {
            boolean success = true;
            for (final DiscoveryNodeRole role : ProtobufDiscoveryNode.roleMap.values()) {
                success &= attrs.containsKey(role.roleName()) == false;
                assert success : role.roleName();
            }
            return success;
        };
        assert predicate.test(attributes) : attributes;
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(roles));
    }

    /** Creates a ProtobufDiscoveryNode representing the local node. */
    public static ProtobufDiscoveryNode createLocal(Settings settings, ProtobufTransportAddress publishAddress, String nodeId) {
        Map<String, String> attributes = Node.NODE_ATTRIBUTES.getAsMap(settings);
        Set<DiscoveryNodeRole> roles = getRolesFromSettings(settings);
        return new ProtobufDiscoveryNode(Node.NODE_NAME_SETTING.get(settings), nodeId, publishAddress, attributes, roles, Version.CURRENT);
    }

    /** extract node roles from the given settings */
    public static Set<DiscoveryNodeRole> getRolesFromSettings(final Settings settings) {
        if (NODE_ROLES_SETTING.exists(settings)) {
            validateLegacySettings(settings, roleMap);
            return Collections.unmodifiableSet(new HashSet<>(NODE_ROLES_SETTING.get(settings)));
        } else {
            return roleMap.values().stream().filter(s -> s.isEnabledByDefault(settings)).collect(Collectors.toSet());
        }
    }

    private static void validateLegacySettings(final Settings settings, final Map<String, DiscoveryNodeRole> roleMap) {
        for (final DiscoveryNodeRole role : roleMap.values()) {
            if (role.legacySetting() != null && role.legacySetting().exists(settings)) {
                final String message = String.format(
                    Locale.ROOT,
                    "can not explicitly configure node roles and use legacy role setting [%s]=[%s]",
                    role.legacySetting().getKey(),
                    role.legacySetting().get(settings)
                );
                throw new IllegalArgumentException(message);
            }
        }
    }

    /**
     * Creates a new {@link ProtobufDiscoveryNode} by reading from the stream provided as argument
    * @param in the stream
    * @throws IOException if there is an error while reading from the stream
    */
    public ProtobufDiscoveryNode(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        this.nodeName = in.readString();
        this.nodeId = in.readString();
        this.ephemeralId = in.readString();
        this.hostName = in.readString();
        this.hostAddress = in.readString();
        this.address = new ProtobufTransportAddress(in);
        int size = in.readInt32();
        this.attributes = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            this.attributes.put(in.readString(), in.readString());
        }
        int rolesSize = in.readInt32();
        final Set<DiscoveryNodeRole> roles = new HashSet<>(rolesSize);
        for (int i = 0; i < rolesSize; i++) {
            final String roleName = in.readString();
            final String roleNameAbbreviation = in.readString();
            final boolean canContainData = in.readBool();
            final DiscoveryNodeRole role = roleMap.get(roleName);
            if (role == null) {
                if (protobufStreamInput.getVersion().onOrAfter(Version.V_2_1_0)) {
                    roles.add(new DiscoveryNodeRole.DynamicRole(roleName, roleNameAbbreviation, canContainData));
                } else {
                    roles.add(new DiscoveryNodeRole.UnknownRole(roleName, roleNameAbbreviation, canContainData));
                }
            } else {
                assert roleName.equals(role.roleName()) : "role name [" + roleName + "] does not match role [" + role.roleName() + "]";
                assert roleNameAbbreviation.equals(role.roleNameAbbreviation()) : "role name abbreviation ["
                    + roleName
                    + "] does not match role ["
                    + role.roleNameAbbreviation()
                    + "]";
                roles.add(role);
            }
        }
        this.roles = Collections.unmodifiableSortedSet(new TreeSet<>(roles));
        this.version = Version.readVersionProtobuf(in);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeStringNoTag(nodeName);
        out.writeStringNoTag(nodeId);
        out.writeStringNoTag(ephemeralId);
        out.writeStringNoTag(hostName);
        out.writeStringNoTag(hostAddress);
        address.writeTo(out);
        out.writeInt32NoTag(attributes.size());
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            out.writeStringNoTag(entry.getKey());
            out.writeStringNoTag(entry.getValue());
        }
        out.writeInt32NoTag(roles.size());
        for (final DiscoveryNodeRole role : roles) {
            final DiscoveryNodeRole compatibleRole = role.getCompatibilityRole(protobufStreamOutput.getVersion());
            out.writeStringNoTag(compatibleRole.roleName());
            out.writeStringNoTag(compatibleRole.roleNameAbbreviation());
            out.writeBoolNoTag(compatibleRole.canContainData());
        }
        out.writeInt32NoTag(version.id);
    }

    /**
     * The address that the node can be communicated with.
    */
    public ProtobufTransportAddress getAddress() {
        return address;
    }

    /**
     * The unique id of the node.
    */
    public String getId() {
        return nodeId;
    }

    /**
     * The unique ephemeral id of the node. Ephemeral ids are meant to be attached the life span
    * of a node process. When ever a node is restarted, it's ephemeral id is required to change (while it's {@link #getId()}
    * will be read from the data folder and will remain the same across restarts).
    */
    public String getEphemeralId() {
        return ephemeralId;
    }

    /**
     * The name of the node.
    */
    public String getName() {
        return this.nodeName;
    }

    /**
     * The node attributes.
    */
    public Map<String, String> getAttributes() {
        return this.attributes;
    }

    /**
     * Should this node hold data (shards) or not.
    */
    public boolean isDataNode() {
        return roles.stream().anyMatch(DiscoveryNodeRole::canContainData);
    }

    /**
     * Can this node become cluster-manager or not.
    */
    public boolean isClusterManagerNode() {
        return roles.contains(DiscoveryNodeRole.MASTER_ROLE) || roles.contains(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE);
    }

    /**
     * Can this node become cluster-manager or not.
    *
    * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link #isClusterManagerNode()}
    */
    @Deprecated
    public boolean isMasterNode() {
        return isClusterManagerNode();
    }

    /**
     * Returns a boolean that tells whether this an ingest node or not
    */
    public boolean isIngestNode() {
        return roles.contains(DiscoveryNodeRole.INGEST_ROLE);
    }

    /**
     * Returns whether or not the node can be a remote cluster client.
    *
    * @return true if the node can be a remote cluster client, false otherwise
    */
    public boolean isRemoteClusterClient() {
        return roles.contains(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE);
    }

    /**
     * Returns whether the node is dedicated to provide search capability.
    *
    * @return true if the node contains search role, false otherwise
    */
    public boolean isSearchNode() {
        return roles.contains(DiscoveryNodeRole.SEARCH_ROLE);
    }

    /**
     * Returns a set of all the roles that the node has. The roles are returned in sorted order by the role name.
    * <p>
    * If a node does not have any specific role, the returned set is empty, which means that the node is a coordinating-only node.
    *
    * @return the sorted set of roles
    */
    public Set<DiscoveryNodeRole> getRoles() {
        return roles;
    }

    public Version getVersion() {
        return this.version;
    }

    public String getHostName() {
        return this.hostName;
    }

    public String getHostAddress() {
        return this.hostAddress;
    }

    private static Map<String, DiscoveryNodeRole> rolesToMap(final Stream<DiscoveryNodeRole> roles) {
        return Collections.unmodifiableMap(roles.collect(Collectors.toMap(DiscoveryNodeRole::roleName, Function.identity())));
    }

    private static Map<String, DiscoveryNodeRole> roleMap = rolesToMap(DiscoveryNodeRole.BUILT_IN_ROLES.stream());

    public static DiscoveryNodeRole getRoleFromRoleName(final String roleName) {
        // As we are supporting dynamic role, should make role name case-insensitive to avoid confusion of role name like "Data"/"DATA"
        String lowerCasedRoleName = Objects.requireNonNull(roleName).toLowerCase(Locale.ROOT);
        if (roleMap.containsKey(lowerCasedRoleName)) {
            return roleMap.get(lowerCasedRoleName);
        }
        return new DiscoveryNodeRole.DynamicRole(lowerCasedRoleName, lowerCasedRoleName, false);
    }

    public static Set<DiscoveryNodeRole> getPossibleRoles() {
        return Collections.unmodifiableSet(new HashSet<>(roleMap.values()));
    }

    public static void setAdditionalRoles(final Set<DiscoveryNodeRole> additionalRoles) {
        assert additionalRoles.stream().allMatch(r -> r.legacySetting() == null || r.legacySetting().isDeprecated()) : additionalRoles;
        final Map<String, DiscoveryNodeRole> roleNameToPossibleRoles = rolesToMap(
            Stream.concat(DiscoveryNodeRole.BUILT_IN_ROLES.stream(), additionalRoles.stream())
        );
        // collect the abbreviation names into a map to ensure that there are not any duplicate abbreviations
        final Map<String, DiscoveryNodeRole> roleNameAbbreviationToPossibleRoles = Collections.unmodifiableMap(
            roleNameToPossibleRoles.values()
                .stream()
                .collect(Collectors.toMap(DiscoveryNodeRole::roleNameAbbreviation, Function.identity()))
        );
        assert roleNameToPossibleRoles.size() == roleNameAbbreviationToPossibleRoles.size() : "roles by name ["
            + roleNameToPossibleRoles
            + "], roles by name abbreviation ["
            + roleNameAbbreviationToPossibleRoles
            + "]";
        roleMap = roleNameToPossibleRoles;
    }

    /**
     * Load the deprecated {@link DiscoveryNodeRole#MASTER_ROLE}.
    * Master role is not added into BUILT_IN_ROLES, because {@link #setAdditionalRoles(Set)} check role name abbreviation duplication,
    * and CLUSTER_MANAGER_ROLE has the same abbreviation name with MASTER_ROLE.
    */
    public static void setDeprecatedMasterRole() {
        final Map<String, DiscoveryNodeRole> modifiableRoleMap = new HashMap<>(roleMap);
        modifiableRoleMap.put(DiscoveryNodeRole.MASTER_ROLE.roleName(), DiscoveryNodeRole.MASTER_ROLE);
        roleMap = Collections.unmodifiableMap(modifiableRoleMap);
    }

    public static Set<String> getPossibleRoleNames() {
        return roleMap.keySet();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getId());
        builder.field("name", getName());
        builder.field("ephemeral_id", getEphemeralId());
        builder.field("transport_address", getAddress().toString());

        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : attributes.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

}
