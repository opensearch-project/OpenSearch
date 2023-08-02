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

package org.opensearch.cluster;

import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNode;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.VersionedNamedWriteable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.Discovery;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.opensearch.cluster.coordination.Coordinator.ZEN1_BWC_TERM;

/**
 * Represents the current state of the cluster.
 * <p>
 * The cluster state object is immutable with the exception of the {@link RoutingNodes} structure, which is
 * built on demand from the {@link RoutingTable}.
 * The cluster state can be updated only on the cluster-manager node. All updates are performed by on a
 * single thread and controlled by the {@link ClusterService}. After every update the
 * {@link Discovery#publish} method publishes a new version of the cluster state to all other nodes in the
 * cluster. The actual publishing mechanism is delegated to the {@link Discovery#publish} method and depends on
 * the type of discovery.
 * <p>
 * The cluster state implements the {@link Diffable} interface in order to support publishing of cluster state
 * differences instead of the entire state on each change. The publishing mechanism should only send differences
 * to a node if this node was present in the previous version of the cluster state. If a node was
 * not present in the previous version of the cluster state, this node is unlikely to have the previous cluster
 * state version and should be sent a complete version. In order to make sure that the differences are applied to the
 * correct version of the cluster state, each cluster state version update generates {@link #stateUUID} that uniquely
 * identifies this version of the state. This uuid is verified by the {@link ClusterStateDiff#apply} method to
 * make sure that the correct diffs are applied. If uuids don’t match, the {@link ClusterStateDiff#apply} method
 * throws the {@link IncompatibleClusterStateVersionException}, which causes the publishing mechanism to send
 * a full version of the cluster state to the node on which this exception was thrown.
 *
 * @opensearch.internal
 */
public class ClusterState implements ToXContentFragment, Diffable<ClusterState> {

    public static final ClusterState EMPTY_STATE = builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();

    /**
     * An interface that implementors use when a class requires a client to maybe have a feature.
     *
     * @opensearch.internal
     */
    public interface FeatureAware {

        /**
         * An optional feature that is required for the client to have.
         *
         * @return an empty optional if no feature is required otherwise a string representing the required feature
         */
        default Optional<String> getRequiredFeature() {
            return Optional.empty();
        }

        /**
         * Tests whether the custom should be serialized. The criterion is that
         * the output stream must be at least the minimum supported version of the custom.
         * <p>
         * That is, we only serialize customs to clients than can understand the custom based on the version of the client.
         *
         * @param out    the output stream
         * @param custom the custom to serialize
         * @param <T>    the type of the custom
         * @return true if the custom should be serialized and false otherwise
         */
        static <T extends VersionedNamedWriteable & FeatureAware> boolean shouldSerialize(final StreamOutput out, final T custom) {
            return out.getVersion().onOrAfter(custom.getMinimalSupportedVersion());
        }

    }

    /**
     * Custom cluster state.
     *
     * @opensearch.internal
     */
    public interface Custom extends NamedDiffable<Custom>, ToXContentFragment, FeatureAware {

        /**
         * Returns <code>true</code> iff this {@link Custom} is private to the cluster and should never be send to a client.
         * The default is <code>false</code>;
         */
        default boolean isPrivate() {
            return false;
        }

    }

    private static final NamedDiffableValueSerializer<Custom> CUSTOM_VALUE_SERIALIZER = new NamedDiffableValueSerializer<>(Custom.class);

    public static final String UNKNOWN_UUID = "_na_";

    public static final long UNKNOWN_VERSION = -1;

    private final long version;

    private final String stateUUID;

    private final RoutingTable routingTable;

    private final DiscoveryNodes nodes;

    private final Metadata metadata;

    private final ClusterBlocks blocks;

    private final Map<String, Custom> customs;

    private final ClusterName clusterName;

    private final boolean wasReadFromDiff;

    private final int minimumClusterManagerNodesOnPublishingClusterManager;

    // built on demand
    private volatile RoutingNodes routingNodes;

    public ClusterState(long version, String stateUUID, ClusterState state) {
        this(
            state.clusterName,
            version,
            stateUUID,
            state.metadata(),
            state.routingTable(),
            state.nodes(),
            state.blocks(),
            state.customs(),
            -1,
            false
        );
    }

    public ClusterState(
        ClusterName clusterName,
        long version,
        String stateUUID,
        Metadata metadata,
        RoutingTable routingTable,
        DiscoveryNodes nodes,
        ClusterBlocks blocks,
        final Map<String, Custom> customs,
        int minimumClusterManagerNodesOnPublishingClusterManager,
        boolean wasReadFromDiff
    ) {
        this.version = version;
        this.stateUUID = stateUUID;
        this.clusterName = clusterName;
        this.metadata = metadata;
        this.routingTable = routingTable;
        this.nodes = nodes;
        this.blocks = blocks;
        this.customs = Collections.unmodifiableMap(customs);
        this.minimumClusterManagerNodesOnPublishingClusterManager = minimumClusterManagerNodesOnPublishingClusterManager;
        this.wasReadFromDiff = wasReadFromDiff;
    }

    public long term() {
        return coordinationMetadata().term();
    }

    public long version() {
        return this.version;
    }

    public long getVersion() {
        return version();
    }

    public long getVersionOrMetadataVersion() {
        // When following a Zen1 cluster-manager, the cluster state version is not guaranteed to increase,
        // so instead it is preferable to use the metadata version to determine the freshest node.
        // However when following a Zen2 cluster-manager the cluster state version should be used.
        return term() == ZEN1_BWC_TERM ? metadata().version() : version();
    }

    /**
     * This stateUUID is automatically generated for for each version of cluster state. It is used to make sure that
     * we are applying diffs to the right previous state.
     */
    public String stateUUID() {
        return this.stateUUID;
    }

    public DiscoveryNodes nodes() {
        return this.nodes;
    }

    public DiscoveryNodes getNodes() {
        return nodes();
    }

    public Metadata metadata() {
        return this.metadata;
    }

    public Metadata getMetadata() {
        return metadata();
    }

    public CoordinationMetadata coordinationMetadata() {
        return metadata.coordinationMetadata();
    }

    public RoutingTable routingTable() {
        return routingTable;
    }

    public RoutingTable getRoutingTable() {
        return routingTable();
    }

    public ClusterBlocks blocks() {
        return this.blocks;
    }

    public ClusterBlocks getBlocks() {
        return blocks;
    }

    public Map<String, Custom> customs() {
        return this.customs;
    }

    public Map<String, Custom> getCustoms() {
        return this.customs;
    }

    public <T extends Custom> T custom(String type) {
        return (T) customs.get(type);
    }

    @SuppressWarnings("unchecked")
    public <T extends Custom> T custom(String type, T defaultValue) {
        return (T) customs.getOrDefault(type, defaultValue);
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return coordinationMetadata().getLastAcceptedConfiguration();
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return coordinationMetadata().getLastCommittedConfiguration();
    }

    public Set<VotingConfigExclusion> getVotingConfigExclusions() {
        return coordinationMetadata().getVotingConfigExclusions();
    }

    /**
     * Returns a built (on demand) routing nodes view of the routing table.
     */
    public RoutingNodes getRoutingNodes() {
        if (routingNodes != null) {
            return routingNodes;
        }
        routingNodes = new RoutingNodes(this);
        return routingNodes;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        final String TAB = "   ";
        sb.append("cluster uuid: ")
            .append(metadata.clusterUUID())
            .append(" [committed: ")
            .append(metadata.clusterUUIDCommitted())
            .append("]")
            .append("\n");
        sb.append("version: ").append(version).append("\n");
        sb.append("state uuid: ").append(stateUUID).append("\n");
        sb.append("from_diff: ").append(wasReadFromDiff).append("\n");
        sb.append("meta data version: ").append(metadata.version()).append("\n");
        sb.append(TAB).append("coordination_metadata:\n");
        sb.append(TAB).append(TAB).append("term: ").append(coordinationMetadata().term()).append("\n");
        sb.append(TAB)
            .append(TAB)
            .append("last_committed_config: ")
            .append(coordinationMetadata().getLastCommittedConfiguration())
            .append("\n");
        sb.append(TAB)
            .append(TAB)
            .append("last_accepted_config: ")
            .append(coordinationMetadata().getLastAcceptedConfiguration())
            .append("\n");
        sb.append(TAB).append(TAB).append("voting tombstones: ").append(coordinationMetadata().getVotingConfigExclusions()).append("\n");
        for (IndexMetadata indexMetadata : metadata) {
            sb.append(TAB).append(indexMetadata.getIndex());
            sb.append(": v[")
                .append(indexMetadata.getVersion())
                .append("], mv[")
                .append(indexMetadata.getMappingVersion())
                .append("], sv[")
                .append(indexMetadata.getSettingsVersion())
                .append("], av[")
                .append(indexMetadata.getAliasesVersion())
                .append("]\n");
            for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                sb.append(TAB).append(TAB).append(shard).append(": ");
                sb.append("p_term [").append(indexMetadata.primaryTerm(shard)).append("], ");
                sb.append("isa_ids ").append(indexMetadata.inSyncAllocationIds(shard)).append("\n");
            }
        }
        if (metadata.customs().isEmpty() == false) {
            sb.append("metadata customs:\n");
            for (final Map.Entry<String, Metadata.Custom> cursor : metadata.customs().entrySet()) {
                final String type = cursor.getKey();
                final Metadata.Custom custom = cursor.getValue();
                sb.append(TAB).append(type).append(": ").append(custom);
            }
            sb.append("\n");
        }
        sb.append(blocks());
        sb.append(nodes());
        sb.append(routingTable());
        sb.append(getRoutingNodes());
        if (customs.isEmpty() == false) {
            sb.append("customs:\n");
            for (final Map.Entry<String, Custom> cursor : customs.entrySet()) {
                final String type = cursor.getKey();
                final Custom custom = cursor.getValue();
                sb.append(TAB).append(type).append(": ").append(custom);
            }
        }
        return sb.toString();
    }

    /**
     * a cluster state supersedes another state if they are from the same cluster-manager and the version of this state is higher than that of the
     * other state.
     * <p>
     * In essence that means that all the changes from the other cluster state are also reflected by the current one
     */
    public boolean supersedes(ClusterState other) {
        return this.nodes().getClusterManagerNodeId() != null
            && this.nodes().getClusterManagerNodeId().equals(other.nodes().getClusterManagerNodeId())
            && this.version() > other.version();

    }

    /**
     * Metrics for cluster state.
     *
     * @opensearch.internal
     */
    public enum Metric {
        VERSION("version"),

        /**
         * @deprecated As of 2.0, because promoting inclusive language, replaced by {@link #CLUSTER_MANAGER_NODE}
         */
        @Deprecated
        MASTER_NODE("master_node"),
        CLUSTER_MANAGER_NODE("cluster_manager_node"),
        BLOCKS("blocks"),
        NODES("nodes"),
        METADATA("metadata"),
        ROUTING_TABLE("routing_table"),
        ROUTING_NODES("routing_nodes"),
        CUSTOMS("customs");

        private static Map<String, Metric> valueToEnum;

        static {
            valueToEnum = new HashMap<>();
            for (Metric metric : Metric.values()) {
                valueToEnum.put(metric.value, metric);
            }
        }

        private final String value;

        Metric(String value) {
            this.value = value;
        }

        public static EnumSet<Metric> parseString(String param, boolean ignoreUnknown) {
            String[] metrics = Strings.splitStringByCommaToArray(param);
            EnumSet<Metric> result = EnumSet.noneOf(Metric.class);
            for (String metric : metrics) {
                if ("_all".equals(metric)) {
                    result = EnumSet.allOf(Metric.class);
                    break;
                }
                Metric m = valueToEnum.get(metric);
                if (m == null) {
                    if (!ignoreUnknown) {
                        throw new IllegalArgumentException("Unknown metric [" + metric + "]");
                    }
                } else {
                    result.add(m);
                }
            }
            return result;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        EnumSet<Metric> metrics = Metric.parseString(params.param("metric", "_all"), true);

        // always provide the cluster_uuid as part of the top-level response (also part of the metadata response)
        builder.field("cluster_uuid", metadata().clusterUUID());

        if (metrics.contains(Metric.VERSION)) {
            builder.field("version", version);
            builder.field("state_uuid", stateUUID);
        }

        if (metrics.contains(Metric.MASTER_NODE)) {
            builder.field("master_node", nodes().getClusterManagerNodeId());
        }

        // Value of the field is identical with the above, and aims to replace the above field.
        if (metrics.contains(Metric.CLUSTER_MANAGER_NODE)) {
            builder.field("cluster_manager_node", nodes().getClusterManagerNodeId());
        }

        if (metrics.contains(Metric.BLOCKS)) {
            builder.startObject("blocks");

            if (blocks().global().isEmpty() == false) {
                builder.startObject("global");
                for (ClusterBlock block : blocks().global()) {
                    block.toXContent(builder, params);
                }
                builder.endObject();
            }

            if (blocks().indices().isEmpty() == false) {
                builder.startObject("indices");
                for (final Map.Entry<String, Set<ClusterBlock>> entry : blocks().indices().entrySet()) {
                    builder.startObject(entry.getKey());
                    for (ClusterBlock block : entry.getValue()) {
                        block.toXContent(builder, params);
                    }
                    builder.endObject();
                }
                builder.endObject();
            }

            builder.endObject();
        }

        // nodes
        if (metrics.contains(Metric.NODES)) {
            builder.startObject("nodes");
            for (DiscoveryNode node : nodes) {
                node.toXContent(builder, params);
            }
            builder.endObject();
        }

        // meta data
        if (metrics.contains(Metric.METADATA)) {
            metadata.toXContent(builder, params);
        }

        // routing table
        if (metrics.contains(Metric.ROUTING_TABLE)) {
            builder.startObject("routing_table");
            builder.startObject("indices");
            for (IndexRoutingTable indexRoutingTable : routingTable()) {
                builder.startObject(indexRoutingTable.getIndex().getName());
                builder.startObject("shards");
                for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                    builder.startArray(Integer.toString(indexShardRoutingTable.shardId().id()));
                    for (ShardRouting shardRouting : indexShardRoutingTable) {
                        shardRouting.toXContent(builder, params);
                    }
                    builder.endArray();
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endObject();
            builder.endObject();
        }

        // routing nodes
        if (metrics.contains(Metric.ROUTING_NODES)) {
            builder.startObject("routing_nodes");
            builder.startArray("unassigned");
            for (ShardRouting shardRouting : getRoutingNodes().unassigned()) {
                shardRouting.toXContent(builder, params);
            }
            builder.endArray();

            builder.startObject("nodes");
            for (RoutingNode routingNode : getRoutingNodes()) {
                builder.startArray(routingNode.nodeId() == null ? "null" : routingNode.nodeId());
                for (ShardRouting shardRouting : routingNode) {
                    shardRouting.toXContent(builder, params);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.endObject();
        }
        if (metrics.contains(Metric.CUSTOMS)) {
            for (final Map.Entry<String, Custom> cursor : customs.entrySet()) {
                builder.startObject(cursor.getKey());
                cursor.getValue().toXContent(builder, params);
                builder.endObject();
            }
        }

        return builder;
    }

    public static Builder builder(ClusterName clusterName) {
        return new Builder(clusterName);
    }

    public static Builder builder(ClusterState state) {
        return new Builder(state);
    }

    /**
     * Builder for cluster state.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private final ClusterName clusterName;
        private long version = 0;
        private String uuid = Strings.UNKNOWN_UUID_VALUE;
        private Metadata metadata = Metadata.EMPTY_METADATA;
        private RoutingTable routingTable = RoutingTable.EMPTY_ROUTING_TABLE;
        private DiscoveryNodes nodes = DiscoveryNodes.EMPTY_NODES;
        private ClusterBlocks blocks = ClusterBlocks.EMPTY_CLUSTER_BLOCK;
        private final Map<String, Custom> customs;
        private boolean fromDiff;
        private int minimumClusterManagerNodesOnPublishingClusterManager = -1;

        public Builder(ClusterState state) {
            this.clusterName = state.clusterName;
            this.version = state.version();
            this.uuid = state.stateUUID();
            this.nodes = state.nodes();
            this.routingTable = state.routingTable();
            this.metadata = state.metadata();
            this.blocks = state.blocks();
            this.customs = new HashMap<>(state.customs());
            this.minimumClusterManagerNodesOnPublishingClusterManager = state.minimumClusterManagerNodesOnPublishingClusterManager;
            this.fromDiff = false;
        }

        public Builder(ClusterName clusterName) {
            customs = new HashMap<>();
            this.clusterName = clusterName;
        }

        public Builder nodes(DiscoveryNodes.Builder nodesBuilder) {
            return nodes(nodesBuilder.build());
        }

        public Builder nodes(DiscoveryNodes nodes) {
            this.nodes = nodes;
            return this;
        }

        public DiscoveryNodes nodes() {
            return nodes;
        }

        public Builder routingTable(RoutingTable routingTable) {
            this.routingTable = routingTable;
            return this;
        }

        public Builder metadata(Metadata.Builder metadataBuilder) {
            return metadata(metadataBuilder.build());
        }

        public Builder metadata(Metadata metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder blocks(ClusterBlocks.Builder blocksBuilder) {
            return blocks(blocksBuilder.build());
        }

        public Builder blocks(ClusterBlocks blocks) {
            this.blocks = blocks;
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder incrementVersion() {
            this.version = version + 1;
            this.uuid = UNKNOWN_UUID;
            return this;
        }

        public Builder stateUUID(String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder minimumClusterManagerNodesOnPublishingClusterManager(int minimumClusterManagerNodesOnPublishingClusterManager) {
            this.minimumClusterManagerNodesOnPublishingClusterManager = minimumClusterManagerNodesOnPublishingClusterManager;
            return this;
        }

        public Builder putCustom(String type, Custom custom) {
            customs.put(type, Objects.requireNonNull(custom, type));
            return this;
        }

        public Builder removeCustom(String type) {
            customs.remove(type);
            return this;
        }

        public Builder customs(final Map<String, Custom> customs) {
            StreamSupport.stream(Spliterators.spliterator(customs.entrySet(), 0), false)
                .forEach(cursor -> Objects.requireNonNull(cursor.getValue(), cursor.getKey()));
            this.customs.putAll(customs);
            return this;
        }

        public Builder fromDiff(boolean fromDiff) {
            this.fromDiff = fromDiff;
            return this;
        }

        public ClusterState build() {
            if (UNKNOWN_UUID.equals(uuid)) {
                uuid = UUIDs.randomBase64UUID();
            }
            return new ClusterState(
                clusterName,
                version,
                uuid,
                metadata,
                routingTable,
                nodes,
                blocks,
                customs,
                minimumClusterManagerNodesOnPublishingClusterManager,
                fromDiff
            );
        }

        public static byte[] toBytes(ClusterState state) throws IOException {
            BytesStreamOutput os = new BytesStreamOutput();
            state.writeTo(os);
            return BytesReference.toBytes(os.bytes());
        }

        /**
         * @param data      input bytes
         * @param localNode used to set the local node in the cluster state.
         */
        public static ClusterState fromBytes(byte[] data, DiscoveryNode localNode, NamedWriteableRegistry registry) throws IOException {
            StreamInput in = new NamedWriteableAwareStreamInput(StreamInput.wrap(data), registry);
            return readFrom(in, localNode);

        }
    }

    @Override
    public Diff<ClusterState> diff(ClusterState previousState) {
        return new ClusterStateDiff(previousState, this);
    }

    public static Diff<ClusterState> readDiffFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        return new ClusterStateDiff(in, localNode);
    }

    public static ClusterState readFrom(StreamInput in, DiscoveryNode localNode) throws IOException {
        ClusterName clusterName = new ClusterName(in);
        Builder builder = new Builder(clusterName);
        builder.version = in.readLong();
        builder.uuid = in.readString();
        builder.metadata = Metadata.readFrom(in);
        builder.routingTable = RoutingTable.readFrom(in);
        builder.nodes = DiscoveryNodes.readFrom(in, localNode);
        builder.blocks = ClusterBlocks.readFrom(in);
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            Custom customIndexMetadata = in.readNamedWriteable(Custom.class);
            builder.putCustom(customIndexMetadata.getWriteableName(), customIndexMetadata);
        }
        builder.minimumClusterManagerNodesOnPublishingClusterManager = in.readVInt();
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        clusterName.writeTo(out);
        out.writeLong(version);
        out.writeString(stateUUID);
        metadata.writeTo(out);
        routingTable.writeTo(out);
        nodes.writeTo(out);
        blocks.writeTo(out);
        // filter out custom states not supported by the other node
        int numberOfCustoms = 0;
        for (final Custom custom : customs.values()) {
            if (FeatureAware.shouldSerialize(out, custom)) {
                numberOfCustoms++;
            }
        }
        out.writeVInt(numberOfCustoms);
        for (final Custom custom : customs.values()) {
            if (FeatureAware.shouldSerialize(out, custom)) {
                out.writeNamedWriteable(custom);
            }
        }
        out.writeVInt(minimumClusterManagerNodesOnPublishingClusterManager);
    }

    /**
     * The cluster state diff.
     *
     * @opensearch.internal
     */
    private static class ClusterStateDiff implements Diff<ClusterState> {

        private final long toVersion;

        private final String fromUuid;

        private final String toUuid;

        private final ClusterName clusterName;

        private final Diff<RoutingTable> routingTable;

        private final Diff<DiscoveryNodes> nodes;

        private final Diff<Metadata> metadata;

        private final Diff<ClusterBlocks> blocks;

        private final Diff<Map<String, Custom>> customs;

        private final int minimumClusterManagerNodesOnPublishingClusterManager;

        ClusterStateDiff(ClusterState before, ClusterState after) {
            fromUuid = before.stateUUID;
            toUuid = after.stateUUID;
            toVersion = after.version;
            clusterName = after.clusterName;
            routingTable = after.routingTable.diff(before.routingTable);
            nodes = after.nodes.diff(before.nodes);
            metadata = after.metadata.diff(before.metadata);
            blocks = after.blocks.diff(before.blocks);
            customs = DiffableUtils.diff(before.customs, after.customs, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            minimumClusterManagerNodesOnPublishingClusterManager = after.minimumClusterManagerNodesOnPublishingClusterManager;
        }

        ClusterStateDiff(StreamInput in, DiscoveryNode localNode) throws IOException {
            clusterName = new ClusterName(in);
            fromUuid = in.readString();
            toUuid = in.readString();
            toVersion = in.readLong();
            routingTable = RoutingTable.readDiffFrom(in);
            nodes = DiscoveryNodes.readDiffFrom(in, localNode);
            metadata = Metadata.readDiffFrom(in);
            blocks = ClusterBlocks.readDiffFrom(in);
            customs = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_VALUE_SERIALIZER);
            minimumClusterManagerNodesOnPublishingClusterManager = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            clusterName.writeTo(out);
            out.writeString(fromUuid);
            out.writeString(toUuid);
            out.writeLong(toVersion);
            routingTable.writeTo(out);
            nodes.writeTo(out);
            metadata.writeTo(out);
            blocks.writeTo(out);
            customs.writeTo(out);
            out.writeVInt(minimumClusterManagerNodesOnPublishingClusterManager);
        }

        @Override
        public ClusterState apply(ClusterState state) {
            Builder builder = new Builder(clusterName);
            if (toUuid.equals(state.stateUUID)) {
                // no need to read the rest - cluster state didn't change
                return state;
            }
            if (fromUuid.equals(state.stateUUID) == false) {
                throw new IncompatibleClusterStateVersionException(state.version, state.stateUUID, toVersion, fromUuid);
            }
            builder.stateUUID(toUuid);
            builder.version(toVersion);
            builder.routingTable(routingTable.apply(state.routingTable));
            builder.nodes(nodes.apply(state.nodes));
            builder.metadata(metadata.apply(state.metadata));
            builder.blocks(blocks.apply(state.blocks));
            builder.customs(customs.apply(state.customs));
            builder.minimumClusterManagerNodesOnPublishingClusterManager(minimumClusterManagerNodesOnPublishingClusterManager);
            builder.fromDiff(true);
            return builder.build();
        }
    }
}
