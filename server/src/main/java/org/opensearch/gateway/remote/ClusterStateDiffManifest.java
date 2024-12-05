/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.StringKeyDiffProvider;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.cluster.DiffableUtils.NonDiffableValueSerializer.getAbstractInstance;
import static org.opensearch.cluster.DiffableUtils.getStringKeySerializer;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V2;
import static org.opensearch.gateway.remote.ClusterMetadataManifest.CODEC_V3;

/**
 * Manifest of diff between two cluster states
 *
 * @opensearch.internal
 */
public class ClusterStateDiffManifest implements ToXContentFragment, Writeable {
    private static final String FROM_STATE_UUID_FIELD = "from_state_uuid";
    private static final String TO_STATE_UUID_FIELD = "to_state_uuid";
    private static final String METADATA_DIFF_FIELD = "metadata_diff";
    private static final String COORDINATION_METADATA_UPDATED_FIELD = "coordination_metadata_diff";
    private static final String SETTINGS_METADATA_UPDATED_FIELD = "settings_metadata_diff";
    private static final String TRANSIENT_SETTINGS_METADATA_UPDATED_FIELD = "transient_settings_metadata_diff";
    private static final String TEMPLATES_METADATA_UPDATED_FIELD = "templates_metadata_diff";
    private static final String HASHES_OF_CONSISTENT_SETTINGS_UPDATED_FIELD = "hashes_of_consistent_settings_diff";
    private static final String INDICES_DIFF_FIELD = "indices_diff";
    private static final String METADATA_CUSTOM_DIFF_FIELD = "metadata_custom_diff";
    private static final String UPSERTS_FIELD = "upserts";
    private static final String DELETES_FIELD = "deletes";
    private static final String DIFF_FIELD = "diff";
    private static final String CLUSTER_BLOCKS_UPDATED_FIELD = "cluster_blocks_diff";
    private static final String DISCOVERY_NODES_UPDATED_FIELD = "discovery_nodes_diff";
    private static final String ROUTING_TABLE_DIFF = "routing_table_diff";
    private static final String CLUSTER_STATE_CUSTOM_DIFF_FIELD = "cluster_state_custom_diff";

    private final String fromStateUUID;
    private final String toStateUUID;
    private final boolean coordinationMetadataUpdated;
    private final boolean settingsMetadataUpdated;
    private final boolean transientSettingsMetadataUpdated;
    private final boolean templatesMetadataUpdated;
    private final List<String> indicesUpdated;
    private final List<String> indicesDeleted;
    private final List<String> customMetadataUpdated;
    private final List<String> customMetadataDeleted;
    private final boolean clusterBlocksUpdated;
    private final boolean discoveryNodesUpdated;
    private final List<String> indicesRoutingUpdated;
    private final List<String> indicesRoutingDeleted;
    private String indicesRoutingDiffPath;
    private final boolean hashesOfConsistentSettingsUpdated;
    private final List<String> clusterStateCustomUpdated;
    private final List<String> clusterStateCustomDeleted;

    public ClusterStateDiffManifest(
        ClusterState state,
        ClusterState previousState,
        int codecVersion,
        StringKeyDiffProvider<IndexRoutingTable> routingTableDiff,
        String indicesRoutingDiffPath
    ) {
        fromStateUUID = previousState.stateUUID();
        toStateUUID = state.stateUUID();
        coordinationMetadataUpdated = !Metadata.isCoordinationMetadataEqual(state.metadata(), previousState.metadata());
        settingsMetadataUpdated = !Metadata.isSettingsMetadataEqual(state.metadata(), previousState.metadata());
        transientSettingsMetadataUpdated = !Metadata.isTransientSettingsMetadataEqual(state.metadata(), previousState.metadata());
        templatesMetadataUpdated = !Metadata.isTemplatesMetadataEqual(state.metadata(), previousState.metadata());
        DiffableUtils.MapDiff<String, IndexMetadata, Map<String, IndexMetadata>> indicesDiff = DiffableUtils.diff(
            previousState.metadata().indices(),
            state.metadata().indices(),
            getStringKeySerializer()
        );
        indicesDeleted = indicesDiff.getDeletes();
        indicesUpdated = new ArrayList<>(indicesDiff.getDiffs().keySet());
        indicesUpdated.addAll(indicesDiff.getUpserts().keySet());
        clusterBlocksUpdated = !state.blocks().equals(previousState.blocks());
        discoveryNodesUpdated = state.nodes().delta(previousState.nodes()).hasChanges();
        DiffableUtils.MapDiff<String, Metadata.Custom, Map<String, Metadata.Custom>> customDiff = DiffableUtils.diff(
            previousState.metadata().customs(),
            state.metadata().customs(),
            getStringKeySerializer(),
            getAbstractInstance()
        );
        customMetadataUpdated = new ArrayList<>(customDiff.getDiffs().keySet());
        customMetadataUpdated.addAll(customDiff.getUpserts().keySet());
        customMetadataDeleted = customDiff.getDeletes();
        indicesRoutingUpdated = new ArrayList<>();
        indicesRoutingDeleted = new ArrayList<>();
        this.indicesRoutingDiffPath = indicesRoutingDiffPath;
        if (codecVersion == CODEC_V2 || codecVersion == CODEC_V3) {
            if (routingTableDiff != null && routingTableDiff.provideDiff() != null) {
                routingTableDiff.provideDiff().getUpserts().forEach((k, v) -> indicesRoutingUpdated.add(k));
                indicesRoutingDeleted.addAll(routingTableDiff.provideDiff().getDeletes());
            }
        }
        hashesOfConsistentSettingsUpdated = !state.metadata()
            .hashesOfConsistentSettings()
            .equals(previousState.metadata().hashesOfConsistentSettings());
        DiffableUtils.MapDiff<String, ClusterState.Custom, Map<String, ClusterState.Custom>> clusterStateCustomDiff = DiffableUtils.diff(
            previousState.customs(),
            state.customs(),
            getStringKeySerializer(),
            getAbstractInstance()
        );
        clusterStateCustomUpdated = new ArrayList<>(clusterStateCustomDiff.getDiffs().keySet());
        clusterStateCustomUpdated.addAll(clusterStateCustomDiff.getUpserts().keySet());
        clusterStateCustomDeleted = clusterStateCustomDiff.getDeletes();
    }

    public ClusterStateDiffManifest(
        String fromStateUUID,
        String toStateUUID,
        boolean coordinationMetadataUpdated,
        boolean settingsMetadataUpdated,
        boolean transientSettingsMetadataUpdate,
        boolean templatesMetadataUpdated,
        List<String> customMetadataUpdated,
        List<String> customMetadataDeleted,
        List<String> indicesUpdated,
        List<String> indicesDeleted,
        boolean clusterBlocksUpdated,
        boolean discoveryNodesUpdated,
        List<String> indicesRoutingUpdated,
        List<String> indicesRoutingDeleted,
        String indicesRoutingDiffPath,
        boolean hashesOfConsistentSettingsUpdated,
        List<String> clusterStateCustomUpdated,
        List<String> clusterStateCustomDeleted
    ) {
        this.fromStateUUID = fromStateUUID;
        this.toStateUUID = toStateUUID;
        this.coordinationMetadataUpdated = coordinationMetadataUpdated;
        this.settingsMetadataUpdated = settingsMetadataUpdated;
        this.transientSettingsMetadataUpdated = transientSettingsMetadataUpdate;
        this.templatesMetadataUpdated = templatesMetadataUpdated;
        this.customMetadataUpdated = Collections.unmodifiableList(customMetadataUpdated);
        this.customMetadataDeleted = Collections.unmodifiableList(customMetadataDeleted);
        this.indicesUpdated = Collections.unmodifiableList(indicesUpdated);
        this.indicesDeleted = Collections.unmodifiableList(indicesDeleted);
        this.clusterBlocksUpdated = clusterBlocksUpdated;
        this.discoveryNodesUpdated = discoveryNodesUpdated;
        this.indicesRoutingUpdated = Collections.unmodifiableList(indicesRoutingUpdated);
        this.indicesRoutingDeleted = Collections.unmodifiableList(indicesRoutingDeleted);
        this.hashesOfConsistentSettingsUpdated = hashesOfConsistentSettingsUpdated;
        this.clusterStateCustomUpdated = Collections.unmodifiableList(clusterStateCustomUpdated);
        this.clusterStateCustomDeleted = Collections.unmodifiableList(clusterStateCustomDeleted);
        this.indicesRoutingDiffPath = indicesRoutingDiffPath;
    }

    public ClusterStateDiffManifest(StreamInput in) throws IOException {
        this.fromStateUUID = in.readString();
        this.toStateUUID = in.readString();
        this.coordinationMetadataUpdated = in.readBoolean();
        this.settingsMetadataUpdated = in.readBoolean();
        this.transientSettingsMetadataUpdated = in.readBoolean();
        this.templatesMetadataUpdated = in.readBoolean();
        this.indicesUpdated = in.readStringList();
        this.indicesDeleted = in.readStringList();
        this.customMetadataUpdated = in.readStringList();
        this.customMetadataDeleted = in.readStringList();
        this.clusterBlocksUpdated = in.readBoolean();
        this.discoveryNodesUpdated = in.readBoolean();
        this.indicesRoutingUpdated = in.readStringList();
        this.indicesRoutingDeleted = in.readStringList();
        this.hashesOfConsistentSettingsUpdated = in.readBoolean();
        this.clusterStateCustomUpdated = in.readStringList();
        this.clusterStateCustomDeleted = in.readStringList();
        this.indicesRoutingDiffPath = in.readOptionalString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FROM_STATE_UUID_FIELD, fromStateUUID);
        builder.field(TO_STATE_UUID_FIELD, toStateUUID);
        builder.startObject(METADATA_DIFF_FIELD);
        {
            builder.field(COORDINATION_METADATA_UPDATED_FIELD, coordinationMetadataUpdated);
            builder.field(SETTINGS_METADATA_UPDATED_FIELD, settingsMetadataUpdated);
            builder.field(TRANSIENT_SETTINGS_METADATA_UPDATED_FIELD, transientSettingsMetadataUpdated);
            builder.field(TEMPLATES_METADATA_UPDATED_FIELD, templatesMetadataUpdated);
            builder.startObject(INDICES_DIFF_FIELD);
            builder.startArray(UPSERTS_FIELD);
            for (String index : indicesUpdated) {
                builder.value(index);
            }
            builder.endArray();
            builder.startArray(DELETES_FIELD);
            for (String index : indicesDeleted) {
                builder.value(index);
            }
            builder.endArray();
            builder.endObject();
            builder.startObject(METADATA_CUSTOM_DIFF_FIELD);
            builder.startArray(UPSERTS_FIELD);
            for (String custom : customMetadataUpdated) {
                builder.value(custom);
            }
            builder.endArray();
            builder.startArray(DELETES_FIELD);
            for (String custom : customMetadataDeleted) {
                builder.value(custom);
            }
            builder.endArray();
            builder.endObject();
            builder.field(HASHES_OF_CONSISTENT_SETTINGS_UPDATED_FIELD, hashesOfConsistentSettingsUpdated);
        }
        builder.endObject();
        builder.field(CLUSTER_BLOCKS_UPDATED_FIELD, clusterBlocksUpdated);
        builder.field(DISCOVERY_NODES_UPDATED_FIELD, discoveryNodesUpdated);
        builder.startObject(ROUTING_TABLE_DIFF);
        if (!indicesRoutingUpdated.isEmpty()) {
            builder.startArray(UPSERTS_FIELD);
            for (String index : indicesRoutingUpdated) {
                builder.value(index);
            }
            builder.endArray();
        }
        if (!indicesRoutingDeleted.isEmpty()) {
            builder.startArray(DELETES_FIELD);
            for (String index : indicesRoutingDeleted) {
                builder.value(index);
            }
            builder.endArray();
        }
        if (indicesRoutingDiffPath != null) {
            builder.field(DIFF_FIELD, indicesRoutingDiffPath);
        }
        builder.endObject();
        builder.startObject(CLUSTER_STATE_CUSTOM_DIFF_FIELD);
        builder.startArray(UPSERTS_FIELD);
        for (String custom : clusterStateCustomUpdated) {
            builder.value(custom);
        }
        builder.endArray();
        builder.startArray(DELETES_FIELD);
        for (String custom : clusterStateCustomDeleted) {
            builder.value(custom);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static ClusterStateDiffManifest fromXContent(XContentParser parser, long codec_version) throws IOException {
        Builder builder = new Builder();
        if (parser.currentToken() == null) { // fresh parser? move to next token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (currentFieldName.equals(METADATA_DIFF_FIELD)) {
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            switch (currentFieldName) {
                                case COORDINATION_METADATA_UPDATED_FIELD:
                                    builder.coordinationMetadataUpdated(parser.booleanValue());
                                    break;
                                case SETTINGS_METADATA_UPDATED_FIELD:
                                    builder.settingsMetadataUpdated(parser.booleanValue());
                                    break;
                                case TRANSIENT_SETTINGS_METADATA_UPDATED_FIELD:
                                    builder.transientSettingsMetadataUpdate(parser.booleanValue());
                                    break;
                                case TEMPLATES_METADATA_UPDATED_FIELD:
                                    builder.templatesMetadataUpdated(parser.booleanValue());
                                    break;
                                case HASHES_OF_CONSISTENT_SETTINGS_UPDATED_FIELD:
                                    builder.hashesOfConsistentSettingsUpdated(parser.booleanValue());
                                    break;
                                default:
                                    throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if (currentFieldName.equals(INDICES_DIFF_FIELD)) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    currentFieldName = parser.currentName();
                                    token = parser.nextToken();
                                    switch (currentFieldName) {
                                        case UPSERTS_FIELD:
                                            builder.indicesUpdated(convertListToString(parser.listOrderedMap()));
                                            break;
                                        case DELETES_FIELD:
                                            builder.indicesDeleted(convertListToString(parser.listOrderedMap()));
                                            break;
                                        default:
                                            throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                                    }
                                }
                            } else if (currentFieldName.equals(METADATA_CUSTOM_DIFF_FIELD)) {
                                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                    currentFieldName = parser.currentName();
                                    token = parser.nextToken();
                                    switch (currentFieldName) {
                                        case UPSERTS_FIELD:
                                            builder.customMetadataUpdated(convertListToString(parser.listOrderedMap()));
                                            break;
                                        case DELETES_FIELD:
                                            builder.customMetadataDeleted(convertListToString(parser.listOrderedMap()));
                                            break;
                                        default:
                                            throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                                    }
                                }
                            } else {
                                throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                            }
                        } else {
                            throw new XContentParseException("Unexpected token [" + token + "]");
                        }
                    }
                } else if (currentFieldName.equals(ROUTING_TABLE_DIFF)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        parser.nextToken();
                        switch (currentFieldName) {
                            case UPSERTS_FIELD:
                                if (codec_version == CODEC_V2 || codec_version == CODEC_V3) {
                                    builder.indicesRoutingUpdated(convertListToString(parser.listOrderedMap()));
                                }
                                break;
                            case DELETES_FIELD:
                                if (codec_version == CODEC_V2 || codec_version == CODEC_V3) {
                                    builder.indicesRoutingDeleted(convertListToString(parser.listOrderedMap()));
                                }
                                break;
                            case DIFF_FIELD:
                                if (codec_version >= CODEC_V3) {
                                    builder.indicesRoutingDiffPath(parser.textOrNull());
                                }
                                break;
                            default:
                                throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                        }
                    }
                } else if (currentFieldName.equals(CLUSTER_STATE_CUSTOM_DIFF_FIELD)) {
                    while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        currentFieldName = parser.currentName();
                        parser.nextToken();
                        switch (currentFieldName) {
                            case UPSERTS_FIELD:
                                builder.clusterStateCustomUpdated(convertListToString(parser.listOrderedMap()));
                                break;
                            case DELETES_FIELD:
                                builder.clusterStateCustomDeleted(convertListToString(parser.listOrderedMap()));
                                break;
                            default:
                                throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                        }
                    }
                } else {
                    throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                switch (currentFieldName) {
                    case FROM_STATE_UUID_FIELD:
                        builder.fromStateUUID(parser.text());
                        break;
                    case TO_STATE_UUID_FIELD:
                        builder.toStateUUID(parser.text());
                        break;
                    case CLUSTER_BLOCKS_UPDATED_FIELD:
                        builder.clusterBlocksUpdated(parser.booleanValue());
                        break;
                    case DISCOVERY_NODES_UPDATED_FIELD:
                        builder.discoveryNodesUpdated(parser.booleanValue());
                        break;
                    default:
                        throw new XContentParseException("Unexpected field [" + currentFieldName + "]");
                }
            } else {
                throw new XContentParseException("Unexpected token [" + token + "]");
            }
        }
        return builder.build();
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    private static List<String> convertListToString(List<Object> list) {
        List<String> convertedList = new ArrayList<>();
        for (Object o : list) {
            convertedList.add(o.toString());
        }
        return convertedList;
    }

    public String getFromStateUUID() {
        return fromStateUUID;
    }

    public String getToStateUUID() {
        return toStateUUID;
    }

    public boolean isCoordinationMetadataUpdated() {
        return coordinationMetadataUpdated;
    }

    public boolean isSettingsMetadataUpdated() {
        return settingsMetadataUpdated;
    }

    public boolean isTransientSettingsMetadataUpdated() {
        return transientSettingsMetadataUpdated;
    }

    public boolean isTemplatesMetadataUpdated() {
        return templatesMetadataUpdated;
    }

    public List<String> getCustomMetadataUpdated() {
        return customMetadataUpdated;
    }

    public List<String> getCustomMetadataDeleted() {
        return customMetadataDeleted;
    }

    public List<String> getIndicesUpdated() {
        return indicesUpdated;
    }

    public List<String> getIndicesDeleted() {
        return indicesDeleted;
    }

    public boolean isClusterBlocksUpdated() {
        return clusterBlocksUpdated;
    }

    public boolean isDiscoveryNodesUpdated() {
        return discoveryNodesUpdated;
    }

    public boolean isHashesOfConsistentSettingsUpdated() {
        return hashesOfConsistentSettingsUpdated;
    }

    public String getIndicesRoutingDiffPath() {
        return indicesRoutingDiffPath;
    }

    public List<String> getIndicesRoutingUpdated() {
        return indicesRoutingUpdated;
    }

    public List<String> getIndicesRoutingDeleted() {
        return indicesRoutingDeleted;
    }

    public List<String> getClusterStateCustomUpdated() {
        return clusterStateCustomUpdated;
    }

    public List<String> getClusterStateCustomDeleted() {
        return clusterStateCustomDeleted;
    }

    public void setIndicesRoutingDiffPath(String indicesRoutingDiffPath) {
        this.indicesRoutingDiffPath = indicesRoutingDiffPath;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterStateDiffManifest that = (ClusterStateDiffManifest) o;
        return coordinationMetadataUpdated == that.coordinationMetadataUpdated
            && settingsMetadataUpdated == that.settingsMetadataUpdated
            && transientSettingsMetadataUpdated == that.transientSettingsMetadataUpdated
            && templatesMetadataUpdated == that.templatesMetadataUpdated
            && clusterBlocksUpdated == that.clusterBlocksUpdated
            && discoveryNodesUpdated == that.discoveryNodesUpdated
            && hashesOfConsistentSettingsUpdated == that.hashesOfConsistentSettingsUpdated
            && Objects.equals(fromStateUUID, that.fromStateUUID)
            && Objects.equals(toStateUUID, that.toStateUUID)
            && Objects.equals(customMetadataUpdated, that.customMetadataUpdated)
            && Objects.equals(customMetadataDeleted, that.customMetadataDeleted)
            && Objects.equals(indicesUpdated, that.indicesUpdated)
            && Objects.equals(indicesDeleted, that.indicesDeleted)
            && Objects.equals(indicesRoutingUpdated, that.indicesRoutingUpdated)
            && Objects.equals(indicesRoutingDeleted, that.indicesRoutingDeleted)
            && Objects.equals(clusterStateCustomUpdated, that.clusterStateCustomUpdated)
            && Objects.equals(clusterStateCustomDeleted, that.clusterStateCustomDeleted)
            && Objects.equals(indicesRoutingDiffPath, that.indicesRoutingDiffPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            fromStateUUID,
            toStateUUID,
            coordinationMetadataUpdated,
            settingsMetadataUpdated,
            transientSettingsMetadataUpdated,
            templatesMetadataUpdated,
            customMetadataUpdated,
            customMetadataDeleted,
            indicesUpdated,
            indicesDeleted,
            clusterBlocksUpdated,
            discoveryNodesUpdated,
            indicesRoutingUpdated,
            indicesRoutingDeleted,
            hashesOfConsistentSettingsUpdated,
            clusterStateCustomUpdated,
            clusterStateCustomDeleted,
            indicesRoutingDiffPath
        );
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(fromStateUUID);
        out.writeString(toStateUUID);
        out.writeBoolean(coordinationMetadataUpdated);
        out.writeBoolean(settingsMetadataUpdated);
        out.writeBoolean(transientSettingsMetadataUpdated);
        out.writeBoolean(templatesMetadataUpdated);
        out.writeStringCollection(indicesUpdated);
        out.writeStringCollection(indicesDeleted);
        out.writeStringCollection(customMetadataUpdated);
        out.writeStringCollection(customMetadataDeleted);
        out.writeBoolean(clusterBlocksUpdated);
        out.writeBoolean(discoveryNodesUpdated);
        out.writeStringCollection(indicesRoutingUpdated);
        out.writeStringCollection(indicesRoutingDeleted);
        out.writeBoolean(hashesOfConsistentSettingsUpdated);
        out.writeStringCollection(clusterStateCustomUpdated);
        out.writeStringCollection(clusterStateCustomDeleted);
        out.writeOptionalString(indicesRoutingDiffPath);
    }

    /**
     * Builder for ClusterStateDiffManifest
     *
     * @opensearch.internal
     */
    public static class Builder {
        private String fromStateUUID;
        private String toStateUUID;
        private boolean coordinationMetadataUpdated;
        private boolean settingsMetadataUpdated;
        private boolean transientSettingsMetadataUpdated;
        private boolean templatesMetadataUpdated;
        private List<String> customMetadataUpdated;
        private List<String> customMetadataDeleted;
        private List<String> indicesUpdated;
        private List<String> indicesDeleted;
        private boolean clusterBlocksUpdated;
        private boolean discoveryNodesUpdated;
        private List<String> indicesRoutingUpdated;
        private List<String> indicesRoutingDeleted;
        private String indicesRoutingDiff;
        private boolean hashesOfConsistentSettingsUpdated;
        private List<String> clusterStateCustomUpdated;
        private List<String> clusterStateCustomDeleted;

        public Builder() {
            customMetadataUpdated = Collections.emptyList();
            customMetadataDeleted = Collections.emptyList();
            indicesUpdated = Collections.emptyList();
            indicesDeleted = Collections.emptyList();
            indicesRoutingUpdated = Collections.emptyList();
            indicesRoutingDeleted = Collections.emptyList();
            clusterStateCustomUpdated = Collections.emptyList();
            clusterStateCustomDeleted = Collections.emptyList();
        }

        public Builder fromStateUUID(String fromStateUUID) {
            this.fromStateUUID = fromStateUUID;
            return this;
        }

        public Builder toStateUUID(String toStateUUID) {
            this.toStateUUID = toStateUUID;
            return this;
        }

        public Builder coordinationMetadataUpdated(boolean coordinationMetadataUpdated) {
            this.coordinationMetadataUpdated = coordinationMetadataUpdated;
            return this;
        }

        public Builder settingsMetadataUpdated(boolean settingsMetadataUpdated) {
            this.settingsMetadataUpdated = settingsMetadataUpdated;
            return this;
        }

        public Builder transientSettingsMetadataUpdate(boolean settingsMetadataUpdated) {
            this.transientSettingsMetadataUpdated = settingsMetadataUpdated;
            return this;
        }

        public Builder templatesMetadataUpdated(boolean templatesMetadataUpdated) {
            this.templatesMetadataUpdated = templatesMetadataUpdated;
            return this;
        }

        public Builder hashesOfConsistentSettingsUpdated(boolean hashesOfConsistentSettingsUpdated) {
            this.hashesOfConsistentSettingsUpdated = hashesOfConsistentSettingsUpdated;
            return this;
        }

        public Builder customMetadataUpdated(List<String> customMetadataUpdated) {
            this.customMetadataUpdated = customMetadataUpdated;
            return this;
        }

        public Builder customMetadataDeleted(List<String> customMetadataDeleted) {
            this.customMetadataDeleted = customMetadataDeleted;
            return this;
        }

        public Builder indicesUpdated(List<String> indicesUpdated) {
            this.indicesUpdated = indicesUpdated;
            return this;
        }

        public Builder indicesDeleted(List<String> indicesDeleted) {
            this.indicesDeleted = indicesDeleted;
            return this;
        }

        public Builder clusterBlocksUpdated(boolean clusterBlocksUpdated) {
            this.clusterBlocksUpdated = clusterBlocksUpdated;
            return this;
        }

        public Builder discoveryNodesUpdated(boolean discoveryNodesUpdated) {
            this.discoveryNodesUpdated = discoveryNodesUpdated;
            return this;
        }

        public Builder indicesRoutingDiffPath(String indicesRoutingDiffPath) {
            this.indicesRoutingDiff = indicesRoutingDiffPath;
            return this;
        }

        public Builder indicesRoutingUpdated(List<String> indicesRoutingUpdated) {
            this.indicesRoutingUpdated = indicesRoutingUpdated;
            return this;
        }

        public Builder indicesRoutingDeleted(List<String> indicesRoutingDeleted) {
            this.indicesRoutingDeleted = indicesRoutingDeleted;
            return this;
        }

        public Builder clusterStateCustomUpdated(List<String> clusterStateCustomUpdated) {
            this.clusterStateCustomUpdated = clusterStateCustomUpdated;
            return this;
        }

        public Builder clusterStateCustomDeleted(List<String> clusterStateCustomDeleted) {
            this.clusterStateCustomDeleted = clusterStateCustomDeleted;
            return this;
        }

        public ClusterStateDiffManifest build() {
            return new ClusterStateDiffManifest(
                fromStateUUID,
                toStateUUID,
                coordinationMetadataUpdated,
                settingsMetadataUpdated,
                transientSettingsMetadataUpdated,
                templatesMetadataUpdated,
                customMetadataUpdated,
                customMetadataDeleted,
                indicesUpdated,
                indicesDeleted,
                clusterBlocksUpdated,
                discoveryNodesUpdated,
                indicesRoutingUpdated,
                indicesRoutingDeleted,
                indicesRoutingDiff,
                hashesOfConsistentSettingsUpdated,
                clusterStateCustomUpdated,
                clusterStateCustomDeleted
            );
        }
    }
}
