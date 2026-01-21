/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.Version;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.metadata.settings.SettingsModel;
import org.opensearch.metadata.stream.MetadataWriteable;
import org.opensearch.metadata.stream.MetadataWriteable.MetadataReader;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Pure data holder class for index metadata without dependencies on OpenSearch server packages.
 */
@ExperimentalApi
public final class IndexMetadataModel implements Writeable {

    private final String index;
    private final long version;
    private final long mappingVersion;
    private final long settingsVersion;
    private final long aliasesVersion;
    private final int routingNumShards;
    private final byte state;
    private final SettingsModel settings;
    private final long[] primaryTerms;
    private final Map<String, MappingMetadataModel> mappings;
    private final Map<String, AliasMetadataModel> aliases;
    private final Map<String, MetadataWriteable> customData;
    private final Map<Integer, Set<String>> inSyncAllocationIds;
    private final Map<String, MetadataWriteable> rolloverInfos;
    private final boolean isSystem;
    private final ContextModel context;
    private final Boolean ingestionPaused;

    /**
     * Constructs a new IndexMetadataModel with all fields.
     *
     * @param index the index name
     * @param version the metadata version
     * @param mappingVersion the mapping version
     * @param settingsVersion the settings version
     * @param aliasesVersion the aliases version
     * @param routingNumShards the number of shards for routing
     * @param state the index state
     * @param settings the index settings
     * @param primaryTerms the primary terms for each shard
     * @param mappings the index mappings
     * @param aliases the index aliases
     * @param customData custom metadata
     * @param inSyncAllocationIds in-sync allocation IDs per shard
     * @param rolloverInfos rollover information
     * @param isSystem whether this is a system index
     * @param context the index context
     * @param ingestionStatus the ingestion paused status
     */
    public IndexMetadataModel(
        String index,
        long version,
        long mappingVersion,
        long settingsVersion,
        long aliasesVersion,
        int routingNumShards,
        byte state,
        SettingsModel settings,
        long[] primaryTerms,
        Map<String, MappingMetadataModel> mappings,
        Map<String, AliasMetadataModel> aliases,
        Map<String, MetadataWriteable> customData,
        Map<Integer, Set<String>> inSyncAllocationIds,
        Map<String, MetadataWriteable> rolloverInfos,
        boolean isSystem,
        ContextModel context,
        Boolean ingestionStatus
    ) {
        this.index = index;
        this.version = version;
        assert mappingVersion >= 0 : mappingVersion;
        this.mappingVersion = mappingVersion;
        assert settingsVersion >= 0 : settingsVersion;
        this.settingsVersion = settingsVersion;
        assert aliasesVersion >= 0 : aliasesVersion;
        this.aliasesVersion = aliasesVersion;
        this.routingNumShards = routingNumShards;
        this.state = state;
        this.settings = settings;
        this.primaryTerms = primaryTerms;
        this.mappings = mappings;
        this.aliases = aliases;
        this.customData = customData;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.rolloverInfos = rolloverInfos;
        this.isSystem = isSystem;
        this.context = context;
        this.ingestionPaused = ingestionStatus;
    }

    /**
     * Deserialization constructor with MetadataReaders for server module types.
     *
     * @param in the stream to read from
     * @param customDataReader reader for customData entries (e.g., DiffableStringMap.METADATA_READER)
     * @param rolloverInfoReader reader for rolloverInfo entries (e.g., RolloverInfo.METADATA_READER)
     * @param rolloverKeyExtractor function to extract the key from a rolloverInfo entry (e.g., RolloverInfo::getAlias)
     * @throws IOException if an I/O error occurs
     */
    public IndexMetadataModel(
        StreamInput in,
        MetadataReader<? extends MetadataWriteable> customDataReader,
        MetadataReader<? extends MetadataWriteable> rolloverInfoReader,
        Function<MetadataWriteable, String> rolloverKeyExtractor
    ) throws IOException {
        this.index = in.readString();
        this.version = in.readLong();
        this.mappingVersion = in.readVLong();
        this.settingsVersion = in.readVLong();
        this.aliasesVersion = in.readVLong();
        this.routingNumShards = in.readInt();
        this.state = in.readByte();
        this.settings = new SettingsModel(in);
        this.primaryTerms = in.readVLongArray();

        int mappingsSize = in.readVInt();
        Map<String, MappingMetadataModel> mappingsBuilder = new HashMap<>(mappingsSize);
        for (int i = 0; i < mappingsSize; i++) {
            MappingMetadataModel mapping = new MappingMetadataModel(in);
            mappingsBuilder.put(mapping.type(), mapping);
        }
        this.mappings = mappingsBuilder;

        int aliasesSize = in.readVInt();
        Map<String, AliasMetadataModel> aliasesBuilder = new HashMap<>(aliasesSize);
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetadataModel alias = new AliasMetadataModel(in);
            aliasesBuilder.put(alias.alias(), alias);
        }
        this.aliases = aliasesBuilder;

        // Read customData via MetadataReader
        int customSize = in.readVInt();
        Map<String, MetadataWriteable> customBuilder = new HashMap<>(customSize);
        for (int i = 0; i < customSize; i++) {
            String key = in.readString();
            MetadataWriteable custom = customDataReader.readFromMetadataStream(in);
            customBuilder.put(key, custom);
        }
        this.customData = customBuilder;

        this.inSyncAllocationIds = in.readMap(StreamInput::readVInt, i -> i.readSet(StreamInput::readString));

        // Read rolloverInfos via MetadataReader
        // Note: RolloverInfo contains alias internally, key is extracted via rolloverKeyExtractor
        int rolloverSize = in.readVInt();
        Map<String, MetadataWriteable> rolloverBuilder = new HashMap<>(rolloverSize);
        for (int i = 0; i < rolloverSize; i++) {
            MetadataWriteable rollover = rolloverInfoReader.readFromMetadataStream(in);
            String key = rolloverKeyExtractor.apply(rollover);
            rolloverBuilder.put(key, rollover);
        }
        this.rolloverInfos = rolloverBuilder;

        this.isSystem = in.readBoolean();

        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            this.context = in.readOptionalWriteable(ContextModel::new);
        } else {
            this.context = null;
        }

        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            this.ingestionPaused = in.readOptionalBoolean();
        } else {
            this.ingestionPaused = null;
        }
    }

    /** Returns the index name. */
    public String index() {
        return index;
    }

    /** Returns the metadata version. */
    public long version() {
        return version;
    }

    /** Returns the mapping version. */
    public long mappingVersion() {
        return mappingVersion;
    }

    /** Returns the settings version. */
    public long settingsVersion() {
        return settingsVersion;
    }

    /** Returns the aliases version. */
    public long aliasesVersion() {
        return aliasesVersion;
    }

    /** Returns the number of shards for routing. */
    public int routingNumShards() {
        return routingNumShards;
    }

    /** Returns the index state. */
    public byte state() {
        return state;
    }

    /** Returns the index settings. */
    public SettingsModel settings() {
        return settings;
    }

    /** Returns the primary terms for each shard. */
    public long[] primaryTerms() {
        return primaryTerms;
    }

    /** Returns the index mappings. */
    public Map<String, MappingMetadataModel> mappings() {
        return mappings;
    }

    /** Returns the index aliases. */
    public Map<String, AliasMetadataModel> aliases() {
        return aliases;
    }

    /** Returns custom metadata. */
    public Map<String, MetadataWriteable> customData() {
        return customData;
    }

    /** Returns in-sync allocation IDs per shard. */
    public Map<Integer, Set<String>> inSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    /** Returns rollover information. */
    public Map<String, MetadataWriteable> rolloverInfos() {
        return rolloverInfos;
    }

    /** Returns whether this is a system index. */
    public boolean isSystem() {
        return isSystem;
    }

    /** Returns the index context. */
    public ContextModel context() {
        return context;
    }

    /** Returns the ingestion paused status. */
    public Boolean ingestionPaused() {
        return ingestionPaused;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeLong(version);
        out.writeVLong(mappingVersion);
        out.writeVLong(settingsVersion);
        out.writeVLong(aliasesVersion);
        out.writeInt(routingNumShards);
        out.writeByte(state);
        settings.writeTo(out);
        out.writeVLongArray(primaryTerms);

        out.writeVInt(mappings.size());
        for (final MappingMetadataModel cursor : mappings.values()) {
            cursor.writeTo(out);
        }

        out.writeVInt(aliases.size());
        for (final AliasMetadataModel cursor : aliases.values()) {
            cursor.writeTo(out);
        }

        // Write customData via MetadataWriteable
        out.writeVInt(customData.size());
        for (Map.Entry<String, MetadataWriteable> entry : customData.entrySet()) {
            out.writeString(entry.getKey());
            entry.getValue().writeToMetadataStream(out);
        }

        out.writeMap(inSyncAllocationIds, StreamOutput::writeVInt, StreamOutput::writeStringCollection);

        // Write rolloverInfos via MetadataWriteable
        // Note: RolloverInfo contains alias internally, so we don't write a separate key
        out.writeVInt(rolloverInfos.size());
        for (MetadataWriteable rollover : rolloverInfos.values()) {
            rollover.writeToMetadataStream(out);
        }

        out.writeBoolean(isSystem);

        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeOptionalWriteable(context);
        }

        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            out.writeOptionalBoolean(ingestionPaused);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexMetadataModel that = (IndexMetadataModel) o;

        return version == that.version
            && mappingVersion == that.mappingVersion
            && settingsVersion == that.settingsVersion
            && aliasesVersion == that.aliasesVersion
            && routingNumShards == that.routingNumShards
            && state == that.state
            && isSystem == that.isSystem
            && Objects.equals(index, that.index)
            && Objects.equals(settings, that.settings)
            && Arrays.equals(primaryTerms, that.primaryTerms)
            && Objects.equals(mappings, that.mappings)
            && Objects.equals(aliases, that.aliases)
            && Objects.equals(customData, that.customData)
            && Objects.equals(inSyncAllocationIds, that.inSyncAllocationIds)
            && Objects.equals(rolloverInfos, that.rolloverInfos)
            && Objects.equals(context, that.context)
            && Objects.equals(ingestionPaused, that.ingestionPaused);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(
            index,
            version,
            mappingVersion,
            settingsVersion,
            aliasesVersion,
            routingNumShards,
            state,
            settings,
            mappings,
            aliases,
            customData,
            inSyncAllocationIds,
            rolloverInfos,
            isSystem,
            context,
            ingestionPaused
        );
        result = 31 * result + Arrays.hashCode(primaryTerms);
        return result;
    }

    /**
     * Builder for IndexMetadataModel.
     */
    public static class Builder {
        private String index;
        private long version = 1;
        private long mappingVersion = 1;
        private long settingsVersion = 1;
        private long aliasesVersion = 1;
        private Integer routingNumShards;
        private byte state = 0; // OPEN
        private SettingsModel settings = SettingsModel.EMPTY;
        private long[] primaryTerms = null;
        private final Map<String, MappingMetadataModel> mappings;
        private final Map<String, AliasMetadataModel> aliases;
        private final Map<String, MetadataWriteable> customData;
        private final Map<Integer, Set<String>> inSyncAllocationIds;
        private final Map<String, MetadataWriteable> rolloverInfos;
        private boolean isSystem;
        private ContextModel context;
        private Boolean ingestionPaused;

        /**
         * Creates a new builder with the given index name.
         * @param index the index name
         */
        public Builder(String index) {
            this.index = index;
            this.mappings = new HashMap<>();
            this.aliases = new HashMap<>();
            this.customData = new HashMap<>();
            this.inSyncAllocationIds = new HashMap<>();
            this.rolloverInfos = new HashMap<>();
            this.isSystem = false;
        }

        /**
         * Creates a new builder from an existing model.
         * @param model the model to copy from
         */
        public Builder(IndexMetadataModel model) {
            this.index = model.index;
            this.version = model.version;
            this.mappingVersion = model.mappingVersion;
            this.settingsVersion = model.settingsVersion;
            this.aliasesVersion = model.aliasesVersion;
            this.routingNumShards = model.routingNumShards;
            this.state = model.state;
            this.settings = model.settings;
            this.primaryTerms = model.primaryTerms != null ? model.primaryTerms.clone() : null;
            this.mappings = new HashMap<>(model.mappings);
            this.aliases = new HashMap<>(model.aliases);
            this.customData = new HashMap<>(model.customData);
            this.inSyncAllocationIds = new HashMap<>(model.inSyncAllocationIds);
            this.rolloverInfos = new HashMap<>(model.rolloverInfos);
            this.isSystem = model.isSystem;
            this.context = model.context;
            this.ingestionPaused = model.ingestionPaused;
        }

        /**
         * Sets the index name.
         * @param index the index name
         */
        public Builder index(String index) {
            this.index = index;
            return this;
        }

        /** Returns the index name. */
        public String index() {
            return index;
        }

        /**
         * Sets the metadata version.
         * @param version the version
         */
        public Builder version(long version) {
            this.version = version;
            return this;
        }

        /** Returns the metadata version. */
        public long version() {
            return version;
        }

        /**
         * Sets the mapping version.
         * @param mappingVersion the mapping version
         */
        public Builder mappingVersion(long mappingVersion) {
            this.mappingVersion = mappingVersion;
            return this;
        }

        /** Returns the mapping version. */
        public long mappingVersion() {
            return mappingVersion;
        }

        /**
         * Sets the settings version.
         * @param settingsVersion the settings version
         */
        public Builder settingsVersion(long settingsVersion) {
            this.settingsVersion = settingsVersion;
            return this;
        }

        /** Returns the settings version. */
        public long settingsVersion() {
            return settingsVersion;
        }

        /**
         * Sets the aliases version.
         * @param aliasesVersion the aliases version
         */
        public Builder aliasesVersion(long aliasesVersion) {
            this.aliasesVersion = aliasesVersion;
            return this;
        }

        /** Returns the aliases version. */
        public long aliasesVersion() {
            return aliasesVersion;
        }

        /**
         * Sets the number of shards for routing.
         * @param routingNumShards the routing num shards
         */
        public Builder routingNumShards(int routingNumShards) {
            this.routingNumShards = routingNumShards;
            return this;
        }

        /** Returns the number of shards for routing. */
        public Integer routingNumShards() {
            return routingNumShards;
        }

        /**
         * Sets the index state.
         * @param state the state
         */
        public Builder state(byte state) {
            this.state = state;
            return this;
        }

        /** Returns the index state. */
        public byte state() {
            return state;
        }

        /**
         * Sets the index settings.
         * @param settings the settings
         */
        public Builder settings(SettingsModel settings) {
            this.settings = settings;
            return this;
        }

        /** Returns the index settings. */
        public SettingsModel settings() {
            return settings;
        }

        /**
         * Sets the primary terms for each shard.
         * @param primaryTerms the primary terms
         */
        public Builder primaryTerms(long[] primaryTerms) {
            this.primaryTerms = primaryTerms != null ? primaryTerms.clone() : null;
            return this;
        }

        /** Returns the primary terms for each shard. */
        public long[] primaryTerms() {
            return primaryTerms;
        }

        /**
         * Sets the mapping, clearing any existing mappings.
         * @param mapping the mapping
         */
        public Builder putMapping(MappingMetadataModel mapping) {
            mappings.clear();
            if (mapping != null) {
                mappings.put(mapping.type(), mapping);
            }
            return this;
        }

        /** Returns the index mappings. */
        public Map<String, MappingMetadataModel> mappings() {
            return mappings;
        }

        /**
         * Adds an alias.
         * @param alias the alias to add
         */
        public Builder putAlias(AliasMetadataModel alias) {
            aliases.put(alias.alias(), alias);
            return this;
        }

        /**
         * Removes an alias.
         * @param alias the alias to remove
         */
        public Builder removeAlias(String alias) {
            aliases.remove(alias);
            return this;
        }

        /** Removes all aliases. */
        public Builder removeAllAliases() {
            aliases.clear();
            return this;
        }

        /** Returns the index aliases. */
        public Map<String, AliasMetadataModel> aliases() {
            return aliases;
        }

        /**
         * Adds custom metadata.
         * @param key the key
         * @param custom the custom metadata
         */
        public Builder putCustom(String key, MetadataWriteable custom) {
            customData.put(key, custom);
            return this;
        }

        /**
         * Removes custom metadata.
         * @param key the key to remove
         */
        public MetadataWriteable removeCustom(String key) {
            return customData.remove(key);
        }

        /** Returns custom metadata. */
        public Map<String, MetadataWriteable> customData() {
            return customData;
        }

        /**
         * Sets in-sync allocation IDs for a shard.
         * @param shardId the shard ID
         * @param allocationIds the allocation IDs
         */
        public Builder putInSyncAllocationIds(int shardId, Set<String> allocationIds) {
            inSyncAllocationIds.put(shardId, allocationIds);
            return this;
        }

        /**
         * Returns in-sync allocation IDs for a shard.
         * @param shardId the shard ID
         */
        public Set<String> getInSyncAllocationIds(int shardId) {
            return inSyncAllocationIds.get(shardId);
        }

        /** Returns all in-sync allocation IDs. */
        public Map<Integer, Set<String>> inSyncAllocationIds() {
            return inSyncAllocationIds;
        }

        /**
         * Adds rollover information.
         * @param alias the alias
         * @param rolloverInfo the rollover info
         */
        public Builder putRolloverInfo(String alias, MetadataWriteable rolloverInfo) {
            rolloverInfos.put(alias, rolloverInfo);
            return this;
        }

        /** Returns rollover information. */
        public Map<String, MetadataWriteable> rolloverInfos() {
            return rolloverInfos;
        }

        /**
         * Sets whether this is a system index.
         * @param isSystem true if system index
         */
        public Builder system(boolean isSystem) {
            this.isSystem = isSystem;
            return this;
        }

        /** Returns whether this is a system index. */
        public boolean isSystem() {
            return isSystem;
        }

        /**
         * Sets the index context.
         * @param context the context
         */
        public Builder context(ContextModel context) {
            this.context = context;
            return this;
        }

        /** Returns the index context. */
        public ContextModel context() {
            return context;
        }

        /**
         * Sets the ingestion paused status.
         * @param ingestionPaused the ingestion paused status
         */
        public Builder ingestionPaused(Boolean ingestionPaused) {
            this.ingestionPaused = ingestionPaused;
            return this;
        }

        /** Returns the ingestion paused status. */
        public Boolean ingestionPaused() {
            return ingestionPaused;
        }

        /** Builds the IndexMetadataModel. */
        public IndexMetadataModel build() {
            return new IndexMetadataModel(
                index,
                version,
                mappingVersion,
                settingsVersion,
                aliasesVersion,
                routingNumShards,
                state,
                settings,
                primaryTerms,
                mappings,
                aliases,
                customData,
                inSyncAllocationIds,
                rolloverInfos,
                isSystem,
                context,
                ingestionPaused
            );
        }
    }
}
