/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.Version;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.common.XContentContext;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.metadata.settings.SettingsModel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Pure data holder class for index metadata without dependencies on OpenSearch server packages.
 */
@ExperimentalApi
public final class IndexMetadataModel implements Writeable, ToXContentFragment {

    // XContent field names
    private static final String KEY_VERSION = "version";
    private static final String KEY_MAPPING_VERSION = "mapping_version";
    private static final String KEY_SETTINGS_VERSION = "settings_version";
    private static final String KEY_ALIASES_VERSION = "aliases_version";
    private static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    private static final String KEY_SETTINGS = "settings";
    private static final String KEY_STATE = "state";
    private static final String KEY_MAPPINGS = "mappings";
    private static final String KEY_ALIASES = "aliases";
    private static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    private static final String KEY_ROLLOVER_INFOS = "rollover_info";
    private static final String KEY_SYSTEM = "system";
    private static final String KEY_PRIMARY_TERMS = "primary_terms";
    private static final String KEY_CONTEXT = "context";
    private static final String KEY_INGESTION_STATUS = "ingestion_status";
    private static final String KEY_INGESTION_PAUSED = "is_paused";

    // State constants
    private static final byte STATE_OPEN = 0;
    private static final byte STATE_CLOSE = 1;

    /**
     * Version at which customData and rolloverInfos fields are written with length prefixes,
     * enabling standalone deserialization without server-side MetadataReaders.
     * <p>
     * For versions >= SKIPPABLE_FIELDS_VERSION, these fields can be skipped during deserialization
     * when readers are not provided. For older versions, readers are required.
     */
    public static final Version SKIPPABLE_FIELDS_VERSION = Version.V_3_2_0;

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
    private final Map<Integer, Set<String>> inSyncAllocationIds;
    private final boolean isSystem;
    private final ContextModel context;
    private final Boolean ingestionPaused;

    // Server-specific fields: null when skipped (standalone mode), populated when readers are provided
    @Nullable
    private final Map<String, Object> customData;
    @Nullable
    private final Map<String, Object> rolloverInfos;

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
     * @param inSyncAllocationIds in-sync allocation IDs per shard
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
        Map<Integer, Set<String>> inSyncAllocationIds,
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
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.isSystem = isSystem;
        this.context = context;
        this.ingestionPaused = ingestionStatus;
        this.customData = null;
        this.rolloverInfos = null;
    }

    /**
     * Constructs a new IndexMetadataModel with all fields including server-specific customData and rolloverInfos.
     * <p>
     * This constructor is used by the Builder when customData and/or rolloverInfos have been parsed
     * (e.g., via fromXContent with readers provided).
     */
    IndexMetadataModel(
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
        Map<Integer, Set<String>> inSyncAllocationIds,
        boolean isSystem,
        ContextModel context,
        Boolean ingestionStatus,
        @Nullable Map<String, Object> customData,
        @Nullable Map<String, Object> rolloverInfos
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
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.isSystem = isSystem;
        this.context = context;
        this.ingestionPaused = ingestionStatus;
        this.customData = customData;
        this.rolloverInfos = rolloverInfos;
    }

    /**
     * Standalone deserialization constructor that skips server-specific fields.
     * <p>
     * Delegates to {@link #IndexMetadataModel(StreamInput, CheckedFunction, CheckedFunction, Function)}
     * with null readers, which causes customData and rolloverInfos to be skipped.
     * <p>
     * Note: For versions prior to {@link #SKIPPABLE_FIELDS_VERSION}, this constructor will throw
     * an IllegalArgumentException because readers are required to deserialize those fields on older versions.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs
     */
    public IndexMetadataModel(StreamInput in) throws IOException {
        this(in, null, null, null);
    }

    /**
     * Deserialization constructor with optional functional readers for server-specific fields.
     * <p>
     * When readers are provided (full mode), customData and rolloverInfos are deserialized as type-erased
     * {@code Map<String, Object>} entries. When null (standalone mode), these fields are skipped via
     * length-prefix logic and will be null in the resulting model.
     * <p>
     * Skipping requires stream version &gt;= {@link #SKIPPABLE_FIELDS_VERSION}; older versions throw
     * IllegalArgumentException if readers are null.
     *
     * @param in                    the stream to read from
     * @param customDataReader      optional reader for customData entries; null to skip
     * @param rolloverInfoReader    optional reader for rolloverInfos entries; null to skip
     * @param rolloverKeyExtractor  extracts map key from each deserialized rolloverInfo; required when reader is provided
     * @param <C> the type of deserialized customData entries
     * @param <R> the type of deserialized rolloverInfo entries
     * @throws IOException if an I/O error occurs
     * @throws IllegalArgumentException if readers are null and stream version is before SKIPPABLE_FIELDS_VERSION
     */
    public <C, R> IndexMetadataModel(
        StreamInput in,
        @Nullable CheckedFunction<StreamInput, C, IOException> customDataReader,
        @Nullable CheckedFunction<StreamInput, R, IOException> rolloverInfoReader,
        @Nullable Function<R, String> rolloverKeyExtractor
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

        // Read customData — skip or parse depending on whether reader is provided
        int customSize = in.readVInt();
        if (customDataReader != null) {
            Map<String, Object> customBuilder = new HashMap<>(customSize);
            for (int i = 0; i < customSize; i++) {
                String key = in.readString();
                if (in.getVersion().onOrAfter(SKIPPABLE_FIELDS_VERSION)) {
                    in.readVInt(); // read length prefix (not used when reader is provided)
                }
                customBuilder.put(key, customDataReader.apply(in));
            }
            this.customData = customBuilder;
        } else {
            for (int i = 0; i < customSize; i++) {
                in.readString(); // key
                if (in.getVersion().onOrAfter(SKIPPABLE_FIELDS_VERSION)) {
                    int length = in.readVInt();
                    long skipped = in.skip(length);
                    if (skipped != length) {
                        throw new IOException("Failed to skip customData bytes: expected " + length + ", skipped " + skipped);
                    }
                } else {
                    throw new IllegalArgumentException("Cannot skip customData without reader for version < " + SKIPPABLE_FIELDS_VERSION);
                }
            }
            this.customData = null;
        }

        this.inSyncAllocationIds = in.readMap(StreamInput::readVInt, i -> i.readSet(StreamInput::readString));

        // Read rolloverInfos — skip or parse depending on whether reader is provided
        int rolloverSize = in.readVInt();
        if (rolloverInfoReader != null) {
            Map<String, Object> rolloverBuilder = new HashMap<>(rolloverSize);
            for (int i = 0; i < rolloverSize; i++) {
                if (in.getVersion().onOrAfter(SKIPPABLE_FIELDS_VERSION)) {
                    in.readVInt(); // read length prefix (not used when reader is provided)
                }
                R info = rolloverInfoReader.apply(in);
                rolloverBuilder.put(rolloverKeyExtractor.apply(info), info);
            }
            this.rolloverInfos = rolloverBuilder;
        } else {
            for (int i = 0; i < rolloverSize; i++) {
                if (in.getVersion().onOrAfter(SKIPPABLE_FIELDS_VERSION)) {
                    int length = in.readVInt();
                    long skipped = in.skip(length);
                    if (skipped != length) {
                        throw new IOException("Failed to skip rolloverInfos bytes: expected " + length + ", skipped " + skipped);
                    }
                } else {
                    throw new IllegalArgumentException(
                        "Cannot skip rolloverInfos without reader for version < " + SKIPPABLE_FIELDS_VERSION
                    );
                }
            }
            this.rolloverInfos = null;
        }

        this.isSystem = in.readBoolean();

        if (in.getVersion().onOrAfter(Version.V_2_17_0)) {
            this.context = in.readOptionalWriteable(ContextModel::new);
        } else {
            this.context = null;
        }

        if (in.getVersion().onOrAfter(Version.V_3_0_0)) {
            // Wire format matches IndexMetadata.writeTo() which uses writeOptionalWriteable(IngestionStatus).
            // That writes: boolean(present) then IngestionStatus.writeTo() which writes boolean(isPaused).
            // This differs from writeOptionalBoolean which uses a single tri-state byte.
            if (in.readBoolean()) {
                this.ingestionPaused = in.readBoolean();
            } else {
                this.ingestionPaused = null;
            }
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

    /** Returns in-sync allocation IDs per shard. */
    public Map<Integer, Set<String>> inSyncAllocationIds() {
        return inSyncAllocationIds;
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

    /**
     * Returns the deserialized customData map, or null if skipped (standalone mode).
     * Values are type-erased; the caller must cast to the appropriate type (e.g., DiffableStringMap).
     */
    @Nullable
    public Map<String, Object> customData() {
        return customData;
    }

    /**
     * Returns the deserialized rolloverInfos map, or null if skipped (standalone mode).
     * Values are type-erased; the caller must cast to the appropriate type (e.g., RolloverInfo).
     */
    @Nullable
    public Map<String, Object> rolloverInfos() {
        return rolloverInfos;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        writeTo(out, null, null);
    }

    /**
     * Writes this model to the given output, using optional writer lambdas for customData and rolloverInfos.
     * When writers are null, writes 0 (standalone mode). When provided, delegates to the writer at the
     * correct position in the stream (server mode).
     *
     * @param out the stream output
     * @param customDataWriter optional writer for custom data entries
     * @param rolloverInfosWriter optional writer for rollover info entries
     */
    public void writeTo(
        StreamOutput out,
        @Nullable CheckedConsumer<StreamOutput, IOException> customDataWriter,
        @Nullable CheckedConsumer<StreamOutput, IOException> rolloverInfosWriter
    ) throws IOException {
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

        if (customDataWriter != null) {
            customDataWriter.accept(out);
        } else {
            out.writeVInt(0);
        }

        out.writeMap(inSyncAllocationIds, StreamOutput::writeVInt, StreamOutput::writeStringCollection);

        if (rolloverInfosWriter != null) {
            rolloverInfosWriter.accept(out);
        } else {
            out.writeVInt(0);
        }

        out.writeBoolean(isSystem);

        if (out.getVersion().onOrAfter(Version.V_2_17_0)) {
            out.writeOptionalWriteable(context);
        }

        if (out.getVersion().onOrAfter(Version.V_3_0_0)) {
            if (ingestionPaused != null) {
                out.writeBoolean(true);
                out.writeBoolean(ingestionPaused);
            } else {
                out.writeBoolean(false);
            }
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
            && Objects.equals(inSyncAllocationIds, that.inSyncAllocationIds)
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
            inSyncAllocationIds,
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
        private final Map<Integer, Set<String>> inSyncAllocationIds;
        private boolean isSystem;
        private ContextModel context;
        private Boolean ingestionPaused;
        @Nullable
        private Map<String, Object> customData;
        @Nullable
        private Map<String, Object> rolloverInfos;

        /**
         * Creates a new builder with the given index name.
         * @param index the index name
         */
        public Builder(String index) {
            this.index = index;
            this.mappings = new HashMap<>();
            this.aliases = new HashMap<>();
            this.inSyncAllocationIds = new HashMap<>();
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
            this.inSyncAllocationIds = new HashMap<>(model.inSyncAllocationIds);
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

        /**
         * Puts a customData entry.
         * @param key the custom data key
         * @param value the custom data value
         */
        public Builder putCustomData(String key, Object value) {
            if (this.customData == null) {
                this.customData = new HashMap<>();
            }
            this.customData.put(key, value);
            return this;
        }

        /** Returns the customData map, or null if not set. */
        @Nullable
        public Map<String, Object> customData() {
            return customData;
        }

        /**
         * Puts a rolloverInfos entry.
         * @param key the rollover info key
         * @param value the rollover info value
         */
        public Builder putRolloverInfo(String key, Object value) {
            if (this.rolloverInfos == null) {
                this.rolloverInfos = new HashMap<>();
            }
            this.rolloverInfos.put(key, value);
            return this;
        }

        /** Returns the rolloverInfos map, or null if not set. */
        @Nullable
        public Map<String, Object> rolloverInfos() {
            return rolloverInfos;
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
                inSyncAllocationIds,
                isSystem,
                context,
                ingestionPaused,
                customData,
                rolloverInfos
            );
        }
    }

    /**
     * Parses an IndexMetadataModel from XContent in standalone mode (no readers).
     * <p>
     * Delegates to {@link #fromXContent(XContentParser, CheckedBiFunction, CheckedBiFunction)}
     * with null parsers, which causes customData and rolloverInfos to be skipped.
     *
     * @param parser the XContent parser
     * @return the parsed IndexMetadataModel
     * @throws IOException if parsing fails
     */
    public static IndexMetadataModel fromXContent(XContentParser parser) throws IOException {
        return fromXContent(parser, null, null);
    }

    /**
     * Parses an IndexMetadataModel from XContent with optional parsers for server-specific fields.
     * Expects the parser to be positioned at the index name field.
     * <p>
     * When parsers are provided (full mode), customData and rolloverInfos are parsed and stored.
     * When null (standalone mode), these fields are skipped via {@code parser.skipChildren()}.
     *
     * @param parser the XContent parser
     * @param customDataParser optional: (parser, fieldName) &rarr; parsed customData entry; null to skip
     * @param rolloverInfoParser optional: (parser, fieldName) &rarr; parsed rolloverInfo entry; null to skip
     * @param <C> the type of deserialized customData entries
     * @param <R> the type of deserialized rolloverInfo entries
     * @return the parsed IndexMetadataModel
     * @throws IOException if parsing fails
     */
    @SuppressWarnings("unchecked")
    public static <C, R> IndexMetadataModel fromXContent(
        XContentParser parser,
        @Nullable CheckedBiFunction<XContentParser, String, C, IOException> customDataParser,
        @Nullable CheckedBiFunction<XContentParser, String, R, IOException> rolloverInfoParser
    ) throws IOException {
        if (parser.currentToken() == null) {
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
            throw new IllegalArgumentException("expected field name but got " + parser.currentToken());
        }

        Builder builder = new Builder(parser.currentName());

        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected object but got " + token);
        }

        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (KEY_SETTINGS.equals(currentFieldName)) {
                    builder.settings(SettingsModel.fromXContent(parser));
                } else if (KEY_MAPPINGS.equals(currentFieldName)) {
                    // Parse mappings object: { "type_name": { ... } }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            // Parser is at the mapping type name field
                            // MappingMetadataModel.fromXContent expects parser at FIELD_NAME
                            builder.putMapping(MappingMetadataModel.fromXContent(parser));
                        }
                    }
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    // Parse aliases object: { "alias_name": { ... }, ... }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            // Parser is now at the alias name field
                            builder.putAlias(AliasMetadataModel.fromXContent(parser));
                        }
                    }
                } else if (KEY_IN_SYNC_ALLOCATIONS.equals(currentFieldName)) {
                    // Parse in_sync_allocations: { "0": ["id1", "id2"], "1": ["id3"] }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            int shardId = Integer.parseInt(currentFieldName);
                            Set<String> allocationIds = new HashSet<>();
                            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                if (token == XContentParser.Token.VALUE_STRING) {
                                    allocationIds.add(parser.text());
                                }
                            }
                            builder.putInSyncAllocationIds(shardId, allocationIds);
                        }
                    }
                } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                    // Parse primary_terms as object: { "0": 1, "1": 2 }
                    Map<Integer, Long> primaryTermsMap = new HashMap<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_NUMBER) {
                            primaryTermsMap.put(Integer.parseInt(currentFieldName), parser.longValue());
                        }
                    }
                    if (!primaryTermsMap.isEmpty()) {
                        int maxShard = primaryTermsMap.keySet().stream().max(Integer::compareTo).orElse(-1);
                        long[] terms = new long[maxShard + 1];
                        for (Map.Entry<Integer, Long> entry : primaryTermsMap.entrySet()) {
                            terms[entry.getKey()] = entry.getValue();
                        }
                        builder.primaryTerms(terms);
                    }
                } else if (KEY_ROLLOVER_INFOS.equals(currentFieldName)) {
                    if (rolloverInfoParser != null) {
                        // Full mode: parse rolloverInfos using provided parser
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                String rolloverKey = parser.currentName();
                                parser.nextToken(); // move to START_OBJECT
                                R info = rolloverInfoParser.apply(parser, rolloverKey);
                                builder.putRolloverInfo(rolloverKey, info);
                            }
                        }
                    } else {
                        // Standalone mode: skip rolloverInfos
                        parser.skipChildren();
                    }
                } else if (KEY_CONTEXT.equals(currentFieldName)) {
                    builder.context(ContextModel.fromXContent(parser));
                } else if (KEY_INGESTION_STATUS.equals(currentFieldName)) {
                    // Parse ingestion_status: { "paused": true/false }
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                            if (KEY_INGESTION_PAUSED.equals(currentFieldName)) {
                                builder.ingestionPaused(parser.booleanValue());
                            }
                        }
                    }
                } else {
                    if (customDataParser != null) {
                        // Full mode: parse customData entry using provided parser
                        C customEntry = customDataParser.apply(parser, currentFieldName);
                        builder.putCustomData(currentFieldName, customEntry);
                    } else {
                        // Standalone mode: skip unknown objects (customData, etc.)
                        parser.skipChildren();
                    }
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (KEY_MAPPINGS.equals(currentFieldName)) {
                    // Parse mappings array format (non-API context)
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                            // Binary format: compressed mapping bytes
                            byte[] compressed = parser.binaryValue();
                            byte[] uncompressed = new CompressedData(compressed).uncompressed();
                            try (XContentParser mappingParser = JsonXContent.jsonXContent.createParser(null, null, uncompressed)) {
                                mappingParser.nextToken(); // START_OBJECT
                                mappingParser.nextToken(); // FIELD_NAME
                                builder.putMapping(MappingMetadataModel.fromXContent(mappingParser));
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                XContentBuilder xBuilder = JsonXContent.contentBuilder();
                                xBuilder.startObject();
                                xBuilder.field(mappingType);
                                xBuilder.map((Map<String, Object>) mapping.get(mappingType));
                                xBuilder.endObject();
                                byte[] bytes = BytesReference.toBytes(BytesReference.bytes(xBuilder));
                                try (XContentParser mappingParser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
                                    mappingParser.nextToken(); // START_OBJECT
                                    mappingParser.nextToken(); // FIELD_NAME
                                    builder.putMapping(MappingMetadataModel.fromXContent(mappingParser));
                                }
                            }
                        }
                    }
                } else if (KEY_ALIASES.equals(currentFieldName)) {
                    // Parse aliases array format (API context) - just alias names
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_STRING) {
                            String aliasName = parser.text();
                            builder.putAlias(new AliasMetadataModel.Builder(aliasName).build());
                        }
                    }
                } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                    // Parse primary_terms as array
                    List<Long> termsList = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        if (token == XContentParser.Token.VALUE_NUMBER) {
                            termsList.add(parser.longValue());
                        }
                    }
                    if (!termsList.isEmpty()) {
                        long[] terms = new long[termsList.size()];
                        for (int i = 0; i < termsList.size(); i++) {
                            terms[i] = termsList.get(i);
                        }
                        builder.primaryTerms(terms);
                    }
                } else {
                    // Skip unknown arrays
                    parser.skipChildren();
                }
            } else if (token.isValue() || token == XContentParser.Token.VALUE_NULL) {
                if (KEY_VERSION.equals(currentFieldName)) {
                    builder.version(parser.longValue());
                } else if (KEY_MAPPING_VERSION.equals(currentFieldName)) {
                    builder.mappingVersion(parser.longValue());
                } else if (KEY_SETTINGS_VERSION.equals(currentFieldName)) {
                    builder.settingsVersion(parser.longValue());
                } else if (KEY_ALIASES_VERSION.equals(currentFieldName)) {
                    builder.aliasesVersion(parser.longValue());
                } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                    builder.routingNumShards(parser.intValue());
                } else if (KEY_STATE.equals(currentFieldName)) {
                    String stateStr = parser.text();
                    builder.state("open".equals(stateStr.toLowerCase(Locale.ROOT)) ? STATE_OPEN : STATE_CLOSE);
                } else if (KEY_SYSTEM.equals(currentFieldName)) {
                    builder.system(parser.booleanValue());
                } else {
                    throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                }
            }
        }

        return builder.build();
    }

    /**
     * Writes this model to XContent in standalone mode — customData and rolloverInfos are omitted.
     *
     * @param builder the XContent builder
     * @param params the serialization parameters
     * @return the XContent builder
     * @throws IOException if an I/O error occurs
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return toXContent(builder, params, null, null);
    }

    /**
     * Writes this model to XContent with optional writer lambdas for customData and rolloverInfos.
     * Supports context mode via {@link XContentContext}:
     * <ul>
     *   <li>GATEWAY: flat_settings forced, array mappings (binary or full map), object aliases (full), array primary_terms</li>
     *   <li>API: settings use params, object mappings (type reduced), array aliases (names only), object primary_terms</li>
     * </ul>
     *
     * @param builder the XContent builder
     * @param params the serialization parameters
     * @param customDataWriter optional writer for custom data entries
     * @param rolloverInfosWriter optional writer for rollover info entries
     * @return the XContent builder
     * @throws IOException if an I/O error occurs
     */
    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable CheckedConsumer<XContentBuilder, IOException> customDataWriter,
        @Nullable CheckedConsumer<XContentBuilder, IOException> rolloverInfosWriter
    ) throws IOException {
        return toXContent(builder, params, null, customDataWriter, rolloverInfosWriter);
    }

    /**
     * Serializes this IndexMetadataModel to XContent with optional server-side writers.
     *
     * @param builder             the XContent builder
     * @param params              the serialization parameters
     * @param settingsWriter      if non-null, writes settings (server uses this for filtering)
     * @param customDataWriter    if non-null, writes custom data entries
     * @param rolloverInfosWriter if non-null, writes rollover info entries
     */
    public XContentBuilder toXContent(
        XContentBuilder builder,
        Params params,
        @Nullable CheckedConsumer<XContentBuilder, IOException> settingsWriter,
        @Nullable CheckedConsumer<XContentBuilder, IOException> customDataWriter,
        @Nullable CheckedConsumer<XContentBuilder, IOException> rolloverInfosWriter
    ) throws IOException {
        XContentContext xContentContext = XContentContext.valueOf(params.param(XContentContext.PARAM_KEY, XContentContext.API.name()));
        boolean binary = params.paramAsBoolean("binary", false);

        builder.startObject(index);

        builder.field(KEY_VERSION, version);
        builder.field(KEY_MAPPING_VERSION, mappingVersion);
        builder.field(KEY_SETTINGS_VERSION, settingsVersion);
        builder.field(KEY_ALIASES_VERSION, aliasesVersion);
        builder.field(KEY_ROUTING_NUM_SHARDS, routingNumShards);
        builder.field(KEY_STATE, state == STATE_OPEN ? "open" : "close");

        builder.startObject(KEY_SETTINGS);
        if (settingsWriter != null) {
            settingsWriter.accept(builder);
        } else if (xContentContext != XContentContext.API) {
            settings.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        } else {
            settings.toXContent(builder, params);
        }
        builder.endObject();

        if (xContentContext != XContentContext.API) {
            builder.startArray(KEY_MAPPINGS);
            for (MappingMetadataModel mapping : mappings.values()) {
                mapping.toXContent(builder, params);
            }
            builder.endArray();
        } else {
            builder.startObject(KEY_MAPPINGS);
            for (MappingMetadataModel mapping : mappings.values()) {
                mapping.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (customDataWriter != null) {
            customDataWriter.accept(builder);
        }

        if (xContentContext != XContentContext.API) {
            builder.startObject(KEY_ALIASES);
            for (AliasMetadataModel alias : aliases.values()) {
                alias.toXContent(builder, params);
            }
            builder.endObject();

            builder.startArray(KEY_PRIMARY_TERMS);
            if (primaryTerms != null) {
                for (long primaryTerm : primaryTerms) {
                    builder.value(primaryTerm);
                }
            }
            builder.endArray();
        } else {
            builder.startArray(KEY_ALIASES);
            for (String aliasName : aliases.keySet()) {
                builder.value(aliasName);
            }
            builder.endArray();

            builder.startObject(KEY_PRIMARY_TERMS);
            if (primaryTerms != null) {
                for (int i = 0; i < primaryTerms.length; i++) {
                    builder.field(Integer.toString(i), primaryTerms[i]);
                }
            }
            builder.endObject();
        }

        builder.startObject(KEY_IN_SYNC_ALLOCATIONS);
        for (Map.Entry<Integer, Set<String>> entry : inSyncAllocationIds.entrySet()) {
            builder.startArray(String.valueOf(entry.getKey()));
            for (String allocationId : entry.getValue()) {
                builder.value(allocationId);
            }
            builder.endArray();
        }
        builder.endObject();

        if (rolloverInfosWriter != null) {
            builder.startObject(KEY_ROLLOVER_INFOS);
            rolloverInfosWriter.accept(builder);
            builder.endObject();
        }

        builder.field(KEY_SYSTEM, isSystem);

        if (context != null) {
            builder.field(KEY_CONTEXT);
            context.toXContent(builder, params);
        }

        if (ingestionPaused != null) {
            builder.startObject(KEY_INGESTION_STATUS);
            builder.field(KEY_INGESTION_PAUSED, ingestionPaused);
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }
}
