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

package org.opensearch.cluster.metadata;

import org.opensearch.core.Assertions;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.rollover.RolloverInfo;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.Diffable;
import org.opensearch.cluster.DiffableUtils;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.cluster.routing.allocation.IndexMetadataUpdater;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.core.index.Index;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.opensearch.cluster.metadata.Metadata.CONTEXT_MODE_PARAM;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.IP_VALIDATOR;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.AND;
import static org.opensearch.cluster.node.DiscoveryNodeFilters.OpType.OR;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * Index metadata information
 *
 * @opensearch.internal
 */
public class IndexMetadata implements Diffable<IndexMetadata>, ToXContentFragment {

    public static final ClusterBlock INDEX_READ_ONLY_BLOCK = new ClusterBlock(
        5,
        "index read-only (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE, ClusterBlockLevel.METADATA_WRITE)
    );
    public static final ClusterBlock INDEX_READ_BLOCK = new ClusterBlock(
        7,
        "index read (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.READ)
    );
    public static final ClusterBlock INDEX_WRITE_BLOCK = new ClusterBlock(
        8,
        "index write (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.WRITE)
    );
    public static final ClusterBlock INDEX_METADATA_BLOCK = new ClusterBlock(
        9,
        "index metadata (api)",
        false,
        false,
        false,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.METADATA_READ)
    );
    public static final ClusterBlock INDEX_READ_ONLY_ALLOW_DELETE_BLOCK = new ClusterBlock(
        12,
        "disk usage exceeded flood-stage watermark, index has read-only-allow-delete block",
        false,
        false,
        true,
        RestStatus.TOO_MANY_REQUESTS,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.WRITE)
    );

    public static final ClusterBlock REMOTE_READ_ONLY_ALLOW_DELETE = new ClusterBlock(
        13,
        "remote index is read-only",
        false,
        false,
        true,
        RestStatus.FORBIDDEN,
        EnumSet.of(ClusterBlockLevel.METADATA_WRITE, ClusterBlockLevel.WRITE)
    );

    /**
     * The state of the index.
     *
     * @opensearch.internal
     */
    public enum State {
        OPEN((byte) 0),
        CLOSE((byte) 1);

        private final byte id;

        State(byte id) {
            this.id = id;
        }

        public byte id() {
            return this.id;
        }

        public static State fromId(byte id) {
            if (id == 0) {
                return OPEN;
            } else if (id == 1) {
                return CLOSE;
            }
            throw new IllegalStateException("No state match for id [" + id + "]");
        }

        public static State fromString(String state) {
            if ("open".equals(state)) {
                return OPEN;
            } else if ("close".equals(state)) {
                return CLOSE;
            }
            throw new IllegalStateException("No state match for [" + state + "]");
        }
    }

    static Setting<Integer> buildNumberOfShardsSetting() {
        /* This is a safety limit that should only be exceeded in very rare and special cases. The assumption is that
         * 99% of the users have less than 1024 shards per index. We also make it a hard check that requires restart of nodes
         * if a cluster should allow to create more than 1024 shards per index. NOTE: this does not limit the number of shards
         * per cluster. this also prevents creating stuff like a new index with millions of shards by accident which essentially
         * kills the entire cluster with OOM on the spot.*/
        final int maxNumShards = Integer.parseInt(System.getProperty(MAX_NUMBER_OF_SHARDS, "1024"));
        if (maxNumShards < 1) {
            throw new IllegalArgumentException(MAX_NUMBER_OF_SHARDS + " must be > 0");
        }
        /* This property should be provided if the default number of shards for any new index has to be changed to a value
         * other than 1. This value is applicable only if the index.number_of_shards setting is not provided as part of the
         * index creation request. */
        final int defaultNumShards = Integer.parseInt(System.getProperty(DEFAULT_NUMBER_OF_SHARDS, "1"));
        if (defaultNumShards < 1 || defaultNumShards > maxNumShards) {
            throw new IllegalArgumentException(
                DEFAULT_NUMBER_OF_SHARDS
                    + " value ["
                    + defaultNumShards
                    + "] must between "
                    + "1 and "
                    + MAX_NUMBER_OF_SHARDS
                    + " ["
                    + maxNumShards
                    + "]"
            );
        }
        return Setting.intSetting(SETTING_NUMBER_OF_SHARDS, defaultNumShards, 1, maxNumShards, Property.IndexScope, Property.Final);
    }

    public static final String INDEX_SETTING_PREFIX = "index.";
    public static final String SETTING_NUMBER_OF_SHARDS = "index.number_of_shards";
    static final String DEFAULT_NUMBER_OF_SHARDS = "opensearch.index.default_number_of_shards";
    static final String MAX_NUMBER_OF_SHARDS = "opensearch.index.max_number_of_shards";
    public static final Setting<Integer> INDEX_NUMBER_OF_SHARDS_SETTING = buildNumberOfShardsSetting();
    public static final String SETTING_NUMBER_OF_REPLICAS = "index.number_of_replicas";
    public static final Setting<Integer> INDEX_NUMBER_OF_REPLICAS_SETTING = Setting.intSetting(
        SETTING_NUMBER_OF_REPLICAS,
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final String SETTING_ROUTING_PARTITION_SIZE = "index.routing_partition_size";
    public static final Setting<Integer> INDEX_ROUTING_PARTITION_SIZE_SETTING = Setting.intSetting(
        SETTING_ROUTING_PARTITION_SIZE,
        1,
        1,
        Property.IndexScope
    );

    public static final Setting<Integer> INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING = Setting.intSetting(
        "index.number_of_routing_shards",
        INDEX_NUMBER_OF_SHARDS_SETTING,
        1,
        new Setting.Validator<Integer>() {

            @Override
            public void validate(final Integer value) {

            }

            @Override
            public void validate(final Integer numRoutingShards, final Map<Setting<?>, Object> settings) {
                int numShards = (int) settings.get(INDEX_NUMBER_OF_SHARDS_SETTING);
                if (numRoutingShards < numShards) {
                    throw new IllegalArgumentException(
                        "index.number_of_routing_shards [" + numRoutingShards + "] must be >= index.number_of_shards [" + numShards + "]"
                    );
                }
                getRoutingFactor(numShards, numRoutingShards);
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = Collections.singletonList(INDEX_NUMBER_OF_SHARDS_SETTING);
                return settings.iterator();
            }

        },
        Property.IndexScope
    );

    /**
     * Used to specify the replication type for the index. By default, document replication is used.
     */
    public static final String SETTING_REPLICATION_TYPE = "index.replication.type";
    public static final Setting<ReplicationType> INDEX_REPLICATION_TYPE_SETTING = new Setting<>(
        SETTING_REPLICATION_TYPE,
        ReplicationType.DOCUMENT.toString(),
        ReplicationType::parseString,
        new Setting.Validator<>() {

            @Override
            public void validate(final ReplicationType value) {}

            @Override
            public void validate(final ReplicationType value, final Map<Setting<?>, Object> settings) {
                final Object remoteStoreEnabled = settings.get(INDEX_REMOTE_STORE_ENABLED_SETTING);
                if (ReplicationType.SEGMENT.equals(value) == false && Objects.equals(remoteStoreEnabled, true)) {
                    throw new IllegalArgumentException(
                        "To enable "
                            + INDEX_REMOTE_STORE_ENABLED_SETTING.getKey()
                            + ", "
                            + INDEX_REPLICATION_TYPE_SETTING.getKey()
                            + " should be set to "
                            + ReplicationType.SEGMENT
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(INDEX_REMOTE_STORE_ENABLED_SETTING);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.Final
    );

    public static final String SETTING_REMOTE_STORE_ENABLED = "index.remote_store.enabled";

    public static final String SETTING_REMOTE_SEGMENT_STORE_REPOSITORY = "index.remote_store.segment.repository";

    public static final String SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY = "index.remote_store.translog.repository";

    /**
     * Used to specify if the index data should be persisted in the remote store.
     */
    public static final Setting<Boolean> INDEX_REMOTE_STORE_ENABLED_SETTING = Setting.boolSetting(
        SETTING_REMOTE_STORE_ENABLED,
        false,
        new Setting.Validator<>() {

            @Override
            public void validate(final Boolean value) {}

            @Override
            public void validate(final Boolean value, final Map<Setting<?>, Object> settings) {
                final Object replicationType = settings.get(INDEX_REPLICATION_TYPE_SETTING);
                if (ReplicationType.SEGMENT.equals(replicationType) == false && value) {
                    throw new IllegalArgumentException(
                        "To enable "
                            + INDEX_REMOTE_STORE_ENABLED_SETTING.getKey()
                            + ", "
                            + INDEX_REPLICATION_TYPE_SETTING.getKey()
                            + " should be set to "
                            + ReplicationType.SEGMENT
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = List.of(INDEX_REPLICATION_TYPE_SETTING);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.PrivateIndex,
        Property.Dynamic
    );

    /**
     * Used to specify remote store repository to use for this index.
     */
    public static final Setting<String> INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING = Setting.simpleString(
        SETTING_REMOTE_SEGMENT_STORE_REPOSITORY,
        new Setting.Validator<>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                if (value == null || value.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Setting "
                            + INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.getKey()
                            + " should be provided with non-empty repository ID"
                    );
                } else {
                    validateRemoteStoreSettingEnabled(settings, INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING);
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = Collections.singletonList(INDEX_REMOTE_STORE_ENABLED_SETTING);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.PrivateIndex,
        Property.Dynamic
    );

    private static void validateRemoteStoreSettingEnabled(final Map<Setting<?>, Object> settings, Setting<?> setting) {
        final Boolean isRemoteSegmentStoreEnabled = (Boolean) settings.get(INDEX_REMOTE_STORE_ENABLED_SETTING);
        if (isRemoteSegmentStoreEnabled == false) {
            throw new IllegalArgumentException(
                "Settings "
                    + setting.getKey()
                    + " can only be set/enabled when "
                    + INDEX_REMOTE_STORE_ENABLED_SETTING.getKey()
                    + " is set to true"
            );
        }
    }

    public static final Setting<String> INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING = Setting.simpleString(
        SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY,
        new Setting.Validator<>() {

            @Override
            public void validate(final String value) {}

            @Override
            public void validate(final String value, final Map<Setting<?>, Object> settings) {
                if (value == null || value.isEmpty()) {
                    throw new IllegalArgumentException(
                        "Setting " + INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey() + " should be provided with non-empty repository ID"
                    );
                } else {
                    final Boolean isRemoteTranslogStoreEnabled = (Boolean) settings.get(INDEX_REMOTE_STORE_ENABLED_SETTING);
                    if (isRemoteTranslogStoreEnabled == null || isRemoteTranslogStoreEnabled == false) {
                        throw new IllegalArgumentException(
                            "Settings "
                                + INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING.getKey()
                                + " can only be set/enabled when "
                                + INDEX_REMOTE_STORE_ENABLED_SETTING.getKey()
                                + " is set to true"
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                final List<Setting<?>> settings = Collections.singletonList(INDEX_REMOTE_STORE_ENABLED_SETTING);
                return settings.iterator();
            }
        },
        Property.IndexScope,
        Property.PrivateIndex,
        Property.Dynamic
    );

    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";
    public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = AutoExpandReplicas.SETTING;

    /**
     * Blocks the API.
     *
     * @opensearch.internal
     */
    public enum APIBlock implements Writeable {
        READ_ONLY("read_only", INDEX_READ_ONLY_BLOCK),
        READ("read", INDEX_READ_BLOCK),
        WRITE("write", INDEX_WRITE_BLOCK),
        METADATA("metadata", INDEX_METADATA_BLOCK),
        READ_ONLY_ALLOW_DELETE("read_only_allow_delete", INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);

        final String name;
        final String settingName;
        final Setting<Boolean> setting;
        final ClusterBlock block;

        APIBlock(String name, ClusterBlock block) {
            this.name = name;
            this.settingName = "index.blocks." + name;
            this.setting = Setting.boolSetting(settingName, false, Property.Dynamic, Property.IndexScope);
            this.block = block;
        }

        public String settingName() {
            return settingName;
        }

        public Setting<Boolean> setting() {
            return setting;
        }

        public ClusterBlock getBlock() {
            return block;
        }

        public static APIBlock fromName(String name) {
            for (APIBlock block : APIBlock.values()) {
                if (block.name.equals(name)) {
                    return block;
                }
            }
            throw new IllegalArgumentException("No block found with name " + name);
        }

        public static APIBlock fromSetting(String settingName) {
            for (APIBlock block : APIBlock.values()) {
                if (block.settingName.equals(settingName)) {
                    return block;
                }
            }
            throw new IllegalArgumentException("No block found with setting name " + settingName);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(ordinal());
        }

        public static APIBlock readFrom(StreamInput input) throws IOException {
            return APIBlock.values()[input.readVInt()];
        }
    }

    public static final String SETTING_READ_ONLY = APIBlock.READ_ONLY.settingName();
    public static final Setting<Boolean> INDEX_READ_ONLY_SETTING = APIBlock.READ_ONLY.setting();

    public static final String SETTING_BLOCKS_READ = APIBlock.READ.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_READ_SETTING = APIBlock.READ.setting();

    public static final String SETTING_BLOCKS_WRITE = APIBlock.WRITE.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_WRITE_SETTING = APIBlock.WRITE.setting();

    public static final String SETTING_BLOCKS_METADATA = APIBlock.METADATA.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_METADATA_SETTING = APIBlock.METADATA.setting();

    public static final String SETTING_READ_ONLY_ALLOW_DELETE = APIBlock.READ_ONLY_ALLOW_DELETE.settingName();
    public static final Setting<Boolean> INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING = APIBlock.READ_ONLY_ALLOW_DELETE.setting();

    public static final String SETTING_VERSION_CREATED = "index.version.created";

    public static final Setting<Version> SETTING_INDEX_VERSION_CREATED = Setting.versionSetting(
        SETTING_VERSION_CREATED,
        Version.V_EMPTY,
        Property.IndexScope,
        Property.PrivateIndex
    );

    public static final String SETTING_VERSION_CREATED_STRING = "index.version.created_string";
    public static final String SETTING_VERSION_UPGRADED = "index.version.upgraded";
    public static final String SETTING_VERSION_UPGRADED_STRING = "index.version.upgraded_string";
    public static final String SETTING_CREATION_DATE = "index.creation_date";
    /**
     * The user provided name for an index. This is the plain string provided by the user when the index was created.
     * It might still contain date math expressions etc. (added in 5.0)
     */
    public static final String SETTING_INDEX_PROVIDED_NAME = "index.provided_name";
    public static final String SETTING_PRIORITY = "index.priority";
    public static final Setting<Integer> INDEX_PRIORITY_SETTING = Setting.intSetting(
        "index.priority",
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final String SETTING_CREATION_DATE_STRING = "index.creation_date_string";
    public static final String SETTING_INDEX_UUID = "index.uuid";
    public static final String SETTING_HISTORY_UUID = "index.history.uuid";
    public static final String SETTING_DATA_PATH = "index.data_path";
    public static final Setting<String> INDEX_DATA_PATH_SETTING = new Setting<>(
        SETTING_DATA_PATH,
        "",
        Function.identity(),
        Property.IndexScope
    );
    public static final String INDEX_UUID_NA_VALUE = Strings.UNKNOWN_UUID_VALUE;

    public static final String INDEX_ROUTING_REQUIRE_GROUP_PREFIX = "index.routing.allocation.require";
    public static final String INDEX_ROUTING_INCLUDE_GROUP_PREFIX = "index.routing.allocation.include";
    public static final String INDEX_ROUTING_EXCLUDE_GROUP_PREFIX = "index.routing.allocation.exclude";
    public static final Setting.AffixSetting<String> INDEX_ROUTING_REQUIRE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_REQUIRE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<String> INDEX_ROUTING_EXCLUDE_GROUP_SETTING = Setting.prefixKeySetting(
        INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + ".",
        key -> Setting.simpleString(key, value -> IP_VALIDATOR.accept(key, value), Property.Dynamic, Property.IndexScope)
    );
    public static final Setting.AffixSetting<String> INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING = Setting.prefixKeySetting(
        "index.routing.allocation.initial_recovery.",
        key -> Setting.simpleString(key)
    );

    /**
     * The number of active shard copies to check for before proceeding with a write operation.
     */
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS = new Setting<>(
        "index.write.wait_for_active_shards",
        "1",
        ActiveShardCount::parseString,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    public static final String SETTING_INDEX_HIDDEN = "index.hidden";
    /**
     * Whether the index is considered hidden or not. A hidden index will not be resolved in
     * normal wildcard searches unless explicitly allowed
     */
    public static final Setting<Boolean> INDEX_HIDDEN_SETTING = Setting.boolSetting(
        SETTING_INDEX_HIDDEN,
        false,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * an internal index format description, allowing us to find out if this index is upgraded or needs upgrading
     */
    private static final String INDEX_FORMAT = "index.format";
    public static final Setting<Integer> INDEX_FORMAT_SETTING = Setting.intSetting(
        INDEX_FORMAT,
        0,
        Setting.Property.IndexScope,
        Setting.Property.Final
    );

    public static final String KEY_IN_SYNC_ALLOCATIONS = "in_sync_allocations";
    static final String KEY_VERSION = "version";
    static final String KEY_MAPPING_VERSION = "mapping_version";
    static final String KEY_SETTINGS_VERSION = "settings_version";
    static final String KEY_ALIASES_VERSION = "aliases_version";
    static final String KEY_ROUTING_NUM_SHARDS = "routing_num_shards";
    static final String KEY_SETTINGS = "settings";
    static final String KEY_STATE = "state";
    static final String KEY_MAPPINGS = "mappings";
    static final String KEY_ALIASES = "aliases";
    static final String KEY_ROLLOVER_INFOS = "rollover_info";
    static final String KEY_SYSTEM = "system";
    public static final String KEY_PRIMARY_TERMS = "primary_terms";

    public static final String INDEX_STATE_FILE_PREFIX = "state-";

    private final int routingNumShards;
    private final int routingFactor;
    private final int routingPartitionSize;

    private final int numberOfShards;
    private final int numberOfReplicas;

    private final Index index;
    private final long version;

    private final long mappingVersion;

    private final long settingsVersion;

    private final long aliasesVersion;

    private final long[] primaryTerms;

    private final State state;

    private final Map<String, AliasMetadata> aliases;

    private final Settings settings;

    private final Map<String, MappingMetadata> mappings;

    private final Map<String, DiffableStringMap> customData;

    private final Map<Integer, Set<String>> inSyncAllocationIds;

    private final transient int totalNumberOfShards;

    private final DiscoveryNodeFilters requireFilters;
    private final DiscoveryNodeFilters includeFilters;
    private final DiscoveryNodeFilters excludeFilters;
    private final DiscoveryNodeFilters initialRecoveryFilters;

    private final Version indexCreatedVersion;
    private final Version indexUpgradedVersion;

    private final ActiveShardCount waitForActiveShards;
    private final Map<String, RolloverInfo> rolloverInfos;
    private final boolean isSystem;

    private IndexMetadata(
        final Index index,
        final long version,
        final long mappingVersion,
        final long settingsVersion,
        final long aliasesVersion,
        final long[] primaryTerms,
        final State state,
        final int numberOfShards,
        final int numberOfReplicas,
        final Settings settings,
        final Map<String, MappingMetadata> mappings,
        final Map<String, AliasMetadata> aliases,
        final Map<String, DiffableStringMap> customData,
        final Map<Integer, Set<String>> inSyncAllocationIds,
        final DiscoveryNodeFilters requireFilters,
        final DiscoveryNodeFilters initialRecoveryFilters,
        final DiscoveryNodeFilters includeFilters,
        final DiscoveryNodeFilters excludeFilters,
        final Version indexCreatedVersion,
        final Version indexUpgradedVersion,
        final int routingNumShards,
        final int routingPartitionSize,
        final ActiveShardCount waitForActiveShards,
        final Map<String, RolloverInfo> rolloverInfos,
        final boolean isSystem
    ) {

        this.index = index;
        this.version = version;
        assert mappingVersion >= 0 : mappingVersion;
        this.mappingVersion = mappingVersion;
        assert settingsVersion >= 0 : settingsVersion;
        this.settingsVersion = settingsVersion;
        assert aliasesVersion >= 0 : aliasesVersion;
        this.aliasesVersion = aliasesVersion;
        this.primaryTerms = primaryTerms;
        assert primaryTerms.length == numberOfShards;
        this.state = state;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.totalNumberOfShards = numberOfShards * (numberOfReplicas + 1);
        this.settings = settings;
        this.mappings = Collections.unmodifiableMap(mappings);
        this.customData = Collections.unmodifiableMap(customData);
        this.aliases = Collections.unmodifiableMap(aliases);
        this.inSyncAllocationIds = Collections.unmodifiableMap(inSyncAllocationIds);
        this.requireFilters = requireFilters;
        this.includeFilters = includeFilters;
        this.excludeFilters = excludeFilters;
        this.initialRecoveryFilters = initialRecoveryFilters;
        this.indexCreatedVersion = indexCreatedVersion;
        this.indexUpgradedVersion = indexUpgradedVersion;
        this.routingNumShards = routingNumShards;
        this.routingFactor = routingNumShards / numberOfShards;
        this.routingPartitionSize = routingPartitionSize;
        this.waitForActiveShards = waitForActiveShards;
        this.rolloverInfos = Collections.unmodifiableMap(rolloverInfos);
        this.isSystem = isSystem;
        assert numberOfShards * routingFactor == routingNumShards : routingNumShards + " must be a multiple of " + numberOfShards;
    }

    public Index getIndex() {
        return index;
    }

    public String getIndexUUID() {
        return index.getUUID();
    }

    /**
     * Test whether the current index UUID is the same as the given one. Returns true if either are _na_
     */
    public boolean isSameUUID(String otherUUID) {
        assert otherUUID != null;
        assert getIndexUUID() != null;
        if (INDEX_UUID_NA_VALUE.equals(otherUUID) || INDEX_UUID_NA_VALUE.equals(getIndexUUID())) {
            return true;
        }
        return otherUUID.equals(getIndexUUID());
    }

    public long getVersion() {
        return this.version;
    }

    public long getMappingVersion() {
        return mappingVersion;
    }

    public long getSettingsVersion() {
        return settingsVersion;
    }

    public long getAliasesVersion() {
        return aliasesVersion;
    }

    /**
     * The term of the current selected primary. This is a non-negative number incremented when
     * a primary shard is assigned after a full cluster restart or a replica shard is promoted to a primary.
     *
     * Note: since we increment the term every time a shard is assigned, the term for any operational shard (i.e., a shard
     * that can be indexed into) is larger than 0. See {@link IndexMetadataUpdater#applyChanges}.
     **/
    public long primaryTerm(int shardId) {
        return this.primaryTerms[shardId];
    }

    /**
     * Return the {@link Version} on which this index has been created. This
     * information is typically useful for backward compatibility.
     */
    public Version getCreationVersion() {
        return indexCreatedVersion;
    }

    /**
     * Return the {@link Version} on which this index has been upgraded. This
     * information is typically useful for backward compatibility.
     */
    public Version getUpgradedVersion() {
        return indexUpgradedVersion;
    }

    public long getCreationDate() {
        return settings.getAsLong(SETTING_CREATION_DATE, -1L);
    }

    public State getState() {
        return this.state;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getRoutingPartitionSize() {
        return routingPartitionSize;
    }

    public boolean isRoutingPartitionedIndex() {
        return routingPartitionSize != 1;
    }

    public int getTotalNumberOfShards() {
        return totalNumberOfShards;
    }

    /**
     * Returns the configured {@link #SETTING_WAIT_FOR_ACTIVE_SHARDS}, which defaults
     * to an active shard count of 1 if not specified.
     */
    public ActiveShardCount getWaitForActiveShards() {
        return waitForActiveShards;
    }

    public Settings getSettings() {
        return settings;
    }

    public Map<String, AliasMetadata> getAliases() {
        return this.aliases;
    }

    /**
     * Return the concrete mapping for this index or {@code null} if this index has no mappings at all.
     */
    @Nullable
    public MappingMetadata mapping() {
        for (final MappingMetadata cursor : mappings.values()) {
            return cursor;
        }
        return null;
    }

    public static final String INDEX_RESIZE_SOURCE_UUID_KEY = "index.resize.source.uuid";
    public static final String INDEX_RESIZE_SOURCE_NAME_KEY = "index.resize.source.name";
    public static final Setting<String> INDEX_RESIZE_SOURCE_UUID = Setting.simpleString(INDEX_RESIZE_SOURCE_UUID_KEY);
    public static final Setting<String> INDEX_RESIZE_SOURCE_NAME = Setting.simpleString(INDEX_RESIZE_SOURCE_NAME_KEY);

    public Index getResizeSourceIndex() {
        return INDEX_RESIZE_SOURCE_UUID.exists(settings)
            ? new Index(INDEX_RESIZE_SOURCE_NAME.get(settings), INDEX_RESIZE_SOURCE_UUID.get(settings))
            : null;
    }

    Map<String, DiffableStringMap> getCustomData() {
        return this.customData;
    }

    public Map<String, String> getCustomData(final String key) {
        return this.customData.get(key);
    }

    public Map<Integer, Set<String>> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Map<String, RolloverInfo> getRolloverInfos() {
        return rolloverInfos;
    }

    public Set<String> inSyncAllocationIds(int shardId) {
        assert shardId >= 0 && shardId < numberOfShards;
        return inSyncAllocationIds.get(shardId);
    }

    @Nullable
    public DiscoveryNodeFilters requireFilters() {
        return requireFilters;
    }

    @Nullable
    public DiscoveryNodeFilters getInitialRecoveryFilters() {
        return initialRecoveryFilters;
    }

    @Nullable
    public DiscoveryNodeFilters includeFilters() {
        return includeFilters;
    }

    @Nullable
    public DiscoveryNodeFilters excludeFilters() {
        return excludeFilters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        IndexMetadata that = (IndexMetadata) o;

        if (version != that.version) {
            return false;
        }

        if (!aliases.equals(that.aliases)) {
            return false;
        }
        if (!index.equals(that.index)) {
            return false;
        }
        if (!mappings.equals(that.mappings)) {
            return false;
        }
        if (!settings.equals(that.settings)) {
            return false;
        }
        if (state != that.state) {
            return false;
        }
        if (!customData.equals(that.customData)) {
            return false;
        }
        if (routingNumShards != that.routingNumShards) {
            return false;
        }
        if (routingFactor != that.routingFactor) {
            return false;
        }
        if (Arrays.equals(primaryTerms, that.primaryTerms) == false) {
            return false;
        }
        if (!inSyncAllocationIds.equals(that.inSyncAllocationIds)) {
            return false;
        }
        if (rolloverInfos.equals(that.rolloverInfos) == false) {
            return false;
        }
        if (isSystem != that.isSystem) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = index.hashCode();
        result = 31 * result + Long.hashCode(version);
        result = 31 * result + state.hashCode();
        result = 31 * result + aliases.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mappings.hashCode();
        result = 31 * result + customData.hashCode();
        result = 31 * result + Long.hashCode(routingFactor);
        result = 31 * result + Long.hashCode(routingNumShards);
        result = 31 * result + Arrays.hashCode(primaryTerms);
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + rolloverInfos.hashCode();
        result = 31 * result + Boolean.hashCode(isSystem);
        return result;
    }

    @Override
    public Diff<IndexMetadata> diff(IndexMetadata previousState) {
        return new IndexMetadataDiff(previousState, this);
    }

    public static Diff<IndexMetadata> readDiffFrom(StreamInput in) throws IOException {
        return new IndexMetadataDiff(in);
    }

    public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }

    /**
     * A diff of index metadata.
     *
     * @opensearch.internal
     */
    private static class IndexMetadataDiff implements Diff<IndexMetadata> {

        private final String index;
        private final int routingNumShards;
        private final long version;
        private final long mappingVersion;
        private final long settingsVersion;
        private final long aliasesVersion;
        private final long[] primaryTerms;
        private final State state;
        private final Settings settings;
        private final Diff<Map<String, MappingMetadata>> mappings;
        private final Diff<Map<String, AliasMetadata>> aliases;
        private final Diff<Map<String, DiffableStringMap>> customData;
        private final Diff<Map<Integer, Set<String>>> inSyncAllocationIds;
        private final Diff<Map<String, RolloverInfo>> rolloverInfos;
        private final boolean isSystem;

        IndexMetadataDiff(IndexMetadata before, IndexMetadata after) {
            index = after.index.getName();
            version = after.version;
            mappingVersion = after.mappingVersion;
            settingsVersion = after.settingsVersion;
            aliasesVersion = after.aliasesVersion;
            routingNumShards = after.routingNumShards;
            state = after.state;
            settings = after.settings;
            primaryTerms = after.primaryTerms;
            mappings = DiffableUtils.diff(before.mappings, after.mappings, DiffableUtils.getStringKeySerializer());
            aliases = DiffableUtils.diff(before.aliases, after.aliases, DiffableUtils.getStringKeySerializer());
            customData = DiffableUtils.diff(before.customData, after.customData, DiffableUtils.getStringKeySerializer());
            inSyncAllocationIds = DiffableUtils.diff(
                before.inSyncAllocationIds,
                after.inSyncAllocationIds,
                DiffableUtils.getVIntKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
            rolloverInfos = DiffableUtils.diff(before.rolloverInfos, after.rolloverInfos, DiffableUtils.getStringKeySerializer());
            isSystem = after.isSystem;
        }

        private static final DiffableUtils.DiffableValueReader<String, AliasMetadata> ALIAS_METADATA_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(AliasMetadata::new, AliasMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, MappingMetadata> MAPPING_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(MappingMetadata::new, MappingMetadata::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, DiffableStringMap> CUSTOM_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(DiffableStringMap::readFrom, DiffableStringMap::readDiffFrom);
        private static final DiffableUtils.DiffableValueReader<String, RolloverInfo> ROLLOVER_INFO_DIFF_VALUE_READER =
            new DiffableUtils.DiffableValueReader<>(RolloverInfo::new, RolloverInfo::readDiffFrom);

        IndexMetadataDiff(StreamInput in) throws IOException {
            index = in.readString();
            routingNumShards = in.readInt();
            version = in.readLong();
            mappingVersion = in.readVLong();
            settingsVersion = in.readVLong();
            aliasesVersion = in.readVLong();
            state = State.fromId(in.readByte());
            settings = Settings.readSettingsFromStream(in);
            primaryTerms = in.readVLongArray();
            mappings = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), MAPPING_DIFF_VALUE_READER);
            aliases = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), ALIAS_METADATA_DIFF_VALUE_READER);
            customData = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), CUSTOM_DIFF_VALUE_READER);
            inSyncAllocationIds = DiffableUtils.readJdkMapDiff(
                in,
                DiffableUtils.getVIntKeySerializer(),
                DiffableUtils.StringSetValueSerializer.getInstance()
            );
            rolloverInfos = DiffableUtils.readJdkMapDiff(in, DiffableUtils.getStringKeySerializer(), ROLLOVER_INFO_DIFF_VALUE_READER);
            isSystem = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(index);
            out.writeInt(routingNumShards);
            out.writeLong(version);
            out.writeVLong(mappingVersion);
            out.writeVLong(settingsVersion);
            out.writeVLong(aliasesVersion);
            out.writeByte(state.id);
            Settings.writeSettingsToStream(settings, out);
            out.writeVLongArray(primaryTerms);
            mappings.writeTo(out);
            aliases.writeTo(out);
            customData.writeTo(out);
            inSyncAllocationIds.writeTo(out);
            rolloverInfos.writeTo(out);
            out.writeBoolean(isSystem);
        }

        @Override
        public IndexMetadata apply(IndexMetadata part) {
            Builder builder = builder(index);
            builder.version(version);
            builder.mappingVersion(mappingVersion);
            builder.settingsVersion(settingsVersion);
            builder.aliasesVersion(aliasesVersion);
            builder.setRoutingNumShards(routingNumShards);
            builder.state(state);
            builder.settings(settings);
            builder.primaryTerms(primaryTerms);
            builder.mappings.putAll(mappings.apply(part.mappings));
            builder.aliases.putAll(aliases.apply(part.aliases));
            builder.customMetadata.putAll(customData.apply(part.customData));
            builder.inSyncAllocationIds.putAll(inSyncAllocationIds.apply(part.inSyncAllocationIds));
            builder.rolloverInfos.putAll(rolloverInfos.apply(part.rolloverInfos));
            builder.system(part.isSystem);
            return builder.build();
        }
    }

    public static IndexMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.version(in.readLong());
        builder.mappingVersion(in.readVLong());
        builder.settingsVersion(in.readVLong());
        builder.aliasesVersion(in.readVLong());
        builder.setRoutingNumShards(in.readInt());
        builder.state(State.fromId(in.readByte()));
        builder.settings(readSettingsFromStream(in));
        builder.primaryTerms(in.readVLongArray());
        int mappingsSize = in.readVInt();
        for (int i = 0; i < mappingsSize; i++) {
            MappingMetadata mappingMd = new MappingMetadata(in);
            builder.putMapping(mappingMd);
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetadata aliasMd = new AliasMetadata(in);
            builder.putAlias(aliasMd);
        }
        int customSize = in.readVInt();
        for (int i = 0; i < customSize; i++) {
            String key = in.readString();
            DiffableStringMap custom = DiffableStringMap.readFrom(in);
            builder.putCustom(key, custom);
        }
        int inSyncAllocationIdsSize = in.readVInt();
        for (int i = 0; i < inSyncAllocationIdsSize; i++) {
            int key = in.readVInt();
            Set<String> allocationIds = DiffableUtils.StringSetValueSerializer.getInstance().read(in, key);
            builder.putInSyncAllocationIds(key, allocationIds);
        }
        int rolloverAliasesSize = in.readVInt();
        for (int i = 0; i < rolloverAliasesSize; i++) {
            builder.putRolloverInfo(new RolloverInfo(in));
        }
        builder.system(in.readBoolean());
        return builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index.getName()); // uuid will come as part of settings
        out.writeLong(version);
        out.writeVLong(mappingVersion);
        out.writeVLong(settingsVersion);
        out.writeVLong(aliasesVersion);
        out.writeInt(routingNumShards);
        out.writeByte(state.id());
        writeSettingsToStream(settings, out);
        out.writeVLongArray(primaryTerms);
        out.writeVInt(mappings.size());
        for (final MappingMetadata cursor : mappings.values()) {
            cursor.writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (final AliasMetadata cursor : aliases.values()) {
            cursor.writeTo(out);
        }
        out.writeVInt(customData.size());
        for (final Map.Entry<String, DiffableStringMap> cursor : customData.entrySet()) {
            out.writeString(cursor.getKey());
            cursor.getValue().writeTo(out);
        }
        out.writeVInt(inSyncAllocationIds.size());
        for (final Map.Entry<Integer, Set<String>> cursor : inSyncAllocationIds.entrySet()) {
            out.writeVInt(cursor.getKey());
            DiffableUtils.StringSetValueSerializer.getInstance().write(cursor.getValue(), out);
        }
        out.writeVInt(rolloverInfos.size());
        for (final RolloverInfo cursor : rolloverInfos.values()) {
            cursor.writeTo(out);
        }
        out.writeBoolean(isSystem);
    }

    public boolean isSystem() {
        return isSystem;
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static Builder builder(IndexMetadata indexMetadata) {
        return new Builder(indexMetadata);
    }

    /**
     * Builder of index metadata.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private String index;
        private State state = State.OPEN;
        private long version = 1;
        private long mappingVersion = 1;
        private long settingsVersion = 1;
        private long aliasesVersion = 1;
        private long[] primaryTerms = null;
        private Settings settings = Settings.Builder.EMPTY_SETTINGS;
        private final Map<String, MappingMetadata> mappings;
        private final Map<String, AliasMetadata> aliases;
        private final Map<String, DiffableStringMap> customMetadata;
        private final Map<Integer, Set<String>> inSyncAllocationIds;
        private final Map<String, RolloverInfo> rolloverInfos;
        private Integer routingNumShards;
        private boolean isSystem;

        public Builder(String index) {
            this.index = index;
            this.mappings = new HashMap<>();
            this.aliases = new HashMap<>();
            this.customMetadata = new HashMap<>();
            this.inSyncAllocationIds = new HashMap<>();
            this.rolloverInfos = new HashMap<>();
            this.isSystem = false;
        }

        public Builder(IndexMetadata indexMetadata) {
            this.index = indexMetadata.getIndex().getName();
            this.state = indexMetadata.state;
            this.version = indexMetadata.version;
            this.mappingVersion = indexMetadata.mappingVersion;
            this.settingsVersion = indexMetadata.settingsVersion;
            this.aliasesVersion = indexMetadata.aliasesVersion;
            this.settings = indexMetadata.getSettings();
            this.primaryTerms = indexMetadata.primaryTerms.clone();
            this.mappings = new HashMap<>(indexMetadata.mappings);
            this.aliases = new HashMap<>(indexMetadata.aliases);
            this.customMetadata = new HashMap<>(indexMetadata.customData);
            this.routingNumShards = indexMetadata.routingNumShards;
            this.inSyncAllocationIds = new HashMap<>(indexMetadata.inSyncAllocationIds);
            this.rolloverInfos = new HashMap<>(indexMetadata.rolloverInfos);
            this.isSystem = indexMetadata.isSystem;
        }

        public Builder index(String index) {
            this.index = index;
            return this;
        }

        public Builder numberOfShards(int numberOfShards) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
            return this;
        }

        /**
         * Sets the number of shards that should be used for routing. This should only be used if the number of shards in
         * an index has changed ie if the index is shrunk.
         */
        public Builder setRoutingNumShards(int routingNumShards) {
            this.routingNumShards = routingNumShards;
            return this;
        }

        /**
         * Returns number of shards that should be used for routing. By default this method will return the number of shards
         * for this index.
         *
         * @see #setRoutingNumShards(int)
         * @see #numberOfShards()
         */
        public int getRoutingNumShards() {
            return routingNumShards == null ? numberOfShards() : routingNumShards;
        }

        /**
         * Returns the number of shards.
         *
         * @return the provided value or -1 if it has not been set.
         */
        public int numberOfShards() {
            return settings.getAsInt(SETTING_NUMBER_OF_SHARDS, -1);
        }

        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        public Builder routingPartitionSize(int routingPartitionSize) {
            settings = Settings.builder().put(settings).put(SETTING_ROUTING_PARTITION_SIZE, routingPartitionSize).build();
            return this;
        }

        public Builder creationDate(long creationDate) {
            settings = Settings.builder().put(settings).put(SETTING_CREATION_DATE, creationDate).build();
            return this;
        }

        public Builder settings(Settings.Builder settings) {
            return settings(settings.build());
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public MappingMetadata mapping() {
            return mappings.get(MapperService.SINGLE_MAPPING_NAME);
        }

        public Builder putMapping(String source) throws IOException {
            putMapping(
                new MappingMetadata(
                    MapperService.SINGLE_MAPPING_NAME,
                    XContentHelper.convertToMap(MediaTypeRegistry.xContent(source).xContent(), source, true)
                )
            );
            return this;
        }

        public Builder putMapping(MappingMetadata mappingMd) {
            mappings.clear();
            if (mappingMd != null) {
                mappings.put(mappingMd.type(), mappingMd);
            }
            return this;
        }

        public Builder state(State state) {
            this.state = state;
            return this;
        }

        public Builder putAlias(AliasMetadata aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata);
            return this;
        }

        public Builder putAlias(AliasMetadata.Builder aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata.build());
            return this;
        }

        public Builder removeAlias(String alias) {
            aliases.remove(alias);
            return this;
        }

        public Builder removeAllAliases() {
            aliases.clear();
            return this;
        }

        public Builder putCustom(String type, Map<String, String> customIndexMetadata) {
            this.customMetadata.put(type, new DiffableStringMap(customIndexMetadata));
            return this;
        }

        public Map<String, String> removeCustom(String type) {
            return this.customMetadata.remove(type);
        }

        public Set<String> getInSyncAllocationIds(int shardId) {
            return inSyncAllocationIds.get(shardId);
        }

        public Builder putInSyncAllocationIds(int shardId, Set<String> allocationIds) {
            inSyncAllocationIds.put(shardId, new HashSet<>(allocationIds));
            return this;
        }

        public Builder putRolloverInfo(RolloverInfo rolloverInfo) {
            rolloverInfos.put(rolloverInfo.getAlias(), rolloverInfo);
            return this;
        }

        public long version() {
            return this.version;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public long mappingVersion() {
            return mappingVersion;
        }

        public Builder mappingVersion(final long mappingVersion) {
            this.mappingVersion = mappingVersion;
            return this;
        }

        public long settingsVersion() {
            return settingsVersion;
        }

        public Builder settingsVersion(final long settingsVersion) {
            this.settingsVersion = settingsVersion;
            return this;
        }

        public long aliasesVersion() {
            return aliasesVersion;
        }

        public Builder aliasesVersion(final long aliasesVersion) {
            this.aliasesVersion = aliasesVersion;
            return this;
        }

        /**
         * returns the primary term for the given shard.
         * See {@link IndexMetadata#primaryTerm(int)} for more information.
         */
        public long primaryTerm(int shardId) {
            if (primaryTerms == null) {
                initializePrimaryTerms();
            }
            return this.primaryTerms[shardId];
        }

        /**
         * sets the primary term for the given shard.
         * See {@link IndexMetadata#primaryTerm(int)} for more information.
         */
        public Builder primaryTerm(int shardId, long primaryTerm) {
            if (primaryTerms == null) {
                initializePrimaryTerms();
            }
            this.primaryTerms[shardId] = primaryTerm;
            return this;
        }

        private void primaryTerms(long[] primaryTerms) {
            this.primaryTerms = primaryTerms.clone();
        }

        private void initializePrimaryTerms() {
            assert primaryTerms == null;
            if (numberOfShards() < 0) {
                throw new IllegalStateException("you must set the number of shards before setting/reading primary terms");
            }
            primaryTerms = new long[numberOfShards()];
            Arrays.fill(primaryTerms, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
        }

        public Builder system(boolean system) {
            this.isSystem = system;
            return this;
        }

        public boolean isSystem() {
            return isSystem;
        }

        public IndexMetadata build() {
            final Map<String, AliasMetadata> tmpAliases = aliases;
            Settings tmpSettings = settings;

            /*
             * We expect that the metadata has been properly built to set the number of shards and the number of replicas, and do not rely
             * on the default values here. Those must have been set upstream.
             */
            if (INDEX_NUMBER_OF_SHARDS_SETTING.exists(settings) == false) {
                throw new IllegalArgumentException("must specify number of shards for index [" + index + "]");
            }
            final int numberOfShards = INDEX_NUMBER_OF_SHARDS_SETTING.get(settings);

            if (INDEX_NUMBER_OF_REPLICAS_SETTING.exists(settings) == false) {
                throw new IllegalArgumentException("must specify number of replicas for index [" + index + "]");
            }
            final int numberOfReplicas = INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings);

            int routingPartitionSize = INDEX_ROUTING_PARTITION_SIZE_SETTING.get(settings);
            if (routingPartitionSize != 1 && routingPartitionSize >= getRoutingNumShards()) {
                throw new IllegalArgumentException(
                    "routing partition size ["
                        + routingPartitionSize
                        + "] should be a positive number"
                        + " less than the number of shards ["
                        + getRoutingNumShards()
                        + "] for ["
                        + index
                        + "]"
                );
            }

            // fill missing slots in inSyncAllocationIds with empty set if needed and make all entries immutable
            final Map<Integer, Set<String>> filledInSyncAllocationIds = new HashMap<>();
            for (int i = 0; i < numberOfShards; i++) {
                if (inSyncAllocationIds.containsKey(i)) {
                    filledInSyncAllocationIds.put(i, Collections.unmodifiableSet(new HashSet<>(inSyncAllocationIds.get(i))));
                } else {
                    filledInSyncAllocationIds.put(i, Collections.emptySet());
                }
            }
            final Map<String, String> requireMap = INDEX_ROUTING_REQUIRE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters requireFilters;
            if (requireMap.isEmpty()) {
                requireFilters = null;
            } else {
                requireFilters = DiscoveryNodeFilters.buildOrUpdateFromKeyValue(null, AND, requireMap);
            }
            Map<String, String> includeMap = INDEX_ROUTING_INCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters includeFilters;
            if (includeMap.isEmpty()) {
                includeFilters = null;
            } else {
                includeFilters = DiscoveryNodeFilters.buildOrUpdateFromKeyValue(null, OR, includeMap);
            }
            Map<String, String> excludeMap = INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters excludeFilters;
            if (excludeMap.isEmpty()) {
                excludeFilters = null;
            } else {
                excludeFilters = DiscoveryNodeFilters.buildOrUpdateFromKeyValue(null, OR, excludeMap);
            }
            Map<String, String> initialRecoveryMap = INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getAsMap(settings);
            final DiscoveryNodeFilters initialRecoveryFilters;
            if (initialRecoveryMap.isEmpty()) {
                initialRecoveryFilters = null;
            } else {
                initialRecoveryFilters = DiscoveryNodeFilters.buildOrUpdateFromKeyValue(null, OR, initialRecoveryMap);
            }
            Version indexCreatedVersion = indexCreated(settings);
            Version indexUpgradedVersion = settings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, indexCreatedVersion);

            if (primaryTerms == null) {
                initializePrimaryTerms();
            } else if (primaryTerms.length != numberOfShards) {
                throw new IllegalStateException(
                    "primaryTerms length is ["
                        + primaryTerms.length
                        + "] but should be equal to number of shards ["
                        + numberOfShards()
                        + "]"
                );
            }

            final ActiveShardCount waitForActiveShards = SETTING_WAIT_FOR_ACTIVE_SHARDS.get(settings);
            if (waitForActiveShards.validate(numberOfReplicas) == false) {
                throw new IllegalArgumentException(
                    "invalid "
                        + SETTING_WAIT_FOR_ACTIVE_SHARDS.getKey()
                        + "["
                        + waitForActiveShards
                        + "]: cannot be greater than "
                        + "number of shard copies ["
                        + (numberOfReplicas + 1)
                        + "]"
                );
            }

            final String uuid = settings.get(SETTING_INDEX_UUID, INDEX_UUID_NA_VALUE);

            return new IndexMetadata(
                new Index(index, uuid),
                version,
                mappingVersion,
                settingsVersion,
                aliasesVersion,
                primaryTerms,
                state,
                numberOfShards,
                numberOfReplicas,
                tmpSettings,
                mappings,
                tmpAliases,
                customMetadata,
                filledInSyncAllocationIds,
                requireFilters,
                initialRecoveryFilters,
                includeFilters,
                excludeFilters,
                indexCreatedVersion,
                indexUpgradedVersion,
                getRoutingNumShards(),
                routingPartitionSize,
                waitForActiveShards,
                rolloverInfos,
                isSystem
            );
        }

        public static void toXContent(IndexMetadata indexMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            Metadata.XContentContext context = Metadata.XContentContext.valueOf(
                params.param(CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_API)
            );

            builder.startObject(indexMetadata.getIndex().getName());

            builder.field(KEY_VERSION, indexMetadata.getVersion());
            builder.field(KEY_MAPPING_VERSION, indexMetadata.getMappingVersion());
            builder.field(KEY_SETTINGS_VERSION, indexMetadata.getSettingsVersion());
            builder.field(KEY_ALIASES_VERSION, indexMetadata.getAliasesVersion());
            builder.field(KEY_ROUTING_NUM_SHARDS, indexMetadata.getRoutingNumShards());

            builder.field(KEY_STATE, indexMetadata.getState().toString().toLowerCase(Locale.ENGLISH));

            boolean binary = params.paramAsBoolean("binary", false);

            builder.startObject(KEY_SETTINGS);
            if (context != Metadata.XContentContext.API) {
                indexMetadata.getSettings().toXContent(builder, new MapParams(Collections.singletonMap("flat_settings", "true")));
            } else {
                indexMetadata.getSettings().toXContent(builder, params);
            }
            builder.endObject();

            if (context != Metadata.XContentContext.API) {
                builder.startArray(KEY_MAPPINGS);
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    if (binary) {
                        builder.value(mmd.source().compressed());
                    } else {
                        builder.map(XContentHelper.convertToMap(mmd.source().uncompressed(), true).v2());
                    }
                }
                builder.endArray();
            } else {
                builder.startObject(KEY_MAPPINGS);
                MappingMetadata mmd = indexMetadata.mapping();
                if (mmd != null) {
                    Map<String, Object> mapping = XContentHelper.convertToMap(mmd.source().uncompressed(), false).v2();
                    if (mapping.size() == 1 && mapping.containsKey(mmd.type())) {
                        // the type name is the root value, reduce it
                        mapping = (Map<String, Object>) mapping.get(mmd.type());
                    }
                    builder.field(mmd.type());
                    builder.map(mapping);
                }
                builder.endObject();
            }

            for (final Map.Entry<String, DiffableStringMap> cursor : indexMetadata.customData.entrySet()) {
                builder.field(cursor.getKey());
                builder.map(cursor.getValue());
            }

            if (context != Metadata.XContentContext.API) {
                builder.startObject(KEY_ALIASES);
                for (final AliasMetadata cursor : indexMetadata.getAliases().values()) {
                    AliasMetadata.Builder.toXContent(cursor, builder, params);
                }
                builder.endObject();

                builder.startArray(KEY_PRIMARY_TERMS);
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    builder.value(indexMetadata.primaryTerm(i));
                }
                builder.endArray();
            } else {
                builder.startArray(KEY_ALIASES);
                for (final String cursor : indexMetadata.getAliases().keySet()) {
                    builder.value(cursor);
                }
                builder.endArray();

                builder.startObject(IndexMetadata.KEY_PRIMARY_TERMS);
                for (int shard = 0; shard < indexMetadata.getNumberOfShards(); shard++) {
                    builder.field(Integer.toString(shard), indexMetadata.primaryTerm(shard));
                }
                builder.endObject();
            }

            builder.startObject(KEY_IN_SYNC_ALLOCATIONS);
            for (final Map.Entry<Integer, Set<String>> cursor : indexMetadata.inSyncAllocationIds.entrySet()) {
                builder.startArray(String.valueOf(cursor.getKey()));
                for (String allocationId : cursor.getValue()) {
                    builder.value(allocationId);
                }
                builder.endArray();
            }
            builder.endObject();

            builder.startObject(KEY_ROLLOVER_INFOS);
            for (final RolloverInfo cursor : indexMetadata.getRolloverInfos().values()) {
                cursor.toXContent(builder, params);
            }
            builder.endObject();
            builder.field(KEY_SYSTEM, indexMetadata.isSystem);

            builder.endObject();
        }

        public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() == null) { // fresh parser? move to the first token
                parser.nextToken();
            }
            if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
                parser.nextToken();
            }
            if (parser.currentToken() != XContentParser.Token.FIELD_NAME) {
                throw new IllegalArgumentException("expected field name but got a " + parser.currentToken());
            }
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected object but got a " + token);
            }
            boolean mappingVersion = false;
            boolean settingsVersion = false;
            boolean aliasesVersion = false;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (KEY_SETTINGS.equals(currentFieldName)) {
                        builder.settings(Settings.fromXContent(parser));
                    } else if (KEY_MAPPINGS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder()
                                    .put(mappingType, parser.mapOrdered())
                                    .map();
                                builder.putMapping(new MappingMetadata(mappingType, mappingSource));
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if (KEY_ALIASES.equals(currentFieldName)) {
                        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                        }
                    } else if (KEY_IN_SYNC_ALLOCATIONS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_ARRAY) {
                                String shardId = currentFieldName;
                                Set<String> allocationIds = new HashSet<>();
                                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                                    if (token == XContentParser.Token.VALUE_STRING) {
                                        allocationIds.add(parser.text());
                                    }
                                }
                                builder.putInSyncAllocationIds(Integer.valueOf(shardId), allocationIds);
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if (KEY_ROLLOVER_INFOS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                builder.putRolloverInfo(RolloverInfo.parse(parser, currentFieldName));
                            } else {
                                throw new IllegalArgumentException("Unexpected token: " + token);
                            }
                        }
                    } else if ("warmers".equals(currentFieldName)) {
                        // TODO: do this in 6.0:
                        // throw new IllegalArgumentException("Warmers are not supported anymore - are you upgrading from 1.x?");
                        // ignore: warmers have been removed in 5.0 and are
                        // simply ignored when upgrading from 2.x
                        assert Version.CURRENT.major <= 5;
                        parser.skipChildren();
                    } else {
                        // assume it's custom index metadata
                        builder.putCustom(currentFieldName, parser.mapStrings());
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (KEY_MAPPINGS.equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                                builder.putMapping(new MappingMetadata(new CompressedXContent(parser.binaryValue())));
                            } else {
                                Map<String, Object> mapping = parser.mapOrdered();
                                if (mapping.size() == 1) {
                                    String mappingType = mapping.keySet().iterator().next();
                                    builder.putMapping(new MappingMetadata(mappingType, mapping));
                                }
                            }
                        }
                    } else if (KEY_PRIMARY_TERMS.equals(currentFieldName)) {
                        final List<Long> list = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_NUMBER) {
                                list.add(parser.longValue());
                            } else {
                                throw new IllegalStateException("found a non-numeric value under [" + KEY_PRIMARY_TERMS + "]");
                            }
                        }
                        builder.primaryTerms(list.stream().mapToLong(i -> i).toArray());
                    } else {
                        throw new IllegalArgumentException("Unexpected field for an array " + currentFieldName);
                    }
                } else if (token.isValue()) {
                    if (KEY_STATE.equals(currentFieldName)) {
                        builder.state(State.fromString(parser.text()));
                    } else if (KEY_VERSION.equals(currentFieldName)) {
                        builder.version(parser.longValue());
                    } else if (KEY_MAPPING_VERSION.equals(currentFieldName)) {
                        mappingVersion = true;
                        builder.mappingVersion(parser.longValue());
                    } else if (KEY_SETTINGS_VERSION.equals(currentFieldName)) {
                        settingsVersion = true;
                        builder.settingsVersion(parser.longValue());
                    } else if (KEY_ALIASES_VERSION.equals(currentFieldName)) {
                        aliasesVersion = true;
                        builder.aliasesVersion(parser.longValue());
                    } else if (KEY_ROUTING_NUM_SHARDS.equals(currentFieldName)) {
                        builder.setRoutingNumShards(parser.intValue());
                    } else if (KEY_SYSTEM.equals(currentFieldName)) {
                        builder.system(parser.booleanValue());
                    } else {
                        throw new IllegalArgumentException("Unexpected field [" + currentFieldName + "]");
                    }
                } else {
                    throw new IllegalArgumentException("Unexpected token " + token);
                }
            }

            final Version indexCreatedVersion = indexCreated(builder.settings);
            // Reference:
            // https://github.com/opensearch-project/OpenSearch/blob/4dde0f2a3b445b2fc61dab29c5a2178967f4a3e3/server/src/main/java/org/opensearch/cluster/metadata/IndexMetadata.java#L1620-L1628
            if (Assertions.ENABLED && indexCreatedVersion.onOrAfter(LegacyESVersion.V_6_5_0)) {
                assert mappingVersion : "mapping version should be present for indices";
                assert settingsVersion : "settings version should be present for indices";
            }
            // Reference:
            // https://github.com/opensearch-project/OpenSearch/blob/2e4b27b243d8bd2c515f66cf86c6d1d6a601307f/server/src/main/java/org/opensearch/cluster/metadata/IndexMetadata.java#L1824
            if (Assertions.ENABLED && indexCreatedVersion.onOrAfter(LegacyESVersion.V_7_2_0)) {
                assert aliasesVersion : "aliases version should be present for indices";
            }
            return builder.build();
        }
    }

    /**
     * Adds human readable version and creation date settings.
     * This method is used to display the settings in a human readable format in REST API
     */
    public static Settings addHumanReadableSettings(Settings settings) {
        Settings.Builder builder = Settings.builder().put(settings);
        Version version = SETTING_INDEX_VERSION_CREATED.get(settings);
        if (version != Version.V_EMPTY) {
            builder.put(SETTING_VERSION_CREATED_STRING, version.toString());
        }
        Version versionUpgraded = settings.getAsVersion(SETTING_VERSION_UPGRADED, null);
        if (versionUpgraded != null) {
            builder.put(SETTING_VERSION_UPGRADED_STRING, versionUpgraded.toString());
        }
        Long creationDate = settings.getAsLong(SETTING_CREATION_DATE, null);
        if (creationDate != null) {
            ZonedDateTime creationDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(creationDate), ZoneOffset.UTC);
            builder.put(SETTING_CREATION_DATE_STRING, creationDateTime.toString());
        }
        return builder.build();
    }

    private static final ToXContent.Params FORMAT_PARAMS;
    static {
        Map<String, String> params = new HashMap<>(2);
        params.put("binary", "true");
        params.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
        FORMAT_PARAMS = new MapParams(params);
    }

    /**
     * Return the version the index was created from the provided index settings
     *
     * This looks for the presence of the {@link Version} object with key {@link IndexMetadata#SETTING_VERSION_CREATED}
     */
    public static Version indexCreated(final Settings indexSettings) {
        final Version indexVersion = SETTING_INDEX_VERSION_CREATED.get(indexSettings);
        if (indexVersion.equals(Version.V_EMPTY)) {
            final String message = String.format(
                Locale.ROOT,
                "[%s] is not present in the index settings for index with UUID [%s]",
                SETTING_INDEX_VERSION_CREATED.getKey(),
                indexSettings.get(IndexMetadata.SETTING_INDEX_UUID)
            );
            throw new IllegalStateException(message);
        }
        return indexVersion;
    }

    /**
     * State format for {@link IndexMetadata} to write to and load from disk
     */
    public static final MetadataStateFormat<IndexMetadata> FORMAT = new MetadataStateFormat<>(INDEX_STATE_FILE_PREFIX) {

        @Override
        public void toXContent(XContentBuilder builder, IndexMetadata state) throws IOException {
            Builder.toXContent(state, builder, FORMAT_PARAMS);
        }

        @Override
        public IndexMetadata fromXContent(XContentParser parser) throws IOException {
            return Builder.fromXContent(parser);
        }
    };

    /**
     * Returns the number of shards that should be used for routing. This basically defines the hash space we use in
     * {@link org.opensearch.cluster.routing.OperationRouting#generateShardId(IndexMetadata, String, String)} to route documents
     * to shards based on their ID or their specific routing value. The default value is {@link #getNumberOfShards()}. This value only
     * changes if and index is shrunk.
     */
    public int getRoutingNumShards() {
        return routingNumShards;
    }

    /**
     * Returns the routing factor for this index. The default is {@code 1}.
     *
     * @see #getRoutingFactor(int, int) for details
     */
    public int getRoutingFactor() {
        return routingFactor;
    }

    /**
     * Returns the source shard ID to split the given target shard off
     * @param shardId the id of the target shard to split into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to split off from
     */
    public static ShardId selectSplitShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        if (shardId >= numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards (" + numTargetShards + ") must be greater than the shard id: " + shardId
            );
        }
        final int routingFactor = getRoutingFactor(numSourceShards, numTargetShards);
        assertSplitMetadata(numSourceShards, numTargetShards, sourceIndexMetadata);
        return new ShardId(sourceIndexMetadata.getIndex(), shardId / routingFactor);
    }

    /**
     * Returns the source shard ID to clone the given target shard off
     * @param shardId the id of the target shard to clone into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to clone from
     */
    public static ShardId selectCloneShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        int numSourceShards = sourceIndexMetadata.getNumberOfShards();
        if (numSourceShards != numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards ("
                    + numTargetShards
                    + ") must be the same as the number of"
                    + " source shards ("
                    + numSourceShards
                    + ")"
            );
        }
        return new ShardId(sourceIndexMetadata.getIndex(), shardId);
    }

    private static void assertSplitMetadata(int numSourceShards, int numTargetShards, IndexMetadata sourceIndexMetadata) {
        if (numSourceShards > numTargetShards) {
            throw new IllegalArgumentException(
                "the number of source shards ["
                    + numSourceShards
                    + "] must be less that the number of target shards ["
                    + numTargetShards
                    + "]"
            );
        }
        // now we verify that the numRoutingShards is valid in the source index
        // note: if the number of shards is 1 in the source index we can just assume it's correct since from 1 we can split into anything
        // this is important to special case here since we use this to validate this in various places in the code but allow to split form
        // 1 to N but we never modify the sourceIndexMetadata to accommodate for that
        int routingNumShards = numSourceShards == 1 ? numTargetShards : sourceIndexMetadata.getRoutingNumShards();
        if (routingNumShards % numTargetShards != 0) {
            throw new IllegalStateException(
                "the number of routing shards [" + routingNumShards + "] must be a multiple of the target shards [" + numTargetShards + "]"
            );
        }
        // this is just an additional assertion that ensures we are a factor of the routing num shards.
        assert sourceIndexMetadata.getNumberOfShards() == 1 // special case - we can split into anything from 1 shard
            || getRoutingFactor(numTargetShards, routingNumShards) >= 0;
    }

    /**
     * Selects the source shards for a local shard recovery. This might either be a split or a shrink operation.
     * @param shardId the target shard ID to select the source shards for
     * @param sourceIndexMetadata the source metadata
     * @param numTargetShards the number of target shards
     */
    public static Set<ShardId> selectRecoverFromShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        if (sourceIndexMetadata.getNumberOfShards() > numTargetShards) {
            return selectShrinkShards(shardId, sourceIndexMetadata, numTargetShards);
        } else if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            return Collections.singleton(selectSplitShard(shardId, sourceIndexMetadata, numTargetShards));
        } else {
            return Collections.singleton(selectCloneShard(shardId, sourceIndexMetadata, numTargetShards));
        }
    }

    /**
     * Returns the source shard ids to shrink into the given shard id.
     * @param shardId the id of the target shard to shrink to
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a set of shard IDs to shrink into the given shard ID.
     */
    public static Set<ShardId> selectShrinkShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        if (shardId >= numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards (" + numTargetShards + ") must be greater than the shard id: " + shardId
            );
        }
        if (sourceIndexMetadata.getNumberOfShards() < numTargetShards) {
            throw new IllegalArgumentException(
                "the number of target shards ["
                    + numTargetShards
                    + "] must be less that the number of source shards ["
                    + sourceIndexMetadata.getNumberOfShards()
                    + "]"
            );
        }
        int routingFactor = getRoutingFactor(sourceIndexMetadata.getNumberOfShards(), numTargetShards);
        Set<ShardId> shards = new HashSet<>(routingFactor);
        for (int i = shardId * routingFactor; i < routingFactor * shardId + routingFactor; i++) {
            shards.add(new ShardId(sourceIndexMetadata.getIndex(), i));
        }
        return shards;
    }

    /**
     * Returns the routing factor for and shrunk index with the given number of target shards.
     * This factor is used in the hash function in
     * {@link org.opensearch.cluster.routing.OperationRouting#generateShardId(IndexMetadata, String, String)} to guarantee consistent
     * hashing / routing of documents even if the number of shards changed (ie. a shrunk index).
     *
     * @param sourceNumberOfShards the total number of shards in the source index
     * @param targetNumberOfShards the total number of shards in the target index
     * @return the routing factor for and shrunk index with the given number of target shards.
     * @throws IllegalArgumentException if the number of source shards is less than the number of target shards or if the source shards
     * are not divisible by the number of target shards.
     */
    public static int getRoutingFactor(int sourceNumberOfShards, int targetNumberOfShards) {
        final int factor;
        if (sourceNumberOfShards < targetNumberOfShards) { // split
            factor = targetNumberOfShards / sourceNumberOfShards;
            if (factor * sourceNumberOfShards != targetNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException(
                    "the number of source shards [" + sourceNumberOfShards + "] must be a " + "factor of [" + targetNumberOfShards + "]"
                );
            }
        } else if (sourceNumberOfShards > targetNumberOfShards) { // shrink
            factor = sourceNumberOfShards / targetNumberOfShards;
            if (factor * targetNumberOfShards != sourceNumberOfShards || factor <= 1) {
                throw new IllegalArgumentException(
                    "the number of source shards [" + sourceNumberOfShards + "] must be a " + "multiple of [" + targetNumberOfShards + "]"
                );
            }
        } else {
            factor = 1;
        }
        return factor;
    }

    /**
     * Parses the number from the rolled over index name. It also supports the date-math format (ie. index name is wrapped in &lt; and &gt;)
     * E.g.
     * - For ".ds-logs-000002" it will return 2
     * - For "&lt;logs-{now/d}-3&gt;" it'll return 3
     * @throws IllegalArgumentException if the index doesn't contain a "-" separator or if the last token after the separator is not a
     * number
     */
    public static int parseIndexNameCounter(String indexName) {
        int numberIndex = indexName.lastIndexOf("-");
        if (numberIndex == -1) {
            throw new IllegalArgumentException("no - separator found in index name [" + indexName + "]");
        }
        try {
            return Integer.parseInt(
                indexName.substring(numberIndex + 1, indexName.endsWith(">") ? indexName.length() - 1 : indexName.length())
            );
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("unable to parse the index name [" + indexName + "] to extract the counter", e);
        }
    }
}
