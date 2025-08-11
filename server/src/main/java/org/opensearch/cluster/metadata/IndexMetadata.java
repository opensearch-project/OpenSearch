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

import org.opensearch.Version;
import org.opensearch.action.admin.indices.rollover.RolloverInfo;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.metadata.core.AbstractIndexMetadata;
import org.opensearch.cluster.metadata.core.AbstractMappingMetadata;
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Index metadata information
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class IndexMetadata extends AbstractIndexMetadata<IndexMetadata> {

    /**
     * The state of the index.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
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

    /**
     * @deprecated use {@link AutoExpandReplicas#SETTING_AUTO_EXPAND_REPLICAS} instead
     */
    @Deprecated
    public static final String SETTING_AUTO_EXPAND_REPLICAS = "index.auto_expand_replicas";

    /**
     * @deprecated use {@link AutoExpandSearchReplicas#SETTING_AUTO_EXPAND_SEARCH_REPLICAS} instead
     */
    @Deprecated
    public static final String SETTING_AUTO_EXPAND_SEARCH_REPLICAS = "index.auto_expand_search_replicas";

    /**
     * @deprecated use {@link AutoExpandReplicas#INDEX_AUTO_EXPAND_REPLICAS_SETTING} instead
     */
    @Deprecated
    public static final Setting<AutoExpandReplicas> INDEX_AUTO_EXPAND_REPLICAS_SETTING = AutoExpandReplicas.SETTING;

    /**
     * @deprecated use {@link AutoExpandSearchReplicas#INDEX_AUTO_EXPAND_SEARCH_REPLICAS_SETTING} instead
     */
    @Deprecated
    public static final Setting<AutoExpandSearchReplicas> INDEX_AUTO_EXPAND_SEARCH_REPLICAS_SETTING = AutoExpandSearchReplicas.SETTING;

    /**
     * Blocks the API.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum APIBlock implements Writeable {
        READ_ONLY("read_only", INDEX_READ_ONLY_BLOCK),
        READ("read", INDEX_READ_BLOCK),
        WRITE("write", INDEX_WRITE_BLOCK),
        METADATA("metadata", INDEX_METADATA_BLOCK),
        READ_ONLY_ALLOW_DELETE("read_only_allow_delete", INDEX_READ_ONLY_ALLOW_DELETE_BLOCK),
        SEARCH_ONLY("search_only", INDEX_SEARCH_ONLY_BLOCK);

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

    /**
     * The number of active shard copies to check for before proceeding with a write operation.
     * @deprecated use {@link AbstractIndexMetadata#SETTING_WAIT_FOR_ACTIVE_SHARDS} instead
     */
    @Deprecated
    public static final Setting<ActiveShardCount> SETTING_WAIT_FOR_ACTIVE_SHARDS = new Setting<>(
        "index.write.wait_for_active_shards",
        "1",
        ActiveShardCount::parseString,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

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
        final int numberOfSearchOnlyReplicas,
        final Settings settings,
        final Map<String, AbstractMappingMetadata> mappings,
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
        final boolean isSystem,
        final int indexTotalShardsPerNodeLimit,
        final int indexTotalPrimaryShardsPerNodeLimit,
        boolean isAppendOnlyIndex,
        final Context context,
        final IngestionStatus ingestionStatus
    ) {
        super(
            index,
            version,
            mappingVersion,
            settingsVersion,
            aliasesVersion,
            primaryTerms,
            AbstractIndexMetadata.State.fromId(state.id),
            numberOfShards,
            numberOfReplicas,
            numberOfSearchOnlyReplicas,
            settings,
            mappings,
            aliases,
            customData,
            inSyncAllocationIds,
            requireFilters,
            initialRecoveryFilters,
            includeFilters,
            excludeFilters,
            indexCreatedVersion,
            indexUpgradedVersion,
            routingNumShards,
            routingPartitionSize,
            waitForActiveShards,
            rolloverInfos,
            isSystem,
            indexTotalShardsPerNodeLimit,
            indexTotalPrimaryShardsPerNodeLimit,
            isAppendOnlyIndex,
            context,
            ingestionStatus
        );
    }

    private static MappingMetadata getMappingMetadata(AbstractMappingMetadata abstractMappingMetadata) {
        if (abstractMappingMetadata instanceof MappingMetadata) {
            return (MappingMetadata) abstractMappingMetadata;
        }

        return new MappingMetadata(
            abstractMappingMetadata.type(),
            abstractMappingMetadata.source(),
            abstractMappingMetadata.routingRequired()
        );
    }

    public State getState() {
        return State.fromId(state.id());
    }

    /**
     * Returns the configured {@link #SETTING_WAIT_FOR_ACTIVE_SHARDS}, which defaults
     * to an active shard count of 1 if not specified.
     */
    public ActiveShardCount getWaitForActiveShards() {
        return ActiveShardCount.of(waitForActiveShards);
    }

    /**
     * Return the concrete mapping for this index or {@code null} if this index has no mappings at all.
     */
    @Nullable
    public MappingMetadata mapping() {
        for (AbstractMappingMetadata cursor : mappings.values()) {
            return getMappingMetadata(cursor);
        }
        return null;
    }

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
    static class IndexMetadataDiff implements Diff<IndexMetadata> {

        private final AbstractIndexMetadataDiff delegate;

        IndexMetadataDiff(IndexMetadata before, IndexMetadata after) {
            this.delegate = new AbstractIndexMetadataDiff(before, after);
        }

        IndexMetadataDiff(StreamInput in) throws IOException {
            this.delegate = new AbstractIndexMetadataDiff(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            delegate.writeTo(out);
        }

        @Override
        public IndexMetadata apply(IndexMetadata part) {
            AbstractIndexMetadata<?> abstractIndexMetadata = delegate.apply(part);
            return builder(abstractIndexMetadata).build();
        }
    }

    public static IndexMetadata readFrom(StreamInput in) throws IOException {
        AbstractIndexMetadata<?> abstractIndexMetadata = AbstractIndexMetadata.readFrom(in);
        return builder(abstractIndexMetadata).build();
    }

    public static Builder builder(String index) {
        return new Builder(index);
    }

    public static Builder builder(IndexMetadata indexMetadata) {
        return new Builder(indexMetadata);
    }

    public static Builder builder(AbstractIndexMetadata<?> baseIndexMetadata) {
        return new Builder(baseIndexMetadata);
    }

    /**
     * Builder of index metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder extends AbstractIndexMetadata.Builder {

        public Builder(String index) {
            super(index);
        }

        public Builder(IndexMetadata indexMetadata) {
            super(indexMetadata);
        }

        public Builder(AbstractIndexMetadata<?> baseIndexMetadata) {
            super(baseIndexMetadata);
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

        public Builder numberOfReplicas(int numberOfReplicas) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
            return this;
        }

        public Builder numberOfSearchReplicas(int numberOfSearchReplicas) {
            settings = Settings.builder().put(settings).put(SETTING_NUMBER_OF_SEARCH_REPLICAS, numberOfSearchReplicas).build();
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
            return getMappingMetadata(mappings.get(MapperService.SINGLE_MAPPING_NAME));
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
            this.state = AbstractIndexMetadata.State.fromId(state.id);
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

        public Builder putInSyncAllocationIds(int shardId, Set<String> allocationIds) {
            inSyncAllocationIds.put(shardId, new HashSet<>(allocationIds));
            return this;
        }

        public Builder putRolloverInfo(RolloverInfo rolloverInfo) {
            rolloverInfos.put(rolloverInfo.getAlias(), rolloverInfo);
            return this;
        }

        public Builder version(long version) {
            this.version = version;
            return this;
        }

        public Builder mappingVersion(final long mappingVersion) {
            this.mappingVersion = mappingVersion;
            return this;
        }

        public Builder settingsVersion(final long settingsVersion) {
            this.settingsVersion = settingsVersion;
            return this;
        }

        public Builder aliasesVersion(final long aliasesVersion) {
            this.aliasesVersion = aliasesVersion;
            return this;
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

        public Builder system(boolean system) {
            this.isSystem = system;
            return this;
        }

        public Builder context(Context context) {
            this.context = context;
            return this;
        }

        public Builder ingestionStatus(IngestionStatus ingestionStatus) {
            this.ingestionStatus = ingestionStatus;
            return this;
        }

        public IndexMetadata build() {
            AbstractIndexMetadata<?> base = super.build();

            return new IndexMetadata(
                base.getIndex(),
                base.getVersion(),
                base.getMappingVersion(),
                base.getSettingsVersion(),
                base.getAliasesVersion(),
                base.getPrimaryTerms(),
                State.fromId(base.getIndexState().id()),
                base.getNumberOfShards(),
                base.getNumberOfReplicas(),
                base.getNumberOfSearchOnlyReplicas(),
                base.getSettings(),
                base.getMappings(),
                base.getAliases(),
                base.getCustomData(),
                base.getInSyncAllocationIds(),
                base.requireFilters(),
                base.getInitialRecoveryFilters(),
                base.includeFilters(),
                base.excludeFilters(),
                base.getCreationVersion(),
                base.getUpgradedVersion(),
                base.getRoutingNumShards(),
                base.getRoutingPartitionSize(),
                ActiveShardCount.of(base.getWaitForActiveShards()),
                base.getRolloverInfos(),
                base.isSystem(),
                base.getIndexTotalShardsPerNodeLimit(),
                base.getIndexTotalPrimaryShardsPerNodeLimit(),
                base.isAppendOnlyIndex(),
                base.context(),
                base.getIngestionStatus()
            );
        }

        public static void toXContent(IndexMetadata indexMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            AbstractIndexMetadata.Builder.toXContent(indexMetadata, builder, params);
        }

        public static IndexMetadata fromXContent(XContentParser parser) throws IOException {
            AbstractIndexMetadata<?> base = AbstractIndexMetadata.Builder.fromXContent(parser);
            return builder(base).build();
        }
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
     * Returns the source shard ID to split the given target shard off
     * @param shardId the id of the target shard to split into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to split off from
     */
    public static ShardId selectSplitShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        return AbstractIndexMetadata.selectSplitShard(shardId, sourceIndexMetadata, numTargetShards);
    }

    /**
     * Returns the source shard ID to clone the given target shard off
     * @param shardId the id of the target shard to clone into
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a the source shard ID to clone from
     */
    public static ShardId selectCloneShard(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        return AbstractIndexMetadata.selectCloneShard(shardId, sourceIndexMetadata, numTargetShards);
    }

    /**
     * Selects the source shards for a local shard recovery. This might either be a split or a shrink operation.
     * @param shardId the target shard ID to select the source shards for
     * @param sourceIndexMetadata the source metadata
     * @param numTargetShards the number of target shards
     */
    public static Set<ShardId> selectRecoverFromShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        return AbstractIndexMetadata.selectRecoverFromShards(shardId, sourceIndexMetadata, numTargetShards);
    }

    /**
     * Returns the source shard ids to shrink into the given shard id.
     * @param shardId the id of the target shard to shrink to
     * @param sourceIndexMetadata the source index metadata
     * @param numTargetShards the total number of shards in the target index
     * @return a set of shard IDs to shrink into the given shard ID.
     */
    public static Set<ShardId> selectShrinkShards(int shardId, IndexMetadata sourceIndexMetadata, int numTargetShards) {
        return AbstractIndexMetadata.selectShrinkShards(shardId, sourceIndexMetadata, numTargetShards);
    }
}
