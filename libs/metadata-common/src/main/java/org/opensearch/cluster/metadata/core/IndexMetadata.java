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

package org.opensearch.cluster.metadata.core;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.rollover.RolloverInfo;
import org.opensearch.action.support.ShardCount;
import org.opensearch.cluster.Diff;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.cluster.metadata.DiffableStringMap;
import org.opensearch.cluster.metadata.IngestionStatus;
import org.opensearch.cluster.node.DiscoveryNodeFilters;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.MetadataStateFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.cluster.metadata.MetadataUtils.SINGLE_MAPPING_NAME;

/**
 * Index metadata information
 *
 * @opensearch.api
 */
@PublicApi(since = "3.2.0")
public class IndexMetadata extends AbstractIndexMetadata<IndexMetadata> {

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
        final ShardCount waitForActiveShards,
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
            state,
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
    @PublicApi(since = "3.2.0")
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

        public AbstractMappingMetadata mapping() {
            return mappings.get(SINGLE_MAPPING_NAME);
        }

        public Builder putMapping(String source) throws IOException {
            putMapping(
                new AbstractMappingMetadata(
                    SINGLE_MAPPING_NAME,
                    XContentHelper.convertToMap(MediaTypeRegistry.xContent(source).xContent(), source, true)
                )
            );
            return this;
        }

        public Builder putMapping(AbstractMappingMetadata mappingMd) {
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
                base.getIndexState(),
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
                base.getWaitForActiveShards(),
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
}
