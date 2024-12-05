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

package org.opensearch.common.settings;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MetadataIndexStateService;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.ShardsLimitAllocationDecider;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.index.IndexingSlowLog;
import org.opensearch.index.LogByteSizeMergePolicyProvider;
import org.opensearch.index.MergePolicyProvider;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.SearchSlowLog;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.indices.IndicesRequestCache;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Encapsulates all valid index level settings.
 * @see Property#IndexScope
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexScopedSettings extends AbstractScopedSettings {

    public static final Predicate<String> INDEX_SETTINGS_KEY_PREDICATE = (s) -> s.startsWith(IndexMetadata.INDEX_SETTING_PREFIX);

    public static final Predicate<String> ARCHIVED_SETTINGS_KEY_PREDICATE = (s) -> s.startsWith(ARCHIVED_SETTINGS_PREFIX);

    public static final Set<Setting<?>> BUILT_IN_INDEX_SETTINGS = Collections.unmodifiableSet(
        new HashSet<>(
            Arrays.asList(
                MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY,
                MergeSchedulerConfig.AUTO_THROTTLE_SETTING,
                MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
                MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
                IndexMetadata.SETTING_INDEX_VERSION_CREATED,
                IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING,
                IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_SETTING,
                IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING,
                IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING,
                IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING,
                IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING,
                IndexMetadata.INDEX_ROUTING_PARTITION_SIZE_SETTING,
                IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING,
                IndexMetadata.INDEX_READ_ONLY_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_SETTING,
                IndexMetadata.INDEX_BLOCKS_WRITE_SETTING,
                IndexMetadata.INDEX_BLOCKS_METADATA_SETTING,
                IndexMetadata.INDEX_BLOCKS_READ_ONLY_ALLOW_DELETE_SETTING,
                IndexMetadata.INDEX_PRIORITY_SETTING,
                IndexMetadata.INDEX_DATA_PATH_SETTING,
                IndexMetadata.INDEX_FORMAT_SETTING,
                IndexMetadata.INDEX_HIDDEN_SETTING,
                IndexMetadata.INDEX_REPLICATION_TYPE_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING,
                SearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_WARN_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_DEBUG_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_INFO_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_THRESHOLD_INDEX_TRACE_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_LEVEL_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_REFORMAT_SETTING,
                IndexingSlowLog.INDEX_INDEXING_SLOWLOG_MAX_SOURCE_CHARS_TO_LOG_SETTING,
                TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
                TieredMergePolicyProvider.INDEX_MERGE_POLICY_RECLAIM_DELETES_WEIGHT_SETTING,
                IndexSortConfig.INDEX_SORT_FIELD_SETTING,
                IndexSortConfig.INDEX_SORT_ORDER_SETTING,
                IndexSortConfig.INDEX_SORT_MISSING_SETTING,
                IndexSortConfig.INDEX_SORT_MODE_SETTING,
                IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING,
                IndexSettings.INDEX_WARMER_ENABLED_SETTING,
                IndexSettings.INDEX_REFRESH_INTERVAL_SETTING,
                IndexSettings.MAX_RESULT_WINDOW_SETTING,
                IndexSettings.MAX_INNER_RESULT_WINDOW_SETTING,
                IndexSettings.MAX_TOKEN_COUNT_SETTING,
                IndexSettings.MAX_DOCVALUE_FIELDS_SEARCH_SETTING,
                IndexSettings.MAX_SCRIPT_FIELDS_SETTING,
                IndexSettings.MAX_NGRAM_DIFF_SETTING,
                IndexSettings.MAX_SHINGLE_DIFF_SETTING,
                IndexSettings.MAX_RESCORE_WINDOW_SETTING,
                IndexSettings.MAX_ADJACENCY_MATRIX_FILTERS_SETTING,
                IndexSettings.MAX_ANALYZED_OFFSET_SETTING,
                IndexSettings.MAX_TERMS_COUNT_SETTING,
                IndexSettings.MAX_NESTED_QUERY_DEPTH_SETTING,
                IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING,
                IndexSettings.DEFAULT_FIELD_SETTING,
                IndexSettings.QUERY_STRING_LENIENT_SETTING,
                IndexSettings.ALLOW_UNMAPPED,
                IndexSettings.INDEX_CHECK_ON_STARTUP,
                IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD,
                IndexSettings.MAX_SLICES_PER_SCROLL,
                IndexSettings.MAX_SLICES_PER_PIT,
                IndexSettings.MAX_REGEX_LENGTH_SETTING,
                ShardsLimitAllocationDecider.INDEX_TOTAL_SHARDS_PER_NODE_SETTING,
                IndexSettings.INDEX_GC_DELETES_SETTING,
                IndexSettings.INDEX_SOFT_DELETES_SETTING,
                IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING,
                IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING,
                IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING,
                UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING,
                EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING,
                IndexSettings.INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING,
                IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING,
                IndexSettings.INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING,
                IndexSettings.INDEX_TRANSLOG_RETENTION_AGE_SETTING,
                IndexSettings.INDEX_TRANSLOG_RETENTION_SIZE_SETTING,
                IndexSettings.INDEX_SEARCH_IDLE_AFTER,
                IndexSettings.INDEX_SEARCH_THROTTLED,
                IndexSettings.INDEX_UNREFERENCED_FILE_CLEANUP,
                IndexFieldDataService.INDEX_FIELDDATA_CACHE_KEY,
                FieldMapper.IGNORE_MALFORMED_SETTING,
                FieldMapper.COERCE_SETTING,
                Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING,
                MapperService.INDEX_MAPPER_DYNAMIC_SETTING,
                MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING,
                MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING,
                MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING,
                MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING,
                MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING,
                BitsetFilterCache.INDEX_LOAD_RANDOM_ACCESS_FILTERS_EAGERLY_SETTING,
                IndexModule.INDEX_STORE_TYPE_SETTING,
                IndexModule.INDEX_STORE_PRE_LOAD_SETTING,
                IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS,
                IndexModule.INDEX_RECOVERY_TYPE_SETTING,
                IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING,
                FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING,
                Store.FORCE_RAM_TERM_DICT,
                EngineConfig.INDEX_CODEC_SETTING,
                EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING,
                EngineConfig.INDEX_OPTIMIZE_AUTO_GENERATED_IDS,
                EngineConfig.INDEX_USE_COMPOUND_FILE,
                IndexMetadata.SETTING_WAIT_FOR_ACTIVE_SHARDS,
                IndexSettings.DEFAULT_PIPELINE,
                IndexSettings.FINAL_PIPELINE,
                MetadataIndexStateService.VERIFIED_BEFORE_CLOSE_SETTING,
                ExistingShardsAllocator.EXISTING_SHARDS_ALLOCATOR_SETTING,
                IndexSettings.INDEX_MERGE_ON_FLUSH_ENABLED,
                IndexSettings.INDEX_MERGE_ON_FLUSH_MAX_FULL_FLUSH_MERGE_WAIT_TIME,
                IndexSettings.INDEX_MERGE_ON_FLUSH_POLICY,
                IndexSettings.INDEX_MERGE_POLICY,
                IndexSettings.INDEX_CHECK_PENDING_FLUSH_ENABLED,
                LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MERGE_FACTOR_SETTING,
                LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MIN_MERGE_SETTING,
                LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_SETTING,
                LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_FOR_FORCED_MERGE_SETTING,
                LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGED_DOCS_SETTING,
                LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING,
                IndexSettings.DEFAULT_SEARCH_PIPELINE,

                // Settings for Searchable Snapshots
                IndexSettings.SEARCHABLE_SNAPSHOT_REPOSITORY,
                IndexSettings.SEARCHABLE_SNAPSHOT_INDEX_ID,
                IndexSettings.SEARCHABLE_SNAPSHOT_ID_NAME,
                IndexSettings.SEARCHABLE_SNAPSHOT_ID_UUID,
                IndexSettings.SEARCHABLE_SNAPSHOT_SHARD_PATH_TYPE,

                // Settings for remote translog
                IndexSettings.INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
                IndexSettings.INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING,

                // Settings for remote store enablement
                IndexMetadata.INDEX_REMOTE_STORE_ENABLED_SETTING,
                IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING,
                IndexMetadata.INDEX_REMOTE_TRANSLOG_REPOSITORY_SETTING,

                IndexSettings.INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING,
                IndexSettings.INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING,

                // Settings for concurrent segment search
                IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING, // deprecated
                IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MODE,
                IndexSettings.INDEX_CONCURRENT_SEGMENT_SEARCH_MAX_SLICE_COUNT,
                IndexSettings.ALLOW_DERIVED_FIELDS,

                // Settings for star tree index
                StarTreeIndexSettings.STAR_TREE_DEFAULT_MAX_LEAF_DOCS,
                StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING,
                StarTreeIndexSettings.STAR_TREE_MAX_FIELDS_SETTING,
                StarTreeIndexSettings.DEFAULT_METRICS_LIST,
                StarTreeIndexSettings.DEFAULT_DATE_INTERVALS,
                StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING,
                StarTreeIndexSettings.STAR_TREE_MAX_BASE_METRICS_SETTING,
                StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING,

                IndexSettings.INDEX_CONTEXT_CREATED_VERSION,
                IndexSettings.INDEX_CONTEXT_CURRENT_VERSION,

                // validate that built-in similarities don't get redefined
                Setting.groupSetting("index.similarity.", (s) -> {
                    Map<String, Settings> groups = s.getAsGroups();
                    for (String key : SimilarityService.BUILT_IN.keySet()) {
                        if (groups.containsKey(key)) {
                            throw new IllegalArgumentException(
                                "illegal value for [index.similarity." + key + "] cannot redefine built-in similarity"
                            );
                        }
                    }
                }, Property.IndexScope), // this allows similarity settings to be passed
                Setting.groupSetting("index.analysis.", Property.IndexScope) // this allows analysis settings to be passed

            )
        )
    );

    /**
     * Map of feature flag name to feature-flagged index setting. Once each feature
     * is ready for production release, the feature flag can be removed, and the
     * setting should be moved to {@link #BUILT_IN_INDEX_SETTINGS}.
     */
    public static final Map<String, List<Setting>> FEATURE_FLAGGED_INDEX_SETTINGS = Map.of(
        FeatureFlags.TIERED_REMOTE_INDEX,
        List.of(IndexModule.INDEX_STORE_LOCALITY_SETTING, IndexModule.INDEX_TIERING_STATE),
        FeatureFlags.READER_WRITER_SPLIT_EXPERIMENTAL,
        List.of(IndexMetadata.INDEX_NUMBER_OF_SEARCH_REPLICAS_SETTING)
    );

    public static final IndexScopedSettings DEFAULT_SCOPED_SETTINGS = new IndexScopedSettings(Settings.EMPTY, BUILT_IN_INDEX_SETTINGS);

    public IndexScopedSettings(Settings settings, Set<Setting<?>> settingsSet) {
        super(settings, settingsSet, Collections.emptySet(), Property.IndexScope);
    }

    private IndexScopedSettings(Settings settings, IndexScopedSettings other, IndexMetadata metadata) {
        super(settings, metadata.getSettings(), other, Loggers.getLogger(IndexScopedSettings.class, metadata.getIndex()));
    }

    public IndexScopedSettings copy(Settings settings, IndexMetadata metadata) {
        return new IndexScopedSettings(settings, this, metadata);
    }

    @Override
    protected void validateSettingKey(Setting setting) {
        if (setting.getKey().startsWith("index.") == false) {
            throw new IllegalArgumentException("illegal settings key: [" + setting.getKey() + "] must start with [index.]");
        }
        super.validateSettingKey(setting);
    }

    @Override
    public boolean isPrivateSetting(String key) {
        switch (key) {
            case IndexMetadata.SETTING_CREATION_DATE:
            case IndexMetadata.SETTING_INDEX_UUID:
            case IndexMetadata.SETTING_HISTORY_UUID:
            case IndexMetadata.SETTING_VERSION_UPGRADED:
            case IndexMetadata.SETTING_INDEX_PROVIDED_NAME:
            case MergePolicyProvider.INDEX_MERGE_ENABLED:
                // we keep the shrink settings for BWC - this can be removed in 8.0
                // we can't remove in 7 since this setting might be baked into an index coming in via a full cluster restart from 6.0
            case "index.shrink.source.uuid":
            case "index.shrink.source.name":
            case IndexMetadata.INDEX_RESIZE_SOURCE_UUID_KEY:
            case IndexMetadata.INDEX_RESIZE_SOURCE_NAME_KEY:
                // we keep this setting for BWC to support indexes created in 1.1.0
                // this can be removed in OpenSearch 3.0
            case "index.plugins.replication.translog.retention_lease.pruning.enabled":
                return true;
            default:
                return IndexMetadata.INDEX_ROUTING_INITIAL_RECOVERY_GROUP_SETTING.getRawKey().match(key);
        }
    }
}
