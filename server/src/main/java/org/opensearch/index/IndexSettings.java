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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.sandbox.index.MergeOnFlushMergePolicy;
import org.opensearch.LegacyESVersion;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.index.remote.RemoteStorePathType;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.ingest.IngestService;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.search.pipeline.SearchPipelineService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.opensearch.Version.V_2_7_0;
import static org.opensearch.common.util.FeatureFlags.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY;
import static org.opensearch.index.codec.fuzzy.FuzzySetParameters.DEFAULT_FALSE_POSITIVE_PROBABILITY;
import static org.opensearch.index.mapper.MapperService.INDEX_MAPPING_DEPTH_LIMIT_SETTING;
import static org.opensearch.index.mapper.MapperService.INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING;
import static org.opensearch.index.mapper.MapperService.INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING;
import static org.opensearch.index.mapper.MapperService.INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING;
import static org.opensearch.index.mapper.MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING;
import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectory.SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;

/**
 * This class encapsulates all index level settings and handles settings updates.
 * It's created per index and available to all index level classes and allows them to retrieve
 * the latest updated settings instance. Classes that need to listen to settings updates can register
 * a settings consumer at index creation via {@link IndexModule#addSettingsUpdateConsumer(Setting, Consumer)} that will
 * be called for each settings update.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class IndexSettings {
    private static final String DEFAULT_POLICY = "default";
    private static final String MERGE_ON_FLUSH_MERGE_POLICY = "merge-on-flush";

    /**
     * Enum representing supported merge policies
     */
    public enum IndexMergePolicy {
        TIERED("tiered"),
        LOG_BYTE_SIZE("log_byte_size"),
        DEFAULT_POLICY(IndexSettings.DEFAULT_POLICY);

        private final String value;

        IndexMergePolicy(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static IndexMergePolicy fromString(String text) {
            for (IndexMergePolicy policy : IndexMergePolicy.values()) {
                if (policy.value.equals(text)) {
                    return policy;
                }
            }
            throw new IllegalArgumentException(
                "The setting has unsupported policy specified: "
                    + text
                    + ". Please use one of: "
                    + String.join(", ", Arrays.stream(IndexMergePolicy.values()).map(IndexMergePolicy::getValue).toArray(String[]::new))
            );
        }
    }

    public static final Setting<List<String>> DEFAULT_FIELD_SETTING = Setting.listSetting(
        "index.query.default_field",
        Collections.singletonList("*"),
        Function.identity(),
        Property.IndexScope,
        Property.Dynamic
    );
    public static final Setting<Boolean> QUERY_STRING_LENIENT_SETTING = Setting.boolSetting(
        "index.query_string.lenient",
        false,
        Property.IndexScope
    );
    public static final Setting<Boolean> QUERY_STRING_ANALYZE_WILDCARD = Setting.boolSetting(
        "indices.query.query_string.analyze_wildcard",
        false,
        Property.NodeScope
    );
    public static final Setting<Boolean> QUERY_STRING_ALLOW_LEADING_WILDCARD = Setting.boolSetting(
        "indices.query.query_string.allowLeadingWildcard",
        true,
        Property.NodeScope
    );
    public static final Setting<Boolean> ALLOW_UNMAPPED = Setting.boolSetting(
        "index.query.parse.allow_unmapped_fields",
        true,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_TRANSLOG_SYNC_INTERVAL_SETTING = Setting.timeSetting(
        "index.translog.sync_interval",
        TimeValue.timeValueSeconds(5),
        TimeValue.timeValueMillis(100),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<TimeValue> INDEX_SEARCH_IDLE_AFTER = Setting.timeSetting(
        "index.search.idle.after",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMinutes(0),
        Property.IndexScope,
        Property.Dynamic
    );
    public static final Setting<Translog.Durability> INDEX_TRANSLOG_DURABILITY_SETTING = new Setting<>(
        "index.translog.durability",
        Translog.Durability.REQUEST.name(),
        (value) -> Translog.Durability.valueOf(value.toUpperCase(Locale.ROOT)),
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Boolean> INDEX_WARMER_ENABLED_SETTING = Setting.boolSetting(
        "index.warmer.enabled",
        true,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<String> INDEX_CHECK_ON_STARTUP = new Setting<>("index.shard.check_on_startup", "false", (s) -> {
        switch (s) {
            case "false":
            case "true":
            case "checksum":
                return s;
            default:
                throw new IllegalArgumentException(
                    "unknown value for [index.shard.check_on_startup] must be one of " + "[true, false, checksum] but was: " + s
                );
        }
    }, Property.IndexScope);

    /**
     * Index setting describing the maximum value of from + size on a query.
     * The Default maximum value of from + size on a query is 10,000. This was chosen as
     * a conservative default as it is sure to not cause trouble. Users can
     * certainly profile their cluster and decide to set it to 100,000
     * safely. 1,000,000 is probably way to high for any cluster to set
     * safely.
     */
    public static final Setting<Integer> MAX_RESULT_WINDOW_SETTING = Setting.intSetting(
        "index.max_result_window",
        10000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing the maximum value of from + size on an individual inner hit definition or
     * top hits aggregation. The default maximum of 100 is defensive for the reason that the number of inner hit responses
     * and number of top hits buckets returned is unbounded. Profile your cluster when increasing this setting.
     */
    public static final Setting<Integer> MAX_INNER_RESULT_WINDOW_SETTING = Setting.intSetting(
        "index.max_inner_result_window",
        100,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum value of allowed `script_fields`that can be retrieved
     * per search request. The default maximum of 32 is defensive for the reason that retrieving
     * script fields is a costly operation.
     */
    public static final Setting<Integer> MAX_SCRIPT_FIELDS_SETTING = Setting.intSetting(
        "index.max_script_fields",
        32,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * A setting describing the maximum number of tokens that can be
     * produced using _analyze API. The default maximum of 10000 is defensive
     * to prevent generating too many token objects.
     */
    public static final Setting<Integer> MAX_TOKEN_COUNT_SETTING = Setting.intSetting(
        "index.analyze.max_token_count",
        10000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * A setting describing the maximum number of characters that will be analyzed for a highlight request.
     * This setting is only applicable when highlighting is requested on a text that was indexed without
     * offsets or term vectors.
     * The default maximum of 1M characters is defensive as for highlighting larger texts,
     * indexing with offsets or term vectors is recommended.
     */
    public static final Setting<Integer> MAX_ANALYZED_OFFSET_SETTING = Setting.intSetting(
        "index.highlight.max_analyzed_offset",
        1000000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum number of terms that can be used in Terms Query.
     * The default maximum of 65536 terms is defensive, as extra processing and memory is involved
     * for each additional term, and a large number of terms degrade the cluster performance.
     */
    public static final Setting<Integer> MAX_TERMS_COUNT_SETTING = Setting.intSetting(
        "index.max_terms_count",
        65536,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum number of nested scopes in queries.
     * The default maximum is 2<sup>31</sup>-1. 1 means once nesting.
     */
    public static final Setting<Integer> MAX_NESTED_QUERY_DEPTH_SETTING = Setting.intSetting(
        "index.query.max_nested_depth",
        Integer.MAX_VALUE,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing for NGramTokenizer and NGramTokenFilter
     * the maximum difference between
     * max_gram (maximum length of characters in a gram) and
     * min_gram (minimum length of characters in a gram).
     * The default value is 1 as this is default difference in NGramTokenizer,
     * and is defensive as it prevents generating too many index terms.
     */
    public static final Setting<Integer> MAX_NGRAM_DIFF_SETTING = Setting.intSetting(
        "index.max_ngram_diff",
        1,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing for ShingleTokenFilter
     * the maximum difference between
     * max_shingle_size and min_shingle_size.
     * The default value is 3 is defensive as it prevents generating too many tokens.
     */
    public static final Setting<Integer> MAX_SHINGLE_DIFF_SETTING = Setting.intSetting(
        "index.max_shingle_diff",
        3,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting describing the maximum value of allowed `docvalue_fields`that can be retrieved
     * per search request. The default maximum of 100 is defensive for the reason that retrieving
     * doc values might incur a per-field per-document seek.
     */
    public static final Setting<Integer> MAX_DOCVALUE_FIELDS_SEARCH_SETTING = Setting.intSetting(
        "index.max_docvalue_fields_search",
        100,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing the maximum size of the rescore window. Defaults to {@link #MAX_RESULT_WINDOW_SETTING}
     * because they both do the same thing: control the size of the heap of hits.
     */
    public static final Setting<Integer> MAX_RESCORE_WINDOW_SETTING = Setting.intSetting(
        "index.max_rescore_window",
        MAX_RESULT_WINDOW_SETTING,
        1,
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * Index setting describing the maximum number of filters clauses that can be used
     * in an adjacency_matrix aggregation. The max number of buckets produced by
     * N filters is (N*N)/2 so a limit of 100 filters is imposed by default.
     */
    public static final Setting<Integer> MAX_ADJACENCY_MATRIX_FILTERS_SETTING = Setting.intSetting(
        "index.max_adjacency_matrix_filters",
        100,
        2,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    public static final TimeValue DEFAULT_REFRESH_INTERVAL = new TimeValue(1, TimeUnit.SECONDS);
    public static final TimeValue MINIMUM_REFRESH_INTERVAL = new TimeValue(-1, TimeUnit.MILLISECONDS);
    public static final Setting<TimeValue> INDEX_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "index.refresh_interval",
        DEFAULT_REFRESH_INTERVAL,
        MINIMUM_REFRESH_INTERVAL,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.flush_threshold_size",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        /*
         * An empty translog occupies 55 bytes on disk. If the flush threshold is below this, the flush thread
         * can get stuck in an infinite loop as the shouldPeriodicallyFlush can still be true after flushing.
         * However, small thresholds are useful for testing so we do not add a large lower bound here.
         */
        new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The minimum size of a merge that triggers a flush in order to free resources
     */
    public static final Setting<ByteSizeValue> INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.flush_after_merge",
        new ByteSizeValue(512, ByteSizeUnit.MB),
        new ByteSizeValue(0, ByteSizeUnit.BYTES), // always flush after merge
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES), // never flush after merge
        Property.Dynamic,
        Property.IndexScope
    );
    /**
     * The maximum size of a translog generation. This is independent of the maximum size of
     * translog operations that have not been flushed.
     */
    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.generation_threshold_size",
        new ByteSizeValue(64, ByteSizeUnit.MB),
        /*
         * An empty translog occupies 55 bytes on disk. If the generation threshold is
         * below this, the flush thread can get stuck in an infinite loop repeatedly
         * rolling the generation as every new generation will already exceed the
         * generation threshold. However, small thresholds are useful for testing so we
         * do not add a large lower bound here.
         */
        new ByteSizeValue(Translog.DEFAULT_HEADER_SIZE_IN_BYTES + 1, ByteSizeUnit.BYTES),
        new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Index setting to enable / disable deletes garbage collection.
     * This setting is realtime updateable
     */
    public static final TimeValue DEFAULT_GC_DELETES = TimeValue.timeValueSeconds(60);
    public static final Setting<TimeValue> INDEX_GC_DELETES_SETTING = Setting.timeSetting(
        "index.gc_deletes",
        DEFAULT_GC_DELETES,
        new TimeValue(-1, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Specifies if the index should use soft-delete instead of hard-delete for update/delete operations.
     * Soft-deletes is enabled by default for Legacy 7.x and 1.x indices and mandatory for 2.0+ indices.
     */
    public static final Setting<Boolean> INDEX_SOFT_DELETES_SETTING = Setting.boolSetting(
        "index.soft_deletes.enabled",
        true,
        Property.IndexScope,
        Property.Final
    );

    /**
     * Controls how many soft-deleted documents will be kept around before being merged away. Keeping more deleted
     * documents increases the chance of operation-based recoveries and allows querying a longer history of documents.
     * If soft-deletes is enabled, an engine by default will retain all operations up to the global checkpoint.
     **/
    public static final Setting<Long> INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING = Setting.longSetting(
        "index.soft_deletes.retention.operations",
        0,
        0,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * Controls how long translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * This setting will be ignored if soft-deletes is used in peer recoveries (default in 7.4).
     **/
    public static final Setting<TimeValue> INDEX_TRANSLOG_RETENTION_AGE_SETTING = Setting.timeSetting(
        "index.translog.retention.age",
        settings -> shouldDisableTranslogRetention(settings) ? TimeValue.MINUS_ONE : TimeValue.timeValueHours(12),
        TimeValue.MINUS_ONE,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls how many translog files that are no longer needed for persistence reasons
     * will be kept around before being deleted. Keeping more files is useful to increase
     * the chance of ops based recoveries for indices with soft-deletes disabled.
     * This setting will be ignored if soft-deletes is used in peer recoveries (default in 7.4).
     **/

    public static final Setting<ByteSizeValue> INDEX_TRANSLOG_RETENTION_SIZE_SETTING = Setting.byteSizeSetting(
        "index.translog.retention.size",
        settings -> shouldDisableTranslogRetention(settings) ? "-1" : "512MB",
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the number of translog files that are no longer needed for persistence reasons will be kept around before being deleted.
     * This is a safeguard making sure that the translog deletion policy won't keep too many translog files especially when they're small.
     * This setting is intentionally not registered, it is only used in tests
     **/
    public static final Setting<Integer> INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING = Setting.intSetting(
        "index.translog.retention.total_files",
        100,
        0,
        Setting.Property.IndexScope
    );

    /**
     * Controls the maximum length of time since a retention lease is created or renewed before it is considered expired.
     */
    public static final Setting<TimeValue> INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING = Setting.timeSetting(
        "index.soft_deletes.retention_lease.period",
        TimeValue.timeValueHours(12),
        TimeValue.ZERO,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public static final Setting<Integer> MAX_REFRESH_LISTENERS_PER_SHARD = Setting.intSetting(
        "index.max_refresh_listeners",
        1000,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum number of slices allowed in a scroll request
     */
    public static final Setting<Integer> MAX_SLICES_PER_SCROLL = Setting.intSetting(
        "index.max_slices_per_scroll",
        1024,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum number of slices allowed in a search request with PIT
     */
    public static final Setting<Integer> MAX_SLICES_PER_PIT = Setting.intSetting(
        "index.max_slices_per_pit",
        1024,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    public static final Setting<Integer> MAX_REGEX_LENGTH_SETTING = Setting.intSetting(
        "index.max_regex_length",
        1000,
        1,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> DEFAULT_PIPELINE = new Setting<>(
        "index.default_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<String> FINAL_PIPELINE = new Setting<>(
        "index.final_pipeline",
        IngestService.NOOP_PIPELINE_NAME,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Marks an index to be searched throttled. This means that never more than one shard of such an index will be searched concurrently
     */
    public static final Setting<Boolean> INDEX_SEARCH_THROTTLED = Setting.boolSetting(
        "index.search.throttled",
        false,
        Property.IndexScope,
        Property.PrivateIndex,
        Property.Dynamic
    );

    /**
     * This setting controls if unreferenced files will be cleaned up in case segment merge fails due to disk full.
     * <p>
     * Defaults to true which means unreferenced files will be cleaned up in case segment merge fails.
     */
    public static final Setting<Boolean> INDEX_UNREFERENCED_FILE_CLEANUP = Setting.boolSetting(
        "index.unreferenced_file_cleanup.enabled",
        true,
        Property.IndexScope,
        Property.Dynamic
    );

    /**
     * Determines a balance between file-based and operations-based peer recoveries. The number of operations that will be used in an
     * operations-based peer recovery is limited to this proportion of the total number of documents in the shard (including deleted
     * documents) on the grounds that a file-based peer recovery may copy all of the documents in the shard over to the new peer, but is
     * significantly faster than replaying the missing operations on the peer, so once a peer falls far enough behind the primary it makes
     * more sense to copy all the data over again instead of replaying history.
     * <p>
     * Defaults to retaining history for up to 10% of the documents in the shard. This can only be changed in tests, since this setting is
     * intentionally unregistered.
     */
    public static final Setting<Double> FILE_BASED_RECOVERY_THRESHOLD_SETTING = Setting.doubleSetting(
        "index.recovery.file_based_threshold",
        0.1d,
        0.0d,
        Setting.Property.IndexScope
    );

    /**
     * Expert: sets the amount of time to wait for merges (during {@link org.apache.lucene.index.IndexWriter#commit}
     * or {@link org.apache.lucene.index.IndexWriter#getReader(boolean, boolean)}) returned by MergePolicy.findFullFlushMerges(...).
     * If this time is reached, we proceed with the commit based on segments merged up to that point. The merges are not
     * aborted, and will still run to completion independent of the commit or getReader call, like natural segment merges.
     */
    public static final Setting<TimeValue> INDEX_MERGE_ON_FLUSH_MAX_FULL_FLUSH_MERGE_WAIT_TIME = Setting.timeSetting(
        "index.merge_on_flush.max_full_flush_merge_wait_time",
        new TimeValue(10, TimeUnit.SECONDS),
        new TimeValue(1, TimeUnit.MILLISECONDS),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_MERGE_ON_FLUSH_ENABLED = Setting.boolSetting(
        "index.merge_on_flush.enabled",
        true, /* https://issues.apache.org/jira/browse/LUCENE-10078 */
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<String> INDEX_MERGE_ON_FLUSH_POLICY = Setting.simpleString(
        "index.merge_on_flush.policy",
        DEFAULT_POLICY,
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<String> INDEX_MERGE_POLICY = Setting.simpleString(
        "index.merge.policy",
        DEFAULT_POLICY,
        IndexMergePolicy::fromString,
        Property.IndexScope
    );

    /**
     * Expert: Makes indexing threads check for pending flushes on update in order to help out
     * flushing indexing buffers to disk. This is an experimental Apache Lucene feature.
     */
    public static final Setting<Boolean> INDEX_CHECK_PENDING_FLUSH_ENABLED = Setting.boolSetting(
        "index.check_pending_flush.enabled",
        true,
        Property.IndexScope
    );

    public static final Setting<String> TIME_SERIES_INDEX_MERGE_POLICY = Setting.simpleString(
        "indices.time_series_index.default_index_merge_policy",
        DEFAULT_POLICY,
        IndexMergePolicy::fromString,
        Property.NodeScope
    );

    public static final Setting<String> SEARCHABLE_SNAPSHOT_REPOSITORY = Setting.simpleString(
        "index.searchable_snapshot.repository",
        Property.IndexScope,
        Property.InternalIndex
    );

    public static final Setting<String> SEARCHABLE_SNAPSHOT_ID_UUID = Setting.simpleString(
        "index.searchable_snapshot.snapshot_id.uuid",
        Property.IndexScope,
        Property.InternalIndex
    );

    public static final Setting<String> SEARCHABLE_SNAPSHOT_ID_NAME = Setting.simpleString(
        "index.searchable_snapshot.snapshot_id.name",
        Property.IndexScope,
        Property.InternalIndex
    );

    public static final Setting<String> SEARCHABLE_SNAPSHOT_INDEX_ID = Setting.simpleString(
        "index.searchable_snapshot.index.id",
        Property.IndexScope,
        Property.InternalIndex
    );

    public static final Setting<String> DEFAULT_SEARCH_PIPELINE = new Setting<>(
        "index.search.default_pipeline",
        SearchPipelineService.NOOP_PIPELINE_ID,
        Function.identity(),
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Boolean> INDEX_CONCURRENT_SEGMENT_SEARCH_SETTING = Setting.boolSetting(
        "index.search.concurrent_segment_search.enabled",
        false,
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<Boolean> INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING = Setting.boolSetting(
        "index.optimize_doc_id_lookup.fuzzy_set.enabled",
        false,
        Property.IndexScope,
        Property.Dynamic
    );

    public static final Setting<Double> INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING = Setting.doubleSetting(
        "index.optimize_doc_id_lookup.fuzzy_set.false_positive_probability",
        DEFAULT_FALSE_POSITIVE_PROBABILITY,
        0.01,
        0.50,
        Property.IndexScope,
        Property.Dynamic
    );

    public static final TimeValue DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL = new TimeValue(650, TimeUnit.MILLISECONDS);
    public static final TimeValue MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL = TimeValue.ZERO;
    public static final Setting<TimeValue> INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING = Setting.timeSetting(
        "index.remote_store.translog.buffer_interval",
        DEFAULT_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        MINIMUM_REMOTE_TRANSLOG_BUFFER_INTERVAL,
        Property.Dynamic,
        Property.IndexScope
    );

    public static final Setting<Integer> INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING = Setting.intSetting(
        "index.remote_store.translog.keep_extra_gen",
        100,
        0,
        Property.Dynamic,
        Property.IndexScope
    );

    private final Index index;
    private final Version version;
    private final Logger logger;
    private final String nodeName;
    private final Settings nodeSettings;
    private final int numberOfShards;
    private final ReplicationType replicationType;
    private final boolean isRemoteStoreEnabled;
    private volatile TimeValue remoteTranslogUploadBufferInterval;
    private final String remoteStoreTranslogRepository;
    private final String remoteStoreRepository;
    private final boolean isRemoteSnapshot;
    private int remoteTranslogKeepExtraGen;
    private Version extendedCompatibilitySnapshotVersion;
    // volatile fields are updated via #updateIndexMetadata(IndexMetadata) under lock
    private volatile Settings settings;
    private volatile IndexMetadata indexMetadata;
    private volatile List<String> defaultFields;
    private final boolean queryStringLenient;
    private final boolean queryStringAnalyzeWildcard;
    private final boolean queryStringAllowLeadingWildcard;
    private final boolean defaultAllowUnmappedFields;
    private volatile Translog.Durability durability;
    private volatile TimeValue syncInterval;
    private volatile TimeValue refreshInterval;
    private volatile ByteSizeValue flushThresholdSize;
    private volatile TimeValue translogRetentionAge;
    private volatile ByteSizeValue translogRetentionSize;
    private volatile ByteSizeValue generationThresholdSize;
    private volatile ByteSizeValue flushAfterMergeThresholdSize;
    private final MergeSchedulerConfig mergeSchedulerConfig;
    private final TieredMergePolicyProvider tieredMergePolicyProvider;
    private final LogByteSizeMergePolicyProvider logByteSizeMergePolicyProvider;
    private final IndexSortConfig indexSortConfig;
    private final IndexScopedSettings scopedSettings;
    private long gcDeletesInMillis = DEFAULT_GC_DELETES.millis();
    private final boolean softDeleteEnabled;
    private volatile long softDeleteRetentionOperations;

    private volatile long retentionLeaseMillis;

    private volatile String defaultSearchPipeline;
    private final boolean widenIndexSortType;
    private final boolean assignedOnRemoteNode;

    /**
     * The maximum age of a retention lease before it is considered expired.
     *
     * @return the maximum age
     */
    public long getRetentionLeaseMillis() {
        return retentionLeaseMillis;
    }

    private void setRetentionLeaseMillis(final TimeValue retentionLease) {
        this.retentionLeaseMillis = retentionLease.millis();
    }

    private volatile boolean warmerEnabled;
    private volatile int maxResultWindow;
    private volatile int maxInnerResultWindow;
    private volatile int maxAdjacencyMatrixFilters;
    private volatile int maxRescoreWindow;
    private volatile int maxDocvalueFields;
    private volatile int maxScriptFields;
    private volatile int maxTokenCount;
    private volatile int maxNgramDiff;
    private volatile int maxShingleDiff;
    private volatile TimeValue searchIdleAfter;
    private volatile int maxAnalyzedOffset;
    private volatile int maxTermsCount;

    private volatile int maxNestedQueryDepth;
    private volatile String defaultPipeline;
    private volatile String requiredPipeline;
    private volatile boolean searchThrottled;
    private volatile boolean shouldCleanupUnreferencedFiles;
    private volatile long mappingNestedFieldsLimit;
    private volatile long mappingNestedDocsLimit;
    private volatile long mappingTotalFieldsLimit;
    private volatile long mappingDepthLimit;
    private volatile long mappingFieldNameLengthLimit;

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    private volatile int maxRefreshListeners;
    /**
     * The maximum number of slices allowed in a scroll request.
     */
    private volatile int maxSlicesPerScroll;
    /**
     * The maximum number of slices allowed in a PIT request.
     */
    private volatile int maxSlicesPerPit;
    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    private volatile int maxRegexLength;

    /**
     * The max amount of time to wait for merges
     */
    private volatile TimeValue maxFullFlushMergeWaitTime;
    /**
     * Is merge of flush enabled or not
     */
    private volatile boolean mergeOnFlushEnabled;
    /**
     * Specialized merge-on-flush policy if provided
     */
    private volatile UnaryOperator<MergePolicy> mergeOnFlushPolicy;
    /**
     * Is flush check by write threads enabled or not
     */
    private final boolean checkPendingFlushEnabled;
    /**
     * Is fuzzy set enabled for doc id
     */
    private volatile boolean enableFuzzySetForDocId;

    /**
     * False positive probability to use while creating fuzzy set.
     */
    private volatile double docIdFuzzySetFalsePositiveProbability;

    /**
     * Returns the default search fields for this index.
     */
    public List<String> getDefaultFields() {
        return defaultFields;
    }

    private void setDefaultFields(List<String> defaultFields) {
        this.defaultFields = defaultFields;
    }

    /**
     * Returns <code>true</code> if query string parsing should be lenient. The default is <code>false</code>
     */
    public boolean isQueryStringLenient() {
        return queryStringLenient;
    }

    /**
     * Returns <code>true</code> if the query string should analyze wildcards. The default is <code>false</code>
     */
    public boolean isQueryStringAnalyzeWildcard() {
        return queryStringAnalyzeWildcard;
    }

    /**
     * Returns <code>true</code> if the query string parser should allow leading wildcards. The default is <code>true</code>
     */
    public boolean isQueryStringAllowLeadingWildcard() {
        return queryStringAllowLeadingWildcard;
    }

    /**
     * Returns <code>true</code> if queries should be lenient about unmapped fields. The default is <code>true</code>
     */
    public boolean isDefaultAllowUnmappedFields() {
        return defaultAllowUnmappedFields;
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings) {
        this(indexMetadata, nodeSettings, IndexScopedSettings.DEFAULT_SCOPED_SETTINGS);
    }

    /**
     * Creates a new {@link IndexSettings} instance. The given node settings will be merged with the settings in the metadata
     * while index level settings will overwrite node settings.
     *
     * @param indexMetadata the index metadata this settings object is associated with
     * @param nodeSettings the nodes settings this index is allocated on.
     */
    public IndexSettings(final IndexMetadata indexMetadata, final Settings nodeSettings, IndexScopedSettings indexScopedSettings) {
        scopedSettings = indexScopedSettings.copy(nodeSettings, indexMetadata);
        this.nodeSettings = nodeSettings;
        this.settings = Settings.builder().put(nodeSettings).put(indexMetadata.getSettings()).build();
        this.index = indexMetadata.getIndex();
        version = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        logger = Loggers.getLogger(getClass(), index);
        nodeName = Node.NODE_NAME_SETTING.get(settings);
        this.indexMetadata = indexMetadata;
        numberOfShards = settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, null);
        replicationType = IndexMetadata.INDEX_REPLICATION_TYPE_SETTING.get(settings);
        isRemoteStoreEnabled = settings.getAsBoolean(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, false);
        remoteStoreTranslogRepository = settings.get(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_REPOSITORY);
        remoteTranslogUploadBufferInterval = INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.get(settings);
        remoteStoreRepository = settings.get(IndexMetadata.SETTING_REMOTE_SEGMENT_STORE_REPOSITORY);
        this.remoteTranslogKeepExtraGen = INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING.get(settings);
        isRemoteSnapshot = IndexModule.Type.REMOTE_SNAPSHOT.match(this.settings);

        if (isRemoteSnapshot && FeatureFlags.isEnabled(SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY)) {
            extendedCompatibilitySnapshotVersion = SEARCHABLE_SNAPSHOT_EXTENDED_COMPATIBILITY_MINIMUM_VERSION;
        } else {
            extendedCompatibilitySnapshotVersion = Version.CURRENT.minimumIndexCompatibilityVersion();
        }
        this.searchThrottled = INDEX_SEARCH_THROTTLED.get(settings);
        this.shouldCleanupUnreferencedFiles = INDEX_UNREFERENCED_FILE_CLEANUP.get(settings);
        this.queryStringLenient = QUERY_STRING_LENIENT_SETTING.get(settings);
        this.queryStringAnalyzeWildcard = QUERY_STRING_ANALYZE_WILDCARD.get(nodeSettings);
        this.queryStringAllowLeadingWildcard = QUERY_STRING_ALLOW_LEADING_WILDCARD.get(nodeSettings);
        this.defaultAllowUnmappedFields = scopedSettings.get(ALLOW_UNMAPPED);
        this.durability = scopedSettings.get(INDEX_TRANSLOG_DURABILITY_SETTING);
        defaultFields = scopedSettings.get(DEFAULT_FIELD_SETTING);
        syncInterval = INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.get(settings);
        refreshInterval = scopedSettings.get(INDEX_REFRESH_INTERVAL_SETTING);
        flushThresholdSize = scopedSettings.get(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING);
        generationThresholdSize = scopedSettings.get(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING);
        flushAfterMergeThresholdSize = scopedSettings.get(INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING);
        mergeSchedulerConfig = new MergeSchedulerConfig(this);
        gcDeletesInMillis = scopedSettings.get(INDEX_GC_DELETES_SETTING).getMillis();
        softDeleteEnabled = scopedSettings.get(INDEX_SOFT_DELETES_SETTING);
        assert softDeleteEnabled || version.before(Version.V_2_0_0) : "soft deletes must be enabled in version " + version;
        softDeleteRetentionOperations = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING);
        retentionLeaseMillis = scopedSettings.get(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING).millis();
        warmerEnabled = scopedSettings.get(INDEX_WARMER_ENABLED_SETTING);
        maxResultWindow = scopedSettings.get(MAX_RESULT_WINDOW_SETTING);
        maxInnerResultWindow = scopedSettings.get(MAX_INNER_RESULT_WINDOW_SETTING);
        maxAdjacencyMatrixFilters = scopedSettings.get(MAX_ADJACENCY_MATRIX_FILTERS_SETTING);
        maxRescoreWindow = scopedSettings.get(MAX_RESCORE_WINDOW_SETTING);
        maxDocvalueFields = scopedSettings.get(MAX_DOCVALUE_FIELDS_SEARCH_SETTING);
        maxScriptFields = scopedSettings.get(MAX_SCRIPT_FIELDS_SETTING);
        maxTokenCount = scopedSettings.get(MAX_TOKEN_COUNT_SETTING);
        maxNgramDiff = scopedSettings.get(MAX_NGRAM_DIFF_SETTING);
        maxShingleDiff = scopedSettings.get(MAX_SHINGLE_DIFF_SETTING);
        maxRefreshListeners = scopedSettings.get(MAX_REFRESH_LISTENERS_PER_SHARD);
        maxSlicesPerScroll = scopedSettings.get(MAX_SLICES_PER_SCROLL);
        maxSlicesPerPit = scopedSettings.get(MAX_SLICES_PER_PIT);
        maxAnalyzedOffset = scopedSettings.get(MAX_ANALYZED_OFFSET_SETTING);
        maxTermsCount = scopedSettings.get(MAX_TERMS_COUNT_SETTING);
        maxNestedQueryDepth = scopedSettings.get(MAX_NESTED_QUERY_DEPTH_SETTING);
        maxRegexLength = scopedSettings.get(MAX_REGEX_LENGTH_SETTING);
        this.tieredMergePolicyProvider = new TieredMergePolicyProvider(logger, this);
        this.logByteSizeMergePolicyProvider = new LogByteSizeMergePolicyProvider(logger, this);
        this.indexSortConfig = new IndexSortConfig(this);
        searchIdleAfter = scopedSettings.get(INDEX_SEARCH_IDLE_AFTER);
        defaultPipeline = scopedSettings.get(DEFAULT_PIPELINE);
        setTranslogRetentionAge(scopedSettings.get(INDEX_TRANSLOG_RETENTION_AGE_SETTING));
        setTranslogRetentionSize(scopedSettings.get(INDEX_TRANSLOG_RETENTION_SIZE_SETTING));
        mappingNestedFieldsLimit = scopedSettings.get(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING);
        mappingNestedDocsLimit = scopedSettings.get(INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING);
        mappingTotalFieldsLimit = scopedSettings.get(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING);
        mappingDepthLimit = scopedSettings.get(INDEX_MAPPING_DEPTH_LIMIT_SETTING);
        mappingFieldNameLengthLimit = scopedSettings.get(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING);
        maxFullFlushMergeWaitTime = scopedSettings.get(INDEX_MERGE_ON_FLUSH_MAX_FULL_FLUSH_MERGE_WAIT_TIME);
        mergeOnFlushEnabled = scopedSettings.get(INDEX_MERGE_ON_FLUSH_ENABLED);
        setMergeOnFlushPolicy(scopedSettings.get(INDEX_MERGE_ON_FLUSH_POLICY));
        checkPendingFlushEnabled = scopedSettings.get(INDEX_CHECK_PENDING_FLUSH_ENABLED);
        defaultSearchPipeline = scopedSettings.get(DEFAULT_SEARCH_PIPELINE);
        /* There was unintentional breaking change got introduced with [OpenSearch-6424](https://github.com/opensearch-project/OpenSearch/pull/6424) (version 2.7).
         * For indices created prior version (prior to 2.7) which has IndexSort type, they used to type cast the SortField.Type
         * to higher bytes size like integer to long. This behavior was changed from OpenSearch 2.7 version not to
         * up cast the SortField to gain some sort query optimizations.
         * Now this sortField (IndexSort) is stored in SegmentInfo and we need to maintain backward compatibility for them.
         */
        widenIndexSortType = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings).before(V_2_7_0);
        assignedOnRemoteNode = RemoteStoreNodeAttribute.isRemoteDataAttributePresent(this.getNodeSettings());

        setEnableFuzzySetForDocId(scopedSettings.get(INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING));
        setDocIdFuzzySetFalsePositiveProbability(scopedSettings.get(INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING));

        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING,
            tieredMergePolicyProvider::setNoCFSRatio
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_DELETES_PCT_ALLOWED_SETTING,
            tieredMergePolicyProvider::setDeletesPctAllowed
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_EXPUNGE_DELETES_ALLOWED_SETTING,
            tieredMergePolicyProvider::setExpungeDeletesAllowed
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_FLOOR_SEGMENT_SETTING,
            tieredMergePolicyProvider::setFloorSegmentSetting
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING,
            tieredMergePolicyProvider::setMaxMergesAtOnce
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_MAX_MERGED_SEGMENT_SETTING,
            tieredMergePolicyProvider::setMaxMergedSegment
        );
        scopedSettings.addSettingsUpdateConsumer(
            TieredMergePolicyProvider.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING,
            tieredMergePolicyProvider::setSegmentsPerTier
        );

        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MERGE_FACTOR_SETTING,
            logByteSizeMergePolicyProvider::setLBSMergeFactor
        );
        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_MERGE_POLICY_MIN_MERGE_SETTING,
            logByteSizeMergePolicyProvider::setLBSMinMergedMB
        );
        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_SETTING,
            logByteSizeMergePolicyProvider::setLBSMaxMergeSegment
        );
        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGE_SEGMENT_FOR_FORCED_MERGE_SETTING,
            logByteSizeMergePolicyProvider::setLBSMaxMergeMBForForcedMerge
        );
        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_MAX_MERGED_DOCS_SETTING,
            logByteSizeMergePolicyProvider::setLBSMaxMergeDocs
        );
        scopedSettings.addSettingsUpdateConsumer(
            LogByteSizeMergePolicyProvider.INDEX_LBS_NO_CFS_RATIO_SETTING,
            logByteSizeMergePolicyProvider::setLBSNoCFSRatio
        );
        scopedSettings.addSettingsUpdateConsumer(
            MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING,
            MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING,
            mergeSchedulerConfig::setMaxThreadAndMergeCount
        );
        scopedSettings.addSettingsUpdateConsumer(MergeSchedulerConfig.AUTO_THROTTLE_SETTING, mergeSchedulerConfig::setAutoThrottle);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_DURABILITY_SETTING, this::setTranslogDurability);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_SYNC_INTERVAL_SETTING, this::setTranslogSyncInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESULT_WINDOW_SETTING, this::setMaxResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_INNER_RESULT_WINDOW_SETTING, this::setMaxInnerResultWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_ADJACENCY_MATRIX_FILTERS_SETTING, this::setMaxAdjacencyMatrixFilters);
        scopedSettings.addSettingsUpdateConsumer(MAX_RESCORE_WINDOW_SETTING, this::setMaxRescoreWindow);
        scopedSettings.addSettingsUpdateConsumer(MAX_DOCVALUE_FIELDS_SEARCH_SETTING, this::setMaxDocvalueFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_SCRIPT_FIELDS_SETTING, this::setMaxScriptFields);
        scopedSettings.addSettingsUpdateConsumer(MAX_TOKEN_COUNT_SETTING, this::setMaxTokenCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_NGRAM_DIFF_SETTING, this::setMaxNgramDiff);
        scopedSettings.addSettingsUpdateConsumer(MAX_SHINGLE_DIFF_SETTING, this::setMaxShingleDiff);
        scopedSettings.addSettingsUpdateConsumer(INDEX_WARMER_ENABLED_SETTING, this::setEnableWarmer);
        scopedSettings.addSettingsUpdateConsumer(INDEX_GC_DELETES_SETTING, this::setGCDeletes);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING, this::setTranslogFlushThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_FLUSH_AFTER_MERGE_THRESHOLD_SIZE_SETTING, this::setFlushAfterMergeThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_GENERATION_THRESHOLD_SIZE_SETTING, this::setGenerationThresholdSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_AGE_SETTING, this::setTranslogRetentionAge);
        scopedSettings.addSettingsUpdateConsumer(INDEX_TRANSLOG_RETENTION_SIZE_SETTING, this::setTranslogRetentionSize);
        scopedSettings.addSettingsUpdateConsumer(INDEX_REFRESH_INTERVAL_SETTING, this::setRefreshInterval);
        scopedSettings.addSettingsUpdateConsumer(MAX_REFRESH_LISTENERS_PER_SHARD, this::setMaxRefreshListeners);
        scopedSettings.addSettingsUpdateConsumer(MAX_ANALYZED_OFFSET_SETTING, this::setHighlightMaxAnalyzedOffset);
        scopedSettings.addSettingsUpdateConsumer(MAX_TERMS_COUNT_SETTING, this::setMaxTermsCount);
        scopedSettings.addSettingsUpdateConsumer(MAX_NESTED_QUERY_DEPTH_SETTING, this::setMaxNestedQueryDepth);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_SCROLL, this::setMaxSlicesPerScroll);
        scopedSettings.addSettingsUpdateConsumer(MAX_SLICES_PER_PIT, this::setMaxSlicesPerPit);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_FIELD_SETTING, this::setDefaultFields);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_IDLE_AFTER, this::setSearchIdleAfter);
        scopedSettings.addSettingsUpdateConsumer(MAX_REGEX_LENGTH_SETTING, this::setMaxRegexLength);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_PIPELINE, this::setDefaultPipeline);
        scopedSettings.addSettingsUpdateConsumer(FINAL_PIPELINE, this::setRequiredPipeline);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING, this::setSoftDeleteRetentionOperations);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SEARCH_THROTTLED, this::setSearchThrottled);
        scopedSettings.addSettingsUpdateConsumer(INDEX_UNREFERENCED_FILE_CLEANUP, this::setShouldCleanupUnreferencedFiles);
        scopedSettings.addSettingsUpdateConsumer(INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING, this::setRetentionLeaseMillis);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING, this::setMappingNestedFieldsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING, this::setMappingNestedDocsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING, this::setMappingTotalFieldsLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_DEPTH_LIMIT_SETTING, this::setMappingDepthLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING, this::setMappingFieldNameLengthLimit);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MERGE_ON_FLUSH_MAX_FULL_FLUSH_MERGE_WAIT_TIME, this::setMaxFullFlushMergeWaitTime);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MERGE_ON_FLUSH_ENABLED, this::setMergeOnFlushEnabled);
        scopedSettings.addSettingsUpdateConsumer(INDEX_MERGE_ON_FLUSH_POLICY, this::setMergeOnFlushPolicy);
        scopedSettings.addSettingsUpdateConsumer(DEFAULT_SEARCH_PIPELINE, this::setDefaultSearchPipeline);
        scopedSettings.addSettingsUpdateConsumer(
            INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING,
            this::setRemoteTranslogUploadBufferInterval
        );
        scopedSettings.addSettingsUpdateConsumer(INDEX_REMOTE_TRANSLOG_KEEP_EXTRA_GEN_SETTING, this::setRemoteTranslogKeepExtraGen);
        scopedSettings.addSettingsUpdateConsumer(INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING, this::setEnableFuzzySetForDocId);
        scopedSettings.addSettingsUpdateConsumer(
            INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING,
            this::setDocIdFuzzySetFalsePositiveProbability
        );
    }

    private void setSearchIdleAfter(TimeValue searchIdleAfter) {
        if (this.isRemoteStoreEnabled) {
            logger.warn("Search idle is not supported for remote backed indices");
        }
        if (this.replicationType == ReplicationType.SEGMENT && this.getNumberOfReplicas() > 0) {
            logger.warn("Search idle is not supported for indices with replicas using 'replication.type: SEGMENT'");
        }
        this.searchIdleAfter = searchIdleAfter;
    }

    private void setTranslogFlushThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushThresholdSize = byteSizeValue;
    }

    private void setFlushAfterMergeThresholdSize(ByteSizeValue byteSizeValue) {
        this.flushAfterMergeThresholdSize = byteSizeValue;
    }

    private void setTranslogRetentionSize(ByteSizeValue byteSizeValue) {
        if (shouldDisableTranslogRetention(settings) && byteSizeValue.getBytes() >= 0) {
            // ignore the translog retention settings if soft-deletes enabled
            this.translogRetentionSize = new ByteSizeValue(-1);
        } else {
            this.translogRetentionSize = byteSizeValue;
        }
    }

    private void setTranslogRetentionAge(TimeValue age) {
        if (shouldDisableTranslogRetention(settings) && age.millis() >= 0) {
            // ignore the translog retention settings if soft-deletes enabled
            this.translogRetentionAge = TimeValue.MINUS_ONE;
        } else {
            this.translogRetentionAge = age;
        }
    }

    private void setGenerationThresholdSize(final ByteSizeValue generationThresholdSize) {
        this.generationThresholdSize = generationThresholdSize;
    }

    private void setGCDeletes(TimeValue timeValue) {
        this.gcDeletesInMillis = timeValue.getMillis();
    }

    private void setRefreshInterval(TimeValue timeValue) {
        this.refreshInterval = timeValue;
    }

    /**
     * Returns the settings for this index. These settings contain the node and index level settings where
     * settings that are specified on both index and node level are overwritten by the index settings.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Returns the index this settings object belongs to
     */
    public Index getIndex() {
        return index;
    }

    /**
     * Returns the indexes UUID
     */
    public String getUUID() {
        return getIndex().getUUID();
    }

    /**
     * Returns <code>true</code> if the index has a custom data path
     */
    public boolean hasCustomDataPath() {
        return Strings.isEmpty(customDataPath()) == false;
    }

    /**
     * Returns the customDataPath for this index, if configured. <code>""</code> o.w.
     */
    public String customDataPath() {
        return IndexMetadata.INDEX_DATA_PATH_SETTING.get(settings);
    }

    /**
     * Returns the version the index was created on.
     * @see IndexMetadata#indexCreated(Settings)
     */
    public Version getIndexVersionCreated() {
        return version;
    }

    /**
     * Returns the current node name
     */
    public String getNodeName() {
        return nodeName;
    }

    /**
     * Returns the current IndexMetadata for this index
     */
    public IndexMetadata getIndexMetadata() {
        return indexMetadata;
    }

    /**
     * Returns the number of shards this index has.
     */
    public int getNumberOfShards() {
        return numberOfShards;
    }

    /**
     * Returns the number of replicas this index has.
     */
    public int getNumberOfReplicas() {
        return settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, null);
    }

    /**
     * Returns true if segment replication is enabled on the index.
     *
     * Every shard on a remote node would also have SegRep enabled even without
     * proper index setting during the migration.
     */
    public boolean isSegRepEnabledOrRemoteNode() {
        return ReplicationType.SEGMENT.equals(replicationType) || isAssignedOnRemoteNode();
    }

    public boolean isSegRepLocalEnabled() {
        return ReplicationType.SEGMENT.equals(replicationType) && !isRemoteStoreEnabled();
    }

    /**
     * Returns if remote store is enabled for this index.
     */
    public boolean isRemoteStoreEnabled() {
        return isRemoteStoreEnabled;
    }

    public boolean isAssignedOnRemoteNode() {
        return assignedOnRemoteNode;
    }

    /**
     * Returns remote store repository configured for this index.
     */
    public String getRemoteStoreRepository() {
        return remoteStoreRepository;
    }

    /**
     * Returns if remote translog store is enabled for this index.
     */
    public boolean isRemoteTranslogStoreEnabled() {
        // Today enabling remote store automatically enables remote translog as well.
        // which is why isRemoteStoreEnabled is used to represent isRemoteTranslogStoreEnabled
        return isRemoteStoreEnabled;
    }

    /**
     * Returns true if this is remote/searchable snapshot
     */
    public boolean isRemoteSnapshot() {
        return isRemoteSnapshot;
    }

    /**
     * If this is a remote snapshot and the extended compatibility
     * feature flag is enabled, this returns the minimum {@link Version}
     * supported. In all other cases, the return value is the
     * {@link Version#minimumIndexCompatibilityVersion()} of {@link Version#CURRENT}.
     */
    public Version getExtendedCompatibilitySnapshotVersion() {
        return extendedCompatibilitySnapshotVersion;
    }

    public String getRemoteStoreTranslogRepository() {
        return remoteStoreTranslogRepository;
    }

    /**
     * Returns the node settings. The settings returned from {@link #getSettings()} are a merged version of the
     * index settings and the node settings where node settings are overwritten by index settings.
     */
    public Settings getNodeSettings() {
        return nodeSettings;
    }

    /**
     * Updates the settings and index metadata and notifies all registered settings consumers with the new settings iff at least one
     * setting has changed.
     *
     * @return <code>true</code> iff any setting has been updated otherwise <code>false</code>.
     */
    public synchronized boolean updateIndexMetadata(IndexMetadata indexMetadata) {
        final Settings newSettings = indexMetadata.getSettings();
        if (version.equals(IndexMetadata.indexCreated(newSettings)) == false) {
            throw new IllegalArgumentException(
                "version mismatch on settings update expected: " + version + " but was: " + IndexMetadata.indexCreated(newSettings)
            );
        }
        final String newUUID = newSettings.get(IndexMetadata.SETTING_INDEX_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newUUID.equals(getUUID()) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + getUUID() + " but was: " + newUUID);
        }
        final String newRestoreUUID = newSettings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        final String restoreUUID = this.settings.get(IndexMetadata.SETTING_HISTORY_UUID, IndexMetadata.INDEX_UUID_NA_VALUE);
        if (newRestoreUUID.equals(restoreUUID) == false) {
            throw new IllegalArgumentException("uuid mismatch on settings update expected: " + restoreUUID + " but was: " + newRestoreUUID);
        }
        this.indexMetadata = indexMetadata;
        final Settings newIndexSettings = Settings.builder().put(nodeSettings).put(newSettings).build();
        if (same(this.settings, newIndexSettings)) {
            // nothing to update, same settings
            return false;
        }
        scopedSettings.applySettings(newSettings);
        this.settings = newIndexSettings;
        return true;
    }

    /**
     * Compare the specified settings for equality.
     *
     * @param left  the left settings
     * @param right the right settings
     * @return true if the settings are the same, otherwise false
     */
    public static boolean same(final Settings left, final Settings right) {
        return left.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE)
            .equals(right.filter(IndexScopedSettings.INDEX_SETTINGS_KEY_PREDICATE))
            && left.filter(IndexScopedSettings.ARCHIVED_SETTINGS_KEY_PREDICATE)
                .equals(right.filter(IndexScopedSettings.ARCHIVED_SETTINGS_KEY_PREDICATE));
    }

    /**
     * Returns the translog durability for this index.
     */
    public Translog.Durability getTranslogDurability() {
        return durability;
    }

    private void setTranslogDurability(Translog.Durability durability) {
        this.durability = durability;
    }

    /**
     * Returns true if index warmers are enabled, otherwise <code>false</code>
     */
    public boolean isWarmerEnabled() {
        return warmerEnabled;
    }

    private void setEnableWarmer(boolean enableWarmer) {
        this.warmerEnabled = enableWarmer;
    }

    /**
     * Returns the translog sync interval. This is the interval in which the transaction log is asynchronously fsynced unless
     * the transaction log is fsyncing on every operations
     */
    public TimeValue getTranslogSyncInterval() {
        return syncInterval;
    }

    public void setTranslogSyncInterval(TimeValue translogSyncInterval) {
        this.syncInterval = translogSyncInterval;
    }

    /**
     * Returns the translog sync/upload buffer interval when remote translog store is enabled and index setting
     * {@code index.translog.durability} is set as {@code request}.
     * @return the buffer interval.
     */
    public TimeValue getRemoteTranslogUploadBufferInterval() {
        return remoteTranslogUploadBufferInterval;
    }

    public int getRemoteTranslogExtraKeep() {
        return remoteTranslogKeepExtraGen;
    }

    /**
     * Returns true iff the remote translog buffer interval setting exists or in other words is explicitly set.
     */
    public boolean isRemoteTranslogBufferIntervalExplicit() {
        return INDEX_REMOTE_TRANSLOG_BUFFER_INTERVAL_SETTING.exists(settings);
    }

    public void setRemoteTranslogUploadBufferInterval(TimeValue remoteTranslogUploadBufferInterval) {
        this.remoteTranslogUploadBufferInterval = remoteTranslogUploadBufferInterval;
    }

    public void setRemoteTranslogKeepExtraGen(int extraGen) {
        this.remoteTranslogKeepExtraGen = extraGen;
    }

    /**
     * Returns this interval in which the shards of this index are asynchronously refreshed. {@code -1} means async refresh is disabled.
     */
    public TimeValue getRefreshInterval() {
        return refreshInterval;
    }

    /**
     * Returns the transaction log threshold size when to forcefully flush the index and clear the transaction log.
     */
    public ByteSizeValue getFlushThresholdSize() {
        return flushThresholdSize;
    }

    /**
     * Returns the merge threshold size when to forcefully flush the index and free resources.
     */
    public ByteSizeValue getFlushAfterMergeThresholdSize() {
        return flushAfterMergeThresholdSize;
    }

    /**
     * Returns the transaction log retention size which controls how much of the translog is kept around to allow for ops based recoveries
     */
    public ByteSizeValue getTranslogRetentionSize() {
        assert shouldDisableTranslogRetention(settings) == false || translogRetentionSize.getBytes() == -1L : translogRetentionSize;
        return translogRetentionSize;
    }

    /**
     * Returns the transaction log retention age which controls the maximum age (time from creation) that translog files will be kept
     * around
     */
    public TimeValue getTranslogRetentionAge() {
        assert shouldDisableTranslogRetention(settings) == false || translogRetentionAge.millis() == -1L : translogRetentionSize;
        return translogRetentionAge;
    }

    /**
     * Returns the maximum number of translog files that that no longer required for persistence should be kept for peer recovery
     * when soft-deletes is disabled.
     */
    public int getTranslogRetentionTotalFiles() {
        return INDEX_TRANSLOG_RETENTION_TOTAL_FILES_SETTING.get(getSettings());
    }

    private static boolean shouldDisableTranslogRetention(Settings settings) {
        return INDEX_SOFT_DELETES_SETTING.get(settings)
            && IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings).onOrAfter(LegacyESVersion.V_7_4_0);
    }

    /**
     * Returns the generation threshold size. As sequence numbers can cause multiple generations to
     * be preserved for rollback purposes, we want to keep the size of individual generations from
     * growing too large to avoid excessive disk space consumption. Therefore, the translog is
     * automatically rolled to a new generation when the current generation exceeds this generation
     * threshold size.
     *
     * @return the generation threshold size
     */
    public ByteSizeValue getGenerationThresholdSize() {
        return generationThresholdSize;
    }

    /**
     * Returns the {@link MergeSchedulerConfig}
     */
    public MergeSchedulerConfig getMergeSchedulerConfig() {
        return mergeSchedulerConfig;
    }

    /**
     * Returns the max result window for search requests, describing the maximum value of from + size on a query.
     */
    public int getMaxResultWindow() {
        return this.maxResultWindow;
    }

    private void setMaxResultWindow(int maxResultWindow) {
        this.maxResultWindow = maxResultWindow;
    }

    /**
     * Returns the max result window for an individual inner hit definition or top hits aggregation.
     */
    public int getMaxInnerResultWindow() {
        return maxInnerResultWindow;
    }

    private void setMaxInnerResultWindow(int maxInnerResultWindow) {
        this.maxInnerResultWindow = maxInnerResultWindow;
    }

    /**
     * Returns the max number of filters in adjacency_matrix aggregation search requests
     * @deprecated This setting will be removed in 8.0
     */
    @Deprecated
    public int getMaxAdjacencyMatrixFilters() {
        return this.maxAdjacencyMatrixFilters;
    }

    /**
     * @param maxAdjacencyFilters the max number of filters in adjacency_matrix aggregation search requests
     * @deprecated This setting will be removed in 8.0
     */
    @Deprecated
    private void setMaxAdjacencyMatrixFilters(int maxAdjacencyFilters) {
        this.maxAdjacencyMatrixFilters = maxAdjacencyFilters;
    }

    /**
     * Returns the maximum rescore window for search requests.
     */
    public int getMaxRescoreWindow() {
        return maxRescoreWindow;
    }

    private void setMaxRescoreWindow(int maxRescoreWindow) {
        this.maxRescoreWindow = maxRescoreWindow;
    }

    /**
     * Returns the maximum number of allowed docvalue_fields to retrieve in a search request
     */
    public int getMaxDocvalueFields() {
        return this.maxDocvalueFields;
    }

    private void setMaxDocvalueFields(int maxDocvalueFields) {
        this.maxDocvalueFields = maxDocvalueFields;
    }

    /**
     * Returns the maximum number of tokens that can be produced
     */
    public int getMaxTokenCount() {
        return maxTokenCount;
    }

    private void setMaxTokenCount(int maxTokenCount) {
        this.maxTokenCount = maxTokenCount;
    }

    /**
     * Returns the maximum allowed difference between max and min length of ngram
     */
    public int getMaxNgramDiff() {
        return this.maxNgramDiff;
    }

    private void setMaxNgramDiff(int maxNgramDiff) {
        this.maxNgramDiff = maxNgramDiff;
    }

    /**
     * Returns the maximum allowed difference between max and min shingle_size
     */
    public int getMaxShingleDiff() {
        return this.maxShingleDiff;
    }

    private void setMaxShingleDiff(int maxShingleDiff) {
        this.maxShingleDiff = maxShingleDiff;
    }

    /**
     *  Returns the maximum number of chars that will be analyzed in a highlight request
     */
    public int getHighlightMaxAnalyzedOffset() {
        return this.maxAnalyzedOffset;
    }

    private void setHighlightMaxAnalyzedOffset(int maxAnalyzedOffset) {
        this.maxAnalyzedOffset = maxAnalyzedOffset;
    }

    /**
     *  Returns the maximum number of terms that can be used in a Terms Query request
     */
    public int getMaxTermsCount() {
        return this.maxTermsCount;
    }

    private void setMaxTermsCount(int maxTermsCount) {
        this.maxTermsCount = maxTermsCount;
    }

    /**
     * @return max level of nested queries and documents
     */
    public int getMaxNestedQueryDepth() {
        return this.maxNestedQueryDepth;
    }

    private void setMaxNestedQueryDepth(int maxNestedQueryDepth) {
        this.maxNestedQueryDepth = maxNestedQueryDepth;
    }

    /**
     * Returns the maximum number of allowed script_fields to retrieve in a search request
     */
    public int getMaxScriptFields() {
        return this.maxScriptFields;
    }

    private void setMaxScriptFields(int maxScriptFields) {
        this.maxScriptFields = maxScriptFields;
    }

    /**
     * Returns the GC deletes cycle in milliseconds.
     */
    public long getGcDeletesInMillis() {
        return gcDeletesInMillis;
    }

    /**
     * Returns the merge policy that should be used for this index.
     * @param isTimeSeriesIndex true if index contains @timestamp field
     */
    public MergePolicy getMergePolicy(boolean isTimeSeriesIndex) {
        String indexScopedPolicy = scopedSettings.get(INDEX_MERGE_POLICY);
        MergePolicyProvider mergePolicyProvider = null;
        IndexMergePolicy indexMergePolicy = IndexMergePolicy.fromString(indexScopedPolicy);
        switch (indexMergePolicy) {
            case TIERED:
                mergePolicyProvider = tieredMergePolicyProvider;
                break;
            case LOG_BYTE_SIZE:
                mergePolicyProvider = logByteSizeMergePolicyProvider;
                break;
            case DEFAULT_POLICY:
                if (isTimeSeriesIndex) {
                    String nodeScopedTimeSeriesIndexPolicy = TIME_SERIES_INDEX_MERGE_POLICY.get(nodeSettings);
                    IndexMergePolicy nodeMergePolicy = IndexMergePolicy.fromString(nodeScopedTimeSeriesIndexPolicy);
                    switch (nodeMergePolicy) {
                        case TIERED:
                        case DEFAULT_POLICY:
                            mergePolicyProvider = tieredMergePolicyProvider;
                            break;
                        case LOG_BYTE_SIZE:
                            mergePolicyProvider = logByteSizeMergePolicyProvider;
                            break;
                    }
                } else {
                    mergePolicyProvider = tieredMergePolicyProvider;
                }
                break;
        }
        assert mergePolicyProvider != null : "should not happen as validation for invalid merge policy values "
            + "are part of setting definition";
        if (logger.isTraceEnabled()) {
            logger.trace("Index: " + this.index.getName() + ", Merge policy used: " + mergePolicyProvider);
        }
        return mergePolicyProvider.getMergePolicy();
    }

    public <T> T getValue(Setting<T> setting) {
        return scopedSettings.get(setting);
    }

    /**
     * The maximum number of refresh listeners allows on this shard.
     */
    public int getMaxRefreshListeners() {
        return maxRefreshListeners;
    }

    private void setMaxRefreshListeners(int maxRefreshListeners) {
        this.maxRefreshListeners = maxRefreshListeners;
    }

    /**
     * The maximum number of slices allowed in a scroll request.
     */
    public int getMaxSlicesPerScroll() {
        return maxSlicesPerScroll;
    }

    private void setMaxSlicesPerScroll(int value) {
        this.maxSlicesPerScroll = value;
    }

    /**
     * The maximum number of slices allowed in a PIT request.
     */
    public int getMaxSlicesPerPit() {
        return maxSlicesPerPit;
    }

    private void setMaxSlicesPerPit(int value) {
        this.maxSlicesPerPit = value;
    }

    /**
     * The maximum length of regex string allowed in a regexp query.
     */
    public int getMaxRegexLength() {
        return maxRegexLength;
    }

    private void setMaxRegexLength(int maxRegexLength) {
        this.maxRegexLength = maxRegexLength;
    }

    /**
     * Returns the index sort config that should be used for this index.
     */
    public IndexSortConfig getIndexSortConfig() {
        return indexSortConfig;
    }

    public IndexScopedSettings getScopedSettings() {
        return scopedSettings;
    }

    /**
     * Returns true iff the refresh setting exists or in other words is explicitly set.
     */
    public boolean isExplicitRefresh() {
        return INDEX_REFRESH_INTERVAL_SETTING.exists(settings);
    }

    /**
     * Returns the time that an index shard becomes search idle unless it's accessed in between
     */
    public TimeValue getSearchIdleAfter() {
        return searchIdleAfter;
    }

    public String getDefaultPipeline() {
        return defaultPipeline;
    }

    public void setDefaultPipeline(String defaultPipeline) {
        this.defaultPipeline = defaultPipeline;
    }

    public String getRequiredPipeline() {
        return requiredPipeline;
    }

    public void setRequiredPipeline(final String requiredPipeline) {
        this.requiredPipeline = requiredPipeline;
    }

    /**
     * Returns <code>true</code> if soft-delete is enabled.
     */
    public boolean isSoftDeleteEnabled() {
        return softDeleteEnabled;
    }

    private void setSoftDeleteRetentionOperations(long ops) {
        this.softDeleteRetentionOperations = ops;
    }

    /**
     * Returns the number of extra operations (i.e. soft-deleted documents) to be kept for recoveries and history purpose.
     */
    public long getSoftDeleteRetentionOperations() {
        return this.softDeleteRetentionOperations;
    }

    /**
     * Returns true if the this index should be searched throttled ie. using the
     * {@link org.opensearch.threadpool.ThreadPool.Names#SEARCH_THROTTLED} thread-pool
     */
    public boolean isSearchThrottled() {
        return searchThrottled;
    }

    private void setSearchThrottled(boolean searchThrottled) {
        this.searchThrottled = searchThrottled;
    }

    /**
     * Returns true if unreferenced files should be cleaned up on merge failure for this index.
     *
     */
    public boolean shouldCleanupUnreferencedFiles() {
        return shouldCleanupUnreferencedFiles;
    }

    private void setShouldCleanupUnreferencedFiles(boolean shouldCleanupUnreferencedFiles) {
        this.shouldCleanupUnreferencedFiles = shouldCleanupUnreferencedFiles;
    }

    public long getMappingNestedFieldsLimit() {
        return mappingNestedFieldsLimit;
    }

    private void setMappingNestedFieldsLimit(long value) {
        this.mappingNestedFieldsLimit = value;
    }

    public long getMappingNestedDocsLimit() {
        return mappingNestedDocsLimit;
    }

    private void setMappingNestedDocsLimit(long value) {
        this.mappingNestedDocsLimit = value;
    }

    public long getMappingTotalFieldsLimit() {
        return mappingTotalFieldsLimit;
    }

    private void setMappingTotalFieldsLimit(long value) {
        this.mappingTotalFieldsLimit = value;
    }

    public long getMappingDepthLimit() {
        return mappingDepthLimit;
    }

    private void setMappingDepthLimit(long value) {
        this.mappingDepthLimit = value;
    }

    public long getMappingFieldNameLengthLimit() {
        return mappingFieldNameLengthLimit;
    }

    private void setMappingFieldNameLengthLimit(long value) {
        this.mappingFieldNameLengthLimit = value;
    }

    private void setMaxFullFlushMergeWaitTime(TimeValue timeValue) {
        this.maxFullFlushMergeWaitTime = timeValue;
    }

    private void setMergeOnFlushEnabled(boolean enabled) {
        this.mergeOnFlushEnabled = enabled;
    }

    public TimeValue getMaxFullFlushMergeWaitTime() {
        return this.maxFullFlushMergeWaitTime;
    }

    public boolean isMergeOnFlushEnabled() {
        return mergeOnFlushEnabled;
    }

    private void setMergeOnFlushPolicy(String policy) {
        if (Strings.isEmpty(policy) || DEFAULT_POLICY.equalsIgnoreCase(policy)) {
            mergeOnFlushPolicy = null;
        } else if (MERGE_ON_FLUSH_MERGE_POLICY.equalsIgnoreCase(policy)) {
            this.mergeOnFlushPolicy = MergeOnFlushMergePolicy::new;
        } else {
            throw new IllegalArgumentException(
                "The "
                    + IndexSettings.INDEX_MERGE_ON_FLUSH_POLICY.getKey()
                    + " has unsupported policy specified: "
                    + policy
                    + ". Please use one of: "
                    + DEFAULT_POLICY
                    + ", "
                    + MERGE_ON_FLUSH_MERGE_POLICY
            );
        }
    }

    public boolean isCheckPendingFlushEnabled() {
        return checkPendingFlushEnabled;
    }

    public Optional<UnaryOperator<MergePolicy>> getMergeOnFlushPolicy() {
        return Optional.ofNullable(mergeOnFlushPolicy);
    }

    public String getDefaultSearchPipeline() {
        return defaultSearchPipeline;
    }

    public void setDefaultSearchPipeline(String defaultSearchPipeline) {
        this.defaultSearchPipeline = defaultSearchPipeline;
    }

    /**
     * Returns true if we need to maintain backward compatibility for index sorted indices created prior to version 2.7
     * @return boolean
     */
    public boolean shouldWidenIndexSortType() {
        return this.widenIndexSortType;
    }

    public boolean isEnableFuzzySetForDocId() {
        return enableFuzzySetForDocId;
    }

    public void setEnableFuzzySetForDocId(boolean enableFuzzySetForDocId) {
        this.enableFuzzySetForDocId = enableFuzzySetForDocId;
    }

    public double getDocIdFuzzySetFalsePositiveProbability() {
        return docIdFuzzySetFalsePositiveProbability;
    }

    public void setDocIdFuzzySetFalsePositiveProbability(double docIdFuzzySetFalsePositiveProbability) {
        this.docIdFuzzySetFalsePositiveProbability = docIdFuzzySetFalsePositiveProbability;
    }

    public RemoteStorePathType getRemoteStorePathType() {
        Map<String, String> remoteCustomData = indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY);
        return remoteCustomData != null && remoteCustomData.containsKey(RemoteStorePathType.NAME)
            ? RemoteStorePathType.parseString(remoteCustomData.get(RemoteStorePathType.NAME))
            : RemoteStorePathType.FIXED;
    }
}
