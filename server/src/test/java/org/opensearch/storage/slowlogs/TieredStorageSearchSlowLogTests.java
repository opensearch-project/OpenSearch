/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.Version;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for TieredStorageSearchSlowLog
 */
public class TieredStorageSearchSlowLogTests extends OpenSearchTestCase {

    private TieredStorageSearchSlowLog slowLog;
    private IndexSettings indexSettings;
    private SearchContext searchContext;
    private SearchShardTask searchTask;
    private TieredStorageQueryMetricService metricService;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Create mock objects
        indexSettings = createMockIndexSettings();
        searchContext = mock(SearchContext.class);
        searchTask = mock(SearchShardTask.class);

        // Mock search context setup
        when(searchContext.getTask()).thenReturn(searchTask);
        SearchShardTarget shardTarget = new SearchShardTarget("testNode", mock(ShardId.class), null, null);
        when(searchContext.shardTarget()).thenReturn(shardTarget);
        when(searchContext.numberOfShards()).thenReturn(1);
        when(searchContext.searchType()).thenReturn(org.opensearch.action.search.SearchType.QUERY_THEN_FETCH);
        when(searchContext.request()).thenReturn(mock(ShardSearchRequest.class));
        when(searchContext.request().source()).thenReturn(null);

        // Mock search task - use string directly to avoid TaskId class issues
        when(searchTask.getParentTaskId()).thenReturn(TaskId.EMPTY_TASK_ID);

        // Create slow log instance
        slowLog = new TieredStorageSearchSlowLog(indexSettings);

        // Mock metric service
        metricService = mock(TieredStorageQueryMetricService.class);
    }

    private IndexSettings createMockIndexSettings() {
        Set<Setting<?>> settingSet = new HashSet<>(IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);
        settingSet.add(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING);
        settingSet.add(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING);

        Settings settings = Settings.builder()
            .put(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED.getKey(), true)
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), "1s")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), "500ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), "100ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), "10ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING.getKey(), "1s")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING.getKey(), "500ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING.getKey(), "100ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING.getKey(), "10ms")
            .put(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey(), "TRACE")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, "uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();

        IndexMetadata metadata = IndexMetadata.builder("Index").settings(settings).build();
        return new IndexSettings(metadata, settings, new IndexScopedSettings(settings, settingSet));
    }

    public void testConstructorInitializesSettings() {
        // Verify that constructor properly initializes all settings
        assertTrue(slowLog.getQueryWarnThreshold() > 0);
        assertTrue(slowLog.getQueryInfoThreshold() > 0);
        assertTrue(slowLog.getQueryDebugThreshold() > 0);
        assertTrue(slowLog.getQueryTraceThreshold() > 0);

        assertTrue(slowLog.getFetchWarnThreshold() > 0);
        assertTrue(slowLog.getFetchInfoThreshold() > 0);
        assertTrue(slowLog.getFetchDebugThreshold() > 0);
        assertTrue(slowLog.getFetchTraceThreshold() > 0);

        assertEquals(SlowLogLevel.TRACE, slowLog.getLevel());
    }

    public void testSetTieredStorageSlowlogEnabled() {
        // Test enabling/disabling slow log
        slowLog.setTieredStorageSlowlogEnabled(true);
        // No direct way to verify, but should not throw exception

        slowLog.setTieredStorageSlowlogEnabled(false);
        // No direct way to verify, but should not throw exception
    }

    public void testOnPreQueryPhase() {
        // onPreQueryPhase should not do anything as per the implementation
        // Just verify it doesn't throw exception
        slowLog.onPreQueryPhase(searchContext);
    }

    public void testOnPreSliceExecutionWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        // Should call setMetricCollector when enabled
        slowLog.onPreSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnPreSliceExecutionWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        // Should not call setMetricCollector when disabled
        slowLog.onPreSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnSliceExecutionWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        // Should call removeMetricCollector when enabled
        slowLog.onSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnSliceExecutionWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        // Should not call removeMetricCollector when disabled
        slowLog.onSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnFailedSliceExecutionWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        // Should call removeMetricCollector when enabled
        slowLog.onFailedSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnFailedSliceExecutionWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        // Should not call removeMetricCollector when disabled
        slowLog.onFailedSliceExecution(searchContext);

        // Verify no exception is thrown
    }

    public void testOnQueryPhaseWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        // Test with time above trace threshold
        long tookInNanos = TimeUnit.MILLISECONDS.toNanos(50); // Above 10ms trace threshold

        slowLog.onQueryPhase(searchContext, tookInNanos);

        // Verify no exception is thrown
    }

    public void testOnQueryPhaseWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        long tookInNanos = TimeUnit.MILLISECONDS.toNanos(50);

        slowLog.onQueryPhase(searchContext, tookInNanos);

        // Verify no exception is thrown
    }

    public void testOnFailedQueryPhaseWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        slowLog.onFailedQueryPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testOnFailedQueryPhaseWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        slowLog.onFailedQueryPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testOnPreFetchPhaseWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        slowLog.onPreFetchPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testOnPreFetchPhaseWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        slowLog.onPreFetchPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testOnFetchPhaseWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        slowLog.onPreFetchPhase(searchContext);

        long tookInNanos = TimeUnit.MILLISECONDS.toNanos(50);

        slowLog.onFetchPhase(searchContext, tookInNanos);

        // Verify no exception is thrown
    }

    public void testOnFetchPhaseWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        long tookInNanos = TimeUnit.MILLISECONDS.toNanos(50);

        slowLog.onFetchPhase(searchContext, tookInNanos);

        // Verify no exception is thrown
    }

    public void testOnFailedFetchPhaseWhenEnabled() {
        // Enable slow log
        slowLog.setTieredStorageSlowlogEnabled(true);

        slowLog.onFailedFetchPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testOnFailedFetchPhaseWhenDisabled() {
        // Disable slow log
        slowLog.setTieredStorageSlowlogEnabled(false);

        slowLog.onFailedFetchPhase(searchContext);

        // Verify no exception is thrown
    }

    public void testThresholdGetters() {
        // Test all threshold getters return expected values
        assertTrue(slowLog.getQueryWarnThreshold() >= 0);
        assertTrue(slowLog.getQueryInfoThreshold() >= 0);
        assertTrue(slowLog.getQueryDebugThreshold() >= 0);
        assertTrue(slowLog.getQueryTraceThreshold() >= 0);

        assertTrue(slowLog.getFetchWarnThreshold() >= 0);
        assertTrue(slowLog.getFetchInfoThreshold() >= 0);
        assertTrue(slowLog.getFetchDebugThreshold() >= 0);
        assertTrue(slowLog.getFetchTraceThreshold() >= 0);
    }

    public void testSlowLogSettings() {
        // Test that all settings are properly defined
        assertNotNull(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING);

        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING);
        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING);

        assertNotNull(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL);
    }

    public void testSlowLogSettingsMap() {
        // Test that settings map contains all expected settings
        assertFalse(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP.isEmpty());
        assertTrue(
            TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP.containsKey(
                TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".enabled"
            )
        );
        assertTrue(
            TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP.containsKey(
                TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn"
            )
        );
        assertTrue(
            TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS_MAP.containsKey(
                TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX + ".level"
            )
        );
    }

    public void testSlowLogSettingsSet() {
        // Test that settings set contains all expected settings
        assertFalse(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS.isEmpty());
        assertEquals(10, TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_SETTINGS.size());
    }

    public void testTieredStorageSlowLogPrinterConstructor() {
        TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter printer = new TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter(
            searchContext,
            TimeUnit.MILLISECONDS.toNanos(100),
            java.util.Collections.emptyList()
        );

        assertNotNull(printer);
    }

    public void testTieredStorageSlowLogPrinterToString() {
        TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter printer = new TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter(
            searchContext,
            TimeUnit.MILLISECONDS.toNanos(100),
            java.util.Collections.emptyList()
        );

        String result = printer.toString();
        assertNotNull(result);
        assertTrue(result.length() > 0);

        // Should contain expected JSON structure
        assertTrue(result.contains("warm_stats"));
        assertTrue(result.contains("took"));
        assertTrue(result.contains("took_millis"));
        assertTrue(result.contains("stats"));
        assertTrue(result.contains("search_type"));
        assertTrue(result.contains("total_shards"));
    }

    public void testTieredStorageSlowLogPrinterWithMetrics() {
        // Create a metric collector
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-0");
        metric.recordFileAccess("file1.block_0_1", true);
        metric.recordPrefetch("file2", 1);
        metric.recordEndTime();

        java.util.List<TieredStoragePerQueryMetric> metrics = java.util.Arrays.asList(metric);

        TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter printer = new TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter(
            searchContext,
            TimeUnit.MILLISECONDS.toNanos(100),
            metrics
        );

        String result = printer.toString();
        assertNotNull(result);
        assertTrue(result.length() > 0);

        // Should contain metric data in warm_stats
        assertTrue(result.contains("warm_stats"));
        assertTrue(result.contains("parentTask"));
        assertTrue(result.contains("task-1"));
    }

    public void testSearchContextWithNullTask() {
        // Test behavior when search task is null
        when(searchContext.getTask()).thenReturn(null);

        slowLog.setTieredStorageSlowlogEnabled(true);

        // Should handle null task gracefully
        slowLog.onPreSliceExecution(searchContext);
        slowLog.onSliceExecution(searchContext);
        slowLog.onPreFetchPhase(searchContext);

        // Verify no exceptions are thrown
    }

    public void testDifferentLogLevels() {
        slowLog.setTieredStorageSlowlogEnabled(true);

        // Test different time thresholds for different log levels

        // Test TRACE level (10ms threshold)
        long traceTime = TimeUnit.MILLISECONDS.toNanos(15);
        slowLog.onQueryPhase(searchContext, traceTime);

        // Test DEBUG level (100ms threshold)
        long debugTime = TimeUnit.MILLISECONDS.toNanos(150);
        slowLog.onQueryPhase(searchContext, debugTime);

        // Test INFO level (500ms threshold)
        long infoTime = TimeUnit.MILLISECONDS.toNanos(600);
        slowLog.onQueryPhase(searchContext, infoTime);

        // Test WARN level (1s threshold)
        long warnTime = TimeUnit.MILLISECONDS.toNanos(1100);
        slowLog.onQueryPhase(searchContext, warnTime);

        // All should complete without exceptions
    }

    public void testFetchPhaseLogging() {
        slowLog.setTieredStorageSlowlogEnabled(true);

        slowLog.onPreFetchPhase(searchContext);
        // Test fetch phase with different thresholds
        long fetchTime = TimeUnit.MILLISECONDS.toNanos(600); // Above info threshold

        slowLog.onFetchPhase(searchContext, fetchTime);

        // Should complete without exceptions
    }

    public void testSettingsPrefix() {
        // Verify the settings prefix is correct
        String expectedPrefix = TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_PREFIX;

        assertTrue(TieredStorageSearchSlowLog.TIERED_STORAGE_SEARCH_SLOWLOG_ENABLED.getKey().startsWith(expectedPrefix));
        assertTrue(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey().startsWith(expectedPrefix));
        assertTrue(TieredStorageSearchSlowLog.INDEX_SEARCH_SLOWLOG_LEVEL.getKey().startsWith(expectedPrefix));
    }

    public void testTimeValueConversion() {
        // Test that time values are properly converted to nanoseconds
        assertTrue(slowLog.getQueryWarnThreshold() > 0);
        assertTrue(slowLog.getQueryInfoThreshold() > 0);
        assertTrue(slowLog.getQueryDebugThreshold() > 0);
        assertTrue(slowLog.getQueryTraceThreshold() > 0);

        // Verify hierarchy: warn > info > debug > trace
        assertTrue(slowLog.getQueryWarnThreshold() >= slowLog.getQueryInfoThreshold());
        assertTrue(slowLog.getQueryInfoThreshold() >= slowLog.getQueryDebugThreshold());
        assertTrue(slowLog.getQueryDebugThreshold() >= slowLog.getQueryTraceThreshold());
    }

    public void testSlowLogPrinterWithNullSource() {
        // Test printer when search request source is null
        when(searchContext.request().source()).thenReturn(null);

        TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter printer = new TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter(
            searchContext,
            TimeUnit.MILLISECONDS.toNanos(100),
            java.util.Collections.emptyList()
        );

        String result = printer.toString();
        assertNotNull(result);
        assertTrue(result.contains("\"source\":null"));
    }

    public void testSlowLogPrinterWithGroupStats() {
        // Mock group stats - use List<String> to match expected type
        java.util.List<String> groupStats = java.util.Arrays.asList("stat1", "stat2");
        when(searchContext.groupStats()).thenReturn(groupStats);

        TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter printer = new TieredStorageSearchSlowLog.TieredStorageSlowLogPrinter(
            searchContext,
            TimeUnit.MILLISECONDS.toNanos(100),
            java.util.Collections.emptyList()
        );

        String result = printer.toString();
        assertNotNull(result);
        assertTrue(result.contains("stats"));
    }
}
