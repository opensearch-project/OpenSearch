/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.SearchSlowLog;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for {@link AnalyticsSearchService#lowestEnabledSlowLogThreshold(IndexSettings)}.
 *
 * <p>This is the gate that decides whether a data node spends CPU extracting DataFusion metrics
 * for the fragment slow log. It must return the lowest ENABLED threshold across warn/info/debug/trace
 * — the four are independent settings, so we cannot assume trace is always the lowest (or enabled).
 */
public class AnalyticsSearchServiceSlowLogThresholdTests extends OpenSearchTestCase {

    /** Builds IndexSettings with the given per-level query slow-log thresholds. Null = leave at default (-1, disabled). */
    private IndexSettings settingsWith(TimeValue warn, TimeValue info, TimeValue debug, TimeValue trace) {
        Settings.Builder b = Settings.builder()
            .put(IndexMetadata.SETTING_INDEX_UUID, "test-uuid")
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        if (warn != null) b.put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING.getKey(), warn);
        if (info != null) b.put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING.getKey(), info);
        if (debug != null) b.put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING.getKey(), debug);
        if (trace != null) b.put(SearchSlowLog.INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING.getKey(), trace);
        IndexMetadata metadata = IndexMetadata.builder("test-index").settings(b.build()).build();
        return new IndexSettings(metadata, Settings.EMPTY);
    }

    public void testAllDisabledReturnsMinusOne() {
        // No thresholds set — every level defaults to -1 (disabled).
        IndexSettings settings = settingsWith(null, null, null, null);
        assertEquals(-1L, AnalyticsSearchService.lowestEnabledSlowLogThreshold(settings));
    }

    public void testOnlyWarnEnabledReturnsWarn() {
        // Regression for the original bug: only warn is set. Must NOT return -1 just because
        // trace is disabled — must return warn's value so metrics are collected for warn-slow queries.
        IndexSettings settings = settingsWith(TimeValue.timeValueSeconds(5), null, null, null);
        assertEquals(TimeValue.timeValueSeconds(5).nanos(), AnalyticsSearchService.lowestEnabledSlowLogThreshold(settings));
    }

    public void testAllEnabledReturnsLowest() {
        // warn=5s, info=1s, debug=500ms, trace=100ms → lowest enabled is trace (100ms).
        IndexSettings settings = settingsWith(
            TimeValue.timeValueSeconds(5),
            TimeValue.timeValueSeconds(1),
            TimeValue.timeValueMillis(500),
            TimeValue.timeValueMillis(100)
        );
        assertEquals(TimeValue.timeValueMillis(100).nanos(), AnalyticsSearchService.lowestEnabledSlowLogThreshold(settings));
    }

    public void testMixedEnabledReturnsLowestEnabled() {
        // warn=5s (enabled), info disabled, debug=200ms (enabled), trace disabled.
        // Lowest enabled is debug (200ms) — proves we ignore disabled levels regardless of position.
        IndexSettings settings = settingsWith(TimeValue.timeValueSeconds(5), null, TimeValue.timeValueMillis(200), null);
        assertEquals(TimeValue.timeValueMillis(200).nanos(), AnalyticsSearchService.lowestEnabledSlowLogThreshold(settings));
    }

    public void testZeroThresholdIsEnabled() {
        // A threshold of exactly 0 is enabled (logs everything). Must be returned, not treated as disabled.
        IndexSettings settings = settingsWith(null, null, null, TimeValue.timeValueMillis(0));
        assertEquals(0L, AnalyticsSearchService.lowestEnabledSlowLogThreshold(settings));
    }
}
