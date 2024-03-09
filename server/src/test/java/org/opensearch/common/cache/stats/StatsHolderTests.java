/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BytesStreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.getUsedDimensionValues;
import static org.opensearch.common.cache.stats.MultiDimensionCacheStatsTests.populateStats;

public class StatsHolderTests extends OpenSearchTestCase {
    // Since StatsHolder does not expose getter methods for aggregating stats,
    // we test the incrementing functionality and the different tracking modes in combination with MultiDimensionCacheStats,
    // in MultiDimensionCacheStatsTests.java.
    public void testSerialization() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2", "dim3");
        Set<Set<String>> specificCombinations = Set.of(Set.of("dim1"), Set.of("dim2", "dim3"));
        StatsHolder statsHolder = new StatsHolder(
            dimensionNames,
            getSettings(10_000),
            StatsHolder.TrackingMode.SPECIFIC_COMBINATIONS,
            specificCombinations
        );
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        populateStats(statsHolder, usedDimensionValues, 100, 10);

        BytesStreamOutput os = new BytesStreamOutput();
        statsHolder.writeTo(os);
        BytesStreamInput is = new BytesStreamInput(BytesReference.toBytes(os.bytes()));
        StatsHolder deserialized = new StatsHolder(is);

        checkStatsHolderEquality(statsHolder, deserialized);
    }

    public void testKeyEquality() throws Exception {
        Set<CacheStatsDimension> dims1 = new HashSet<>();
        dims1.add(new CacheStatsDimension("a", "1"));
        dims1.add(new CacheStatsDimension("b", "2"));
        dims1.add(new CacheStatsDimension("c", "3"));
        StatsHolder.Key key1 = new StatsHolder.Key(dims1);

        List<CacheStatsDimension> dims2 = new ArrayList<>();
        dims2.add(new CacheStatsDimension("c", "3"));
        dims2.add(new CacheStatsDimension("a", "1"));
        dims2.add(new CacheStatsDimension("b", "2"));
        StatsHolder.Key key2 = new StatsHolder.Key(dims2);

        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    public void testReset() throws Exception {
        List<String> dimensionNames = List.of("dim1", "dim2");
        StatsHolder statsHolder = new StatsHolder(dimensionNames, getSettings(20_000), StatsHolder.TrackingMode.ALL_COMBINATIONS);
        Map<String, List<String>> usedDimensionValues = getUsedDimensionValues(statsHolder, 10);
        Map<Set<CacheStatsDimension>, CacheStatsResponse> expected = populateStats(statsHolder, usedDimensionValues, 100, 10);

        statsHolder.reset();

        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            CacheStatsResponse originalResponse = expected.get(dimSet);
            originalResponse.memorySize = new CounterMetric();
            originalResponse.entries = new CounterMetric();

            StatsHolder.Key key = new StatsHolder.Key(dimSet);
            CacheStatsResponse actual = statsHolder.getStatsMap().get(key);
            assertEquals(originalResponse, actual);
        }

        CacheStatsResponse expectedTotal = new CacheStatsResponse();
        for (Set<CacheStatsDimension> dimSet : expected.keySet()) {
            expectedTotal.add(expected.get(dimSet));
        }
        expectedTotal.memorySize = new CounterMetric();
        expectedTotal.entries = new CounterMetric();

        assertEquals(expectedTotal, statsHolder.getTotalStats());
    }

    static void checkStatsHolderEquality(StatsHolder statsHolder, StatsHolder deserialized) {
        assertEquals(statsHolder.getStatsMap(), deserialized.getStatsMap());
        assertEquals(statsHolder.getDimensionNames(), deserialized.getDimensionNames());
        assertEquals(statsHolder.totalStats, deserialized.totalStats);
        assertEquals(statsHolder.mode, deserialized.mode);
        assertEquals(statsHolder.getSpecificCombinations(), deserialized.getSpecificCombinations());
    }

    static Settings getSettings(int maxDimensionValues) {
        return Settings.builder().put(StatsHolder.MAX_DIMENSION_VALUES_SETTING.getKey(), maxDimensionValues).build();
    }
}
