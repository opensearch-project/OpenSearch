/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.profile.query;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.store.Directory;
import org.opensearch.search.profile.ContextualProfileBreakdown;
import org.opensearch.search.profile.ProfileMetric;
import org.opensearch.search.profile.ProfileMetricTests;
import org.opensearch.search.profile.ProfileMetricUtil;
import org.opensearch.search.profile.Timer;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.search.profile.Timer.TIMING_TYPE_COUNT_SUFFIX;
import static org.opensearch.search.profile.Timer.TIMING_TYPE_START_TIME_SUFFIX;
import static org.opensearch.search.profile.query.ConcurrentQueryProfileBreakdown.MIN_PREFIX;
import static org.opensearch.search.profile.query.ConcurrentQueryProfileBreakdown.SLICE_END_TIME_SUFFIX;
import static org.opensearch.search.profile.query.ConcurrentQueryProfileBreakdown.SLICE_START_TIME_SUFFIX;
import static org.mockito.Mockito.mock;

public class ConcurrentQueryProfileBreakdownTests extends OpenSearchTestCase {
    private ConcurrentQueryProfileBreakdown testQueryProfileBreakdown;
    private ConcurrentQueryProfileBreakdown testQueryProfileBreakdownCombined;
    private Timer createWeightTimer;

    @Before
    public void setup() {
        testQueryProfileBreakdown = new ConcurrentQueryProfileBreakdown(ProfileMetricUtil.getDefaultQueryProfileMetrics());
        Collection<Supplier<ProfileMetric>> combinedMetrics = ProfileMetricUtil.getDefaultQueryProfileMetrics();
        combinedMetrics.addAll(ProfileMetricTests.getNonTimingMetric());
        testQueryProfileBreakdownCombined = new ConcurrentQueryProfileBreakdown(combinedMetrics);
        createWeightTimer = testQueryProfileBreakdown.getTimer(QueryTimingType.CREATE_WEIGHT);
        try {
            createWeightTimer.start();
            Thread.sleep(10);
        } catch (InterruptedException ex) {
            // ignore
        } finally {
            createWeightTimer.stop();
        }
    }

    public void testBreakdownMapWithNoLeafContext() throws Exception {
        final Map<String, Long> queryBreakDownMap = testQueryProfileBreakdown.toBreakdownMap();
        assertFalse(queryBreakDownMap == null || queryBreakDownMap.isEmpty());
        assertEquals(66, queryBreakDownMap.size());
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingTypeKey = queryTimingType.toString();
            String timingTypeCountKey = queryTimingType + TIMING_TYPE_COUNT_SUFFIX;

            if (queryTimingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                final long createWeightTime = queryBreakDownMap.get(timingTypeKey);
                assertTrue(createWeightTime > 0);
                assertEquals(1, (long) queryBreakDownMap.get(timingTypeCountKey));
                // verify there is no min/max/avg for weight type stats
                assertFalse(
                    queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey)
                );
                // verify total/min/max/avg node time is same as weight time
                assertEquals(createWeightTime, testQueryProfileBreakdown.toNodeTime());
                assertEquals(0, testQueryProfileBreakdown.getMaxSliceNodeTime());
                assertEquals(0, testQueryProfileBreakdown.getMinSliceNodeTime());
                assertEquals(0, testQueryProfileBreakdown.getAvgSliceNodeTime());
                continue;
            }
            assertEquals(0, (long) queryBreakDownMap.get(timingTypeKey));
            assertEquals(0, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey));
            assertEquals(0, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey));
            assertEquals(0, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeKey));
            assertEquals(0, (long) queryBreakDownMap.get(timingTypeCountKey));
            assertEquals(0, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey));
            assertEquals(0, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeCountKey));
            assertEquals(0, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey));
        }
    }

    public void testBuildSliceLevelBreakdownWithSingleSlice() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(1);
        final Directory directory = directoryReader.directory();
        final LeafReaderContext sliceLeaf = directoryReader.leaves().get(0);
        final Collector sliceCollector = mock(Collector.class);
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap = getLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final ContextualProfileBreakdown leafProfileBreakdown = new TestQueryProfileBreakdown(leafProfileBreakdownMap);
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector, sliceLeaf);
        testQueryProfileBreakdown.getContexts().put(sliceLeaf, leafProfileBreakdown);
        final Map<Collector, Map<String, Long>> sliceBreakdownMap = testQueryProfileBreakdown.buildSliceLevelBreakdown();
        assertFalse(sliceBreakdownMap == null || sliceBreakdownMap.isEmpty());
        assertEquals(1, sliceBreakdownMap.size());
        assertTrue(sliceBreakdownMap.containsKey(sliceCollector));

        final Map<String, Long> sliceBreakdown = sliceBreakdownMap.entrySet().iterator().next().getValue();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            String timingTypeKey = timingType.toString();
            String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;

            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                // there should be no entry for create weight at slice level breakdown map
                assertNull(sliceBreakdown.get(timingTypeKey));
                assertNull(sliceBreakdown.get(timingTypeCountKey));
                continue;
            }

            // for other timing type we will have all the value and will be same as leaf breakdown as there is single slice and single leaf
            assertEquals(leafProfileBreakdownMap.get(timingTypeKey), sliceBreakdown.get(timingTypeKey));
            assertEquals(leafProfileBreakdownMap.get(timingTypeCountKey), sliceBreakdown.get(timingTypeCountKey));
            assertEquals(
                leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX),
                sliceBreakdown.get(timingTypeKey + SLICE_START_TIME_SUFFIX)
            );
            assertEquals(
                leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX) + leafProfileBreakdownMap.get(timingTypeKey),
                (long) sliceBreakdown.get(timingTypeKey + SLICE_END_TIME_SUFFIX)
            );
        }
        assertEquals(10, testQueryProfileBreakdown.getMaxSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdown.getMinSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdown.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    public void testBuildSliceLevelBreakdownWithMultipleSlices() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(2);
        final Directory directory = directoryReader.directory();
        final Collector sliceCollector_1 = mock(Collector.class);
        final Collector sliceCollector_2 = mock(Collector.class);
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap_1 = getLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final Map<String, Long> leafProfileBreakdownMap_2 = getLeafBreakdownMap(createWeightEndTime + 40, 10, 1);
        final ContextualProfileBreakdown leafProfileBreakdown_1 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_1);
        final ContextualProfileBreakdown leafProfileBreakdown_2 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_2);
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_1, directoryReader.leaves().get(0));
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_2, directoryReader.leaves().get(1));
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(0), leafProfileBreakdown_1);
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(1), leafProfileBreakdown_2);
        final Map<Collector, Map<String, Long>> sliceBreakdownMap = testQueryProfileBreakdown.buildSliceLevelBreakdown();
        assertFalse(sliceBreakdownMap == null || sliceBreakdownMap.isEmpty());
        assertEquals(2, sliceBreakdownMap.size());

        for (Map.Entry<Collector, Map<String, Long>> sliceBreakdowns : sliceBreakdownMap.entrySet()) {
            Map<String, Long> sliceBreakdown = sliceBreakdowns.getValue();
            Map<String, Long> leafProfileBreakdownMap;
            if (sliceBreakdowns.getKey().equals(sliceCollector_1)) {
                leafProfileBreakdownMap = leafProfileBreakdownMap_1;
            } else {
                leafProfileBreakdownMap = leafProfileBreakdownMap_2;
            }
            for (QueryTimingType timingType : QueryTimingType.values()) {
                String timingTypeKey = timingType.toString();
                String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;

                if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                    // there should be no entry for create weight at slice level breakdown map
                    assertNull(sliceBreakdown.get(timingTypeKey));
                    assertNull(sliceBreakdown.get(timingTypeCountKey));
                    continue;
                }

                // for other timing type we will have all the value and will be same as leaf breakdown as there is single slice and single
                // leaf
                assertEquals(leafProfileBreakdownMap.get(timingTypeKey), sliceBreakdown.get(timingTypeKey));
                assertEquals(leafProfileBreakdownMap.get(timingTypeCountKey), sliceBreakdown.get(timingTypeCountKey));
                assertEquals(
                    leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX),
                    sliceBreakdown.get(timingTypeKey + SLICE_START_TIME_SUFFIX)
                );
                assertEquals(
                    leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX) + leafProfileBreakdownMap.get(timingTypeKey),
                    (long) sliceBreakdown.get(timingTypeKey + SLICE_END_TIME_SUFFIX)
                );
            }
        }

        assertEquals(10, testQueryProfileBreakdown.getMaxSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdown.getMinSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdown.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    public void testBreakDownMapWithMultipleSlices() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(2);
        final Directory directory = directoryReader.directory();
        final Collector sliceCollector_1 = mock(Collector.class);
        final Collector sliceCollector_2 = mock(Collector.class);
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap_1 = getLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final Map<String, Long> leafProfileBreakdownMap_2 = getLeafBreakdownMap(createWeightEndTime + 40, 20, 1);
        final ContextualProfileBreakdown leafProfileBreakdown_1 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_1);
        final ContextualProfileBreakdown leafProfileBreakdown_2 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_2);
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_1, directoryReader.leaves().get(0));
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_2, directoryReader.leaves().get(1));
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(0), leafProfileBreakdown_1);
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(1), leafProfileBreakdown_2);

        Map<String, Long> queryBreakDownMap = testQueryProfileBreakdown.toBreakdownMap();
        assertFalse(queryBreakDownMap == null || queryBreakDownMap.isEmpty());
        assertEquals(66, queryBreakDownMap.size());

        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingTypeKey = queryTimingType.toString();
            String timingTypeCountKey = queryTimingType + TIMING_TYPE_COUNT_SUFFIX;

            if (queryTimingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                final long createWeightTime = queryBreakDownMap.get(timingTypeKey);
                assertEquals(createWeightTimer.getApproximateTiming(), createWeightTime);
                assertEquals(1, (long) queryBreakDownMap.get(timingTypeCountKey));
                // verify there is no min/max/avg for weight type stats
                assertFalse(
                    queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey)
                );
                continue;
            }
            assertEquals(50, (long) queryBreakDownMap.get(timingTypeKey));
            assertEquals(20, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey));
            assertEquals(15, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey));
            assertEquals(10, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeKey));
            assertEquals(2, (long) queryBreakDownMap.get(timingTypeCountKey));
            assertEquals(1, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey));
            assertEquals(1, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeCountKey));
            assertEquals(1, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey));
        }

        assertEquals(20, testQueryProfileBreakdown.getMaxSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdown.getMinSliceNodeTime());
        assertEquals(15, testQueryProfileBreakdown.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    public void testBreakDownMapWithMultipleSlicesAndOneSliceWithNoLeafContext() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(2);
        final Directory directory = directoryReader.directory();
        final Collector sliceCollector_1 = mock(Collector.class);
        final Collector sliceCollector_2 = mock(Collector.class);
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap_1 = getLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final ContextualProfileBreakdown leafProfileBreakdown_1 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_1);
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_1, directoryReader.leaves().get(0));
        testQueryProfileBreakdown.associateCollectorToLeaves(sliceCollector_2, directoryReader.leaves().get(1));
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(0), leafProfileBreakdown_1);
        // leaf2 profile breakdown is not present in contexts map

        Map<String, Long> queryBreakDownMap = testQueryProfileBreakdown.toBreakdownMap();
        assertFalse(queryBreakDownMap == null || queryBreakDownMap.isEmpty());
        assertEquals(66, queryBreakDownMap.size());

        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingTypeKey = queryTimingType.toString();
            String timingTypeCountKey = queryTimingType + TIMING_TYPE_COUNT_SUFFIX;

            if (queryTimingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                final long createWeightTime = queryBreakDownMap.get(timingTypeKey);
                assertEquals(createWeightTimer.getApproximateTiming(), createWeightTime);
                assertEquals(1, (long) queryBreakDownMap.get(timingTypeCountKey));
                // verify there is no min/max/avg for weight type stats
                assertFalse(
                    queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey)
                );
                continue;
            }
            assertEquals(10, (long) queryBreakDownMap.get(timingTypeKey));
            assertEquals(10, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey));
            assertEquals(5, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey));
            assertEquals(0, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeKey));
            assertEquals(1, (long) queryBreakDownMap.get(timingTypeCountKey));
            assertEquals(1, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey));
            // min of 0 means one of the slice didn't worked on any leaf context
            assertEquals(0, (long) queryBreakDownMap.get(MIN_PREFIX + timingTypeCountKey));
            assertEquals(0, (long) queryBreakDownMap.get(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey));
        }

        assertEquals(10, testQueryProfileBreakdown.getMaxSliceNodeTime());
        assertEquals(0, testQueryProfileBreakdown.getMinSliceNodeTime());
        assertEquals(5, testQueryProfileBreakdown.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    public void testOneLeafContextWithEmptySliceCollectorsToLeaves() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(1);
        final Directory directory = directoryReader.directory();
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap_1 = getLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final ContextualProfileBreakdown leafProfileBreakdown_1 = new TestQueryProfileBreakdown(leafProfileBreakdownMap_1);
        testQueryProfileBreakdown.getContexts().put(directoryReader.leaves().get(0), leafProfileBreakdown_1);
        final Map<String, Long> queryBreakDownMap = testQueryProfileBreakdown.toBreakdownMap();
        assertFalse(queryBreakDownMap == null || queryBreakDownMap.isEmpty());
        assertEquals(26, queryBreakDownMap.size());
        for (QueryTimingType queryTimingType : QueryTimingType.values()) {
            String timingTypeKey = queryTimingType.toString();
            String timingTypeCountKey = queryTimingType + TIMING_TYPE_COUNT_SUFFIX;

            if (queryTimingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                final long createWeightTime = queryBreakDownMap.get(timingTypeKey);
                assertEquals(createWeightTimer.getApproximateTiming(), createWeightTime);
                assertEquals(1, (long) queryBreakDownMap.get(timingTypeCountKey));
                // verify there is no min/max/avg for weight type stats
                assertFalse(
                    queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeCountKey)
                        || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey)
                );
                continue;
            }
            assertNotNull(queryBreakDownMap.get(timingTypeKey));
            assertNotNull(queryBreakDownMap.get(timingTypeCountKey));
            // verify there is no min/max/avg for current breakdown type stats
            assertFalse(
                queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeKey)
                    || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeKey)
                    || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeKey)
                    || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.MAX_PREFIX + timingTypeCountKey)
                    || queryBreakDownMap.containsKey(MIN_PREFIX + timingTypeCountKey)
                    || queryBreakDownMap.containsKey(ConcurrentQueryProfileBreakdown.AVG_PREFIX + timingTypeCountKey)
            );
        }
        assertEquals(0, testQueryProfileBreakdown.getMaxSliceNodeTime());
        assertEquals(0, testQueryProfileBreakdown.getMinSliceNodeTime());
        assertEquals(0, testQueryProfileBreakdown.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    public void testBuildSliceLevelBreakdownWithSingleSliceCombinedMetricOnly() throws Exception {
        final DirectoryReader directoryReader = getDirectoryReader(1);
        final Directory directory = directoryReader.directory();
        final LeafReaderContext sliceLeaf = directoryReader.leaves().get(0);
        final Collector sliceCollector = mock(Collector.class);
        final long createWeightEarliestStartTime = createWeightTimer.getEarliestTimerStartTime();
        final long createWeightEndTime = createWeightEarliestStartTime + createWeightTimer.getApproximateTiming();
        final Map<String, Long> leafProfileBreakdownMap = getCombinedLeafBreakdownMap(createWeightEndTime + 10, 10, 1);
        final ContextualProfileBreakdown leafProfileBreakdown = new TestQueryProfileBreakdown(leafProfileBreakdownMap);
        testQueryProfileBreakdownCombined.associateCollectorToLeaves(sliceCollector, sliceLeaf);
        testQueryProfileBreakdownCombined.getContexts().put(sliceLeaf, leafProfileBreakdown);
        final Map<Collector, Map<String, Long>> sliceBreakdownMap = testQueryProfileBreakdownCombined.buildSliceLevelBreakdown();
        assertFalse(sliceBreakdownMap == null || sliceBreakdownMap.isEmpty());
        assertEquals(1, sliceBreakdownMap.size());
        assertTrue(sliceBreakdownMap.containsKey(sliceCollector));

        final Map<String, Long> sliceBreakdown = sliceBreakdownMap.entrySet().iterator().next().getValue();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            String timingTypeKey = timingType.toString();
            String timingTypeCountKey = timingTypeKey + TIMING_TYPE_COUNT_SUFFIX;

            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                // there should be no entry for create weight at slice level breakdown map
                assertNull(sliceBreakdown.get(timingTypeKey));
                assertNull(sliceBreakdown.get(timingTypeCountKey));
                continue;
            }

            // for other timing type we will have all the value and will be same as leaf breakdown as there is single slice and single leaf
            assertEquals(leafProfileBreakdownMap.get(timingTypeKey), sliceBreakdown.get(timingTypeKey));
            assertEquals(leafProfileBreakdownMap.get(timingTypeCountKey), sliceBreakdown.get(timingTypeCountKey));
            assertEquals(
                leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX),
                sliceBreakdown.get(timingTypeKey + SLICE_START_TIME_SUFFIX)
            );
            assertEquals(
                leafProfileBreakdownMap.get(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX) + leafProfileBreakdownMap.get(timingTypeKey),
                (long) sliceBreakdown.get(timingTypeKey + SLICE_END_TIME_SUFFIX)
            );
        }
        assertEquals(leafProfileBreakdownMap.get("value"), sliceBreakdown.get("value"));
        assertEquals(10, testQueryProfileBreakdownCombined.getMaxSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdownCombined.getMinSliceNodeTime());
        assertEquals(10, testQueryProfileBreakdownCombined.getAvgSliceNodeTime());
        directoryReader.close();
        directory.close();
    }

    private Map<String, Long> getLeafBreakdownMap(long startTime, long timeTaken, long count) {
        Map<String, Long> leafBreakDownMap = new HashMap<>();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                // don't add anything
                continue;
            }
            String timingTypeKey = timingType.toString();
            leafBreakDownMap.put(timingTypeKey, timeTaken);
            leafBreakDownMap.put(timingTypeKey + TIMING_TYPE_COUNT_SUFFIX, count);
            leafBreakDownMap.put(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX, startTime);
        }
        return leafBreakDownMap;
    }

    private Map<String, Long> getCombinedLeafBreakdownMap(long startTime, long timeTaken, long count) {
        Map<String, Long> leafBreakDownMap = new HashMap<>();
        for (QueryTimingType timingType : QueryTimingType.values()) {
            if (timingType.equals(QueryTimingType.CREATE_WEIGHT)) {
                // don't add anything
                continue;
            }
            String timingTypeKey = timingType.toString();
            leafBreakDownMap.put(timingTypeKey, timeTaken);
            leafBreakDownMap.put(timingTypeKey + TIMING_TYPE_COUNT_SUFFIX, count);
            leafBreakDownMap.put(timingTypeKey + TIMING_TYPE_START_TIME_SUFFIX, startTime);
        }
        leafBreakDownMap.put("test_metric", 123L);
        return leafBreakDownMap;
    }

    private DirectoryReader getDirectoryReader(int numLeaves) throws Exception {
        final Directory directory = newDirectory();
        IndexWriter iw = new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()).setMergePolicy(NoMergePolicy.INSTANCE));

        for (int i = 0; i < numLeaves; ++i) {
            Document document = new Document();
            document.add(new StringField("field1", "value" + i, Field.Store.NO));
            document.add(new StringField("field2", "value" + i, Field.Store.NO));
            iw.addDocument(document);
            iw.commit();
        }
        iw.deleteDocuments(new Term("field1", "value3"));
        iw.close();
        return DirectoryReader.open(directory);
    }

    private static class TestQueryProfileBreakdown extends ContextualProfileBreakdown {
        private Map<String, Long> breakdownMap;

        public TestQueryProfileBreakdown(Map<String, Long> breakdownMap) {
            super(List.of());
            this.breakdownMap = breakdownMap;
        }

        @Override
        public Map<String, Long> toBreakdownMap() {
            return breakdownMap;
        }

        @Override
        public ContextualProfileBreakdown context(Object context) {
            return null;
        }
    }
}
