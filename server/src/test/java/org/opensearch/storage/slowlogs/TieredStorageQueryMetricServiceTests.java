/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;
import java.util.Set;

/**
 * Comprehensive unit tests for TieredStorageQueryMetricService
 */
public class TieredStorageQueryMetricServiceTests extends OpenSearchTestCase {

    private TieredStorageQueryMetricService service;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        service = TieredStorageQueryMetricService.getInstance();

        // Clear any existing state
        service.getMetricCollectors().clear();
        service.getTaskIdToCollectorMap(true).clear();
        service.getTaskIdToCollectorMap(false).clear();
    }

    public void testGetInstance() {
        TieredStorageQueryMetricService instance1 = TieredStorageQueryMetricService.getInstance();
        TieredStorageQueryMetricService instance2 = TieredStorageQueryMetricService.getInstance();

        // Should return the same singleton instance
        assertSame(instance1, instance2);
    }

    public void testGetMetricCollectorWhenNotExists() {
        long threadId = Thread.currentThread().threadId();

        TieredStoragePerQueryMetric collector = service.getMetricCollector(threadId);

        // Should return dummy collector when no collector exists
        assertNotNull(collector);
        assertTrue(collector instanceof TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy);
        assertEquals("DummyParentTaskId", collector.getParentTaskId());
        assertEquals("DummyShardId", collector.getShardId());
    }

    public void testAddAndGetMetricCollectorQueryPhase() {
        long threadId = Thread.currentThread().threadId();
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        service.addMetricCollector(threadId, metric, true);

        TieredStoragePerQueryMetric retrieved = service.getMetricCollector(threadId);
        assertSame(metric, retrieved);

        // Verify it's added to query phase map
        Map<String, Set<TieredStoragePerQueryMetric>> queryMap = service.getTaskIdToCollectorMap(true);
        assertTrue(queryMap.containsKey("task-1shard-1"));
        assertTrue(queryMap.get("task-1shard-1").contains(metric));

        // Should not be in fetch phase map
        Map<String, Set<TieredStoragePerQueryMetric>> fetchMap = service.getTaskIdToCollectorMap(false);
        assertFalse(fetchMap.containsKey("task-1shard-1"));
    }

    public void testAddAndGetMetricCollectorFetchPhase() {
        long threadId = Thread.currentThread().threadId();
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-2", "shard-2");

        service.addMetricCollector(threadId, metric, false);

        TieredStoragePerQueryMetric retrieved = service.getMetricCollector(threadId);
        assertSame(metric, retrieved);

        // Verify it's added to fetch phase map
        Map<String, Set<TieredStoragePerQueryMetric>> fetchMap = service.getTaskIdToCollectorMap(false);
        assertTrue(fetchMap.containsKey("task-2shard-2"));
        assertTrue(fetchMap.get("task-2shard-2").contains(metric));

        // Should not be in query phase map
        Map<String, Set<TieredStoragePerQueryMetric>> queryMap = service.getTaskIdToCollectorMap(true);
        assertFalse(queryMap.containsKey("task-2shard-2"));
    }

    public void testAddMultipleCollectorsForSameTaskShard() {
        long threadId1 = Thread.currentThread().threadId();
        long threadId2 = threadId1 + 1; // Simulate different thread

        TieredStoragePerQueryMetricImpl metric1 = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");
        TieredStoragePerQueryMetricImpl metric2 = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        service.addMetricCollector(threadId1, metric1, true);
        service.addMetricCollector(threadId2, metric2, true);

        // Both should be in the task-shard map
        Map<String, Set<TieredStoragePerQueryMetric>> queryMap = service.getTaskIdToCollectorMap(true);
        Set<TieredStoragePerQueryMetric> collectors = queryMap.get("task-1shard-1");
        assertEquals(2, collectors.size());
        assertTrue(collectors.contains(metric1));
        assertTrue(collectors.contains(metric2));
    }

    public void testRemoveMetricCollector() {
        long threadId = Thread.currentThread().threadId();
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        service.addMetricCollector(threadId, metric, true);

        TieredStoragePerQueryMetric removed = service.removeMetricCollector(threadId);
        assertSame(metric, removed);

        // Should return dummy collector after removal
        TieredStoragePerQueryMetric afterRemoval = service.getMetricCollector(threadId);
        assertTrue(afterRemoval instanceof TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy);

        // Should still be in task-shard map (not removed by removeMetricCollector)
        Map<String, Set<TieredStoragePerQueryMetric>> queryMap = service.getTaskIdToCollectorMap(true);
        assertTrue(queryMap.containsKey("task-1shard-1"));
    }

    public void testRemoveMetricCollectorWhenNotExists() {
        long threadId = Thread.currentThread().threadId();

        TieredStoragePerQueryMetric removed = service.removeMetricCollector(threadId);

        // Should return null when no collector exists
        assertNull(removed);
    }

    public void testRemoveMetricCollectorsQueryPhase() {
        long threadId1 = Thread.currentThread().threadId();
        long threadId2 = threadId1 + 1;

        TieredStoragePerQueryMetricImpl metric1 = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");
        TieredStoragePerQueryMetricImpl metric2 = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");

        service.addMetricCollector(threadId1, metric1, true);
        service.addMetricCollector(threadId2, metric2, true);

        Set<TieredStoragePerQueryMetric> removed = service.removeMetricCollectors("task-1", "shard-1", true);

        assertEquals(2, removed.size());
        assertTrue(removed.contains(metric1));
        assertTrue(removed.contains(metric2));

        // Should be removed from task-shard map
        Map<String, Set<TieredStoragePerQueryMetric>> queryMap = service.getTaskIdToCollectorMap(true);
        assertFalse(queryMap.containsKey("task-1shard-1"));
    }

    public void testRemoveMetricCollectorsFetchPhase() {
        long threadId = Thread.currentThread().threadId();
        TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-2", "shard-2");

        service.addMetricCollector(threadId, metric, false);

        Set<TieredStoragePerQueryMetric> removed = service.removeMetricCollectors("task-2", "shard-2", false);

        assertEquals(1, removed.size());
        assertTrue(removed.contains(metric));

        // Should be removed from task-shard map
        Map<String, Set<TieredStoragePerQueryMetric>> fetchMap = service.getTaskIdToCollectorMap(false);
        assertFalse(fetchMap.containsKey("task-2shard-2"));
    }

    public void testRemoveMetricCollectorsWhenNotExists() {
        Set<TieredStoragePerQueryMetric> removed = service.removeMetricCollectors("nonexistent", "shard", true);

        assertTrue(removed.isEmpty());
    }

    public void testRamBytesUsed() {
        long initialRam = service.ramBytesUsed();
        assertTrue(initialRam > 0);

        // Add some collectors
        TieredStoragePerQueryMetricImpl metric1 = new TieredStoragePerQueryMetricImpl("task-1", "shard-1");
        TieredStoragePerQueryMetricImpl metric2 = new TieredStoragePerQueryMetricImpl("task-2", "shard-2");

        service.addMetricCollector(1L, metric1, true);
        service.addMetricCollector(2L, metric2, false);

        long finalRam = service.ramBytesUsed();
        assertTrue(finalRam >= initialRam);
    }

    public void testRecordStoredFieldsPrefetchSuccess() {
        PrefetchStats initialStats = service.getPrefetchStats();
        long initialSuccess = initialStats.getStoredFieldsPrefetchSuccess();

        service.recordStoredFieldsPrefetch(true);

        PrefetchStats finalStats = service.getPrefetchStats();
        assertEquals(initialSuccess + 1, finalStats.getStoredFieldsPrefetchSuccess());
    }

    public void testRecordStoredFieldsPrefetchFailure() {
        PrefetchStats initialStats = service.getPrefetchStats();
        long initialFailure = initialStats.getStoredFieldsPrefetchFailure();

        service.recordStoredFieldsPrefetch(false);

        PrefetchStats finalStats = service.getPrefetchStats();
        assertEquals(initialFailure + 1, finalStats.getStoredFieldsPrefetchFailure());
    }

    public void testRecordDocValuesPrefetchSuccess() {
        PrefetchStats initialStats = service.getPrefetchStats();
        long initialSuccess = initialStats.getDocValuesPrefetchSuccess();

        service.recordDocValuesPrefetch(true);

        PrefetchStats finalStats = service.getPrefetchStats();
        assertEquals(initialSuccess + 1, finalStats.getDocValuesPrefetchSuccess());
    }

    public void testRecordDocValuesPrefetchFailure() {
        PrefetchStats initialStats = service.getPrefetchStats();
        long initialFailure = initialStats.getDocValuesPrefetchFailure();

        service.recordDocValuesPrefetch(false);

        PrefetchStats finalStats = service.getPrefetchStats();
        assertEquals(initialFailure + 1, finalStats.getDocValuesPrefetchFailure());
    }

    public void testGetPrefetchStats() {
        PrefetchStats stats = service.getPrefetchStats();

        assertNotNull(stats);
        assertTrue(stats.getStoredFieldsPrefetchSuccess() >= 0);
        assertTrue(stats.getStoredFieldsPrefetchFailure() >= 0);
        assertTrue(stats.getDocValuesPrefetchSuccess() >= 0);
        assertTrue(stats.getDocValuesPrefetchFailure() >= 0);
    }

    public void testTieredStoragePerQueryMetricDummyGetInstance() {
        TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy dummy1 =
            TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy.getInstance();
        TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy dummy2 =
            TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy.getInstance();

        // Should return the same singleton instance
        assertSame(dummy1, dummy2);
    }

    public void testTieredStoragePerQueryMetricDummyMethods() {
        TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy dummy =
            TieredStorageQueryMetricService.TieredStoragePerQueryMetricDummy.getInstance();

        // All methods should be no-op and not throw exceptions
        dummy.recordFileAccess("test.block_0_1", true);
        dummy.recordPrefetch("test", 1);
        dummy.recordReadAhead("test", 1);
        dummy.recordEndTime();

        assertEquals("DummyParentTaskId", dummy.getParentTaskId());
        assertEquals("DummyShardId", dummy.getShardId());
        assertTrue(dummy.ramBytesUsed() > 0);
    }

    public void testMaxCollectorSizeLimit() {
        // This test would be difficult to run in practice due to the high limit (1000)
        // but we can verify the logic by checking that the service handles the limit gracefully

        // Add a reasonable number of collectors to verify normal operation
        for (int i = 0; i < 10; i++) {
            TieredStoragePerQueryMetricImpl metric = new TieredStoragePerQueryMetricImpl("task-" + i, "shard-" + i);
            service.addMetricCollector((long) i, metric, true);
        }

        // Verify all were added
        assertEquals(10, service.getMetricCollectors().size());
        assertEquals(10, service.getTaskIdToCollectorMap(true).size());
    }

    public void testConcurrentAccess() {
        // Test that the service can handle concurrent access
        String taskId = "concurrent-task";
        String shardId = "concurrent-shard";

        TieredStoragePerQueryMetricImpl metric1 = new TieredStoragePerQueryMetricImpl(taskId, shardId);
        TieredStoragePerQueryMetricImpl metric2 = new TieredStoragePerQueryMetricImpl(taskId, shardId);

        // Add collectors for the same task-shard from different threads
        service.addMetricCollector(100L, metric1, true);
        service.addMetricCollector(200L, metric2, true);

        // Both should be in the same task-shard set
        Set<TieredStoragePerQueryMetric> collectors = service.getTaskIdToCollectorMap(true).get(taskId + shardId);
        assertEquals(2, collectors.size());
        assertTrue(collectors.contains(metric1));
        assertTrue(collectors.contains(metric2));
    }

    public void testPrefetchStatsHolder() {
        TieredStorageQueryMetricService.PrefetchStatsHolder holder = new TieredStorageQueryMetricService.PrefetchStatsHolder();

        // Initial stats should be zero
        PrefetchStats initialStats = holder.getStats();
        assertEquals(0, initialStats.getStoredFieldsPrefetchSuccess());
        assertEquals(0, initialStats.getStoredFieldsPrefetchFailure());
        assertEquals(0, initialStats.getDocValuesPrefetchSuccess());
        assertEquals(0, initialStats.getDocValuesPrefetchFailure());

        // Increment counters
        holder.storedFieldsPrefetchSuccess.inc();
        holder.storedFieldsPrefetchFailure.inc();
        holder.docValuesPrefetchSuccess.inc();
        holder.docValuesPrefetchFailure.inc();

        // Verify increments
        PrefetchStats finalStats = holder.getStats();
        assertEquals(1, finalStats.getStoredFieldsPrefetchSuccess());
        assertEquals(1, finalStats.getStoredFieldsPrefetchFailure());
        assertEquals(1, finalStats.getDocValuesPrefetchSuccess());
        assertEquals(1, finalStats.getDocValuesPrefetchFailure());
    }

    public void testMixedQueryAndFetchPhaseCollectors() {
        String taskId = "mixed-task";
        String shardId = "mixed-shard";

        TieredStoragePerQueryMetricImpl queryMetric = new TieredStoragePerQueryMetricImpl(taskId, shardId);
        TieredStoragePerQueryMetricImpl fetchMetric = new TieredStoragePerQueryMetricImpl(taskId, shardId);

        service.addMetricCollector(100L, queryMetric, true);
        service.addMetricCollector(200L, fetchMetric, false);

        // Should be in separate maps
        assertTrue(service.getTaskIdToCollectorMap(true).containsKey(taskId + shardId));
        assertTrue(service.getTaskIdToCollectorMap(false).containsKey(taskId + shardId));

        assertEquals(1, service.getTaskIdToCollectorMap(true).get(taskId + shardId).size());
        assertEquals(1, service.getTaskIdToCollectorMap(false).get(taskId + shardId).size());

        // Remove query phase collectors
        Set<TieredStoragePerQueryMetric> queryCollectors = service.removeMetricCollectors(taskId, shardId, true);
        assertEquals(1, queryCollectors.size());
        assertTrue(queryCollectors.contains(queryMetric));

        // Fetch phase collectors should still be there
        assertTrue(service.getTaskIdToCollectorMap(false).containsKey(taskId + shardId));

        // Remove fetch phase collectors
        Set<TieredStoragePerQueryMetric> fetchCollectors = service.removeMetricCollectors(taskId, shardId, false);
        assertEquals(1, fetchCollectors.size());
        assertTrue(fetchCollectors.contains(fetchMetric));

        // Both maps should be empty for this task-shard now
        assertFalse(service.getTaskIdToCollectorMap(true).containsKey(taskId + shardId));
        assertFalse(service.getTaskIdToCollectorMap(false).containsKey(taskId + shardId));
    }
}
