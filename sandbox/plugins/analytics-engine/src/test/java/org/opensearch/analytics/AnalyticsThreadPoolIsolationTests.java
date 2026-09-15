/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

/**
 * Guards the thread-pool isolation that keeps blocking fragments from deadlocking each other.
 *
 * <p>A shuffle WORKER fragment blocks in its shuffle scan until its producers deliver, and in a
 * multi-level cascade those producers are themselves fragments that need a thread. If consumers and
 * producers draw from one bounded pool, then as soon as a node's worker tasks outnumber that pool every
 * thread ends up held by a consumer waiting on a producer queued behind it: no progress, no rejections,
 * and the drain eventually fails having received nothing. Observed directly on a shuffling multi-way
 * join — the search pool pinned at its full size with a queue that never drained.
 *
 * <p>Worker tasks scale with the shuffle partition count, so this is also what bounds how far partitions
 * can be raised, which is the lever that decides whether a memory-heavy join fits.
 */
public class AnalyticsThreadPoolIsolationTests extends OpenSearchTestCase {

    /** Worker fragments must have their own pool, not share one with the fragments they wait on. */
    public void testWorkerFragmentsGetADedicatedPool() {
        assertEquals(
            "the plugin must register a worker pool alongside the scheduler and reduce pools",
            3,
            new AnalyticsPlugin().getExecutorBuilders(Settings.EMPTY).size()
        );
    }

    /** It must be distinct from SEARCH (where the shard-scan producers run) and from the other pools. */
    public void testWorkerPoolIsDistinctFromSearchAndTheOtherAnalyticsPools() {
        assertNotEquals(ThreadPool.Names.SEARCH, AnalyticsPlugin.WORKER_THREAD_POOL_NAME);
        assertNotEquals(AnalyticsPlugin.REDUCE_THREAD_POOL_NAME, AnalyticsPlugin.WORKER_THREAD_POOL_NAME);
        assertNotEquals(AnalyticsPlugin.SCHEDULER_THREAD_POOL_NAME, AnalyticsPlugin.WORKER_THREAD_POOL_NAME);
    }

    /**
     * The pool must be sized well above the core count. These threads are blocked on data arrival rather
     * than computing, and the pool has to exceed the deepest cascade's per-node task count — a
     * core-count-sized pool is exactly the configuration that deadlocked.
     */
    public void testWorkerPoolIsSizedForBlockedTasksNotForCpus() {
        int size = AnalyticsPlugin.workerPoolSize();
        int processors = Runtime.getRuntime().availableProcessors();
        assertTrue(
            "worker pool ("
                + size
                + ") must exceed the processor count ("
                + processors
                + "): a pool sized for "
                + "CPUs deadlocks once a cascade's per-node worker tasks outnumber it",
            size > processors
        );
        assertTrue("worker pool must have a usable floor on small hosts", size >= 16);
        assertTrue("the worker pool must be larger than the scheduler pool", size > AnalyticsPlugin.schedulerPoolSize());
    }
}
