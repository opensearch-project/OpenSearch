/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Generic stats of repository-s3 plugin.
 */
public class GenericStatsMetricPublisher {

    private final AtomicLong normalPriorityQSize = new AtomicLong();
    private final AtomicInteger normalPriorityPermits = new AtomicInteger();
    private final AtomicLong lowPriorityQSize = new AtomicLong();
    private final AtomicInteger lowPriorityPermits = new AtomicInteger();

    public void updateNormalPriorityQSize(long qSize) {
        normalPriorityQSize.addAndGet(qSize);
    }

    public void updateLowPriorityQSize(long qSize) {
        lowPriorityQSize.addAndGet(qSize);
    }

    public void updateNormalPermits(boolean increment) {
        if (increment) {
            normalPriorityPermits.incrementAndGet();
        } else {
            normalPriorityPermits.decrementAndGet();
        }
    }

    public void updateLowPermits(boolean increment) {
        if (increment) {
            lowPriorityPermits.incrementAndGet();
        } else {
            lowPriorityPermits.decrementAndGet();
        }
    }

    public long getNormalPriorityQSize() {
        return normalPriorityQSize.get();
    }

    public int getAcquiredNormalPriorityPermits() {
        return normalPriorityPermits.get();
    }

    public long getLowPriorityQSize() {
        return lowPriorityQSize.get();
    }

    public int getAcquiredLowPriorityPermits() {
        return lowPriorityPermits.get();
    }

    Map<String, Long> stats() {
        final Map<String, Long> results = new HashMap<>();
        results.put("NormalPriorityQSize", normalPriorityQSize.get());
        results.put("LowPriorityQSize", lowPriorityQSize.get());
        results.put("AcquiredNormalPriorityPermits", (long) normalPriorityPermits.get());
        results.put("AcquiredLowPriorityPermits", (long) lowPriorityPermits.get());
        return results;
    }
}
