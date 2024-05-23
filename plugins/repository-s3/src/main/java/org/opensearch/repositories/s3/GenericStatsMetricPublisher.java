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
    private final long normalPriorityQCapacity;
    private final int maxNormalPriorityPermits;
    private final long lowPriorityQCapacity;
    private final int maxLowPriorityPermits;

    public GenericStatsMetricPublisher(
        long normalPriorityQCapacity,
        int maxNormalPriorityPermits,
        long lowPriorityQCapacity,
        int maxLowPriorityPermits
    ) {
        this.normalPriorityQCapacity = normalPriorityQCapacity;
        this.maxNormalPriorityPermits = maxNormalPriorityPermits;
        this.lowPriorityQCapacity = lowPriorityQCapacity;
        this.maxLowPriorityPermits = maxLowPriorityPermits;
    }

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
        results.put("NormalPriorityQUtilization", (normalPriorityQSize.get() * 100) / normalPriorityQCapacity);
        results.put("LowPriorityQUtilization", (lowPriorityQSize.get() * 100) / lowPriorityQCapacity);
        results.put("NormalPriorityPermitsUtilization", (normalPriorityPermits.get() * 100L) / maxNormalPriorityPermits);
        results.put("LowPriorityPermitsUtilization", (lowPriorityPermits.get() * 100L) / maxLowPriorityPermits);
        return results;
    }
}
