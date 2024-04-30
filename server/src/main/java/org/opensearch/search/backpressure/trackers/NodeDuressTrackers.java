/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.trackers;

import org.opensearch.common.util.Streak;

import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

/**
 * NodeDuressTrackers is used to check if the node is in duress based on various resources.
 *
 * @opensearch.internal
 */
public class NodeDuressTrackers {

    private final NodeDuressTracker heapDuressTracker;
    private final NodeDuressTracker cpuDuressTracker;

    public NodeDuressTrackers(final NodeDuressTracker heapDuressTracker, final NodeDuressTracker cpuDuressTracker) {
        this.heapDuressTracker = heapDuressTracker;
        this.cpuDuressTracker = cpuDuressTracker;
    }

    /**
     * Method to check the heap duress
     * @return true if heap is in duress
     */
    public boolean isHeapInDuress() {
        return heapDuressTracker.test();
    }

    /**
     * Method to check the CPU duress
     * @return true if cpu is in duress
     */
    public boolean isCPUInDuress() {
        return cpuDuressTracker.test();
    }

    /**
     * Method to evaluate whether the node is in duress or not
     * @return true if node is in duress because of either system resource
     */
    public boolean isNodeInDuress() {
        return isCPUInDuress() || isHeapInDuress();
    }

    /**
     * NodeDuressTracker is used to check if the node is in duress
     * @opensearch.internal
     */
    public static class NodeDuressTracker {
        /**
         * Tracks the number of consecutive breaches.
         */
        private final Streak breaches = new Streak();

        /**
         * Predicate that returns true when the node is in duress.
         */
        private final BooleanSupplier isNodeInDuress;

        /**
         * Predicate that returns the max number of breaches allowed for this resource before we mark it as in duress
         */
        private final IntSupplier maxBreachAllowedSupplier;

        public NodeDuressTracker(BooleanSupplier isNodeInDuress, IntSupplier maxBreachAllowedSupplier) {
            this.isNodeInDuress = isNodeInDuress;
            this.maxBreachAllowedSupplier = maxBreachAllowedSupplier;
        }

        /**
         * Returns true if the node is in duress consecutively for the past 'n' observations.
         */
        public boolean test() {
            return breaches.record(isNodeInDuress.getAsBoolean()) >= maxBreachAllowedSupplier.getAsInt();
        }
    }
}
