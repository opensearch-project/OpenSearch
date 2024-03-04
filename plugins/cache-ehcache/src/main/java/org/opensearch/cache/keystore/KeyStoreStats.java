/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.keystore;

import org.opensearch.common.metrics.CounterMetric;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stats holder for use in KeyLookupStore implementations.
 * Getters should be exposed by the KeyLookupStore which uses it.
 */
public class KeyStoreStats {
    /** Number of entries */
    protected CounterMetric size;
    /** Memory cap in bytes */
    protected long memSizeCapInBytes;
    /** Number of add attempts */
    protected CounterMetric numAddAttempts;
    /** Number of collisions */
    protected CounterMetric numCollisions;
    /** True if the store is at capacity */
    protected AtomicBoolean atCapacity;
    /** Number of removal attempts */
    protected CounterMetric numRemovalAttempts;
    /** Number of successful removal attempts */
    protected CounterMetric numSuccessfulRemovals;

    /**
     * Constructor using memory cap.
     * @param memSizeCapInBytes The memory cap in bytes.
     */
    protected KeyStoreStats(long memSizeCapInBytes) {
        this.size = new CounterMetric();
        this.numAddAttempts = new CounterMetric();
        this.numCollisions = new CounterMetric();
        this.memSizeCapInBytes = memSizeCapInBytes;
        this.atCapacity = new AtomicBoolean(false);
        this.numRemovalAttempts = new CounterMetric();
        this.numSuccessfulRemovals = new CounterMetric();
    }
}
