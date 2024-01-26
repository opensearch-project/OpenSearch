/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.common.metrics.CounterMetric;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A stats holder for use in KeyLookupStore implementations.
 * Getters should be exposed by the KeyLookupStore which uses it.
 */
public class KeyStoreStats {
    protected CounterMetric size;
    protected long memSizeCapInBytes;
    protected CounterMetric numAddAttempts;
    protected CounterMetric numCollisions;
    protected AtomicBoolean atCapacity;
    protected CounterMetric numRemovalAttempts;
    protected CounterMetric numSuccessfulRemovals;

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
