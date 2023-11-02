/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.tier.keystore;

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
    protected boolean guaranteesNoFalseNegatives;
    protected final int maxNumEntries;
    protected AtomicBoolean atCapacity;
    protected CounterMetric numRemovalAttempts;
    protected CounterMetric numSuccessfulRemovals;

    protected KeyStoreStats(long memSizeCapInBytes, int maxNumEntries) {
        this.size = new CounterMetric();
        this.numAddAttempts = new CounterMetric();
        this.numCollisions = new CounterMetric();
        this.memSizeCapInBytes = memSizeCapInBytes;
        this.maxNumEntries = maxNumEntries;
        this.atCapacity = new AtomicBoolean(false);
        this.numRemovalAttempts = new CounterMetric();
        this.numSuccessfulRemovals = new CounterMetric();
    }
}
