/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.cache.tier.keystore;

import org.opensearch.common.metrics.CounterMetric;

import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.roaringbitmap.RoaringBitmap;

/**
 * This class implements KeyLookupStore<Integer> using a roaring bitmap with a modulo applied to values.
 * The modulo increases the density of values, which makes RBMs more memory-efficient. The recommended modulo is ~2^28.
 * It also maintains a hash set of values which have had collisions. Values which haven't had collisions can be
 * safely removed from the store. The fraction of collided values should be low,
 * about 0.5% for a store with 10^7 values and a modulo of 2^28.
 * The store estimates its memory footprint and will stop adding more values once it reaches its memory cap.
 */
public class RBMIntKeyLookupStore implements KeyLookupStore<Integer> {
    /**
     * An enum representing modulo values for use in the keystore
     */
    public enum KeystoreModuloValue {
        NONE(0), // No modulo applied
        TWO_TO_THIRTY_ONE((int) Math.pow(2, 31)),
        TWO_TO_TWENTY_NINE((int) Math.pow(2, 29)), // recommended value
        TWO_TO_TWENTY_EIGHT((int) Math.pow(2, 28)),
        TWO_TO_TWENTY_SIX((int) Math.pow(2, 26));

        private final int value;

        private KeystoreModuloValue(int value) {
            this.value = value;
        }

        public int getValue() {
            return this.value;
        }
    }

    protected final int modulo;
    KeyStoreStats stats;
    protected RoaringBitmap rbm;
    private HashSet<Integer> collidedInts;
    protected RBMSizeEstimator sizeEstimator;
    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    protected final Lock readLock = lock.readLock();
    protected final Lock writeLock = lock.writeLock();

    // Default constructor sets modulo = 2^28
    public RBMIntKeyLookupStore(long memSizeCapInBytes) {
        this(KeystoreModuloValue.TWO_TO_TWENTY_EIGHT, memSizeCapInBytes);
    }

    public RBMIntKeyLookupStore(KeystoreModuloValue moduloValue, long memSizeCapInBytes) {
        this.modulo = moduloValue.getValue();
        sizeEstimator = new RBMSizeEstimator(modulo);
        this.stats = new KeyStoreStats(memSizeCapInBytes, calculateMaxNumEntries(memSizeCapInBytes));
        this.rbm = new RoaringBitmap();
        collidedInts = new HashSet<>();
    }

    protected int calculateMaxNumEntries(long memSizeCapInBytes) {
        if (memSizeCapInBytes == 0) {
            return Integer.MAX_VALUE;
        }
        return sizeEstimator.getNumEntriesFromSizeInBytes(memSizeCapInBytes);
    }

    private final int transform(int value) {
        return modulo == 0 ? value : value % modulo;
    }

    private void handleCollisions(int transformedValue) {
        stats.numCollisions.inc();
        collidedInts.add(transformedValue);
    }

    @Override
    public boolean add(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        stats.numAddAttempts.inc();
        if (stats.size.count() == stats.maxNumEntries) {
            stats.atCapacity.set(true);
            return false;
        }
        int transformedValue = transform(value);

        writeLock.lock();
        try {
            boolean alreadyContained;
            // saves calling transform() an additional time
            alreadyContained = rbm.contains(transformedValue);
            if (!alreadyContained) {
                rbm.add(transformedValue);
                stats.size.inc();
                return true;
            }
            handleCollisions(transformedValue);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean contains(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            return rbm.contains(transformedValue);
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public Integer getInternalRepresentation(Integer value) {
        if (value == null) {
            return 0;
        }
        return Integer.valueOf(transform(value));
    }

    @Override
    public boolean remove(Integer value) throws Exception {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            if (!rbm.contains(transformedValue)) { // saves additional transform() call
                return false;
            }
            stats.numRemovalAttempts.inc();
            if (collidedInts.contains(transformedValue)) {
                return false;
            }
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            rbm.remove(transformedValue);
            stats.size.dec();
            stats.numSuccessfulRemovals.inc();
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public int getSize() {
        readLock.lock();
        try {
            return (int) stats.size.count();
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int getAddAttempts() {
        return (int) stats.numAddAttempts.count();
    }

    @Override
    public int getCollisions() {
        return (int) stats.numCollisions.count();
    }

    @Override
    public boolean isCollision(Integer value1, Integer value2) {
        if (value1 == null || value2 == null) {
            return false;
        }
        return transform(value1) == transform(value2);
    }

    @Override
    public long getMemorySizeInBytes() {
        return sizeEstimator.getSizeInBytes((int) stats.size.count()) + RBMSizeEstimator.getHashsetMemSizeInBytes(collidedInts.size());
    }

    @Override
    public long getMemorySizeCapInBytes() {
        return stats.memSizeCapInBytes;
    }

    @Override
    public boolean isFull() {
        return stats.atCapacity.get();
    }

    @Override
    public void regenerateStore(Integer[] newValues) throws Exception {
        rbm.clear();
        collidedInts = new HashSet<>();
        stats.size = new CounterMetric();
        stats.numAddAttempts = new CounterMetric();
        stats.numCollisions = new CounterMetric();
        stats.guaranteesNoFalseNegatives = true;
        stats.numRemovalAttempts = new CounterMetric();
        stats.numSuccessfulRemovals = new CounterMetric();
        for (int i = 0; i < newValues.length; i++) {
            if (newValues[i] != null) {
                add(newValues[i]);
            }
        }
    }

    @Override
    public void clear() throws Exception {
        regenerateStore(new Integer[] {});
    }

    public int getNumRemovalAttempts() {
        return (int) stats.numRemovalAttempts.count();
    }

    public int getNumSuccessfulRemovals() {
        return (int) stats.numSuccessfulRemovals.count();
    }

    public boolean valueHasHadCollision(Integer value) {
        if (value == null) {
            return false;
        }
        return collidedInts.contains(transform(value));
    }
}
