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

import java.util.ArrayList;
import java.util.HashMap;
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
    private HashMap<Integer, CounterMetric> collidedIntCounters;
    private HashMap<Integer, HashSet<Integer>> removalSets;
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
        this.collidedIntCounters = new HashMap<>();
        this.removalSets = new HashMap<>();
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
        CounterMetric numCollisions = collidedIntCounters.get(transformedValue);
        if (numCollisions == null) { // First time the transformedValue has had a collision
            numCollisions = new CounterMetric();
            numCollisions.inc(2);
            collidedIntCounters.put(transformedValue, numCollisions); // Initialize the number of colliding keys to 2
        } else {
            numCollisions.inc();
        }
    }

    @Override
    public boolean add(Integer value) {
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
            // If the value is already pending removal, take it out of the removalList
            HashSet<Integer> removalSet = removalSets.get(transformedValue);
            if (removalSet != null) {
                removalSet.remove(value);
                // Don't increment the counter - this is handled by handleCollisions() later
                if (removalSet.isEmpty()) {
                    removalSets.remove(transformedValue);
                }
            }

            handleCollisions(transformedValue);
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean contains(Integer value) {
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

    /**
     * Attempts to remove a value from the keystore. WARNING: Removing keys which have not been added to the keystore
     * may cause undefined behavior, including future false negatives!!
     * @param value The value to attempt to remove.
     * @return true if the value was removed, false otherwise
     */
    @Override
    public boolean remove(Integer value) {
        if (value == null) {
            return false;
        }
        int transformedValue = transform(value);
        readLock.lock();
        try {
            if (!rbm.contains(transformedValue)) { // saves additional transform() call
                return false;
            }
            // move below into write lock
            stats.numRemovalAttempts.inc();
        } finally {
            readLock.unlock();
        }
        writeLock.lock();
        try {
            CounterMetric numCollisions = collidedIntCounters.get(transformedValue);
            if (numCollisions != null) {
                // This transformed value has had a collision before
                HashSet<Integer> removalSet = removalSets.get(transformedValue);
                if (removalSet == null) {
                    // First time a removal has been attempted for this transformed value
                    HashSet<Integer> newRemovalSet = new HashSet<>();
                    newRemovalSet.add(value); // Add the key value, not the transformed value, to the list of attempted removals for this transformedValue
                    removalSets.put(transformedValue, newRemovalSet);
                    numCollisions.dec();
                } else {
                    if (removalSet.contains(value)) {
                        return false; // We have already attempted to remove this value. Do nothing
                    }
                    removalSet.add(value);
                    numCollisions.dec();
                    // If numCollisions has reached zero, we can safely remove all values in removalList
                    if (numCollisions.count() == 0) {
                        removeFromRBM(transformedValue);
                        collidedIntCounters.remove(transformedValue);
                        removalSets.remove(transformedValue);
                        return true;
                    }
                }
                return false;
            }
            // Otherwise, there's not been a collision for this transformedValue, so we can safely remove
            removeFromRBM(transformedValue);
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    // Helper fn for remove()
    private void removeFromRBM(int transformedValue) {
        rbm.remove(transformedValue);
        stats.size.dec();
        stats.numSuccessfulRemovals.inc();
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
        return sizeEstimator.getSizeInBytes((int) stats.size.count()); // + RBMSizeEstimator.getHashsetMemSizeInBytes(collidedInts.size());
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
    public void regenerateStore(Integer[] newValues) {
        rbm.clear();
        collidedIntCounters = new HashMap<>();
        removalSets = new HashMap<>();
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
    public void clear() {
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
        return collidedIntCounters.containsKey(transform(value));
    }

    CounterMetric getNumCollisionsForValue(int value) { // package private for testing
        return collidedIntCounters.get(transform(value));
    }

    HashSet<Integer> getRemovalSetForValue(int value) {
        return removalSets.get(transform(value));
    }
}
