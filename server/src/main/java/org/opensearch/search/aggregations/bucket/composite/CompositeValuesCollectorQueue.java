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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A specialized {@link PriorityQueue} implementation for composite buckets.
 * Can think of this as a max heap that holds the top small buckets slots in order.
 * Each slot holds the values of the composite bucket key it represents.
 *
 * @opensearch.internal
 */
final class CompositeValuesCollectorQueue extends PriorityQueue<Integer> implements Releasable {
    private class Slot {
        int value;

        Slot(int initial) {
            this.value = initial;
        }

        // This is to be only for reusable slot
        public void set(int newValue) {
            this.value = newValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Slot slot = (Slot) o;
            return CompositeValuesCollectorQueue.this.equals(value, slot.value);
        }

        @Override
        public int hashCode() {
            return CompositeValuesCollectorQueue.this.hashCode(value);
        }
    }

    // the slot for the current candidate
    private static final int CANDIDATE_SLOT = Integer.MAX_VALUE;

    private final BigArrays bigArrays;
    private final int maxSize;
    private final Map<Slot, Integer> map; // to quickly find the slot for a value
    private final SingleDimensionValuesSource<?>[] arrays;

    /**
     * A reusable, flyweight Slot instance to avoid object allocation and reduce GC pressure
     * during map lookups in the high-frequency collect() path. This object is NOT
     * thread-safe, but is safe here because each collector instance is confined to a
     * single thread.
     */
    private final Slot reusableSlot = new Slot(0);

    private LongArray docCounts;
    private boolean afterKeyIsSet = false;

    /**
     * Constructs a composite queue with the specified size and sources.
     *
     * @param sources The list of {@link CompositeValuesSourceConfig} to build the composite buckets.
     * @param size The number of composite buckets to keep.
     * @param afterKey composite key
     */
    CompositeValuesCollectorQueue(BigArrays bigArrays, SingleDimensionValuesSource<?>[] sources, int size, CompositeKey afterKey) {
        super(size);
        this.bigArrays = bigArrays;
        this.maxSize = size;
        this.arrays = sources;
        this.map = new HashMap<>(size);
        if (afterKey != null) {
            assert afterKey.size() == sources.length;
            afterKeyIsSet = true;
            for (int i = 0; i < afterKey.size(); i++) {
                sources[i].setAfter(afterKey.get(i));
            }
        }
        this.docCounts = bigArrays.newLongArray(1, false);
    }

    @Override
    protected boolean lessThan(Integer a, Integer b) {
        return compare(a, b) > 0; // max heap
    }

    /**
     * Whether the queue is full or not.
     */
    boolean isFull() {
        return size() >= maxSize;
    }

    /**
     * Try to get the slot of the current/candidate values in the queue and returns
     * the slot if the candidate is already in the queue or null if the candidate is not present.
     */
    Integer getCurrentSlot() {
        reusableSlot.set(CANDIDATE_SLOT); // Update the state of the reusable slot
        return map.get(reusableSlot);     // Use the single reusable slot instance for the lookup
    }

    /**
     * Returns the lowest value (exclusive) of the leading source.
     */
    Comparable getLowerValueLeadSource() {
        return afterKeyIsSet ? arrays[0].getAfter() : null;
    }

    /**
     * Returns the upper value (inclusive) of the leading source.
     */
    Comparable getUpperValueLeadSource() throws IOException {
        return size() >= maxSize ? arrays[0].toComparable(top()) : null;
    }

    /**
     * Returns the document count in <code>slot</code>.
     */
    long getDocCount(int slot) {
        return docCounts.get(slot);
    }

    /**
     * Copies the current value in <code>slot</code>.
     */
    private void copyCurrent(int slot, long value) {
        for (int i = 0; i < arrays.length; i++) {
            arrays[i].copyCurrent(slot);
        }
        docCounts = bigArrays.grow(docCounts, slot + 1);
        docCounts.set(slot, value);
    }

    /**
     * Compares the values in <code>slot1</code> with the values in <code>slot2</code>.
     */
    int compare(int slot1, int slot2) {
        assert slot2 != CANDIDATE_SLOT;
        for (int i = 0; i < arrays.length; i++) {
            final int cmp;
            if (slot1 == CANDIDATE_SLOT) {
                cmp = arrays[i].compareCurrent(slot2);
            } else {
                cmp = arrays[i].compare(slot1, slot2);
            }
            if (cmp != 0) {
                return cmp > 0 ? i + 1 : -(i + 1);
            }
        }
        return 0;
    }

    /**
     * Returns true if the values in <code>slot1</code> are equals to the value in <code>slot2</code>.
     */
    boolean equals(int slot1, int slot2) {
        assert slot2 != CANDIDATE_SLOT;
        for (int i = 0; i < arrays.length; i++) {
            final int cmp;
            if (slot1 == CANDIDATE_SLOT) {
                cmp = arrays[i].compareCurrent(slot2);
            } else {
                cmp = arrays[i].compare(slot1, slot2);
            }
            if (cmp != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a hash code value for the values in <code>slot</code>.
     */
    int hashCode(int slot) {
        int result = 1;
        for (int i = 0; i < arrays.length; i++) {
            result = 31 * result + (slot == CANDIDATE_SLOT ? arrays[i].hashCodeCurrent() : arrays[i].hashCode(slot));
        }
        return result;
    }

    /**
     * Compares the after values with the values in <code>slot</code>.
     */
    private int compareCurrentWithAfter() {
        for (int i = 0; i < arrays.length; i++) {
            int cmp = arrays[i].compareCurrentWithAfter();
            if (cmp != 0) {
                return cmp > 0 ? i + 1 : -(i + 1);
            }
        }
        return 0;
    }

    /**
     * Builds the {@link CompositeKey} for <code>slot</code>.
     */
    CompositeKey toCompositeKey(int slot) throws IOException {
        assert slot < maxSize;
        Comparable[] values = new Comparable[arrays.length];
        for (int i = 0; i < values.length; i++) {
            values[i] = arrays[i].toComparable(slot);
        }
        return new CompositeKey(values);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector in) throws IOException {
        return getLeafCollector(null, context, in);
    }

    /**
     * Creates the collector that will visit the composite buckets of the matching documents.
     * If <code>forceLeadSourceValue</code> is not null, the leading source will use this value
     * for each document.
     * The provided collector <code>in</code> is called on each composite bucket.
     */
    LeafBucketCollector getLeafCollector(Comparable forceLeadSourceValue, LeafReaderContext context, LeafBucketCollector in)
        throws IOException {
        int last = arrays.length - 1;
        LeafBucketCollector collector = in;
        while (last > 0) {
            collector = arrays[last--].getLeafCollector(context, collector);
        }
        if (forceLeadSourceValue != null) {
            collector = arrays[last].getLeafCollector(forceLeadSourceValue, context, collector);
        } else {
            collector = arrays[last].getLeafCollector(context, collector);
        }
        return collector;
    }

    /**
     * Check if the current candidate should be added in the queue.
     * @return <code>true</code> if the candidate is competitive (added or already in the queue).
     */
    boolean addIfCompetitive(long inc) {
        return addIfCompetitive(0, inc);
    }

    /**
     * Add or update the current composite key in the queue if the values are competitive.
     *
     * @param indexSortSourcePrefix 0 if the index sort is null or doesn't match any of the sources field,
     *                              a value greater than 0 indicates the prefix len of the sources that match the index sort
     *                              and a negative value indicates that the index sort match the source field but the order is reversed.
     * @return <code>true</code> if the candidate is competitive (added or already in the queue).
     *
     * @throws CollectionTerminatedException if the current collection can be terminated early due to index sorting.
     */
    boolean addIfCompetitive(int indexSortSourcePrefix, long inc) {
        // checks if the candidate key is competitive
        Integer curSlot = getCurrentSlot();
        if (curSlot != null) {
            // this key is already in the top N, skip it
            docCounts.increment(curSlot, inc);
            return true;
        }

        if (afterKeyIsSet) {
            int cmp = compareCurrentWithAfter();
            if (cmp <= 0) {
                if (indexSortSourcePrefix < 0 && cmp == indexSortSourcePrefix) {
                    // the leading index sort is and the leading source order are both reversed,
                    // so we can early terminate when we reach a document that is smaller
                    // than the after key (collected on a previous page).
                    throw new CollectionTerminatedException();
                }
                // the key was collected on a previous page, skip it.
                return false;
            }
        }

        // the heap is full, check if the candidate key larger than max heap top
        if (size() >= maxSize) {
            int cmp = compare(CANDIDATE_SLOT, top());
            if (cmp > 0) {
                if (cmp <= indexSortSourcePrefix) {
                    // index sort guarantees the following documents will have a key larger than the current candidate,
                    // so we can early terminate.
                    throw new CollectionTerminatedException();
                }
                // the candidate key is not competitive, skip it.
                return false;
            }
        }
        // the candidate key is competitive
        final int newSlot;
        if (size() >= maxSize) {
            // the queue is full, we replace the last key with this candidate
            int slot = pop();
            reusableSlot.set(slot);          // Use reusable for remove
            map.remove(reusableSlot);
            // and we recycle the deleted slot
            newSlot = slot;
        } else {
            newSlot = size();
        }
        // move the candidate key to its new slot by copy its values to the new slot
        copyCurrent(newSlot, inc);
        map.put(new Slot(newSlot), newSlot);
        add(newSlot);
        return true;
    }

    @Override
    public void close() {
        Releasables.close(docCounts);
    }
}
