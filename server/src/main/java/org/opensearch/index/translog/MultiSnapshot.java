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

package org.opensearch.index.translog;

import org.opensearch.index.seqno.CountedBitSet;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A snapshot composed out of multiple snapshots
 *
 * @opensearch.internal
 */
final class MultiSnapshot implements Translog.Snapshot {

    private final TranslogSnapshot[] translogs;
    private final int totalOperations;
    private int overriddenOperations;
    private final Closeable onClose;
    private int index;
    private final SeqNoSet seenSeqNo;

    /**
     * Creates a new point in time snapshot of the given snapshots. Those snapshots are always iterated in-order.
     */
    MultiSnapshot(TranslogSnapshot[] translogs, Closeable onClose) {
        this.translogs = translogs;
        this.totalOperations = Arrays.stream(translogs).mapToInt(TranslogSnapshot::totalOperations).sum();
        this.overriddenOperations = 0;
        this.onClose = onClose;
        this.seenSeqNo = new SeqNoSet();
        this.index = translogs.length - 1;
    }

    @Override
    public int totalOperations() {
        return totalOperations;
    }

    @Override
    public int skippedOperations() {
        return Arrays.stream(translogs).mapToInt(TranslogSnapshot::skippedOperations).sum() + overriddenOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        // TODO: Read translog forward in 9.0+
        for (; index >= 0; index--) {
            final TranslogSnapshot current = translogs[index];
            Translog.Operation op;
            while ((op = current.next()) != null) {
                if (op.seqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO || seenSeqNo.getAndSet(op.seqNo()) == false) {
                    return op;
                } else {
                    overriddenOperations++;
                }
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        onClose.close();
    }

    /**
     * Sequence number set
     *
     * @opensearch.internal
     */
    static final class SeqNoSet {
        static final short BIT_SET_SIZE = 1024;
        private final Map<Long, CountedBitSet> bitSets = new HashMap<>();

        /**
         * Marks this sequence number and returns {@code true} if it is seen before.
         */
        boolean getAndSet(long value) {
            assert value >= 0;
            final long key = value / BIT_SET_SIZE;
            CountedBitSet bitset = bitSets.get(key);
            if (bitset == null) {
                bitset = new CountedBitSet(BIT_SET_SIZE);
                bitSets.put(key, bitset);
            }
            final int index = Math.toIntExact(value % BIT_SET_SIZE);
            final boolean wasOn = bitset.get(index);
            bitset.set(index);
            return wasOn;
        }
    }
}
