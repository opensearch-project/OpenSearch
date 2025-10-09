/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import java.io.IOException;

import org.apache.lucene.search.CheckedIntConsumer;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.MathUtil;

/**
 * DocIdStream implementation using FixedBitSet. This is duplicate of the implementation in Lucene
 * and should ideally eventually be removed.
 *
 * @opensearch.internal
 */
public final class BitSetDocIdStream extends DocIdStream {
    private final FixedBitSet bitSet;
    private final int offset, max;
    private int upTo;

    public BitSetDocIdStream(FixedBitSet bitSet, int offset) {
        this.bitSet = bitSet;
        this.offset = offset;
        upTo = offset;
        max = MathUtil.unsignedMin(Integer.MAX_VALUE, offset + bitSet.length());
    }

    @Override
    public boolean mayHaveRemaining() {
        return upTo < max;
    }

    @Override
    public void forEach(int upTo, CheckedIntConsumer<IOException> consumer) throws IOException {
        if (upTo > this.upTo) {
            upTo = Math.min(upTo, max);
            bitSet.forEach(this.upTo - offset, upTo - offset, offset, consumer);
            this.upTo = upTo;
        }
    }

    @Override
    public int count(int upTo) throws IOException {
        if (upTo > this.upTo) {
            upTo = Math.min(upTo, max);
            int count = bitSet.cardinality(this.upTo - offset, upTo - offset);
            this.upTo = upTo;
            return count;
        } else {
            return 0;
        }
    }
}

