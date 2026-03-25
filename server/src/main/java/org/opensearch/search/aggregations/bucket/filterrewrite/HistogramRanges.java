/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.document.LongPoint;

/**
 * Lazy {@link Ranges} that encodes histogram bucket bounds on demand.
 *
 * @opensearch.internal
 */
final class HistogramRanges extends Ranges {

    private final long[] lowers;
    private final long[] uppers;

    private final ThreadLocal<Cache> cache = ThreadLocal.withInitial(Cache::new);

    public HistogramRanges(long[] lowers, long[] uppers) {
        super(lowers.length, Long.BYTES);
        this.lowers = lowers;
        this.uppers = uppers;
    }

    @Override
    public byte[] lower(int index) {
        return cacheFor(index).lower;
    }

    @Override
    public byte[] upper(int index) {
        return cacheFor(index).upper;
    }

    private Cache cacheFor(int index) {
        Cache c = cache.get();
        if (c.index != index) {
            LongPoint.encodeDimension(lowers[index], c.lower, 0);
            LongPoint.encodeDimension(uppers[index], c.upper, 0);
            c.index = index;
        }
        return c;
    }

    private static final class Cache {
        int index = -1;
        final byte[] lower = new byte[Long.BYTES];
        final byte[] upper = new byte[Long.BYTES];
    }
}
