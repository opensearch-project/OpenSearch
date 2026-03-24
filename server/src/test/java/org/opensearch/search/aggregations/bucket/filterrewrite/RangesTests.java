/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.document.LongPoint;
import org.opensearch.test.OpenSearchTestCase;

public class RangesTests extends OpenSearchTestCase {

    public void testLazyEagerValues() {
        int count = 100;
        long[] rawLowers = new long[count];
        long[] rawUppers = new long[count];
        byte[][] byteLowers = new byte[count][];
        byte[][] byteUppers = new byte[count][];

        for (int i = 0; i < count; i++) {
            rawLowers[i] = (long) i * 60_000;
            rawUppers[i] = (long) (i + 1) * 60_000;
            byteLowers[i] = new byte[Long.BYTES];
            byteUppers[i] = new byte[Long.BYTES];
            LongPoint.encodeDimension(rawLowers[i], byteLowers[i], 0);
            LongPoint.encodeDimension(rawUppers[i], byteUppers[i], 0);
        }

        Ranges eager = Ranges.forByteArrays(byteLowers, byteUppers);
        Ranges lazy = new HistogramRanges(rawLowers, rawUppers);

        for (int i = 0; i < count; i++) {
            assertArrayEquals(eager.lower(i), lazy.lower(i));
            assertArrayEquals(eager.upper(i), lazy.upper(i));
        }
    }

    public void testFirstRangeIndex() {
        int count = 5;
        long interval = 100;
        long[] rawLowers = new long[count];
        long[] rawUppers = new long[count];
        byte[][] byteLowers = new byte[count][];
        byte[][] byteUppers = new byte[count][];

        for (int i = 0; i < count; i++) {
            rawLowers[i] = (long) i * interval;
            rawUppers[i] = (long) (i + 1) * interval;
            byteLowers[i] = new byte[Long.BYTES];
            byteUppers[i] = new byte[Long.BYTES];
            LongPoint.encodeDimension(rawLowers[i], byteLowers[i], 0);
            LongPoint.encodeDimension(rawUppers[i], byteUppers[i], 0);
        }

        Ranges eager = Ranges.forByteArrays(byteLowers, byteUppers);
        Ranges lazy = new HistogramRanges(rawLowers, rawUppers);

        byte[] min = encode(150);
        byte[] max = encode(350);
        assertEquals(eager.firstRangeIndex(min, max), lazy.firstRangeIndex(min, max));
    }

    public void testWithinBounds() {
        long[] rawLowers = { 0, 1000, 2000 };
        long[] rawUppers = { 1000, 2000, 3000 };
        byte[][] byteLowers = new byte[3][];
        byte[][] byteUppers = new byte[3][];
        for (int i = 0; i < 3; i++) {
            byteLowers[i] = new byte[Long.BYTES];
            byteUppers[i] = new byte[Long.BYTES];
            LongPoint.encodeDimension(rawLowers[i], byteLowers[i], 0);
            LongPoint.encodeDimension(rawUppers[i], byteUppers[i], 0);
        }

        Ranges eager = Ranges.forByteArrays(byteLowers, byteUppers);
        Ranges lazy = new HistogramRanges(rawLowers, rawUppers);

        byte[] value = encode(1500);
        for (int i = 0; i < 3; i++) {
            assertEquals(eager.withinLowerBound(i, value), lazy.withinLowerBound(i, value));
            assertEquals(eager.withinUpperBound(i, value), lazy.withinUpperBound(i, value));
        }
    }

    public void testEmpty() {
        Ranges lazy = new HistogramRanges(new long[0], new long[0]);
        assertEquals(0, lazy.getSize());
        assertEquals(-1, lazy.firstRangeIndex(encode(0), encode(100)));
    }

    private static byte[] encode(long value) {
        byte[] result = new byte[Long.BYTES];
        LongPoint.encodeDimension(value, result, 0);
        return result;
    }
}
