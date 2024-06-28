/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class BloomFilterTests extends OpenSearchTestCase {

    public void testBloomFilterSerializationDeserialization() throws IOException {
        int elementCount = randomIntBetween(1, 100);
        long maxDocs = elementCount * 10L; // Keeping this high so that it ensures some bits are not set.
        BloomFilter filter = new BloomFilter(maxDocs, getFpp(), () -> idIterator(elementCount));
        byte[] buffer = new byte[(int) maxDocs * 5];
        ByteArrayDataOutput out = new ByteArrayDataOutput(buffer);

        // Write in the format readable through factory
        out.writeString(filter.setType().getSetName());
        filter.writeTo(out);

        FuzzySet reconstructedFilter = FuzzySetFactory.deserializeFuzzySet(new ByteArrayIndexInput("filter", buffer));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, reconstructedFilter.setType());

        Iterator<BytesRef> idIterator = idIterator(elementCount);
        while (idIterator.hasNext()) {
            BytesRef element = idIterator.next();
            assertEquals(FuzzySet.Result.MAYBE, reconstructedFilter.contains(element));
            assertEquals(FuzzySet.Result.MAYBE, filter.contains(element));
        }
    }

    public void testBloomFilterIsSaturated_returnsTrue() throws IOException {
        BloomFilter bloomFilter = new BloomFilter(1L, getFpp(), () -> idIterator(1000));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
        assertEquals(true, bloomFilter.isSaturated());
    }

    public void testBloomFilterIsSaturated_returnsFalse() throws IOException {
        int elementCount = randomIntBetween(1, 100);
        BloomFilter bloomFilter = new BloomFilter(20000, getFpp(), () -> idIterator(elementCount));
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
        assertEquals(false, bloomFilter.isSaturated());
    }

    public void testBloomFilterWithLargeCapacity() throws IOException {
        long maxDocs = randomLongBetween(Integer.MAX_VALUE, 5L * Integer.MAX_VALUE);
        BloomFilter bloomFilter = new BloomFilter(maxDocs, getFpp(), () -> List.of(new BytesRef("bar")).iterator());
        assertEquals(FuzzySet.SetType.BLOOM_FILTER_V1, bloomFilter.setType());
    }

    private double getFpp() {
        return randomDoubleBetween(0.01, 0.50, true);
    }

    private Iterator<BytesRef> idIterator(int count) {
        return new Iterator<BytesRef>() {
            int cnt = count;

            @Override
            public boolean hasNext() {
                return cnt-- > 0;
            }

            @Override
            public BytesRef next() {
                return new BytesRef(Integer.toString(cnt));
            }
        };
    }
}
