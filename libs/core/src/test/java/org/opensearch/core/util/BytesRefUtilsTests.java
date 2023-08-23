/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.Counter;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Tests for the {@link BytesRefUtils} class
 */
public class BytesRefUtilsTests extends OpenSearchTestCase {
    public void testSortAndDedupByteRefArray() {
        SortedSet<BytesRef> set = new TreeSet<>();
        final int numValues = scaledRandomIntBetween(0, 10000);
        List<BytesRef> tmpList = new ArrayList<>();
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        for (int i = 0; i < numValues; i++) {
            String s = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            set.add(new BytesRef(s));
            tmpList.add(new BytesRef(s));
            array.append(new BytesRef(s));
        }
        if (randomBoolean()) {
            Collections.shuffle(tmpList, random());
            for (BytesRef ref : tmpList) {
                array.append(ref);
            }
        }
        int[] indices = new int[array.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        int numUnique = BytesRefUtils.sortAndDedup(array, indices);
        assertThat(numUnique, equalTo(set.size()));
        Iterator<BytesRef> iterator = set.iterator();

        BytesRefBuilder spare = new BytesRefBuilder();
        for (int i = 0; i < numUnique; i++) {
            assertThat(iterator.hasNext(), is(true));
            assertThat(array.get(spare, indices[i]), equalTo(iterator.next()));
        }
    }

    public void testSortByteRefArray() {
        List<BytesRef> values = new ArrayList<>();
        final int numValues = scaledRandomIntBetween(0, 10000);
        BytesRefArray array = new BytesRefArray(Counter.newCounter());
        for (int i = 0; i < numValues; i++) {
            String s = randomRealisticUnicodeOfCodepointLengthBetween(1, 100);
            values.add(new BytesRef(s));
            array.append(new BytesRef(s));
        }
        if (randomBoolean()) {
            Collections.shuffle(values, random());
        }
        int[] indices = new int[array.size()];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = i;
        }
        BytesRefUtils.sort(array, indices);
        Collections.sort(values);
        Iterator<BytesRef> iterator = values.iterator();

        BytesRefBuilder spare = new BytesRefBuilder();
        for (int i = 0; i < values.size(); i++) {
            assertThat(iterator.hasNext(), is(true));
            assertThat(array.get(spare, indices[i]), equalTo(iterator.next()));
        }
    }

    public void testBytesToLong() {
        final long value = randomLong();
        final BytesReference buffer = BytesReference.fromByteBuffer(ByteBuffer.allocate(8).putLong(value).flip());
        assertThat(BytesRefUtils.bytesToLong(buffer.toBytesRef()), equalTo(value));
    }
}
