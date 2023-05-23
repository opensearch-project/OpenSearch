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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.core.util;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefArray;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.InPlaceMergeSorter;

import java.util.Comparator;

/**
 * Utilities for sorting Lucene {@link BytesRefArray}
 *
 * @opensearch.internal
 */
public class BytesRefUtils {
    public static void sort(final BytesRefArray bytes, final int[] indices) {
        sort(new BytesRefBuilder(), new BytesRefBuilder(), bytes, indices);
    }

    private static void sort(
        final BytesRefBuilder scratch,
        final BytesRefBuilder scratch1,
        final BytesRefArray bytes,
        final int[] indices
    ) {

        final int numValues = bytes.size();
        assert indices.length >= numValues;
        if (numValues > 1) {
            new InPlaceMergeSorter() {
                final Comparator<BytesRef> comparator = Comparator.naturalOrder();

                @Override
                protected int compare(int i, int j) {
                    return comparator.compare(bytes.get(scratch, indices[i]), bytes.get(scratch1, indices[j]));
                }

                @Override
                protected void swap(int i, int j) {
                    int value_i = indices[i];
                    indices[i] = indices[j];
                    indices[j] = value_i;
                }
            }.sort(0, numValues);
        }

    }

    public static int sortAndDedup(final BytesRefArray bytes, final int[] indices) {
        final BytesRefBuilder scratch = new BytesRefBuilder();
        final BytesRefBuilder scratch1 = new BytesRefBuilder();
        final int numValues = bytes.size();
        assert indices.length >= numValues;
        if (numValues <= 1) {
            return numValues;
        }
        sort(scratch, scratch1, bytes, indices);
        int uniqueCount = 1;
        BytesRefBuilder previous = scratch;
        BytesRefBuilder current = scratch1;
        bytes.get(previous, indices[0]);
        for (int i = 1; i < numValues; ++i) {
            bytes.get(current, indices[i]);
            if (!previous.get().equals(current.get())) {
                indices[uniqueCount++] = indices[i];
            }
            BytesRefBuilder tmp = previous;
            previous = current;
            current = tmp;
        }
        return uniqueCount;
    }

    public static long bytesToLong(BytesRef bytes) {
        int high = (bytes.bytes[bytes.offset + 0] << 24) | ((bytes.bytes[bytes.offset + 1] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 2]
            & 0xff) << 8) | (bytes.bytes[bytes.offset + 3] & 0xff);
        int low = (bytes.bytes[bytes.offset + 4] << 24) | ((bytes.bytes[bytes.offset + 5] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 6]
            & 0xff) << 8) | (bytes.bytes[bytes.offset + 7] & 0xff);
        return (((long) high) << 32) | (low & 0x0ffffffffL);
    }

}
