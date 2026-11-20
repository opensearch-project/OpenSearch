/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.opensearch.common.util.BitMixer;

import java.util.Arrays;

/**
 * Counts how often each Unicode code point occurs in the UTF-8 encoded values it is given, using memory in proportion to the number
 * of distinct code points.
 * <p>
 * The values of a keyword field are valid UTF-8, but other fields that expose bytes, such as {@code binary}, are not, so decoding is
 * lenient: every byte that does not belong to a well-formed sequence counts as one U+FFFD replacement character. Overlong encodings,
 * encoded UTF-16 surrogates, and code points above U+10FFFF are not well-formed.
 *
 * @opensearch.internal
 */
final class CharacterCounts {

    private static final int REPLACEMENT_CHARACTER = 0xFFFD;
    private static final int INITIAL_CAPACITY = 16;
    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(CharacterCounts.class);

    /** The code point plus one for each occupied slot, so that a new array is all empty slots. */
    private int[] keys = new int[INITIAL_CAPACITY];
    private long[] counts = new long[INITIAL_CAPACITY];
    private int size;
    private int threshold = threshold(INITIAL_CAPACITY);

    private static int threshold(int capacity) {
        // the table is at most three quarters full, which keeps probe sequences short and always leaves an empty slot
        return capacity - (capacity >>> 2);
    }

    private static boolean isContinuation(byte value) {
        return (value & 0xC0) == 0x80;
    }

    /**
     * Counts the code points of a UTF-8 encoded value.
     *
     * @return the number of code points in the value, which is the length of the value in characters
     */
    int addUtf8(BytesRef value) {
        final byte[] bytes = value.bytes;
        final int end = value.offset + value.length;
        int position = value.offset;
        int length = 0;
        while (position < end) {
            final int lead = bytes[position] & 0xFF;
            final int remaining = end - position;
            // anything that is not a well-formed sequence is a single replacement character that consumes one byte
            int codePoint = REPLACEMENT_CHARACTER;
            int width = 1;
            if (lead < 0x80) {
                codePoint = lead;
            } else if (lead >= 0xC2 && lead <= 0xDF) {
                if (remaining >= 2 && isContinuation(bytes[position + 1])) {
                    codePoint = ((lead & 0x1F) << 6) | (bytes[position + 1] & 0x3F);
                    width = 2;
                }
            } else if (lead >= 0xE0 && lead <= 0xEF) {
                if (remaining >= 3 && isContinuation(bytes[position + 1]) && isContinuation(bytes[position + 2])) {
                    final int candidate = ((lead & 0x0F) << 12) | ((bytes[position + 1] & 0x3F) << 6) | (bytes[position + 2] & 0x3F);
                    if (candidate >= 0x800 && (candidate < Character.MIN_SURROGATE || candidate > Character.MAX_SURROGATE)) {
                        codePoint = candidate;
                        width = 3;
                    }
                }
            } else if (lead >= 0xF0 && lead <= 0xF4) {
                if (remaining >= 4
                    && isContinuation(bytes[position + 1])
                    && isContinuation(bytes[position + 2])
                    && isContinuation(bytes[position + 3])) {
                    final int candidate = ((lead & 0x07) << 18) | ((bytes[position + 1] & 0x3F) << 12) | ((bytes[position + 2] & 0x3F) << 6)
                        | (bytes[position + 3] & 0x3F);
                    if (candidate >= 0x10000 && candidate <= Character.MAX_CODE_POINT) {
                        codePoint = candidate;
                        width = 4;
                    }
                }
            }
            add(codePoint, 1);
            position += width;
            length++;
        }
        return length;
    }

    /**
     * Adds {@code count} occurrences of a code point.
     */
    void add(int codePoint, long count) {
        assert codePoint >= 0 && codePoint <= Character.MAX_CODE_POINT : "not a code point: " + codePoint;
        assert count > 0 : "a count must be positive: " + count;
        final int key = codePoint + 1;
        final int mask = keys.length - 1;
        int slot = BitMixer.mix32(codePoint) & mask;
        while (keys[slot] != 0) {
            if (keys[slot] == key) {
                counts[slot] += count;
                return;
            }
            slot = (slot + 1) & mask;
        }
        keys[slot] = key;
        counts[slot] = count;
        if (++size > threshold) {
            grow();
        }
    }

    /**
     * @return how often the code point was counted, or zero if it never was
     */
    long get(int codePoint) {
        final int key = codePoint + 1;
        final int mask = keys.length - 1;
        int slot = BitMixer.mix32(codePoint) & mask;
        while (keys[slot] != 0) {
            if (keys[slot] == key) {
                return counts[slot];
            }
            slot = (slot + 1) & mask;
        }
        return 0L;
    }

    /**
     * @return the number of distinct code points counted
     */
    int size() {
        return size;
    }

    /**
     * @return the distinct code points counted, in ascending order
     */
    int[] sortedCodePoints() {
        final int[] codePoints = new int[size];
        int next = 0;
        for (int key : keys) {
            if (key != 0) {
                codePoints[next++] = key - 1;
            }
        }
        Arrays.sort(codePoints);
        return codePoints;
    }

    /**
     * @param sortedCodePoints distinct code points that were counted, which {@link #sortedCodePoints()} gives
     * @return how often each of them was counted, in the same order
     */
    long[] countsOf(int[] sortedCodePoints) {
        final long[] result = new long[sortedCodePoints.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = get(sortedCodePoints[i]);
        }
        return result;
    }

    /**
     * @return an estimate of the heap this object holds
     */
    long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(keys) + RamUsageEstimator.sizeOf(counts);
    }

    private void grow() {
        final int[] oldKeys = keys;
        final long[] oldCounts = counts;
        final int capacity = oldKeys.length << 1;
        keys = new int[capacity];
        counts = new long[capacity];
        threshold = threshold(capacity);
        final int mask = capacity - 1;
        for (int i = 0; i < oldKeys.length; i++) {
            if (oldKeys[i] != 0) {
                int slot = BitMixer.mix32(oldKeys[i] - 1) & mask;
                while (keys[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                keys[slot] = oldKeys[i];
                counts[slot] = oldCounts[i];
            }
        }
    }
}
