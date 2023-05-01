/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

/**
 * Contains various {@link BitMixer} implementations.
 */
public final class BitMixers {
    private BitMixers() {}

    /**
     * Bit-mixer that returns the input as it is.
     */
    public static final BitMixer IDENTITY = new BitMixer() {
        @Override
        public long mix(long value) {
            return value;
        }
    };

    /**
     * Bit-mixer implementation using the golden ratio (phi).
     */
    public static final BitMixer PHI = new BitMixer() {
        @Override
        public long mix(long value) {
            value *= 0x9e3779b97f4a7c15L;
            value ^= value >>> 32;
            return value;
        }
    };

    /**
     * Bit-mixer implementation from <a href="https://code.google.com/archive/p/fast-hash/">fast-hash</a>.
     */
    public static final BitMixer FASTHASH = new BitMixer() {
        @Override
        public long mix(long value) {
            value ^= value >>> 23;
            value *= 0x2127599bf4325c37L;
            value ^= value >>> 47;
            return value;
        }
    };

    /**
     * Bit-mixer implementation from <a href="https://github.com/Cyan4973/xxHash">xxHash</a>.
     */
    public static final BitMixer XXHASH = new BitMixer() {
        @Override
        public long mix(long value) {
            value ^= value >>> 37;
            value *= 0x165667919e3779f9L;
            value ^= value >>> 32;
            return value;
        }
    };

    /**
     * Bit-mixer implementation using an MXM construction.
     */
    public static final BitMixer MXM = new BitMixer() {
        @Override
        public long mix(long value) {
            value *= 0xbf58476d1ce4e5b9L;
            value ^= value >>> 56;
            value *= 0x94d049bb133111ebL;
            return value;
        }
    };

    /**
     * Bit-mixer implementation from <a href="https://en.wikipedia.org/wiki/MurmurHash">MurmurHash3</a> with modified constants.
     */
    public static final BitMixer MURMUR3 = new BitMixer() {
        @Override
        public long mix(long value) {
            return org.apache.lucene.util.hppc.BitMixer.mix64(value);
        }
    };
}
