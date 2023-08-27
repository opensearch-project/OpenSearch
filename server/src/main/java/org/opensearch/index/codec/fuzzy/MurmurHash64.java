/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

public class MurmurHash64 {
    private static final long M64 = 0xc6a4a7935bd1e995L;
    private static final int R64 = 47;
    public static final MurmurHash64 INSTANCE = new MurmurHash64();

    /**
     * Generates a 64-bit hash from byte array of the given length and seed.
     *
     * @param data The input byte array
     * @param seed The initial seed value
     * @param length The length of the array
     * @return The 64-bit hash of the given array
     */
    public static long hash64(byte[] data, int seed, int offset, int length) {
        long h = (seed & 0xffffffffL) ^ (length * M64);

        final int nblocks = length >> 3;

        // body
        for (int i = 0; i < nblocks; i++) {

            long k = (long) BitUtil.VH_LE_LONG.get(data, offset);
            k *= M64;
            k ^= k >>> R64;
            k *= M64;

            h ^= k;
            h *= M64;

            offset += Long.BYTES;
        }

        int remaining = length & 0x07;
        if (0 < remaining) {
            for (int i = 0; i < remaining; i++) {
                h ^= ((long) data[offset + i] & 0xff) << (Byte.SIZE * i);
            }
            h *= M64;
        }

        h ^= h >>> R64;
        h *= M64;
        h ^= h >>> R64;

        return h;
    }

    public final long hash(BytesRef br) {
        return hash64(br.bytes, 0xe17a1465, br.offset, br.length);
    }
}
