/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

public class ChecksumUtils {

    private static final int GF2_DIM = 32;

    public static long combine(long crc1, long crc2, long len2){
        long row;
        long[] even = new long[GF2_DIM];
        long[] odd = new long[GF2_DIM];

        // degenerate case (also disallow negative lengths)
        if (len2 <= 0)
            return crc1;

        // put operator for one zero bit in odd
        odd[0] = 0xedb88320L;          // CRC-32 polynomial
        row = 1;
        for (int n = 1; n < GF2_DIM; n++) {
            odd[n] = row;
            row <<= 1;
        }

        // put operator for two zero bits in even
        gf2_matrix_square(even, odd);

        // put operator for four zero bits in odd
        gf2_matrix_square(odd, even);

        // apply len2 zeros to crc1 (first square will put the operator for one
        // zero byte, eight zero bits, in even)
        do {
            // apply zeros operator for this bit of len2
            gf2_matrix_square(even, odd);
            if ((len2 & 1)!=0)
                crc1 = gf2_matrix_times(even, crc1);
            len2 >>= 1;

            // if no more bits set, then done
            if (len2 == 0)
                break;

            // another iteration of the loop with odd and even swapped
            gf2_matrix_square(odd, even);
            if ((len2 & 1)!=0)
                crc1 = gf2_matrix_times(odd, crc1);
            len2 >>= 1;

            // if no more bits set, then done
        } while (len2 != 0);

        /* return combined crc */
        crc1 ^= crc2;
        return crc1;
    }

    private static long gf2_matrix_times(long[] mat, long vec){
        long sum = 0;
        int index = 0;
        while (vec!=0) {
            if ((vec & 1)!=0)
                sum ^= mat[index];
            vec >>= 1;
            index++;
        }
        return sum;
    }

    static final void gf2_matrix_square(long[] square, long[] mat) {
        for (int n = 0; n < GF2_DIM; n++)
            square[n] = gf2_matrix_times(mat, mat[n]);
    }
}
