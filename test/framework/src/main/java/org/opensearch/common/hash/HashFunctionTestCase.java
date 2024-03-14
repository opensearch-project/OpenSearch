/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Random;

/**
 * Base class for testing the quality of hash functions.
 */
public abstract class HashFunctionTestCase extends OpenSearchTestCase {
    private static final int[] INPUT_BITS = new int[] { 24, 32, 40, 48, 56, 64, 72, 80, 96, 112, 128, 160, 512, 1024 };
    private static final int ITERATIONS = 1000;
    private static final double BIAS_THRESHOLD = 0.01; // 1%

    public abstract byte[] hash(byte[] input);

    public abstract int outputBits();

    /**
     * Tests if the hash function shows an avalanche effect, i.e, flipping a single input bit
     * should flip half the output bits.
     */
    public void testAvalanche() {
        for (int inputBits : INPUT_BITS) {
            AvalancheStats stats = simulate(inputBits);
            if (stats.bias() >= BIAS_THRESHOLD) {
                fail("bias exceeds threshold: " + stats);
            }
        }
    }

    private AvalancheStats simulate(int inputBits) {
        int outputBits = outputBits();
        assert inputBits % 8 == 0; // using full bytes for simplicity
        assert outputBits % 8 == 0; // using full bytes for simplicity
        byte[] input = new byte[inputBits >>> 3];
        Random random = Randomness.get();
        int[][] flips = new int[inputBits][outputBits];

        for (int iter = 0; iter < ITERATIONS; iter++) {
            random.nextBytes(input);
            byte[] hash = Arrays.copyOf(hash(input), outputBits >>> 3); // copying since the underlying byte-array is reused

            for (int i = 0; i < inputBits; i++) {
                flipBit(input, i); // flip one bit
                byte[] newHash = hash(input); // recompute the hash; half the bits should have flipped
                flipBit(input, i); // return to original

                for (int o = 0; o < outputBits; o++) {
                    flips[i][o] += getBit(hash, o) ^ getBit(newHash, o);
                }
            }
        }

        return new AvalancheStats(flips, ITERATIONS);
    }

    private static void flipBit(byte[] input, int position) {
        int offset = position / 8;
        int bit = position & 7;
        input[offset] ^= (1 << bit);
    }

    private static int getBit(byte[] input, int position) {
        int offset = position / 8;
        int bit = position & 7;
        return (input[offset] >>> bit) & 1;
    }
}
