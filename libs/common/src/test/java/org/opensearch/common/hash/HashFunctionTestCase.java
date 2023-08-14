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

import java.util.Locale;
import java.util.Random;

public abstract class HashFunctionTestCase extends OpenSearchTestCase {
    private static final int[] INPUT_BITS = new int[] { 24, 32, 40, 48, 56, 64, 72, 80, 96, 112, 128, 160, 512, 1024 };
    private static final int OUTPUT_BITS = 64;
    private static final int ITERATIONS = 1000;
    private static final double BIAS_THRESHOLD = 0.01; // 1%

    public abstract long hash(byte[] input);

    /**
     * Tests if the hash function shows an avalanche effect, i.e, flipping a single input bit
     * should flip half the output bits.
     */
    public final void testAvalanche() {
        for (int inputBits : INPUT_BITS) {
            AvalancheStats stats = simulate(inputBits, OUTPUT_BITS, new RandomInputGenerator(inputBits));
            if (stats.bias() >= BIAS_THRESHOLD) {
                fail("bias exceeds threshold: " + stats);
            }
        }
    }

    private AvalancheStats simulate(int inputBits, int outputBits, InputGenerator inputGenerator) {
        int[][] flips = new int[inputBits][outputBits];

        for (int iter = 0; iter < ITERATIONS; iter++) {
            byte[] input = inputGenerator.next();
            long hash = hash(input);

            for (int i = 0; i < inputBits; i++) {
                flip(input, i); // flip one bit
                long newHash = hash(input); // recompute the hash; half the bits should have flipped
                flip(input, i); // return to original

                long diff = hash ^ newHash;
                for (int o = 0; o < OUTPUT_BITS; o++) {
                    if ((diff & 1) == 1) {
                        flips[i][o] += 1;
                    }
                    diff >>>= 1;
                }
            }
        }

        return new AvalancheStats(flips);
    }

    private static void flip(byte[] input, int position) {
        int offset = position / 8;
        int bit = position & 7;
        input[offset] ^= (1 << bit);
    }

    @FunctionalInterface
    interface InputGenerator {
        byte[] next();
    }

    private static class RandomInputGenerator implements InputGenerator {
        private final Random random = Randomness.get();
        private final byte[] input;

        public RandomInputGenerator(int size) {
            input = new byte[size];
        }

        @Override
        public byte[] next() {
            random.nextBytes(input);
            return input;
        }
    }

    private static class AvalancheStats {
        private final int inputBits;
        private final int outputBits;
        private final double bias;
        private final double sumOfSquaredErrors;

        public AvalancheStats(int[][] flips) {
            this.inputBits = flips.length;
            this.outputBits = flips[0].length;
            double sumOfBiases = 0;
            double sumOfSquaredErrors = 0;

            for (int i = 0; i < inputBits; i++) {
                for (int o = 0; o < outputBits; o++) {
                    sumOfSquaredErrors += Math.pow(0.5 - ((double) flips[i][o] / ITERATIONS), 2);
                    sumOfBiases += 2 * ((double) flips[i][o] / ITERATIONS) - 1;
                }
            }

            this.bias = Math.abs(sumOfBiases / (inputBits * outputBits));
            this.sumOfSquaredErrors = sumOfSquaredErrors;
        }

        public double bias() {
            return bias;
        }

        public double diffusion() {
            return 1 - bias;
        }

        public double sumOfSquaredErrors() {
            return sumOfSquaredErrors;
        }

        @Override
        public String toString() {
            return String.format(
                Locale.ROOT,
                "AvalancheStats{inputBits=%d, outputBits=%d, bias=%.4f%%, diffusion=%.4f%%, sumOfSquaredErrors=%.2f}",
                inputBits,
                outputBits,
                bias() * 100,
                diffusion() * 100,
                sumOfSquaredErrors()
            );
        }
    }
}
