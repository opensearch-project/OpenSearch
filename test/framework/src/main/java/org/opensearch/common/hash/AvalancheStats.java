/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

import java.util.Locale;

/**
 * Represents the avalanche statistics of a hash function.
 */
public class AvalancheStats {
    private final int inputBits;
    private final int outputBits;
    private final double bias;
    private final double sumOfSquaredErrors;

    public AvalancheStats(int[][] flips, int iterations) {
        this.inputBits = flips.length;
        this.outputBits = flips[0].length;
        double sumOfBiases = 0;
        double sumOfSquaredErrors = 0;

        for (int i = 0; i < inputBits; i++) {
            for (int o = 0; o < outputBits; o++) {
                sumOfSquaredErrors += Math.pow(0.5 - ((double) flips[i][o] / iterations), 2);
                sumOfBiases += 2 * ((double) flips[i][o] / iterations) - 1;
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
