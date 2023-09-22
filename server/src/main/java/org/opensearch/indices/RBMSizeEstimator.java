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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.indices;

/**
 * A class used to estimate roaring bitmap memory sizes (and hash set sizes).
 * Values based on experiments with adding randomly distributed integers, which matches the use case for KeyLookupStore.
 * In this use case, true values are much higher than an RBM's self-reported size, especially for small RBMs: see
 * https://github.com/RoaringBitmap/RoaringBitmap/issues/257
 */
public class RBMSizeEstimator {
    public static final int BYTES_IN_MB = 1048576;
    public static final double HASHSET_MEM_SLOPE = 6.46 * Math.pow(10, -5);
    protected final double slope;
    protected final double bufferMultiplier;
    protected final double intercept;

    RBMSizeEstimator(int modulo) {
        double[] memValues = calculateMemoryCoefficients(modulo);
        this.bufferMultiplier = memValues[0];
        this.slope = memValues[1];
        this.intercept = memValues[2];
    }

    public static double[] calculateMemoryCoefficients(int modulo) {
        // Sets up values to help estimate RBM size given a modulo
        // Returns an array of {bufferMultiplier, slope, intercept}

        double modifiedModulo;
        if (modulo == 0) {
            modifiedModulo = 32.0;
        } else {
            modifiedModulo = Math.log(modulo) / Math.log(2);
        }
        // we "round up" the modulo to the nearest tested value
        double highCutoff = 29.001; // Floating point makes 29 not work
        double mediumCutoff = 28.0;
        double lowCutoff = 26.0;
        double bufferMultiplier = 1.0;
        double slope;
        double intercept;
        if (modifiedModulo > highCutoff) {
            // modulo > 2^29
            bufferMultiplier = 1.2;
            slope = 0.637;
            intercept = 3.091;
        } else if (modifiedModulo > mediumCutoff) {
            // 2^29 >= modulo > 2^28
            slope = 0.619;
            intercept = 2.993;
        } else if (modifiedModulo > lowCutoff) {
            // 2^28 >= modulo > 2^26
            slope = 0.614;
            intercept = 2.905;
        } else {
            slope = 0.628;
            intercept = 2.603;
        }
        return new double[] { bufferMultiplier, slope, intercept };
    }

    public long getSizeInBytes(int numEntries) {
        // Based on a linear fit in log-log space, so that we minimize the error as a proportion rather than as
        // an absolute value. Should be within ~50% of the true value at worst, and should overestimate rather
        // than underestimate the memory usage
        return (long) ((long) Math.pow(numEntries, slope) * (long) Math.pow(10, intercept) * bufferMultiplier);
    }

    public int getNumEntriesFromSizeInBytes(long sizeInBytes) {
        // This function has some precision issues especially when composed with its inverse:
        // numEntries = getNumEntriesFromSizeInBytes(getSizeInBytes(numEntries))
        // In this case the result can be off by up to a couple percent
        // However, this shouldn't really matter as both functions are based on memory estimates with higher errors than a couple percent
        // and this composition won't happen outside of tests
        return (int) Math.pow(sizeInBytes / (bufferMultiplier * Math.pow(10, intercept)), 1 / slope);

    }

    public static long getSizeInBytesWithModulo(int numEntries, int modulo) {
        double[] memValues = calculateMemoryCoefficients(modulo);
        return (long) ((long) Math.pow(numEntries, memValues[1]) * (long) Math.pow(10, memValues[2]) * memValues[0]);
    }

    public static int getNumEntriesFromSizeInBytesWithModulo(long sizeInBytes, int modulo) {
        double[] memValues = calculateMemoryCoefficients(modulo);
        return (int) Math.pow(sizeInBytes / (memValues[0] * Math.pow(10, memValues[2])), 1 / memValues[1]);
    }


    protected static long convertMBToBytes(double valMB) {
        return (long) (valMB * BYTES_IN_MB);
    }

    protected static double convertBytesToMB(long valBytes) {
        return (double) valBytes / BYTES_IN_MB;
    }

    protected static long getHashsetMemSizeInBytes(int numEntries) {
        return convertMBToBytes(HASHSET_MEM_SLOPE * numEntries);
    }
}
