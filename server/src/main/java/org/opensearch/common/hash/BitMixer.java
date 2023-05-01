/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

/**
 * A bit-mixer (also known as a "finalizer") takes an intermediate hash value
 * and thoroughly mixes it to increase its entropy. This improves the distribution
 * and reduces collisions among hashes.
 */
public interface BitMixer {

    /**
     * Mixes and returns a 32-bit value.
     *
     * @param value the input value to be mixed
     * @return value after being mixed
     */
    default int mix(int value) {
        throw new UnsupportedOperationException("not implemented");
    }

    /**
     * Mixes and returns a 64-bit value.
     *
     * @param value the input value to be mixed
     * @return value after being mixed
     */
    default long mix(long value) {
        throw new UnsupportedOperationException("not implemented");
    }
}
