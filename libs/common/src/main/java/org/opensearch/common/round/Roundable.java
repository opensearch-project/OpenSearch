/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.round;

import org.opensearch.common.annotation.InternalApi;

/**
 * Interface to round-off values.
 *
 * @opensearch.internal
 */
@InternalApi
@FunctionalInterface
public interface Roundable {
    /**
     * Returns the greatest lower bound of the given key.
     * In other words, it returns the largest value such that {@code value <= key}.
     * @param key to floor
     * @return the floored value
     */
    long floor(long key);
}
