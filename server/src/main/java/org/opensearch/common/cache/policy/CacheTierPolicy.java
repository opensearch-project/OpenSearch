/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.policy;

/**
 * An interface for policies that inspect data of type T to decide whether they are admitted into a cache tier.
 */
public interface CacheTierPolicy<T> {
    /**
     * Determines whether this policy allows the data into its cache tier.
     * @param data The data to check
     * @return true if accepted, otherwise false
     */
    boolean checkData(T data);
}
