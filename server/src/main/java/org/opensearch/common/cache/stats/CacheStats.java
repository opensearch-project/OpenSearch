/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

/**
 * Interface for any cache specific stats.
 * TODO: Add rest of stats like hits/misses.
 */
public interface CacheStats {
    // Provides the number of entries in cache.
    long count();
}
