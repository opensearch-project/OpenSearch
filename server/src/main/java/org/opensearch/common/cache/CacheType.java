/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

/**
 * Cache types available to be integrated.
 */
public enum CacheType {

    ON_HEAP("on_heap");
    // TODO: Uncomment once Tiered caching is complete.
    // TIERED("tiered");

    private final String cacheType;

    CacheType(String cacheType) {
        this.cacheType = cacheType;
    }

    public String getCacheType() {
        return cacheType;
    }
}
