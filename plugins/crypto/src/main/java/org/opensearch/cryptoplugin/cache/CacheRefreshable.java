/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.cryptoplugin.cache;

public interface CacheRefreshable {
    /**
     * @return true/false on the basis of whether refresh should be called
     */
    boolean shouldRefresh();

    /**
     * Refreshes the cache if shouldRefresh returns true.
     */
    void refresh();
}
