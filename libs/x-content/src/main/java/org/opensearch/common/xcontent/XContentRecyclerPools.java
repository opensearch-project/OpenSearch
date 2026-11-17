/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.annotation.InternalApi;

import tools.jackson.core.util.BufferRecycler;
import tools.jackson.core.util.JsonRecyclerPools;
import tools.jackson.core.util.RecyclerPool;

/**
 * Resolves the Jackson {@link RecyclerPool} used by the XContent factories.
 *
 * @opensearch.internal
 */
@InternalApi
public final class XContentRecyclerPools {
    public static final String RECYCLER_POOL_PROPERTY = "opensearch.xcontent.recycler.pool";

    private XContentRecyclerPools() {}

    public static RecyclerPool<BufferRecycler> recyclerPool() {
        // Jackson 3 defaults to a concurrent-deque pool that contends under concurrent bulk indexing,
        // so prefer the thread-local pool by default.
        final String value = System.getProperty(RECYCLER_POOL_PROPERTY, "thread_local");
        switch (value) {
            case "thread_local":
                return JsonRecyclerPools.threadLocalPool();
            case "shared_concurrent_deque":
                return JsonRecyclerPools.sharedConcurrentDequePool();
            case "default":
            case "concurrent_deque":
                return JsonRecyclerPools.defaultPool();
            default:
                throw new IllegalArgumentException("Unknown value [" + value + "] for property [" + RECYCLER_POOL_PROPERTY + "]");
        }
    }
}
