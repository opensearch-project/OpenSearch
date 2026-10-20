/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.xcontent;

import org.opensearch.common.SuppressForbidden;
import org.opensearch.test.OpenSearchTestCase;

import tools.jackson.core.util.JsonRecyclerPools;

import static org.hamcrest.Matchers.sameInstance;

public class XContentRecyclerPoolsTests extends OpenSearchTestCase {

    private static final Object PROPERTY_LOCK = new Object();

    public void testDefaultRecyclerPoolIsThreadLocal() {
        assertThat(XContentRecyclerPools.recyclerPool(), sameInstance(JsonRecyclerPools.threadLocalPool()));
    }

    public void testRecyclerPoolOptions() {
        synchronized (PROPERTY_LOCK) { // sync for sys property
            final String previous = System.getProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY);
            try {
                setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, "thread_local");
                assertThat(XContentRecyclerPools.recyclerPool(), sameInstance(JsonRecyclerPools.threadLocalPool()));

                // defaultPool() returns a new non-shared ConcurrentDequePool instance per call
                setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, "default");
                assertTrue(XContentRecyclerPools.recyclerPool() instanceof JsonRecyclerPools.ConcurrentDequePool);

                setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, "concurrent_deque");
                assertTrue(XContentRecyclerPools.recyclerPool() instanceof JsonRecyclerPools.ConcurrentDequePool);

                setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, "shared_concurrent_deque");
                assertThat(XContentRecyclerPools.recyclerPool(), sameInstance(JsonRecyclerPools.sharedConcurrentDequePool()));

                setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, "unknown");
                expectThrows(IllegalArgumentException.class, () -> XContentRecyclerPools.recyclerPool());
            } finally {
                if (previous == null) {
                    clearSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY);
                } else {
                    setSystemProperty(XContentRecyclerPools.RECYCLER_POOL_PROPERTY, previous);
                }
            }
        }
    }

    @SuppressForbidden(reason = "Testing system property functionality")
    private String setSystemProperty(String key, String value) {
        return System.setProperty(key, value);
    }

    @SuppressForbidden(reason = "Testing system property functionality")
    private void clearSystemProperty(String key) {
        System.clearProperty(key);
    }
}
