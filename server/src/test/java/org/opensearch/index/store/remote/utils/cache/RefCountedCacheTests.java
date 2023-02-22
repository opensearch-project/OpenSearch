/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.junit.Before;
import org.mockito.Mockito;
import org.opensearch.common.cache.RemovalListener;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RefCountedCacheTests extends OpenSearchTestCase {
    private static final long SIZE = 100;
    private RemovalListener<String, Long> removalListener;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        this.removalListener = Mockito.mock(RemovalListener.class);
    }

    public void testLRUCache() {
        executeRefCountedCacheTests(new LRUCache<>(SIZE, removalListener, value -> value));
    }

    public void testSegmentedCache() {
        executeRefCountedCacheTests(
            SegmentedCache.<String, Long>builder()
                .capacity(SIZE)
                .weigher(value -> value)
                .listener(removalListener)
                .concurrencyLevel(1)
                .build()
        );
    }

    void executeRefCountedCacheTests(RefCountedCache<String, Long> refCountedCache) {
        // basic get and put operation
        assertNull(refCountedCache.get("1"));
        refCountedCache.put("1", 10L);
        assertEquals(10L, (long) refCountedCache.get("1"));

        // cache usage with ref ++ and --
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());
        refCountedCache.incRef("1");
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(10L, refCountedCache.usage().activeUsage());
        refCountedCache.decRef("1");
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());

        // put all delegation
        Map<String, Long> toPutIntoCache = new HashMap<>() {
            {
                put("2", 20L);
                put("3", 30L);
            }
        };
        refCountedCache.putAll(toPutIntoCache);
        toPutIntoCache.forEach((k, v) -> { assertEquals(v, refCountedCache.get(k)); });
        assertEquals(60L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());

        // since all entries has ref count = 0 first added one will be evicted first once usage >= capacity
        refCountedCache.put("4", 40L);
        refCountedCache.put("5", 10L);
        assertNull(refCountedCache.get("1"));
        assertNull(refCountedCache.get("2"));
        Arrays.asList("3", "4", "5").forEach(k -> assertNotNull(refCountedCache.get(k)));
        assertEquals(80L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());

        // simple compute if present when present
        refCountedCache.computeIfPresent("3", (k, v) -> { return v + 5; });
        assertEquals(35L, (long) refCountedCache.get("3"));
        assertEquals(85L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());

        // simple compute if present when not present
        refCountedCache.computeIfPresent("1", (k, v) -> {
            fail("should not reach here");
            return v + 5;
        });
        assertNull(refCountedCache.get("1"));

        // inc ref all entries to prevent cache evictions
        refCountedCache.incRef("3");
        refCountedCache.incRef("4");
        refCountedCache.incRef("5");
        assertEquals(85L, refCountedCache.usage().usage());
        assertEquals(85L, refCountedCache.usage().activeUsage());

        // adding cache entry while > capacity won't put entry to cache
        refCountedCache.put("6", 15L);
        assertNull(refCountedCache.get("6"));
        assertEquals(85L, refCountedCache.usage().usage());
        assertEquals(85L, refCountedCache.usage().activeUsage());

        // dec ref to add 6 instead of 3
        refCountedCache.decRef("3");
        refCountedCache.put("6", 15L);
        assertNull(refCountedCache.get("3"));
        assertEquals(15L, (long) refCountedCache.get("6"));
        assertEquals(65L, refCountedCache.usage().usage());
        assertEquals(50L, refCountedCache.usage().activeUsage());

        // check stats
        assertEquals(4, refCountedCache.stats().evictionCount());
        assertEquals(9, refCountedCache.stats().hitCount());
        assertEquals(7, refCountedCache.stats().missCount());
        assertEquals(0, refCountedCache.stats().removeCount());
        assertEquals(1, refCountedCache.stats().replaceCount());

        // remove one entry
        refCountedCache.remove("6");
        assertNull(refCountedCache.get("6"));
        assertEquals(50L, refCountedCache.usage().usage());
        assertEquals(50L, refCountedCache.usage().activeUsage());
        assertEquals(1, refCountedCache.stats().removeCount());
    }
}
