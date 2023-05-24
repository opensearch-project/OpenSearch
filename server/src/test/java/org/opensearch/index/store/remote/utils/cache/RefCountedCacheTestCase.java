/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.utils.cache;

import org.opensearch.test.OpenSearchTestCase;

abstract class RefCountedCacheTestCase extends OpenSearchTestCase {
    static final int CAPACITY = 100;

    private final RefCountedCache<String, Long> refCountedCache;

    protected RefCountedCacheTestCase(RefCountedCache<String, Long> refCountedCache) {
        this.refCountedCache = refCountedCache;
    }

    public void testBasicGetAndPutAndRemove() {
        assertNull(refCountedCache.get("1"));
        refCountedCache.put("1", 10L);
        assertEquals(10L, (long) refCountedCache.get("1"));
        refCountedCache.remove("1");
        assertNull(refCountedCache.get("1"));
    }

    public void testUsageWithIncrementAndDecrement() {
        refCountedCache.put("1", 10L);
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(10L, refCountedCache.usage().activeUsage());

        refCountedCache.decRef("1");
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());

        refCountedCache.incRef("1");
        assertEquals(10L, refCountedCache.usage().usage());
        assertEquals(10L, refCountedCache.usage().activeUsage());
    }

    public void testEviction() {
        for (int i = 1; i <= 5; i++) {
            final String key = Integer.toString(i);
            refCountedCache.put(key, 25L);
            refCountedCache.decRef(key);
        }
        assertNull(refCountedCache.get("1"));
        assertNull(refCountedCache.get("2"));
        assertNotNull(refCountedCache.get("3"));
        assertNotNull(refCountedCache.get("4"));
        assertNotNull(refCountedCache.get("5"));

        assertEquals(75L, refCountedCache.usage().usage());
        assertEquals(75L, refCountedCache.usage().activeUsage());
    }

    public void testComputeRemoveWhenExists() {
        refCountedCache.put("1", 25L);
        refCountedCache.decRef("1");
        assertEquals(0, refCountedCache.usage().activeUsage());
        assertEquals(25L, refCountedCache.usage().usage());

        assertNull(refCountedCache.compute("1", (k, v) -> null));
        assertNull(refCountedCache.get("1"));
        assertEquals(0, refCountedCache.usage().activeUsage());
        assertEquals(0L, refCountedCache.usage().usage());
    }

    public void testComputeRemoveWhenNotExists() {
        assertUsage(0, 0);
        assertNull(refCountedCache.compute("1", (k, v) -> null));
        assertNull(refCountedCache.get("1"));
        assertUsage(0, 0);
    }

    public void testComputeRemapExists() {
        assertUsage(0, 0);
        refCountedCache.put("1", 25L);
        refCountedCache.decRef("1");
        assertUsage(25, 0);

        final long newValue = refCountedCache.compute("1", (k, v) -> v + 5);
        assertEquals(30L, newValue);
        assertUsage(30, 30);

        refCountedCache.decRef("1");
        assertUsage(30, 0);

        assertEquals(30L, (long) refCountedCache.get("1"));
    }

    public void testComputeRemapNotExists() {
        assertUsage(0, 0);
        final long newValue = refCountedCache.compute("1", (k, v) -> 30L);
        assertEquals(30L, newValue);
        assertUsage(30, 30);

        refCountedCache.decRef("1");
        assertUsage(30, 0);

        assertEquals(30L, (long) refCountedCache.get("1"));
    }

    public void testActiveUsageGreaterThanCapacity() {
        for (int i = 1; i <= 5; i++) {
            final String key = Integer.toString(i);
            refCountedCache.put(key, 25L);
        }
        assertEquals(125L, refCountedCache.usage().usage());
        assertEquals(125L, refCountedCache.usage().activeUsage());
    }

    public void testReferenceCountingItemsThatDoNotExist() {
        assertNull(refCountedCache.get("1"));
        assertUsage(0, 0);
        refCountedCache.incRef("1");
        assertNull(refCountedCache.get("1"));
        assertUsage(0, 0);
        refCountedCache.decRef("1");
        assertNull(refCountedCache.get("1"));
        assertEquals(0L, refCountedCache.usage().usage());
        assertEquals(0L, refCountedCache.usage().activeUsage());
    }

    public void testPrune() {
        refCountedCache.put("1", 10L);
        refCountedCache.decRef("1");
        refCountedCache.put("2", 10L);
        refCountedCache.decRef("2");
        refCountedCache.put("3", 10L);

        assertEquals(20L, refCountedCache.prune());
        assertNull(refCountedCache.get("1"));
        assertNull(refCountedCache.get("2"));
        assertEquals(10L, (long) refCountedCache.get("3"));
    }

    public void testPruneWithPredicate() {
        refCountedCache.put("1", 10L);
        refCountedCache.decRef("1");
        refCountedCache.put("2", 10L);
        refCountedCache.decRef("2");
        refCountedCache.put("3", 10L);

        assertEquals(0L, refCountedCache.prune(path -> false));

        assertEquals(20L, refCountedCache.prune(path -> true));
        assertNull(refCountedCache.get("1"));
        assertNull(refCountedCache.get("2"));
        assertEquals(10L, (long) refCountedCache.get("3"));
    }

    public void testStats() {
        assertEquals(0, refCountedCache.stats().hitCount());
        refCountedCache.put("1", 1L);
        refCountedCache.get("1");
        assertEquals(1, refCountedCache.stats().hitCount());

        assertEquals(0, refCountedCache.stats().replaceCount());
        refCountedCache.put("1", 2L);
        assertEquals(1, refCountedCache.stats().replaceCount());
        assertEquals(1, refCountedCache.stats().hitCount());

        assertEquals(0, refCountedCache.stats().evictionCount());
        refCountedCache.put("2", 80L);
        refCountedCache.decRef("2");
        refCountedCache.put("3", 80L);
        assertEquals(1, refCountedCache.stats().evictionCount());

        assertEquals(0, refCountedCache.stats().missCount());
        assertNull(refCountedCache.get("2"));
        assertEquals(1, refCountedCache.stats().missCount());

        assertEquals(0, refCountedCache.stats().removeCount());
        refCountedCache.remove("3");
        assertEquals(1, refCountedCache.stats().removeCount());
    }

    public void testComputeStats() {
        refCountedCache.compute("1", (k, v) -> null);
        assertEquals(0, refCountedCache.stats().missCount());
        assertEquals(0, refCountedCache.stats().hitCount());
        assertEquals(0, refCountedCache.stats().replaceCount());
        assertEquals(0, refCountedCache.stats().removeCount());

        refCountedCache.compute("1", (k, v) -> 10L);
        assertEquals(1, refCountedCache.stats().missCount());
        assertEquals(0, refCountedCache.stats().hitCount());
        assertEquals(0, refCountedCache.stats().replaceCount());
        assertEquals(0, refCountedCache.stats().removeCount());

        refCountedCache.compute("1", (k, v) -> 10L);
        assertEquals(1, refCountedCache.stats().missCount());
        assertEquals(1, refCountedCache.stats().hitCount());
        assertEquals(0, refCountedCache.stats().replaceCount());
        assertEquals(0, refCountedCache.stats().removeCount());

        refCountedCache.compute("1", (k, v) -> 20L);
        assertEquals(1, refCountedCache.stats().missCount());
        assertEquals(2, refCountedCache.stats().hitCount());
        assertEquals(1, refCountedCache.stats().replaceCount());
        assertEquals(0, refCountedCache.stats().removeCount());

        refCountedCache.compute("1", (k, v) -> null);
        assertEquals(1, refCountedCache.stats().missCount());
        assertEquals(2, refCountedCache.stats().hitCount());
        assertEquals(1, refCountedCache.stats().replaceCount());
        assertEquals(1, refCountedCache.stats().removeCount());
    }

    private void assertUsage(long usage, long activeUsage) {
        assertEquals(usage, refCountedCache.usage().usage());
        assertEquals(activeUsage, refCountedCache.usage().activeUsage());
    }
}
