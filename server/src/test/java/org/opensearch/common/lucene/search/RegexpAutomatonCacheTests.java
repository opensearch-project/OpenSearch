/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.lucene.search;

import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.AutomatonProvider;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class RegexpAutomatonCacheTests extends OpenSearchTestCase {

    /**
     * Large enough that no test not specifically exercising size limits ever triggers eviction.
     */
    private static final long GENEROUS_MAX_BYTES = 10 * 1024 * 1024;

    /**
     * Each test isolates state on a fresh cache instance so the global singleton stays untouched.
     * The package-private constructor lets us construct one with a controlled max size in bytes.
     */
    private RegexpAutomatonCache newCache(long maxSizeBytes) {
        RegexpAutomatonCache cache = new RegexpAutomatonCache(maxSizeBytes);
        cache.setEnabled(true);
        return cache;
    }

    private RegexpAutomatonCache newCache() {
        return newCache(GENEROUS_MAX_BYTES);
    }

    /** Measures the compiled weight of a single pattern in isolation, for sizing test budgets. */
    private long weightOf(String pattern) {
        RegexpAutomatonCache probe = newCache(GENEROUS_MAX_BYTES);
        return probe.getCompiledAutomaton(pattern, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT).ramBytesUsed();
    }

    public void testReturnsAutomatonEquivalentToDirectConstruction() {
        RegexpAutomatonCache cache = newCache();
        String pattern = "foo.*bar";
        Automaton expected = Operations.determinize(
            new RegExp(pattern, RegExp.ALL, 0).toAutomaton(RegexpQuery.DEFAULT_PROVIDER),
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT
        );
        CompiledAutomaton compiled = cache.getCompiledAutomaton(
            pattern,
            RegExp.ALL,
            0,
            Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
            RegexpQuery.DEFAULT_PROVIDER
        );
        assertNotNull(compiled);
        assertNotNull(compiled.runAutomaton);
        byte[] b1 = "fooXbar".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(compiled.runAutomaton.run(b1, 0, b1.length));
        byte[] b2 = "foobar".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertTrue(compiled.runAutomaton.run(b2, 0, b2.length));
        byte[] b3 = "fooba".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertFalse(compiled.runAutomaton.run(b3, 0, b3.length));
        byte[] b4 = "hello".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        assertFalse(compiled.runAutomaton.run(b4, 0, b4.length));
    }

    public void testSecondGetReturnsSameCompiledAutomatonInstance() {
        RegexpAutomatonCache cache = newCache();
        CompiledAutomaton first = cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        CompiledAutomaton second = cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertSame(first, second);
        assertEquals(1, cache.count());
        assertEquals(1, cache.hits());
        assertEquals(1, cache.misses());
    }

    public void testDifferentFlagsProduceDistinctEntries() {
        RegexpAutomatonCache cache = newCache();
        Automaton a = cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        Automaton b = cache.getAutomaton("foo.*", RegExp.ALL, RegExp.ASCII_CASE_INSENSITIVE, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertNotSame(a, b);
        assertEquals(2, cache.count());
        assertEquals(2, cache.misses());
        assertEquals(0, cache.hits());
    }

    public void testDifferentPatternsProduceDistinctEntries() {
        RegexpAutomatonCache cache = newCache();
        Automaton a = cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        Automaton b = cache.getAutomaton("bar.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertNotSame(a, b);
        assertEquals(2, cache.count());
    }

    public void testDisabledBypassesCache() {
        RegexpAutomatonCache cache = newCache();
        cache.setEnabled(false);
        Automaton a = cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        Automaton b = cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertNotSame(a, b);
        assertEquals(0, cache.count());
    }

    public void testDisablingClearsExistingEntries() {
        RegexpAutomatonCache cache = newCache();
        cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertEquals(1, cache.count());
        cache.setEnabled(false);
        assertEquals(0, cache.count());
        // Re-enabling should leave the cache empty (clean start).
        cache.setEnabled(true);
        assertEquals(0, cache.count());
    }

    public void testDisablingSwapsCacheInsteadOfInvalidating() {
        RegexpAutomatonCache cache = newCache();
        cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // hit
        assertEquals(1, cache.hits());
        assertEquals(1, cache.misses());

        // Disabling swaps in a fresh empty cache, but cumulative stats are preserved
        // via the persistent accumulators.
        cache.setEnabled(false);
        assertEquals(0, cache.count());
        assertEquals(1, cache.hits());
        assertEquals(1, cache.misses());

        cache.setEnabled(true);
        assertEquals(0, cache.count());
        assertEquals(1, cache.hits());
    }

    public void testCustomProviderBypassesCache() {
        RegexpAutomatonCache cache = newCache();
        // Non-default provider should not be cached because named-automaton substitutions may
        // change the resulting automaton even for the same pattern + flags.
        AutomatonProvider provider = name -> null;
        cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, provider);
        cache.getAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, provider);
        assertEquals(0, cache.count());
    }

    public void testInvalidPatternThrowsAndIsNotCached() {
        RegexpAutomatonCache cache = newCache();
        // `[` without a matching `]` is invalid regex syntax.
        expectThrows(
            IllegalArgumentException.class,
            () -> cache.getAutomaton("[abc", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT)
        );
        assertEquals(0, cache.count());
    }

    public void testResizePreservesWarmEntries() {
        RegexpAutomatonCache cache = newCache();
        CompiledAutomaton before = cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertEquals(1, cache.count());
        cache.resize(GENEROUS_MAX_BYTES * 2);
        assertEquals(1, cache.count());
        // The cached entry should still be reachable after resize — same object, not recompiled.
        CompiledAutomaton after = cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertSame(before, after);
    }

    public void testResizeDownEvictsLruEntries() {
        RegexpAutomatonCache cache = newCache();
        // Fill cache with 4 entries
        for (int i = 0; i < 4; i++) {
            cache.getAutomaton("pattern" + i, RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        }
        assertEquals(4, cache.count());

        // Resize down to a budget sized for exactly two single-character automatons (identical
        // compiled weight) -- warm entries are copied, but the new cache's eviction trims to fit.
        long singleCharWeight = weightOf("a");
        cache.resize(singleCharWeight * 2);
        assertTrue("expected at most 2 entries after resize-down, got " + cache.count(), cache.count() <= 2);

        // New insertions still respect the smaller limit.
        cache.getAutomaton("a", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        cache.getAutomaton("b", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        cache.getAutomaton("c", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertEquals(2, cache.count());
    }

    public void testMaxSizeSettingIsDynamic() {
        assertTrue("CACHE_MAX_SIZE_SETTING should be dynamic", RegexpAutomatonCache.CACHE_MAX_SIZE_SETTING.isDynamic());
    }

    public void testEvictsBeyondMaxSize() {
        // Budget sized for exactly two of these (identically-shaped, single-character) automatons.
        long singleCharWeight = weightOf("a");
        RegexpAutomatonCache cache = newCache(singleCharWeight * 2);
        cache.getAutomaton("a", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        cache.getAutomaton("b", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        cache.getAutomaton("c", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        // With a two-entry budget and three distinct patterns, at least one must have been evicted.
        assertEquals(2, cache.count());
        assertTrue("expected at least one eviction, got " + cache.evictions(), cache.evictions() >= 1);
    }

    public void testRamBytesUsedTracksCacheWeight() {
        RegexpAutomatonCache cache = newCache();
        assertEquals(0, cache.ramBytesUsed());
        CompiledAutomaton compiled = cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        assertEquals(compiled.ramBytesUsed(), cache.ramBytesUsed());
    }

    public void testMaxSizeBytesReflectsConfiguredLimit() {
        RegexpAutomatonCache cache = newCache(12345L);
        assertEquals(12345L, cache.maxSizeBytes());
        cache.resize(67890L);
        assertEquals(67890L, cache.maxSizeBytes());
    }

    public void testMetricsPreservedAcrossResize() {
        RegexpAutomatonCache cache = newCache();
        cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // miss
        cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // hit
        cache.getCompiledAutomaton("bar.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // miss
        assertEquals(1, cache.hits());
        assertEquals(2, cache.misses());

        cache.resize(GENEROUS_MAX_BYTES * 2);

        // Hits/misses accumulated before the resize are preserved.
        assertEquals(1, cache.hits());
        assertEquals(2, cache.misses());

        // New activity after resize continues accumulating on top.
        cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // hit (warm entry survived)
        assertEquals(2, cache.hits());
        assertEquals(2, cache.misses());
    }

    public void testMetricsPreservedAcrossDisable() {
        RegexpAutomatonCache cache = newCache();
        cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // miss
        cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // hit
        assertEquals(1, cache.hits());
        assertEquals(1, cache.misses());

        cache.setEnabled(false);
        assertEquals(1, cache.hits());
        assertEquals(1, cache.misses());

        cache.setEnabled(true);
        cache.getCompiledAutomaton("baz.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT); // miss
        assertEquals(1, cache.hits());
        assertEquals(2, cache.misses());
    }

    public void testConcurrentLoadOfSameKeyComputesOnce() throws Exception {
        RegexpAutomatonCache cache = newCache();
        int threads = 16;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        List<AtomicReference<CompiledAutomaton>> results = new ArrayList<>();
        try {
            for (int i = 0; i < threads; i++) {
                AtomicReference<CompiledAutomaton> result = new AtomicReference<>();
                results.add(result);
                pool.submit(() -> {
                    try {
                        start.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    result.set(cache.getCompiledAutomaton("foo.*", RegExp.ALL, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT));
                });
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(30, TimeUnit.SECONDS));
        } finally {
            if (pool.isTerminated() == false) {
                pool.shutdownNow();
            }
        }
        CompiledAutomaton canonical = results.get(0).get();
        assertNotNull(canonical);
        for (AtomicReference<CompiledAutomaton> result : results) {
            assertSame(canonical, result.get());
        }
        assertEquals(1, cache.count());
    }

    public void testGlobalInstanceIsReachable() {
        // Smoke test the singleton; toggling enabled is reversible and must not throw.
        RegexpAutomatonCache instance = RegexpAutomatonCache.getInstance();
        boolean originalEnabled = instance.isEnabled();
        try {
            instance.setEnabled(true);
            assertTrue(instance.isEnabled());
            instance.setEnabled(false);
            assertFalse(instance.isEnabled());
        } finally {
            instance.setEnabled(originalEnabled);
        }
    }
}
