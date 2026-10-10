/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.canmatch;

import org.opensearch.analytics.spi.ShardSortBounds;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Unit coverage for {@link TopNGate}. Every case is a wrong-results guard, not a performance one —
 * the gate's failure mode is dropping rows that belonged in the answer.
 */
public class TopNGateTests extends OpenSearchTestCase {

    private static final byte KIND = ShardSortBounds.VALUE_KIND_INT64;

    // ---- the bar, and who clears it ----

    /** The bar is the K-th best key, so the same probe is eliminable against a strong heap but not a weak one. */
    public void testEliminatesOnlyWhenTheBarIsStrongEnough() {
        TopNGate strong = new TopNGate(3, true);
        offerAll(strong, 100, 98, 95);
        assertEquals(95L, strong.bottom());
        assertTrue("Case A: max 80 is strictly below the bar 95", strong.canEliminate(bounds(0, 80)));

        TopNGate weak = new TopNGate(3, true);
        offerAll(weak, 100, 60, 55);
        assertEquals(55L, weak.bottom());
        assertFalse("Case B: max 80 beats the bar 55 — the shard owns rows in the answer", weak.canEliminate(bounds(0, 80)));
    }

    /** The comparison is strict: a shard whose max ties the bar must be kept, one unit past it is dropped. */
    public void testBoundaryIsStrictDescending() {
        TopNGate gate = new TopNGate(3, true);
        offerAll(gate, 100, 98, 95);

        assertTrue("one unit past the bar is a real loss", gate.canEliminate(bounds(0, 94)));
        assertFalse("max == bar must be kept", gate.canEliminate(bounds(0, 95)));
    }

    /** ASC reads the other end of the range — min, not max — and lower wins. Strictness is the same. */
    public void testAscendingUsesMinAndIsEquallyStrict() {
        TopNGate gate = new TopNGate(3, false);
        offerAll(gate, 10, 12, 15);

        assertEquals("ASC bar is the largest of the best three", 15L, gate.bottom());
        assertTrue("min 20 is strictly worse than the bar 15", gate.canEliminate(bounds(20, 30)));
        assertFalse("min 14 beats the bar", gate.canEliminate(bounds(14, 30)));
        assertFalse("min == bar must be kept", gate.canEliminate(bounds(15, 900)));
    }

    // ---- fail-open ----

    /** Until K keys exist there is no bar, so nothing can be shown to lose — however hopeless it looks. */
    public void testPartialHeapNeverEliminates() {
        ShardSortBounds hopeless = bounds(Long.MIN_VALUE, Long.MIN_VALUE + 1);

        assertFalse("an empty heap has no bar", new TopNGate(1, true).canEliminate(hopeless));

        TopNGate partial = new TopNGate(3, true);
        offerAll(partial, 100, 98);
        assertFalse("gate must not be armed on a partial heap", partial.isArmed());
        assertFalse(partial.canEliminate(hopeless));
    }

    /**
     * Two shards that can never be eliminated: one with no bounds, and one reporting nulls — {@code DESC}
     * maps to {@code NULLS FIRST}, so a null outranks every real value.
     */
    public void testUnjudgeableAndNullBearingShardsAreKept() {
        TopNGate gate = new TopNGate(3, true);
        offerAll(gate, 100, 98, 95);

        assertFalse("no bounds, no verdict", gate.canEliminate(null));
        assertFalse(
            "hasNulls vetoes elimination outright",
            gate.canEliminate(new ShardSortBounds(Long.MIN_VALUE, Long.MIN_VALUE + 1, true, KIND))
        );
    }

    // ---- monotonicity ----

    /** The bar only ever improves in either direction, so a verdict can't flip as more keys arrive. */
    public void testBottomIsMonotoneAsKeysArrive() {
        for (boolean descending : new boolean[] { true, false }) {
            TopNGate gate = new TopNGate(4, descending);
            long previous = descending ? Long.MIN_VALUE : Long.MAX_VALUE;
            for (long key : new long[] { 50, 90, 10, 70, 30, 100, 20, 80, 60, 40 }) {
                gate.offer(key);
                if (gate.isArmed()) {
                    long bottom = gate.bottom();
                    assertTrue(
                        "bar must not regress (descending=" + descending + "): " + bottom + " after " + previous,
                        descending ? bottom >= previous : bottom <= previous
                    );
                    previous = bottom;
                }
            }
            // Keys are 10..100 by tens: best four are 100/90/80/70 descending, 10/20/30/40 ascending.
            assertEquals(descending ? 70L : 40L, gate.bottom());
        }
    }

    /** Arrival order is network timing, so it must not affect any verdict: same keys shuffled, same verdicts. */
    public void testEliminationSetIsIndependentOfArrivalOrder() {
        List<Long> keys = new ArrayList<>();
        for (long i = 1; i <= 40; i++) {
            keys.add(i * 3);
        }
        List<ShardSortBounds> probes = List.of(bounds(0, 60), bounds(0, 90), bounds(0, 120), bounds(100, 200), bounds(0, 91));

        List<Boolean> expected = null;
        for (int trial = 0; trial < 25; trial++) {
            Collections.shuffle(keys, random());
            TopNGate gate = new TopNGate(5, true);
            for (long key : keys) {
                gate.offer(key);
            }
            assertEquals("top-5 of 3..120 by threes → bar is 108", 108L, gate.bottom());

            List<Boolean> verdicts = new ArrayList<>(probes.size());
            for (ShardSortBounds probe : probes) {
                verdicts.add(gate.canEliminate(probe));
            }
            if (expected == null) {
                expected = verdicts;
            } else {
                assertEquals("arrival order must not change any verdict", expected, verdicts);
            }
        }
    }

    // ---- heap mechanics ----

    /** Duplicates fill capacity like any other key — they don't collapse. */
    public void testDuplicateKeysConsumeCapacity() {
        TopNGate gate = new TopNGate(3, true);
        offerAll(gate, 42, 42, 42);

        assertTrue(gate.isArmed());
        assertEquals(42L, gate.bottom());
    }

    /** Keys worse than the bar are dropped, not admitted — the bar must not degrade. */
    public void testLosingKeysDoNotDisplaceTheTopK() {
        TopNGate gate = new TopNGate(3, true);
        offerAll(gate, 100, 98, 95);
        offerAll(gate, 1, 2, 3, -50);

        assertEquals("losers must not enter the heap", 95L, gate.bottom());
    }

    public void testCapacityOneTracksTheSingleBest() {
        TopNGate gate = new TopNGate(1, true);
        offerAll(gate, 5, 90, 40);

        assertEquals(90L, gate.bottom());
        assertTrue(gate.canEliminate(bounds(0, 89)));
    }

    /** Extremes are compared, never subtracted, so the full long range is usable. */
    public void testExtremeValuesDoNotOverflowComparisons() {
        TopNGate gate = new TopNGate(2, true);
        offerAll(gate, Long.MAX_VALUE, 0L);

        assertEquals(0L, gate.bottom());
        assertTrue(gate.canEliminate(bounds(Long.MIN_VALUE, -1L)));
        assertFalse(gate.canEliminate(bounds(Long.MIN_VALUE, 0L)));
    }

    public void testBottomBeforeArmedThrows() {
        TopNGate gate = new TopNGate(2, true);
        gate.offer(1L);

        expectThrows(IllegalStateException.class, gate::bottom);
    }

    public void testConstructorRejectsNonPositiveCapacity() {
        expectThrows(IllegalArgumentException.class, () -> new TopNGate(0, true));
        expectThrows(IllegalArgumentException.class, () -> new TopNGate(-1, true));
    }

    // ---- create() ----

    /** The spec's limit becomes the capacity and its direction carries over. */
    public void testCreateCarriesTheSpecsLimitAndDirection() {
        TopNGate gate = TopNGate.create(new SortSpec("@timestamp", true, 30));
        assertNotNull(gate);
        for (int i = 0; i < 29; i++) {
            gate.offer(i);
        }
        assertFalse("29 of 30 keys → not armed", gate.isArmed());
        gate.offer(29);
        assertTrue("the 30th key arms the gate", gate.isArmed());

        TopNGate ascending = TopNGate.create(new SortSpec("size", false, 2));
        ascending.offer(10);
        ascending.offer(20);
        assertTrue("ASC direction must survive create()", ascending.canEliminate(bounds(21, 99)));
    }

    /** No spec, or a limit past the cap whose heap realistic row counts can't fill: build no gate. */
    public void testCreateRefusesUngateableShapes() {
        assertNull(TopNGate.create(null));
        assertNull(TopNGate.create(new SortSpec("@timestamp", true, TopNGate.MAX_CAPACITY + 1)));
        assertNotNull(TopNGate.create(new SortSpec("@timestamp", true, TopNGate.MAX_CAPACITY)));
    }

    // ---- concurrency ----

    /**
     * Response threads offer keys while dispatch threads test, so concurrent access must leave the bar
     * exactly where a single-threaded run would.
     */
    public void testConcurrentOfferAndTestKeepsHeapConsistent() throws Exception {
        final int threads = 4;
        final int perThread = 500;
        TopNGate gate = new TopNGate(10, true);
        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            final int base = t * perThread;
            Thread worker = new Thread(() -> {
                for (int i = 0; i < perThread; i++) {
                    gate.offer(base + i);
                    // Interleave reads to exercise the shared critical section.
                    gate.canEliminate(bounds(0, base + i));
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join();
        }

        // Keys are 0..(threads*perThread - 1); the best ten are the top ten of that range.
        long expectedBottom = (long) threads * perThread - 10;
        assertEquals("concurrent offers must not corrupt the heap", expectedBottom, gate.bottom());
    }

    // ---- helpers ----

    private static void offerAll(TopNGate gate, long... keys) {
        for (long key : keys) {
            gate.offer(key);
        }
    }

    private static ShardSortBounds bounds(long min, long max) {
        return new ShardSortBounds(min, max, false, KIND);
    }
}
