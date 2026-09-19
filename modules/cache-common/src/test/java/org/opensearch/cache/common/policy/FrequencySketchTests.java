/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.common.policy;

import org.opensearch.test.OpenSearchTestCase;

public class FrequencySketchTests extends OpenSearchTestCase {

    public void testFrequencyCountsIncrements() {
        FrequencySketch<String> sketch = new FrequencySketch<>(1024);
        for (int i = 0; i < 5; i++) {
            sketch.increment("a");
        }
        assertEquals(5, sketch.frequency("a"));
    }

    public void testUntouchedKeyIsZero() {
        FrequencySketch<String> sketch = new FrequencySketch<>(1024);
        sketch.increment("a");
        // With a 1024-slot table and four hashes, an unrelated key colliding on all four counters is vanishingly
        // unlikely, so a never-incremented key reads back as 0.
        assertEquals(0, sketch.frequency("some-other-key"));
    }

    public void testCounterSaturatesAt15() {
        FrequencySketch<String> sketch = new FrequencySketch<>(1024);
        for (int i = 0; i < 100; i++) {
            sketch.increment("hot");
        }
        assertEquals(15, sketch.frequency("hot"));
    }

    public void testResetHalvesCounters() {
        FrequencySketch<String> sketch = new FrequencySketch<>(1024);
        for (int i = 0; i < 100; i++) {
            sketch.increment("hot");
        }
        assertEquals(15, sketch.frequency("hot"));
        sketch.reset();
        assertEquals(7, sketch.frequency("hot")); // 15 >>> 1
    }

    public void testAutomaticAgingKeepsFrequencyBounded() {
        // Small capacity -> small sampleSize -> aging triggers within the loop, so frequency never runs away.
        FrequencySketch<String> sketch = new FrequencySketch<>(8);
        for (int i = 0; i < 100_000; i++) {
            sketch.increment("hot");
        }
        assertTrue("saturating counter must stay within 4-bit range", sketch.frequency("hot") <= 15);
        assertTrue("a frequently accessed key should retain a positive estimate", sketch.frequency("hot") > 0);
    }

    public void testDoorkeeperDistinguishesRepeatFromOneTimer() {
        FrequencySketch<String> sketch = new FrequencySketch<>(4096);
        sketch.increment("repeat");
        sketch.increment("repeat"); // seen twice
        sketch.increment("oneTimer"); // seen once
        int minFrequencyToAdmit = 2;
        assertTrue("repeated key should be admitted", sketch.frequency("repeat") >= minFrequencyToAdmit);
        assertFalse("one-timer should be rejected", sketch.frequency("oneTimer") >= minFrequencyToAdmit);
    }
}
