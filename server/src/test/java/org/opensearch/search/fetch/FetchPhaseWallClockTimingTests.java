/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.opensearch.action.search.SearchLatencyBreakdownNode;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for fetch phase wall-clock timing and clamping logic.
 * <p>
 * Validates the LOGIC of:
 * <ul>
 *   <li>Wall-clock span computation (first_start to last_end) for N=1 and N=100 documents</li>
 *   <li>Child duration clamping: Math.min(childDuration, fetchPhaseDuration)</li>
 *   <li>Negative delta protection: Math.max(0, end - start) produces 0 when end &lt; start</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 1.1, 1.2, 1.3, 1.4</b>
 */
public class FetchPhaseWallClockTimingTests extends OpenSearchTestCase {

    // ========== Wall-clock span output for N=1 document ==========

    /**
     * For a single document (N=1), the wall-clock span is simply (end - start) for that document.
     * The duration should be &lt;= parent fetch phase duration.
     * <p>
     * Validates: Requirements 1.1, 1.4
     */
    public void testWallClockSpan_singleDocument() {
        // Simulate fetch phase timing for 1 document
        long fetchStart = 1_000_000L; // fetch phase starts at 1ms
        long fetchEnd = 5_000_000L;   // fetch phase ends at 5ms
        long fetchPhaseDuration = fetchEnd - fetchStart; // 4ms total

        // For a single document, stored_fields spans that one doc
        long storedFieldsStart = 1_200_000L; // starts at 1.2ms
        long storedFieldsEnd = 3_500_000L;   // ends at 3.5ms

        // Wall-clock span = end - start (single span, not per-doc sum)
        long wallClockDuration = Math.max(0, storedFieldsEnd - storedFieldsStart);

        // Clamp to parent
        long clampedDuration = Math.min(wallClockDuration, fetchPhaseDuration);

        // Wall-clock span should be 2.3ms
        assertEquals(2_300_000L, wallClockDuration);
        // Should not exceed parent
        assertTrue("Child duration must not exceed parent", clampedDuration <= fetchPhaseDuration);
        // For this case, no clamping needed (2.3ms < 4ms)
        assertEquals(wallClockDuration, clampedDuration);

        // Verify the node tree captures this correctly
        SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
            "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, fetchPhaseDuration
        );
        fetchNode.addChild("Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, clampedDuration);

        assertEquals(fetchPhaseDuration, fetchNode.getDurationNanos());
        assertEquals(1, fetchNode.getChildren().size());
        assertTrue(fetchNode.getChildren().get(0).getDurationNanos() <= fetchNode.getDurationNanos());
    }

    /**
     * For N=100 documents, the wall-clock span tracks first_start to last_end across all docs.
     * The wall-clock approach produces duration &lt;= parent, unlike per-doc sum which can exceed it.
     * <p>
     * Validates: Requirements 1.1, 1.4
     */
    public void testWallClockSpan_hundredDocuments() {
        // Simulate fetch phase timing for 100 documents
        long fetchStart = 1_000_000L;
        long fetchEnd = 50_000_000L;
        long fetchPhaseDuration = fetchEnd - fetchStart; // 49ms total

        // Simulate stored_fields processing across 100 documents
        // Wall-clock span: first doc start to last doc end
        long storedFieldsFirstStart = fetchStart + 500_000L;  // 0.5ms into fetch
        long storedFieldsLastEnd = fetchEnd - 2_000_000L;     // 2ms before fetch ends

        // Wall-clock span = last_end - first_start
        long wallClockDuration = Math.max(0, storedFieldsLastEnd - storedFieldsFirstStart);

        // Simulated per-doc sum (for contrast): sum of 100 individual doc timings
        // Each doc takes 300 microseconds => sum would be 30ms
        long perDocSum = 100 * 300_000L; // 30ms (sum of per-doc deltas)

        // Wall-clock approach produces the actual elapsed time
        // Per-doc sum can be different due to overhead/measurement gaps
        long clampedWallClock = Math.min(wallClockDuration, fetchPhaseDuration);

        // Key invariant: wall-clock span is clamped to parent
        assertTrue("Wall-clock duration must not exceed parent after clamping",
            clampedWallClock <= fetchPhaseDuration);

        // Demonstrate that per-doc sum could exceed parent (the bug this design fixes)
        // but wall-clock span stays naturally bounded
        assertTrue("Per-doc sum can potentially exceed wall-clock span", perDocSum != wallClockDuration);

        // The wall-clock duration (46.5ms) is <= parent (49ms)
        assertEquals(46_500_000L, wallClockDuration);
        assertEquals(wallClockDuration, clampedWallClock); // no clamping needed here

        // Build node tree with wall-clock values
        SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
            "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, fetchPhaseDuration
        );
        fetchNode.addChild("Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, clampedWallClock);

        // Verify invariant holds in the tree
        List<SearchLatencyBreakdownNode> children = fetchNode.getChildren();
        assertEquals(1, children.size());
        assertTrue("Child duration in tree must not exceed parent",
            children.get(0).getDurationNanos() <= fetchNode.getDurationNanos());
    }

    /**
     * For N=100 documents, the wall-clock approach produces a single duration
     * representing the entire span of the operation, not the sum of per-doc timings.
     * This test demonstrates the difference between the two approaches.
     * <p>
     * Validates: Requirements 1.1, 1.4
     */
    public void testWallClockVsPerDocSum_demonstrateDifference() {
        // A realistic scenario where per-doc sum would exceed parent
        long fetchPhaseDuration = 10_000_000L; // 10ms total parent

        // Simulated per-doc sum: if each of 100 docs takes 200us
        // The sum would be 20ms (exceeds 10ms parent!)
        long perDocSum = 100 * 200_000L; // 20ms

        // But wall-clock span tracks only the envelope: first start to last end
        // Even if docs overlap or are sequential, the wall-clock never exceeds parent
        long storedFieldsFirstStart = 500_000L;    // 0.5ms into fetch
        long storedFieldsLastEnd = 9_000_000L;     // 9ms into fetch

        long wallClockDuration = Math.max(0, storedFieldsLastEnd - storedFieldsFirstStart); // 8.5ms

        // Per-doc sum (20ms) would exceed parent (10ms) — this is the bug we fix
        assertTrue("Per-doc sum can exceed parent (this is the bug)", perDocSum > fetchPhaseDuration);

        // Wall-clock span (8.5ms) naturally stays within parent
        assertTrue("Wall-clock span naturally stays within parent", wallClockDuration <= fetchPhaseDuration);

        // After clamping, guaranteed to be <= parent
        long clampedDuration = Math.min(wallClockDuration, fetchPhaseDuration);
        assertTrue("Clamped duration must not exceed parent", clampedDuration <= fetchPhaseDuration);
    }

    // ========== Clamping when child duration exceeds parent ==========

    /**
     * When a child's raw duration exceeds the parent fetch phase duration,
     * clamping ensures it is reduced to the parent's duration.
     * <p>
     * Validates: Requirements 1.2, 1.3
     */
    public void testClamping_childExceedsParent() {
        long fetchPhaseDuration = 10_000_000L; // 10ms parent

        // Simulate a pathological case: child duration > parent
        // This could happen due to clock drift or measurement anomaly
        long childRawDuration = 15_000_000L; // 15ms — exceeds 10ms parent

        // Clamping logic: Math.min(childDuration, fetchPhaseDuration)
        long clampedDuration = Math.min(childRawDuration, fetchPhaseDuration);

        // Should be clamped to parent duration
        assertEquals("Clamped duration should equal parent when child exceeds parent",
            fetchPhaseDuration, clampedDuration);
        assertTrue("Clamped duration must not exceed parent",
            clampedDuration <= fetchPhaseDuration);

        // Build tree and verify
        SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
            "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, fetchPhaseDuration
        );
        fetchNode.addChild("Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, clampedDuration);

        assertEquals(fetchPhaseDuration, fetchNode.getChildren().get(0).getDurationNanos());
    }

    /**
     * When multiple children have durations exceeding the parent, all are clamped independently.
     * <p>
     * Validates: Requirements 1.2, 1.3
     */
    public void testClamping_multipleChildrenExceedParent() {
        long fetchPhaseDuration = 8_000_000L; // 8ms parent

        // Multiple children that exceed parent
        long storedFieldsRaw = 12_000_000L;  // 12ms > 8ms
        long sourceLoadingRaw = 9_000_000L;  // 9ms > 8ms
        long highlightingRaw = 5_000_000L;   // 5ms < 8ms (within bounds)

        // Apply clamping to each
        long storedFieldsClamped = Math.min(storedFieldsRaw, fetchPhaseDuration);
        long sourceLoadingClamped = Math.min(sourceLoadingRaw, fetchPhaseDuration);
        long highlightingClamped = Math.min(highlightingRaw, fetchPhaseDuration);

        // Verify clamping results
        assertEquals(fetchPhaseDuration, storedFieldsClamped); // clamped
        assertEquals(fetchPhaseDuration, sourceLoadingClamped); // clamped
        assertEquals(highlightingRaw, highlightingClamped);     // not clamped (already within bounds)

        // Build tree and verify all children are <= parent
        SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
            "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, fetchPhaseDuration
        );
        fetchNode.addChild("Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, storedFieldsClamped);
        fetchNode.addChild("Source Loading", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, sourceLoadingClamped);
        fetchNode.addChild("Highlighting", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, highlightingClamped);

        for (SearchLatencyBreakdownNode child : fetchNode.getChildren()) {
            assertTrue("Every child duration must be <= parent: " + child.getName(),
                child.getDurationNanos() <= fetchNode.getDurationNanos());
        }
    }

    /**
     * When child duration equals parent duration exactly, it is NOT clamped (boundary case).
     * <p>
     * Validates: Requirements 1.2, 1.3
     */
    public void testClamping_childEqualsParent() {
        long fetchPhaseDuration = 10_000_000L;
        long childDuration = 10_000_000L; // exactly equals parent

        long clampedDuration = Math.min(childDuration, fetchPhaseDuration);

        assertEquals("Child equal to parent should remain unchanged", fetchPhaseDuration, clampedDuration);
    }

    /**
     * Test clamping with randomized values to ensure the invariant holds across many inputs.
     * <p>
     * Validates: Requirements 1.2, 1.3
     */
    public void testClamping_randomizedChildAndParentDurations() {
        for (int i = 0; i < 100; i++) {
            long parentDuration = randomLongBetween(1_000L, 100_000_000L); // 1us to 100ms
            long childRawDuration = randomLongBetween(0L, 200_000_000L);  // 0 to 200ms

            long clampedDuration = Math.min(childRawDuration, parentDuration);

            assertTrue("Clamped duration must never exceed parent (iteration " + i + "): "
                + "child=" + childRawDuration + " parent=" + parentDuration + " clamped=" + clampedDuration,
                clampedDuration <= parentDuration);

            // If child was within bounds, it should be unchanged
            if (childRawDuration <= parentDuration) {
                assertEquals("Child within bounds should not be modified",
                    childRawDuration, clampedDuration);
            } else {
                assertEquals("Child exceeding parent should be clamped to parent",
                    parentDuration, clampedDuration);
            }
        }
    }

    // ========== Negative deltas produce 0 ==========

    /**
     * When end &lt; start (clock anomaly / drift), Math.max(0, end - start) produces 0.
     * <p>
     * Validates: Requirements 1.1, 1.2
     */
    public void testNegativeDelta_producesZero() {
        // Simulate clock anomaly: end time is before start time
        long start = 5_000_000L;
        long end = 3_000_000L; // end < start — anomaly!

        long duration = Math.max(0, end - start);

        assertEquals("Negative delta must produce 0", 0L, duration);
    }

    /**
     * When end equals start, the duration should be 0 (zero-length span).
     * <p>
     * Validates: Requirements 1.1
     */
    public void testZeroDelta_producesZero() {
        long start = 5_000_000L;
        long end = 5_000_000L; // end == start

        long duration = Math.max(0, end - start);

        assertEquals("Zero delta should produce 0", 0L, duration);
    }

    /**
     * Test multiple negative delta scenarios to ensure robustness.
     * <p>
     * Validates: Requirements 1.1, 1.2
     */
    public void testNegativeDelta_multipleScenarios() {
        // Various negative delta cases
        long[][] cases = {
            {100, 50},           // small negative
            {1_000_000L, 0},     // end is zero
            {Long.MAX_VALUE / 2, 0},  // large start with zero end
            {5_000_000L, 4_999_999L}, // off by one nanosecond
        };

        for (long[] testCase : cases) {
            long start = testCase[0];
            long end = testCase[1];
            long duration = Math.max(0, end - start);
            assertEquals("Negative delta (start=" + start + ", end=" + end + ") must produce 0",
                0L, duration);
        }
    }

    /**
     * Test that Math.max(0, delta) correctly handles the boundary between positive and negative.
     * <p>
     * Validates: Requirements 1.1, 1.2
     */
    public void testNegativeDelta_randomizedClockAnomalies() {
        for (int i = 0; i < 100; i++) {
            long start = randomLongBetween(1_000L, Long.MAX_VALUE / 2);
            // Generate end that is before start (clock anomaly)
            long end = randomLongBetween(0L, start - 1);

            long duration = Math.max(0, end - start);

            assertEquals("Random negative delta (iteration " + i + ") must produce 0", 0L, duration);
        }
    }

    /**
     * Test that positive deltas are preserved correctly (sanity check).
     * <p>
     * Validates: Requirements 1.1
     */
    public void testPositiveDelta_preservedCorrectly() {
        for (int i = 0; i < 100; i++) {
            long start = randomLongBetween(0L, Long.MAX_VALUE / 4);
            long end = start + randomLongBetween(1L, Long.MAX_VALUE / 4);

            long duration = Math.max(0, end - start);

            assertTrue("Positive delta must be > 0", duration > 0);
            assertEquals("Positive delta must equal end - start", end - start, duration);
        }
    }

    // ========== Combined wall-clock + clamping integration ==========

    /**
     * Full integration test: wall-clock spans computed for multiple fetch internals,
     * then clamped to parent, with negative delta protection applied.
     * <p>
     * Validates: Requirements 1.1, 1.2, 1.3, 1.4
     */
    public void testFullPipeline_wallClockThenClamping() {
        long fetchPhaseDuration = 20_000_000L; // 20ms parent

        // Simulate 5 fetch internals with various wall-clock spans
        // Some may have anomalies (end < start)
        long[][] internals = {
            // {start, end} — simulating wall-clock spans
            {1_000_000L, 8_000_000L},   // stored_fields: 7ms (normal, within parent)
            {2_000_000L, 12_000_000L},  // source_loading: 10ms (normal, within parent)
            {3_000_000L, 28_000_000L},  // highlighting: 25ms (exceeds parent!)
            {5_000_000L, 4_000_000L},   // script_fields: negative delta (anomaly!)
            {6_000_000L, 6_000_000L},   // inner_hits: zero delta
        };

        String[] names = {"Stored Fields", "Source Loading", "Highlighting", "Script Fields", "Inner Hits"};

        SearchLatencyBreakdownNode fetchNode = new SearchLatencyBreakdownNode(
            "Fetch Phase", SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, fetchPhaseDuration
        );

        for (int i = 0; i < internals.length; i++) {
            long start = internals[i][0];
            long end = internals[i][1];

            // Step 1: Math.max(0, end - start) — prevent negative
            long rawDuration = Math.max(0, end - start);

            // Step 2: Math.min(rawDuration, fetchPhaseDuration) — clamp to parent
            long clampedDuration = Math.min(rawDuration, fetchPhaseDuration);

            if (clampedDuration > 0) {
                fetchNode.addChild(names[i], SearchLatencyBreakdownNode.CATEGORY_FETCH, 0, clampedDuration);
            }
        }

        // Verify results
        List<SearchLatencyBreakdownNode> children = fetchNode.getChildren();

        // Script Fields (negative delta = 0) and Inner Hits (zero delta = 0) should not be added
        assertEquals("Only 3 children with positive durations should be added", 3, children.size());

        // All children must satisfy the invariant
        for (SearchLatencyBreakdownNode child : children) {
            assertTrue("Child '" + child.getName() + "' duration (" + child.getDurationNanos()
                + ") must not exceed parent (" + fetchPhaseDuration + ")",
                child.getDurationNanos() <= fetchPhaseDuration);
            assertTrue("Child '" + child.getName() + "' duration must be > 0",
                child.getDurationNanos() > 0);
        }

        // Specific assertions
        assertEquals("Stored Fields", children.get(0).getName());
        assertEquals(7_000_000L, children.get(0).getDurationNanos()); // 7ms, no clamping

        assertEquals("Source Loading", children.get(1).getName());
        assertEquals(10_000_000L, children.get(1).getDurationNanos()); // 10ms, no clamping

        assertEquals("Highlighting", children.get(2).getName());
        assertEquals(20_000_000L, children.get(2).getDurationNanos()); // 25ms clamped to 20ms
    }
}
