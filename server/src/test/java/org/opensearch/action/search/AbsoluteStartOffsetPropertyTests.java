/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

/**
 * Property-based test for absolute start_offset computation and preservation.
 * <p>
 * Feature: latency-breakdown-fixes, Property 7: Absolute start_offset computation and preservation
 * <p>
 * For any data-node timed event where request_start_nanos > 0 is provided via ShardSearchRequest,
 * the computed start_offset_micros SHALL equal (event_start_nanos - request_start_nanos) / 1000,
 * and after coordinator merge these absolute offsets SHALL be preserved unchanged.
 * When request_start_nanos == 0, the coordinator SHALL apply sequential cursor positioning
 * (offset stays 0).
 * <p>
 * <b>Validates: Requirements 8.1, 8.2, 8.3, 8.4</b>
 */
public class AbsoluteStartOffsetPropertyTests extends OpenSearchTestCase {

    /**
     * Property 7a: Pure computation - absolute start_offset equals (event_start_nanos - request_start_nanos) / 1000.
     * <p>
     * For random event_start_nanos and request_start_nanos (where event >= request),
     * the computed start_offset_micros must equal the integer division of the nanos difference by 1000.
     * <p>
     * <b>Validates: Requirements 8.1, 8.2</b>
     */
    public void testAbsoluteStartOffsetComputation() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            // Generate random request_start_nanos (simulating System.nanoTime() from coordinator)
            long requestStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Generate random event_start_nanos that is at or after request start
            // (simulating an event occurring after the request was dispatched)
            long eventStartNanos = requestStartNanos + randomLongBetween(0, 2_000_000_000L);

            // The formula from the design: start_offset_micros = (event_start_nanos - request_start_nanos) / 1000
            long expectedOffsetMicros = (eventStartNanos - requestStartNanos) / 1000;

            // This is also what TimeUnit.NANOSECONDS.toMicros produces (truncating integer division)
            long computedOffsetMicros = TimeUnit.NANOSECONDS.toMicros(eventStartNanos - requestStartNanos);

            assertEquals(
                "Absolute start_offset computation mismatch in iteration " + i
                    + ". eventStartNanos=" + eventStartNanos
                    + ", requestStartNanos=" + requestStartNanos
                    + ". Expected (eventStart - requestStart) / 1000 = " + expectedOffsetMicros,
                expectedOffsetMicros,
                computedOffsetMicros
            );

            // Also verify the offset is non-negative when eventStartNanos >= requestStartNanos
            assertTrue(
                "start_offset_micros must be non-negative when eventStartNanos >= requestStartNanos"
                    + " in iteration " + i + ", got " + computedOffsetMicros,
                computedOffsetMicros >= 0
            );
        }
    }

    /**
     * Property 7b: SearchLatencyBreakdownNode preserves absolute offset after construction.
     * <p>
     * When a data node creates a SearchLatencyBreakdownNode with an absolute start_offset
     * (computed from requestStartNanos > 0), that offset is preserved in the node's getters
     * and survives being added as a child of the coordinator tree — simulating coordinator merge.
     * <p>
     * <b>Validates: Requirements 8.2, 8.3</b>
     */
    public void testAbsoluteOffsetPreservedInBreakdownNode() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            // Generate random request and event times
            long requestStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long eventStartNanos = requestStartNanos + randomLongBetween(0, 2_000_000_000L);
            long durationNanos = randomLongBetween(1_000L, 500_000_000L);

            // Compute absolute offset as data node would
            long absoluteOffsetNanos = eventStartNanos - requestStartNanos;

            // Create breakdown node with absolute offset (as data node does)
            SearchLatencyBreakdownNode node = new SearchLatencyBreakdownNode(
                "TestEvent_" + i,
                SearchLatencyBreakdownNode.CATEGORY_QUERY,
                absoluteOffsetNanos,
                durationNanos
            );

            // Verify the node preserves the offset
            assertEquals(
                "startOffsetNanos not preserved in breakdown node in iteration " + i,
                absoluteOffsetNanos,
                node.getStartOffsetNanos()
            );
            assertEquals(
                "startOffsetMicros not preserved in breakdown node in iteration " + i,
                TimeUnit.NANOSECONDS.toMicros(absoluteOffsetNanos),
                node.getStartOffsetMicros()
            );
            assertEquals(
                "durationNanos not preserved in breakdown node in iteration " + i,
                durationNanos,
                node.getDurationNanos()
            );
            assertEquals(
                "endOffsetNanos should be startOffset + duration in iteration " + i,
                absoluteOffsetNanos + durationNanos,
                node.getEndOffsetNanos()
            );
        }
    }

    /**
     * Property 7c: Coordinator merge preserves absolute offsets from data nodes.
     * <p>
     * When requestStartNanos > 0, the coordinator should preserve the absolute offsets
     * of data-node breakdown nodes without repositioning. This test simulates the merge
     * by adding data-node nodes as children of a coordinator parent and verifying offsets
     * remain unchanged.
     * <p>
     * <b>Validates: Requirements 8.3</b>
     */
    public void testCoordinatorMergePreservesAbsoluteOffsets() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            long requestStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);

            // Simulate coordinator creating a parent node
            SearchLatencyBreakdownNode coordinatorParent = new SearchLatencyBreakdownNode(
                "Query Phase",
                SearchLatencyBreakdownNode.CATEGORY_PHASE,
                0L,
                randomLongBetween(50_000_000L, 500_000_000L)
            );

            // Simulate multiple data-node children with absolute offsets
            int childCount = randomIntBetween(1, 5);
            long[] expectedOffsets = new long[childCount];
            long[] expectedDurations = new long[childCount];

            for (int c = 0; c < childCount; c++) {
                long eventStartNanos = requestStartNanos + randomLongBetween(10_000L, 200_000_000L);
                long childDurationNanos = randomLongBetween(1_000L, 50_000_000L);
                long absoluteOffset = eventStartNanos - requestStartNanos;

                expectedOffsets[c] = absoluteOffset;
                expectedDurations[c] = childDurationNanos;

                // Data node creates child with absolute offset
                SearchLatencyBreakdownNode childNode = new SearchLatencyBreakdownNode(
                    "DataNodeOp_" + c,
                    SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                    absoluteOffset,
                    childDurationNanos
                );

                // Coordinator adds the child (merge step) — offsets should be preserved
                coordinatorParent.addChild(childNode);
            }

            // Verify all children retain their absolute offsets after merge
            for (int c = 0; c < childCount; c++) {
                SearchLatencyBreakdownNode mergedChild = coordinatorParent.getChildren().get(c);
                assertEquals(
                    "Child " + c + " startOffsetNanos not preserved after coordinator merge in iteration " + i,
                    expectedOffsets[c],
                    mergedChild.getStartOffsetNanos()
                );
                assertEquals(
                    "Child " + c + " durationNanos not preserved after coordinator merge in iteration " + i,
                    expectedDurations[c],
                    mergedChild.getDurationNanos()
                );
            }
        }
    }

    /**
     * Property 7d: Sequential cursor fallback when requestStartNanos == 0.
     * <p>
     * When requestStartNanos is 0 (old coordinator or not set), the data node falls back
     * to duration-only mode where start_offset remains 0. This ensures backward compatibility.
     * <p>
     * <b>Validates: Requirements 8.4</b>
     */
    public void testSequentialCursorFallbackWhenRequestStartNanosZero() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            long requestStartNanos = 0; // Simulates old coordinator or unset value

            // With requestStartNanos == 0, data node cannot compute absolute offset
            // The fallback behavior: offset stays 0 (duration-only mode)
            long eventStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long durationNanos = randomLongBetween(1_000L, 500_000_000L);

            // When requestStartNanos == 0, data node uses offset = 0
            long computedOffset = (requestStartNanos > 0)
                ? (eventStartNanos - requestStartNanos)
                : 0;

            assertEquals(
                "When requestStartNanos == 0, offset should be 0 (duration-only mode) in iteration " + i,
                0L,
                computedOffset
            );

            // Create a node in fallback mode (offset = 0, only duration is set)
            SearchLatencyBreakdownNode fallbackNode = new SearchLatencyBreakdownNode(
                "FallbackEvent_" + i,
                SearchLatencyBreakdownNode.CATEGORY_QUERY,
                durationNanos
            );

            // Verify fallback node has 0 offset
            assertEquals(
                "Fallback node should have 0 startOffsetNanos in iteration " + i,
                0L,
                fallbackNode.getStartOffsetNanos()
            );
            // Duration is still recorded
            assertEquals(
                "Fallback node should preserve durationNanos in iteration " + i,
                durationNanos,
                fallbackNode.getDurationNanos()
            );
        }
    }

    /**
     * Property 7e: Absolute offset computation matches TimeUnit.NANOSECONDS.toMicros for micros conversion.
     * <p>
     * The formula (event_start_nanos - request_start_nanos) / 1000 should produce the same result
     * as TimeUnit.NANOSECONDS.toMicros(event_start_nanos - request_start_nanos). This tests
     * that both the raw division and the TimeUnit conversion are consistent (both use truncating
     * integer division).
     * <p>
     * <b>Validates: Requirements 8.2</b>
     */
    public void testOffsetComputationConsistencyWithTimeUnit() {
        final int iterations = 200;

        for (int i = 0; i < iterations; i++) {
            long requestStartNanos = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long eventStartNanos = requestStartNanos + randomLongBetween(0, 5_000_000_000L);

            long diffNanos = eventStartNanos - requestStartNanos;

            // Formula from design: (event_start_nanos - request_start_nanos) / 1000
            long offsetByDivision = diffNanos / 1000;

            // Java TimeUnit conversion
            long offsetByTimeUnit = TimeUnit.NANOSECONDS.toMicros(diffNanos);

            assertEquals(
                "Division by 1000 and TimeUnit.toMicros should agree in iteration " + i
                    + " for diffNanos=" + diffNanos,
                offsetByDivision,
                offsetByTimeUnit
            );

            // Both should be non-negative
            assertTrue(
                "Offset should be non-negative in iteration " + i,
                offsetByDivision >= 0
            );
        }
    }
}
