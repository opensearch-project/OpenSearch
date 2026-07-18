/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Property-based test validating phase containment: children never exceed parent bounds.
 * <p>
 * Feature: breakdown-ordering-fix, Property 9: Phase containment — children never exceed parent bounds
 * <p>
 * For any parent-child relationship in the breakdown tree, after calling
 * {@link SearchLatencyBreakdown#enforcePhaseContainment()}:
 * <ul>
 *   <li>child.start &gt;= parent.start (clamped up)</li>
 *   <li>child.end (start + duration) &lt;= parent.end (duration reduced if needed)</li>
 *   <li>Duration is never negative after clamping</li>
 * </ul>
 * <p>
 * <b>Validates: Requirements 9.1, 9.2, 9.3</b>
 */
public class PhaseContainmentPropertyTests extends OpenSearchTestCase {

    /** Minimum 100 iterations as required by the design document. */
    private static final int ITERATIONS = 150;

    /**
     * Property 9: Children that start before parent are clamped to parent start.
     * <p>
     * Generates random query phase timing, then records child events with start times
     * BEFORE the query phase start. After enforcePhaseContainment(), asserts that
     * child start &gt;= parent start.
     * <p>
     * <b>Validates: Requirements 9.1</b>
     */
    public void testChildStartClampedToParentStart() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(10_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Generate a child event that starts BEFORE the query phase
            long childStartBefore = queryPhaseStart - randomLongBetween(1L, 100_000_000L);
            long childDuration = randomLongBetween(1_000L, queryPhaseDuration / 2);
            long childEnd = childStartBefore + childDuration;

            // Use a known query-phase child event name
            breakdown.recordTimedEvent("acquire_reader_context", childStartBefore, childEnd);

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify: child start >= parent start
            long[] childTiming = breakdown.getNamedEventTiming("acquire_reader_context");
            assertNotNull("Child event timing should exist after containment, iteration " + i, childTiming);
            assertTrue(
                "Child start (" + childTiming[0] + ") must be >= parent start (" + queryPhaseStart
                    + ") after containment, iteration " + i,
                childTiming[0] >= queryPhaseStart
            );

            // Verify: duration is non-negative
            assertTrue(
                "Child duration (" + childTiming[1] + ") must be >= 0 after containment, iteration " + i,
                childTiming[1] >= 0
            );
        }
    }

    /**
     * Property 9: Children that end after parent are clamped so duration is reduced.
     * <p>
     * Generates random query phase timing, then records child events with end times
     * AFTER the query phase end. After enforcePhaseContainment(), asserts that
     * child end (start + duration) &lt;= parent end.
     * <p>
     * <b>Validates: Requirements 9.2</b>
     */
    public void testChildEndClampedToParentEnd() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(10_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Generate a child event that starts within parent but ends AFTER parent end
            long childStart = queryPhaseStart + randomLongBetween(0, queryPhaseDuration / 2);
            long excessDuration = randomLongBetween(1L, 200_000_000L);
            long childEnd = queryPhaseEnd + excessDuration;

            // Use a known query-phase child event name
            breakdown.recordTimedEvent("search_context_creation", childStart, childEnd);

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify: child end <= parent end
            long[] childTiming = breakdown.getNamedEventTiming("search_context_creation");
            assertNotNull("Child event timing should exist after containment, iteration " + i, childTiming);
            long clampedEnd = childTiming[0] + childTiming[1];
            assertTrue(
                "Child end (" + clampedEnd + ") must be <= parent end (" + queryPhaseEnd
                    + ") after containment, iteration " + i,
                clampedEnd <= queryPhaseEnd
            );

            // Verify: duration is non-negative
            assertTrue(
                "Child duration (" + childTiming[1] + ") must be >= 0 after containment, iteration " + i,
                childTiming[1] >= 0
            );
        }
    }

    /**
     * Property 9: Children with both start &lt; parent.start AND end &gt; parent.end are correctly double-clamped.
     * <p>
     * Generates children that exceed BOTH parent boundaries. After enforcePhaseContainment(),
     * the child should be fully contained within the parent bounds.
     * <p>
     * <b>Validates: Requirements 9.1, 9.2</b>
     */
    public void testDoubleClamping_childExceedsBothBounds() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(10_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Generate a child that starts BEFORE and ends AFTER parent
            long childStartBefore = queryPhaseStart - randomLongBetween(1L, 100_000_000L);
            long childEndAfter = queryPhaseEnd + randomLongBetween(1L, 100_000_000L);

            // Use a known query-phase child event name
            breakdown.recordTimedEvent("request_cache_lookup", childStartBefore, childEndAfter);

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify: child start >= parent start AND child end <= parent end
            long[] childTiming = breakdown.getNamedEventTiming("request_cache_lookup");
            assertNotNull("Child event timing should exist after containment, iteration " + i, childTiming);

            assertTrue(
                "Double-clamped child start (" + childTiming[0] + ") must be >= parent start ("
                    + queryPhaseStart + "), iteration " + i,
                childTiming[0] >= queryPhaseStart
            );

            long clampedEnd = childTiming[0] + childTiming[1];
            assertTrue(
                "Double-clamped child end (" + clampedEnd + ") must be <= parent end ("
                    + queryPhaseEnd + "), iteration " + i,
                clampedEnd <= queryPhaseEnd
            );

            // Verify: duration is non-negative
            assertTrue(
                "Double-clamped child duration (" + childTiming[1] + ") must be >= 0, iteration " + i,
                childTiming[1] >= 0
            );
        }
    }

    /**
     * Property 9: Children already within bounds are not modified by enforcePhaseContainment().
     * <p>
     * Generates children that are strictly within parent bounds. After containment,
     * the child timing should remain unchanged.
     * <p>
     * <b>Validates: Requirements 9.1, 9.2</b>
     */
    public void testChildrenWithinBoundsAreNotModified() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(50_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Generate a child that is strictly WITHIN the parent bounds
            long margin = queryPhaseDuration / 4;
            long childStart = queryPhaseStart + randomLongBetween(1L, margin);
            long maxChildDuration = queryPhaseEnd - childStart;
            long childDuration = randomLongBetween(1L, Math.max(1L, maxChildDuration - 1));
            long childEnd = childStart + childDuration;

            // Use a known query-phase child event name
            breakdown.recordTimedEvent("agg_post_process", childStart, childEnd);

            // Capture original timing before containment
            long originalStart = childStart;
            long originalDuration = childDuration;

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify: timing is unchanged
            long[] childTiming = breakdown.getNamedEventTiming("agg_post_process");
            assertNotNull("Child event timing should exist after containment, iteration " + i, childTiming);

            assertEquals(
                "Within-bounds child start should not be modified, iteration " + i,
                originalStart, childTiming[0]
            );
            assertEquals(
                "Within-bounds child duration should not be modified, iteration " + i,
                originalDuration, childTiming[1]
            );
        }
    }

    /**
     * Property 9: Top-level events (wire bars, phase bars) are NOT affected by containment.
     * <p>
     * Wire bars and phase bars have no parent phase (getParentPhase returns null),
     * so enforcePhaseContainment() should not modify them regardless of their position.
     * <p>
     * <b>Validates: Requirements 9.3</b>
     */
    public void testTopLevelEventsNotAffectedByContainment() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(10_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Record top-level wire bars that may exceed phase boundaries
            // wire_out_query should be BEFORE query phase, so it starts before query
            long wireOutStart = queryPhaseStart - randomLongBetween(1_000_000L, 50_000_000L);
            long wireOutDuration = randomLongBetween(100_000L, 30_000_000L);
            long wireOutEnd = wireOutStart + wireOutDuration;
            breakdown.recordTimedEvent("wire_out_query", wireOutStart, wireOutEnd);

            // wire_back_query should be AFTER query phase
            long wireBackStart = queryPhaseEnd + randomLongBetween(0, 10_000_000L);
            long wireBackDuration = randomLongBetween(100_000L, 30_000_000L);
            long wireBackEnd = wireBackStart + wireBackDuration;
            breakdown.recordTimedEvent("wire_back_query", wireBackStart, wireBackEnd);

            // Record a fetch phase that starts after query
            long fetchStart = wireBackEnd + randomLongBetween(1_000_000L, 20_000_000L);
            long fetchDuration = randomLongBetween(5_000_000L, 100_000_000L);
            long fetchEnd = fetchStart + fetchDuration;
            breakdown.recordTimedEvent("fetch", fetchStart, fetchEnd);

            // Record wire_out_fetch and wire_back_fetch as top-level events
            long wireOutFetchStart = queryPhaseEnd + randomLongBetween(0, 5_000_000L);
            long wireOutFetchDuration = randomLongBetween(100_000L, 20_000_000L);
            breakdown.recordTimedEvent("wire_out_fetch", wireOutFetchStart, wireOutFetchStart + wireOutFetchDuration);

            long wireBackFetchStart = fetchEnd + randomLongBetween(0, 5_000_000L);
            long wireBackFetchDuration = randomLongBetween(100_000L, 20_000_000L);
            breakdown.recordTimedEvent("wire_back_fetch", wireBackFetchStart, wireBackFetchStart + wireBackFetchDuration);

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify: wire_out_query is NOT modified
            long[] wireOutTiming = breakdown.getNamedEventTiming("wire_out_query");
            assertNotNull("wire_out_query should exist after containment, iteration " + i, wireOutTiming);
            assertEquals(
                "wire_out_query start should not be modified by containment, iteration " + i,
                wireOutStart, wireOutTiming[0]
            );
            assertEquals(
                "wire_out_query duration should not be modified by containment, iteration " + i,
                wireOutDuration, wireOutTiming[1]
            );

            // Verify: wire_back_query is NOT modified
            long[] wireBackTiming = breakdown.getNamedEventTiming("wire_back_query");
            assertNotNull("wire_back_query should exist after containment, iteration " + i, wireBackTiming);
            assertEquals(
                "wire_back_query start should not be modified by containment, iteration " + i,
                wireBackStart, wireBackTiming[0]
            );
            assertEquals(
                "wire_back_query duration should not be modified by containment, iteration " + i,
                wireBackDuration, wireBackTiming[1]
            );

            // Verify: wire_out_fetch is NOT modified
            long[] wireOutFetchTiming = breakdown.getNamedEventTiming("wire_out_fetch");
            assertNotNull("wire_out_fetch should exist after containment, iteration " + i, wireOutFetchTiming);
            assertEquals(
                "wire_out_fetch start should not be modified by containment, iteration " + i,
                wireOutFetchStart, wireOutFetchTiming[0]
            );
            assertEquals(
                "wire_out_fetch duration should not be modified by containment, iteration " + i,
                wireOutFetchDuration, wireOutFetchTiming[1]
            );

            // Verify: wire_back_fetch is NOT modified
            long[] wireBackFetchTiming = breakdown.getNamedEventTiming("wire_back_fetch");
            assertNotNull("wire_back_fetch should exist after containment, iteration " + i, wireBackFetchTiming);
            assertEquals(
                "wire_back_fetch start should not be modified by containment, iteration " + i,
                wireBackFetchStart, wireBackFetchTiming[0]
            );
            assertEquals(
                "wire_back_fetch duration should not be modified by containment, iteration " + i,
                wireBackFetchDuration, wireBackFetchTiming[1]
            );

            // Verify: phase bars themselves are NOT modified
            long[] queryTiming = breakdown.getNamedEventTiming("query");
            assertNotNull("query phase should exist after containment, iteration " + i, queryTiming);
            assertEquals(
                "query phase start should not be modified by containment, iteration " + i,
                queryPhaseStart, queryTiming[0]
            );
            assertEquals(
                "query phase duration should not be modified by containment, iteration " + i,
                queryPhaseDuration, queryTiming[1]
            );
        }
    }

    /**
     * Property 9: Multiple children of the same parent are all independently clamped.
     * <p>
     * Generates multiple query-phase child events with various out-of-bounds scenarios
     * and verifies that after containment, ALL children satisfy the bounds invariant.
     * <p>
     * <b>Validates: Requirements 9.1, 9.2, 9.3</b>
     */
    public void testMultipleChildrenAllClamped() {
        // Known query-phase child event names from QUERY_PHASE_CHILDREN
        String[] childNames = {
            "acquire_reader_context",
            "search_context_creation",
            "request_cache_lookup",
            "agg_post_process",
            "agg_initialize",
            "star_tree_setup",
            "data_node_queue_wait"
        };

        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random query phase timing
            long queryPhaseStart = randomLongBetween(1_000_000_000L, 10_000_000_000L);
            long queryPhaseDuration = randomLongBetween(50_000_000L, 500_000_000L);
            long queryPhaseEnd = queryPhaseStart + queryPhaseDuration;

            // Record the query phase
            breakdown.recordTimedEvent("query", queryPhaseStart, queryPhaseEnd);

            // Record multiple child events with randomly out-of-bounds timings
            for (String childName : childNames) {
                long childStart;
                long childEnd;

                int scenario = randomIntBetween(0, 3);
                switch (scenario) {
                    case 0:
                        // Child starts before parent
                        childStart = queryPhaseStart - randomLongBetween(1L, 50_000_000L);
                        childEnd = childStart + randomLongBetween(1_000L, queryPhaseDuration / 3);
                        break;
                    case 1:
                        // Child ends after parent
                        childStart = queryPhaseStart + randomLongBetween(0, queryPhaseDuration / 2);
                        childEnd = queryPhaseEnd + randomLongBetween(1L, 50_000_000L);
                        break;
                    case 2:
                        // Child exceeds both bounds
                        childStart = queryPhaseStart - randomLongBetween(1L, 50_000_000L);
                        childEnd = queryPhaseEnd + randomLongBetween(1L, 50_000_000L);
                        break;
                    default:
                        // Child within bounds (should remain unchanged)
                        childStart = queryPhaseStart + randomLongBetween(1L, queryPhaseDuration / 3);
                        long maxDur = queryPhaseEnd - childStart;
                        childEnd = childStart + randomLongBetween(1L, Math.max(1L, maxDur - 1));
                        break;
                }

                breakdown.recordTimedEvent(childName, childStart, childEnd);
            }

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify ALL children satisfy the containment invariant
            for (String childName : childNames) {
                long[] childTiming = breakdown.getNamedEventTiming(childName);
                assertNotNull("Child '" + childName + "' should exist after containment, iteration " + i, childTiming);

                // child.start >= parent.start
                assertTrue(
                    "Child '" + childName + "' start (" + childTiming[0] + ") must be >= parent start ("
                        + queryPhaseStart + "), iteration " + i,
                    childTiming[0] >= queryPhaseStart
                );

                // child.end <= parent.end
                long childEndVal = childTiming[0] + childTiming[1];
                assertTrue(
                    "Child '" + childName + "' end (" + childEndVal + ") must be <= parent end ("
                        + queryPhaseEnd + "), iteration " + i,
                    childEndVal <= queryPhaseEnd
                );

                // duration >= 0
                assertTrue(
                    "Child '" + childName + "' duration (" + childTiming[1] + ") must be >= 0, iteration " + i,
                    childTiming[1] >= 0
                );
            }
        }
    }

    /**
     * Property 9: Fetch phase children are also contained within the fetch phase.
     * <p>
     * Validates that containment works for the fetch phase hierarchy as well,
     * not just the query phase.
     * <p>
     * <b>Validates: Requirements 9.1, 9.2</b>
     */
    public void testFetchPhaseChildrenContained() {
        String[] fetchChildren = {
            "fetch_stored_fields",
            "fetch_source_loading",
            "fetch_highlighting",
            "fetch_script_fields",
            "fetch_inner_hits"
        };

        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random fetch phase timing
            long fetchPhaseStart = randomLongBetween(2_000_000_000L, 10_000_000_000L);
            long fetchPhaseDuration = randomLongBetween(10_000_000L, 300_000_000L);
            long fetchPhaseEnd = fetchPhaseStart + fetchPhaseDuration;

            // Record the fetch phase
            breakdown.recordTimedEvent("fetch", fetchPhaseStart, fetchPhaseEnd);

            // Record fetch children that intentionally exceed bounds
            for (String childName : fetchChildren) {
                long childStart;
                long childEnd;

                if (randomBoolean()) {
                    // Start before fetch phase
                    childStart = fetchPhaseStart - randomLongBetween(1L, 50_000_000L);
                } else {
                    // Start within fetch phase
                    childStart = fetchPhaseStart + randomLongBetween(0, fetchPhaseDuration / 2);
                }

                if (randomBoolean()) {
                    // End after fetch phase
                    childEnd = fetchPhaseEnd + randomLongBetween(1L, 50_000_000L);
                } else {
                    // End within fetch phase
                    childEnd = childStart + randomLongBetween(1_000L, Math.max(1_000L, fetchPhaseDuration / 3));
                }

                breakdown.recordTimedEvent(childName, childStart, childEnd);
            }

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify ALL fetch children satisfy the containment invariant
            for (String childName : fetchChildren) {
                long[] childTiming = breakdown.getNamedEventTiming(childName);
                assertNotNull("Fetch child '" + childName + "' should exist after containment, iteration " + i, childTiming);

                // child.start >= parent.start
                assertTrue(
                    "Fetch child '" + childName + "' start (" + childTiming[0] + ") must be >= parent start ("
                        + fetchPhaseStart + "), iteration " + i,
                    childTiming[0] >= fetchPhaseStart
                );

                // child.end <= parent.end
                long childEndVal = childTiming[0] + childTiming[1];
                assertTrue(
                    "Fetch child '" + childName + "' end (" + childEndVal + ") must be <= parent end ("
                        + fetchPhaseEnd + "), iteration " + i,
                    childEndVal <= fetchPhaseEnd
                );

                // duration >= 0
                assertTrue(
                    "Fetch child '" + childName + "' duration (" + childTiming[1] + ") must be >= 0, iteration " + i,
                    childTiming[1] >= 0
                );
            }
        }
    }

    /**
     * Property 9: Reduce phase children are also contained within the reduce phase.
     * <p>
     * Validates that containment works for the reduce phase hierarchy.
     * <p>
     * <b>Validates: Requirements 9.1, 9.2</b>
     */
    public void testReducePhaseChildrenContained() {
        String[] reduceChildren = {
            "reduce_top_docs",
            "reduce_aggregations",
            "reduce_pipeline_aggs",
            "reduce_suggestions"
        };

        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            // Generate random reduce phase timing
            long reducePhaseStart = randomLongBetween(3_000_000_000L, 10_000_000_000L);
            long reducePhaseDuration = randomLongBetween(5_000_000L, 200_000_000L);
            long reducePhaseEnd = reducePhaseStart + reducePhaseDuration;

            // Record the reduce phase
            breakdown.recordTimedEvent("reduce", reducePhaseStart, reducePhaseEnd);

            // Record reduce children that intentionally exceed bounds
            for (String childName : reduceChildren) {
                long childStart;
                long childEnd;

                if (randomBoolean()) {
                    childStart = reducePhaseStart - randomLongBetween(1L, 30_000_000L);
                } else {
                    childStart = reducePhaseStart + randomLongBetween(0, reducePhaseDuration / 2);
                }

                if (randomBoolean()) {
                    childEnd = reducePhaseEnd + randomLongBetween(1L, 30_000_000L);
                } else {
                    childEnd = childStart + randomLongBetween(1_000L, Math.max(1_000L, reducePhaseDuration / 3));
                }

                breakdown.recordTimedEvent(childName, childStart, childEnd);
            }

            // Enforce containment
            breakdown.enforcePhaseContainment();

            // Verify ALL reduce children satisfy the containment invariant
            for (String childName : reduceChildren) {
                long[] childTiming = breakdown.getNamedEventTiming(childName);
                assertNotNull("Reduce child '" + childName + "' should exist after containment, iteration " + i, childTiming);

                // child.start >= parent.start
                assertTrue(
                    "Reduce child '" + childName + "' start (" + childTiming[0] + ") must be >= parent start ("
                        + reducePhaseStart + "), iteration " + i,
                    childTiming[0] >= reducePhaseStart
                );

                // child.end <= parent.end
                long childEndVal = childTiming[0] + childTiming[1];
                assertTrue(
                    "Reduce child '" + childName + "' end (" + childEndVal + ") must be <= parent end ("
                        + reducePhaseEnd + "), iteration " + i,
                    childEndVal <= reducePhaseEnd
                );

                // duration >= 0
                assertTrue(
                    "Reduce child '" + childName + "' duration (" + childTiming[1] + ") must be >= 0, iteration " + i,
                    childTiming[1] >= 0
                );
            }
        }
    }
}
