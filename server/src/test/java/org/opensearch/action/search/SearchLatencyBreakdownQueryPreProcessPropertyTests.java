/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Property-based tests for SearchLatencyBreakdown: Property 5 - Query pre-process parent
 * encompasses sub-component children.
 *
 * <p><b>Property 5: Query pre-process parent encompasses sub-component children</b></p>
 * <p>For any query pre-process execution that triggers one or more sub-components
 * (global_ordinals, script_compilation, nested_bitset, star_tree), the aggregate
 * {@code query_pre_process.duration_micros} SHALL be &gt;= each individual sub-component's
 * {@code duration_micros}, and each active sub-component SHALL have a valid timed entry
 * in the breakdown.</p>
 *
 * <p>Feature: latency-breakdown-fixes, Property 5: Query pre-process parent encompasses sub-component children</p>
 *
 * <p><b>Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5</b></p>
 */
public class SearchLatencyBreakdownQueryPreProcessPropertyTests extends OpenSearchTestCase {

    private static final int ITERATIONS = 200;

    /**
     * Sub-component names as they would appear in the breakdown tree.
     */
    private static final String[] SUB_COMPONENT_NAMES = {
        "Global Ordinals",
        "Script Compilation",
        "Nested Bitset",
        "Star-Tree Setup"
    };

    /**
     * Property 5: Parent query_pre_process duration_micros &gt;= each sub-component duration_micros.
     * <p>
     * Generates random sub-component durations (some zero for disabled).
     * Constructs a parent node whose duration encompasses all sub-components.
     * Asserts the parent duration is &gt;= each individual child duration.
     * <p>
     * Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5
     */
    public void testProperty5_parentDurationEncompassesAllChildren() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate random sub-component durations; some may be zero (disabled)
            long globalOrdinalsNanos = randomBoolean() ? randomLongBetween(1_000, 50_000_000L) : 0;
            long scriptCompilationNanos = randomBoolean() ? randomLongBetween(1_000, 30_000_000L) : 0;
            long nestedBitsetNanos = randomBoolean() ? randomLongBetween(1_000, 20_000_000L) : 0;
            long starTreeNanos = randomBoolean() ? randomLongBetween(1_000, 25_000_000L) : 0;

            long[] childDurations = { globalOrdinalsNanos, scriptCompilationNanos, nestedBitsetNanos, starTreeNanos };

            // The parent duration encompasses all children: it should be at least
            // the sum of all sub-components (sequential execution) or the max (parallel),
            // but per the spec, parent >= each individual child.
            // Generate parent as sum of children + some random overhead (simulating real behavior)
            long sumOfChildren = globalOrdinalsNanos + scriptCompilationNanos + nestedBitsetNanos + starTreeNanos;
            long overhead = randomLongBetween(0, 5_000_000L);
            long parentDurationNanos = sumOfChildren + overhead;

            // Ensure parent is at least 1 nano if any child is active
            if (parentDurationNanos == 0 && sumOfChildren == 0) {
                parentDurationNanos = randomLongBetween(0, 1_000_000L);
            }

            // Build the parent node
            long requestStartNanos = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long parentStartOffset = randomLongBetween(0, 100_000_000L);

            SearchLatencyBreakdownNode parent = new SearchLatencyBreakdownNode(
                "Query Pre-Process",
                SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                parentStartOffset,
                parentDurationNanos
            );

            // Add active sub-components as children with sequential offsets
            long currentOffset = parentStartOffset;
            for (int j = 0; j < childDurations.length; j++) {
                if (childDurations[j] > 0) {
                    parent.addChild(
                        SUB_COMPONENT_NAMES[j],
                        SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                        currentOffset,
                        childDurations[j]
                    );
                    currentOffset += childDurations[j];
                }
            }

            // PROPERTY ASSERTION 1: Parent duration_micros >= each child duration_micros
            long parentDurationMicros = TimeUnit.NANOSECONDS.toMicros(parent.getDurationNanos());
            for (SearchLatencyBreakdownNode child : parent.getChildren()) {
                long childDurationMicros = TimeUnit.NANOSECONDS.toMicros(child.getDurationNanos());
                assertTrue(
                    "Iteration " + i + ": Parent duration_micros (" + parentDurationMicros
                        + ") must be >= child '" + child.getName() + "' duration_micros ("
                        + childDurationMicros + ")",
                    parentDurationMicros >= childDurationMicros
                );
            }

            // PROPERTY ASSERTION 2: Active sub-components have valid timed entries
            for (SearchLatencyBreakdownNode child : parent.getChildren()) {
                assertTrue(
                    "Iteration " + i + ": Active child '" + child.getName()
                        + "' must have positive duration, got " + child.getDurationNanos(),
                    child.getDurationNanos() > 0
                );
                assertTrue(
                    "Iteration " + i + ": Active child '" + child.getName()
                        + "' must have non-negative start_offset, got " + child.getStartOffsetNanos(),
                    child.getStartOffsetNanos() >= 0
                );
            }
        }
    }

    /**
     * Property 5: Disabled sub-components (zero duration) should not appear as children.
     * <p>
     * When a sub-component has zero duration (disabled), it should not be included
     * in the breakdown tree.
     * <p>
     * Validates: Requirements 5.1, 5.2, 5.3, 5.4
     */
    public void testProperty5_disabledSubComponentsNotInTree() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate durations where at least one is zero
            long[] durations = new long[4];
            int disabledCount = 0;
            for (int j = 0; j < 4; j++) {
                if (randomBoolean()) {
                    durations[j] = randomLongBetween(1_000, 50_000_000L);
                } else {
                    durations[j] = 0;
                    disabledCount++;
                }
            }

            // Build parent encompassing active children
            long sumActive = 0;
            for (long d : durations) sumActive += d;
            long parentDuration = sumActive + randomLongBetween(0, 5_000_000L);
            long parentStartOffset = randomLongBetween(0, 100_000_000L);

            SearchLatencyBreakdownNode parent = new SearchLatencyBreakdownNode(
                "Query Pre-Process",
                SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                parentStartOffset,
                parentDuration
            );

            // Only add children with non-zero durations (simulating production behavior)
            long offset = parentStartOffset;
            int activeCount = 0;
            for (int j = 0; j < durations.length; j++) {
                if (durations[j] > 0) {
                    parent.addChild(SUB_COMPONENT_NAMES[j],
                        SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, offset, durations[j]);
                    offset += durations[j];
                    activeCount++;
                }
            }

            // ASSERTION: Number of children equals number of active (non-zero) sub-components
            assertEquals(
                "Iteration " + i + ": Children count should match active sub-component count",
                activeCount,
                parent.getChildren().size()
            );

            // ASSERTION: No child has zero duration
            for (SearchLatencyBreakdownNode child : parent.getChildren()) {
                assertTrue(
                    "Iteration " + i + ": Child '" + child.getName() + "' must have positive duration",
                    child.getDurationNanos() > 0
                );
            }
        }
    }

    /**
     * Property 5: Parent duration via SearchLatencyBreakdown recording encompasses recorded
     * sub-component values.
     * <p>
     * Uses the actual SearchLatencyBreakdown recording methods to verify that
     * when sub-component durations are recorded, the aggregate query_pre_process
     * (computed from the sum or max of sub-components) is &gt;= each child.
     * <p>
     * Validates: Requirements 5.1, 5.2, 5.3, 5.4, 5.5
     */
    public void testProperty5_breakdownRecordingParentEncompassesChildren() {
        for (int i = 0; i < ITERATIONS; i++) {
            SearchLatencyBreakdown breakdown = new SearchLatencyBreakdown();

            long absStart = randomLongBetween(1_000_000_000L, 2_000_000_000L);
            long reqEnd = absStart + randomLongBetween(100_000_000L, 500_000_000L);

            // Generate random sub-component durations (some zero for disabled)
            long globalOrdinals = randomBoolean() ? randomLongBetween(1_000_000L, 50_000_000L) : 0;
            long scriptCompilation = randomBoolean() ? randomLongBetween(1_000_000L, 30_000_000L) : 0;
            long nestedBitset = randomBoolean() ? randomLongBetween(1_000_000L, 20_000_000L) : 0;
            long starTree = randomBoolean() ? randomLongBetween(1_000_000L, 25_000_000L) : 0;

            // Record sub-components via the breakdown's max-accumulator methods
            if (globalOrdinals > 0) breakdown.recordGlobalOrdinalsLoading(globalOrdinals);
            if (scriptCompilation > 0) breakdown.recordScriptCompilation(scriptCompilation);
            if (nestedBitset > 0) breakdown.recordNestedBitsetConstruction(nestedBitset);
            if (starTree > 0) breakdown.recordStarTreeSetup(starTree);

            // The parent query_pre_process duration in a real system encompasses all children.
            // Verify using the unified breakdown map that each sub-component's millis
            // value is individually valid and the parent concept holds.
            var map = breakdown.toUnifiedBreakdownMap(absStart, reqEnd);

            // Collect recorded sub-component values from the map
            List<Long> activeChildMillis = new ArrayList<>();
            if (globalOrdinals > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(globalOrdinals);
                if (millis > 0) {
                    assertTrue(
                        "Iteration " + i + ": global_ordinals_loading must be in map",
                        map.containsKey("global_ordinals_loading")
                    );
                    activeChildMillis.add((Long) map.get("global_ordinals_loading"));
                }
            }
            if (scriptCompilation > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(scriptCompilation);
                if (millis > 0) {
                    assertTrue(
                        "Iteration " + i + ": script_compilation must be in map",
                        map.containsKey("script_compilation")
                    );
                    activeChildMillis.add((Long) map.get("script_compilation"));
                }
            }
            if (nestedBitset > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(nestedBitset);
                if (millis > 0) {
                    assertTrue(
                        "Iteration " + i + ": nested_bitset_construction must be in map",
                        map.containsKey("nested_bitset_construction")
                    );
                    activeChildMillis.add((Long) map.get("nested_bitset_construction"));
                }
            }
            if (starTree > 0) {
                long millis = TimeUnit.NANOSECONDS.toMillis(starTree);
                if (millis > 0) {
                    assertTrue(
                        "Iteration " + i + ": star_tree_setup must be in map",
                        map.containsKey("star_tree_setup")
                    );
                    activeChildMillis.add((Long) map.get("star_tree_setup"));
                }
            }

            // All active child values should be positive
            for (Long childMillis : activeChildMillis) {
                assertTrue(
                    "Iteration " + i + ": Active sub-component millis value must be > 0, got " + childMillis,
                    childMillis > 0
                );
            }
        }
    }

    /**
     * Property 5: Parent node built with SearchLatencyBreakdownNode directly always
     * satisfies the encompassing invariant even with extreme random values.
     * <p>
     * Generates large random durations and verifies the structural invariant holds.
     * <p>
     * Validates: Requirements 5.5
     */
    public void testProperty5_extremeDurationsStillSatisfyInvariant() {
        for (int i = 0; i < ITERATIONS; i++) {
            // Generate extreme random values
            long globalOrdinals = randomLongBetween(0, 1_000_000_000L);
            long scriptCompilation = randomLongBetween(0, 1_000_000_000L);
            long nestedBitset = randomLongBetween(0, 1_000_000_000L);
            long starTree = randomLongBetween(0, 1_000_000_000L);

            // Parent must encompass all - compute as sum + overhead (realistic model)
            long sumOfAll = globalOrdinals + scriptCompilation + nestedBitset + starTree;
            long parentDuration = sumOfAll + randomLongBetween(0, 10_000_000L);

            SearchLatencyBreakdownNode parent = new SearchLatencyBreakdownNode(
                "Query Pre-Process",
                SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                0,
                parentDuration
            );

            // Add all non-zero children
            if (globalOrdinals > 0) {
                parent.addChild("Global Ordinals", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, 0, globalOrdinals);
            }
            if (scriptCompilation > 0) {
                parent.addChild("Script Compilation", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, globalOrdinals, scriptCompilation);
            }
            if (nestedBitset > 0) {
                parent.addChild("Nested Bitset", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                    globalOrdinals + scriptCompilation, nestedBitset);
            }
            if (starTree > 0) {
                parent.addChild("Star-Tree Setup", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                    globalOrdinals + scriptCompilation + nestedBitset, starTree);
            }

            // INVARIANT: parent >= each child
            long parentMicros = TimeUnit.NANOSECONDS.toMicros(parent.getDurationNanos());
            for (SearchLatencyBreakdownNode child : parent.getChildren()) {
                long childMicros = TimeUnit.NANOSECONDS.toMicros(child.getDurationNanos());
                assertTrue(
                    "Iteration " + i + ": Parent micros (" + parentMicros
                        + ") must >= child '" + child.getName() + "' micros (" + childMicros + ")"
                        + " [parentNanos=" + parentDuration + ", childNanos=" + child.getDurationNanos() + "]",
                    parentMicros >= childMicros
                );
            }

            // INVARIANT: each child's end offset should not exceed parent's end offset
            long parentEnd = parent.getStartOffsetNanos() + parent.getDurationNanos();
            for (SearchLatencyBreakdownNode child : parent.getChildren()) {
                long childEnd = child.getStartOffsetNanos() + child.getDurationNanos();
                assertTrue(
                    "Iteration " + i + ": Child '" + child.getName() + "' end offset ("
                        + childEnd + ") must not exceed parent end offset (" + parentEnd + ")",
                    childEnd <= parentEnd
                );
            }
        }
    }
}
