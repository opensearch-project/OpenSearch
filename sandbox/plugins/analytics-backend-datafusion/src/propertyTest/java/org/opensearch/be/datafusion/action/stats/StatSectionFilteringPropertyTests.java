/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.action.stats;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.opensearch.be.datafusion.stats.DataFusionStats;
import org.opensearch.be.datafusion.stats.NativeExecutorsStats;
import org.opensearch.be.datafusion.stats.PartitionGateStats;
import org.opensearch.be.datafusion.stats.RuntimeMetrics;
import org.opensearch.be.datafusion.stats.TaskMonitorStats;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property-based tests for stat section filtering correctness.
 *
 * <p>Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
 *
 * <p>For any non-empty subset of valid stat section names and any {@link DataFusionStats}
 * instance, when the subset is applied as a filter via
 * {@link TransportDataFusionStatsAction#filteredStats}, the rendered JSON for that node
 * contains exactly the sections in the subset and no others.
 *
 * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
 */
public class StatSectionFilteringPropertyTests {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** All 8 valid stat section names. */
    private static final List<String> ALL_SECTIONS = List.of(
        "io_runtime",
        "cpu_runtime",
        "coordinator_reduce",
        "query_execution",
        "stream_next",
        "plan_setup",
        "datanode_gate",
        "coordinator_gate"
    );

    // ---- Object generators ----

    @Provide
    Arbitrary<RuntimeMetrics> runtimeMetrics() {
        return Arbitraries.longs()
            .between(0, Long.MAX_VALUE / 2)
            .list()
            .ofSize(9)
            .map(l -> new RuntimeMetrics(l.get(0), l.get(1), l.get(2), l.get(3), l.get(4), l.get(5), l.get(6), l.get(7), l.get(8)));
    }

    @Provide
    Arbitrary<TaskMonitorStats> taskMonitorStats() {
        Arbitrary<Long> nonNeg = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(nonNeg, nonNeg, nonNeg).as(TaskMonitorStats::new);
    }

    /** DataFusionStats with all sections populated (CPU runtime present). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsFullCpuPresent() {
        return Combinators.combine(runtimeMetrics(), runtimeMetrics().map(rt -> {
            if (rt.workersCount == 0) {
                return new RuntimeMetrics(
                    1,
                    rt.totalPollsCount,
                    rt.totalBusyDurationMs,
                    rt.totalOverflowCount,
                    rt.globalQueueDepth,
                    rt.blockingQueueDepth,
                    rt.numAliveTasks,
                    rt.spawnedTasksCount,
                    rt.totalLocalQueueDepth
                );
            }
            return rt;
        }), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats()).as((io, cpu, cr, qe, sn, ps) -> {
            Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
            monitors.put("coordinator_reduce", cr);
            monitors.put("query_execution", qe);
            monitors.put("stream_next", sn);
            monitors.put("plan_setup", ps);
            return new DataFusionStats(
                new NativeExecutorsStats(io, cpu, monitors),
                new PartitionGateStats("datanode_gate", 12, 3, 100, 50),
                new PartitionGateStats("coordinator_gate", 8, 1, 200, 75)
            );
        });
    }

    /** DataFusionStats with CPU runtime absent. */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStatsFullCpuAbsent() {
        return Combinators.combine(runtimeMetrics(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats(), taskMonitorStats())
            .as((io, cr, qe, sn, ps) -> {
                Map<String, TaskMonitorStats> monitors = new LinkedHashMap<>();
                monitors.put("coordinator_reduce", cr);
                monitors.put("query_execution", qe);
                monitors.put("stream_next", sn);
                monitors.put("plan_setup", ps);
                return new DataFusionStats(
                    new NativeExecutorsStats(io, null, monitors),
                    new PartitionGateStats("datanode_gate", 12, 3, 100, 50),
                    new PartitionGateStats("coordinator_gate", 8, 1, 200, 75)
                );
            });
    }

    /** Combined DataFusionStats generator (CPU present or absent). */
    @Provide
    Arbitrary<DataFusionStats> dataFusionStats() {
        return Arbitraries.oneOf(dataFusionStatsFullCpuPresent(), dataFusionStatsFullCpuAbsent());
    }

    /** Generates a non-empty subset of the 8 valid stat section names. */
    @Provide
    Arbitrary<Set<String>> statSectionSubset() {
        return Arbitraries.of(ALL_SECTIONS).set().ofMinSize(1).ofMaxSize(8);
    }

    // ---- Property 3: Stat section filtering correctness ----

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     *
     * <p>For any non-empty subset of valid stat section names and any DataFusionStats
     * instance, filtered output contains exactly the requested sections and no others.
     *
     * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
     */
    @Property(tries = 150)
    void filteredStatsContainsExactlyRequestedSections(
        @ForAll("dataFusionStats") DataFusionStats stats,
        @ForAll("statSectionSubset") Set<String> requestedSections
    ) throws IOException {
        DataFusionStats filtered = TransportDataFusionStatsAction.filteredStats(stats, requestedSections);

        // Render filtered stats to JSON
        String json = renderJson(filtered);
        JsonNode root = MAPPER.readTree(json);

        // Collect all top-level keys from the rendered JSON
        Set<String> actualSections = new HashSet<>();
        Iterator<String> fieldNames = root.fieldNames();
        while (fieldNames.hasNext()) {
            actualSections.add(fieldNames.next());
        }

        // Determine expected sections: only sections that were both requested
        // AND present in the original stats will appear in the output.
        Set<String> expectedSections = computeExpectedSections(stats, requestedSections);

        // Verify: actual sections == expected sections
        assertEquals(
            expectedSections,
            actualSections,
            "Filtered JSON must contain exactly the requested (and available) sections. " + "Requested: " + requestedSections
        );
    }

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     * (complement check)
     *
     * <p>For any non-empty subset of valid stat section names and any DataFusionStats
     * instance, sections NOT in the filter are absent from the rendered JSON.
     *
     * <p><b>Validates: Requirements 3.1, 3.2, 3.4, 5.3</b>
     */
    @Property(tries = 150)
    void filteredStatsExcludesUnrequestedSections(
        @ForAll("dataFusionStats") DataFusionStats stats,
        @ForAll("statSectionSubset") Set<String> requestedSections
    ) throws IOException {
        DataFusionStats filtered = TransportDataFusionStatsAction.filteredStats(stats, requestedSections);

        // Render filtered stats to JSON
        String json = renderJson(filtered);
        JsonNode root = MAPPER.readTree(json);

        // Sections NOT requested must be absent from the output
        Set<String> excludedSections = new HashSet<>(ALL_SECTIONS);
        excludedSections.removeAll(requestedSections);

        for (String excluded : excludedSections) {
            assertFalse(
                root.has(excluded),
                "Section '" + excluded + "' should NOT be present in filtered output. " + "Requested: " + requestedSections
            );
        }
    }

    /**
     * Feature: datafusion-cluster-stats, Property 3: Stat section filtering correctness
     * (empty/null filter returns all)
     *
     * <p>When the filter is null or empty, all sections from the original stats are preserved.
     *
     * <p><b>Validates: Requirements 3.4, 5.3</b>
     */
    @Property(tries = 100)
    void emptyFilterReturnsAllSections(@ForAll("dataFusionStats") DataFusionStats stats) throws IOException {
        // Null filter returns same object
        DataFusionStats filteredNull = TransportDataFusionStatsAction.filteredStats(stats, null);
        assertTrue(stats == filteredNull, "Null filter must return the original stats object");

        // Empty filter returns same object
        DataFusionStats filteredEmpty = TransportDataFusionStatsAction.filteredStats(stats, Set.of());
        assertTrue(stats == filteredEmpty, "Empty filter must return the original stats object");
    }

    // ---- Helper methods ----

    /**
     * Computes the set of section names expected in the filtered JSON output.
     * A section appears only if it was requested AND the original stats had
     * non-null data for that section.
     */
    private Set<String> computeExpectedSections(DataFusionStats stats, Set<String> requested) {
        Set<String> expected = new HashSet<>();
        for (String section : requested) {
            switch (section) {
                case "cpu_runtime":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getCpuRuntime() != null) {
                        expected.add(section);
                    }
                    break;
                case "io_runtime":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getIoRuntime() != null) {
                        expected.add(section);
                    }
                    break;
                case "coordinator_reduce":
                case "query_execution":
                case "stream_next":
                case "plan_setup":
                    if (stats.getNativeExecutorsStats() != null && stats.getNativeExecutorsStats().getTaskMonitors().get(section) != null) {
                        expected.add(section);
                    }
                    break;
                case "datanode_gate":
                    if (stats.getDatanodeGateStats() != null) {
                        expected.add(section);
                    }
                    break;
                case "coordinator_gate":
                    if (stats.getCoordinatorGateStats() != null) {
                        expected.add(section);
                    }
                    break;
                default:
                    break;
            }
        }
        return expected;
    }

    private String renderJson(DataFusionStats stats) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        if (stats != null) {
            stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        builder.endObject();
        return builder.toString();
    }
}
