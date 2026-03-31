/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.vectorized.execution.metrics.DataFusionPluginStats;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * Property-based tests for {@link NativeExecutorsStats} — the server-side
 * Writeable + ToXContentFragment wrapper around {@link DataFusionPluginStats}.
 *
 * Validates: Requirements 6
 */
public class NativeExecutorsStatsTests {

    // --- Arbitraries (same pattern as NodeStatsNativeMetricRoundTripTests) ---

    @Provide
    Arbitrary<DataFusionPluginStats.RuntimeValues> runtimeValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong, posLong, posLong, posLong)
            .as((a, b, c, d, e, f) -> {
                long[] data = new long[] { a, b, c, d, e, f };
                return new DataFusionPluginStats.RuntimeValues(data, 0);
            });
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong)
            .as((a, b, c) -> {
                long[] data = new long[] { a, b, c };
                return new DataFusionPluginStats.TaskMonitorValues(data, 0);
            });
    }

    @Provide
    Arbitrary<DataFusionPluginStats> dataFusionPluginStats() {
        Arbitrary<DataFusionPluginStats.RuntimeValues> rv = runtimeValues();
        Arbitrary<DataFusionPluginStats.TaskMonitorValues> tm = taskMonitorValues();
        return Combinators.combine(
            rv.injectNull(0.3),    // ioRuntime (30% chance null)
            rv.injectNull(0.3),    // cpuRuntime (30% chance null)
            tm, tm, tm, tm, tm     // 5 task monitors (always non-null)
        ).as(DataFusionPluginStats::new);
    }

    // --- Property 1: Writeable round-trip ---

    /**
     * For any valid NativeExecutorsStats, serializing via writeTo(StreamOutput)
     * and deserializing via new NativeExecutorsStats(StreamInput) should produce
     * an equal object.
     *
     * **Validates: Requirements 6**
     */
    @Property(tries = 100)
    void writeableRoundTripProducesEqualObject(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats original = new NativeExecutorsStats(pluginStats);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        StreamInput in = out.bytes().streamInput();
        NativeExecutorsStats deserialized = new NativeExecutorsStats(in);

        assertEquals(original, deserialized,
            "NativeExecutorsStats should be equal after Writeable round-trip");
        assertEquals(original.getDataFusionPluginStats(), deserialized.getDataFusionPluginStats(),
            "Wrapped DataFusionPluginStats should be equal after round-trip");
    }

    // --- Property 2: XContent structure ---

    /**
     * For any valid NativeExecutorsStats, toXContent() should produce JSON
     * containing the expected keys and matching field values.
     *
     * **Validates: Requirements 6**
     */
    @Property(tries = 100)
    @SuppressWarnings("unchecked")
    void xContentContainsExpectedStructure(
            @ForAll("dataFusionPluginStats") DataFusionPluginStats pluginStats) throws IOException {
        NativeExecutorsStats stats = new NativeExecutorsStats(pluginStats);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        stats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        Map<String, Object> json = XContentHelper.convertToMap(
            BytesReference.bytes(builder), true, builder.contentType()
        ).v2();

        // task_monitors key always present with all 4 sub-keys
        assertTrue(json.containsKey("task_monitors"), "task_monitors key must be present");
        Map<String, Object> taskMonitors = (Map<String, Object>) json.get("task_monitors");
        assertNotNull(taskMonitors, "task_monitors must not be null");
        assertTrue(taskMonitors.containsKey("query_execution"), "query_execution must be present");
        assertTrue(taskMonitors.containsKey("stream_next"), "stream_next must be present");
        assertTrue(taskMonitors.containsKey("fetch_phase"), "fetch_phase must be present");
        assertTrue(taskMonitors.containsKey("segment_stats"), "segment_stats must be present");
        assertTrue(taskMonitors.containsKey("indexed_query_execution"), "indexed_query_execution must be present");

        // io_runtime present when non-null, absent when null
        if (pluginStats.getIoRuntime() != null) {
            assertTrue(json.containsKey("io_runtime"), "io_runtime should be present when non-null");
            Map<String, Object> ioRuntime = (Map<String, Object>) json.get("io_runtime");
            assertEquals(pluginStats.getIoRuntime().getWorkersCount(),
                ((Number) ioRuntime.get("workers_count")).longValue(),
                "workers_count should match for io_runtime");
        } else {
            assertFalse(json.containsKey("io_runtime"), "io_runtime should be absent when null");
        }

        // cpu_runtime present when non-null, absent when null
        if (pluginStats.getCpuRuntime() != null) {
            assertTrue(json.containsKey("cpu_runtime"), "cpu_runtime should be present when non-null");
            Map<String, Object> cpuRuntime = (Map<String, Object>) json.get("cpu_runtime");
            assertEquals(pluginStats.getCpuRuntime().getWorkersCount(),
                ((Number) cpuRuntime.get("workers_count")).longValue(),
                "workers_count should match for cpu_runtime");
        } else {
            assertFalse(json.containsKey("cpu_runtime"), "cpu_runtime should be absent when null");
        }

        // Verify task monitor field values match
        Map<String, Object> queryExec = (Map<String, Object>) taskMonitors.get("query_execution");
        assertEquals(pluginStats.getQueryExecution().getTotalPollDurationMs(),
            ((Number) queryExec.get("total_poll_duration_ms")).longValue(),
            "total_poll_duration_ms should match for query_execution");
        assertEquals(pluginStats.getQueryExecution().getTotalScheduledDurationMs(),
            ((Number) queryExec.get("total_scheduled_duration_ms")).longValue(),
            "total_scheduled_duration_ms should match for query_execution");
    }

    // --- Property 3: Null handling ---

    /**
     * NativeExecutorsStats constructor should throw NullPointerException
     * when passed null DataFusionPluginStats.
     *
     * **Validates: Requirements 6**
     */
    @Property(tries = 1)
    void constructorRejectsNullDataFusionPluginStats() {
        assertThrows(NullPointerException.class, () -> new NativeExecutorsStats((DataFusionPluginStats) null),
            "Constructor should throw NullPointerException for null DataFusionPluginStats");
    }
}
