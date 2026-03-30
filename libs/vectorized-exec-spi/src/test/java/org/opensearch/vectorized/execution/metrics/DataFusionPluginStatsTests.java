/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import org.opensearch.vectorized.execution.metrics.proto.DataFusionStatsProto;
import org.opensearch.vectorized.execution.metrics.proto.TokioMetricsProto;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Property-based tests for DataFusionPluginStats protobuf decode using jqwik.
 *
 * Writeable round-trip and XContent tests have moved to NativeExecutorsStatsTests
 * in the server module, since DataFusionPluginStats is now a pure POJO.
 *
 * Feature: native-metrics-stats-wrapper
 */
public class DataFusionPluginStatsTests {

    // --- Arbitraries ---

    @Provide
    Arbitrary<long[]> longArray6() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(6);
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        return Combinators.combine(posLong, posLong, posLong)
            .as(DataFusionPluginStats.TaskMonitorValues::new);
    }

    // --- Property 1: Protobuf decode round-trip ---

    /**
     * Property 1: DataFusionPluginStats protobuf decode round-trip
     *
     * For any valid set of runtime + task monitor metric values, encoding to protobuf
     * and decoding via DataFusionPluginStats.decode(byte[]) SHALL produce an object
     * with field values matching the originals.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 100)
    void protobufDecodeRoundTrip(@ForAll("longArray6") long[] ioFields,
                                  @ForAll("longArray6") long[] cpuFields,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues qe,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues sn,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues fp,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues ss,
                                  @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues iqe) {
        // Build protobuf message
        TokioMetricsProto.RuntimeMetrics ioProto = buildRuntimeMetricsProto(ioFields);
        TokioMetricsProto.RuntimeMetrics cpuProto = buildRuntimeMetricsProto(cpuFields);

        DataFusionStatsProto.DataFusionStats proto = DataFusionStatsProto.DataFusionStats.newBuilder()
            .setIoRuntime(ioProto)
            .setCpuRuntime(cpuProto)
            .setTaskMonitors(DataFusionStatsProto.TaskMonitors.newBuilder()
                .setQueryExecution(buildTaskMonitorProto(qe))
                .setStreamNext(buildTaskMonitorProto(sn))
                .setFetchPhase(buildTaskMonitorProto(fp))
                .setSegmentStats(buildTaskMonitorProto(ss))
                .setIndexedQueryExecution(buildTaskMonitorProto(iqe))
                .build())
            .build();

        byte[] bytes = proto.toByteArray();
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(bytes);

        // Verify IO runtime fields
        assertNotNull(decoded.getIoRuntime());
        assertRuntimeValuesMatch(decoded.getIoRuntime(), ioFields);

        // Verify CPU runtime fields
        assertNotNull(decoded.getCpuRuntime());
        assertRuntimeValuesMatch(decoded.getCpuRuntime(), cpuFields);

        // Verify task monitor fields
        assertTaskMonitorEquals(qe, decoded.getQueryExecution());
        assertTaskMonitorEquals(sn, decoded.getStreamNext());
        assertTaskMonitorEquals(fp, decoded.getFetchPhase());
        assertTaskMonitorEquals(ss, decoded.getSegmentStats());
        assertTaskMonitorEquals(iqe, decoded.getIndexedQueryExecution());
    }

    // --- Edge case: absent CPU runtime → null cpuRuntime ---

    /**
     * Edge case: When protobuf message has no cpu_runtime field set,
     * decoded DataFusionPluginStats should have null cpuRuntime.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 50)
    void absentCpuRuntimeDecodesToNull(@ForAll("longArray6") long[] ioFields,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues qe,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues sn,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues fp,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues ss,
                                        @ForAll("taskMonitorValues") DataFusionPluginStats.TaskMonitorValues iqe) {
        // Build protobuf WITHOUT cpu_runtime
        DataFusionStatsProto.DataFusionStats proto = DataFusionStatsProto.DataFusionStats.newBuilder()
            .setIoRuntime(buildRuntimeMetricsProto(ioFields))
            .setTaskMonitors(DataFusionStatsProto.TaskMonitors.newBuilder()
                .setQueryExecution(buildTaskMonitorProto(qe))
                .setStreamNext(buildTaskMonitorProto(sn))
                .setFetchPhase(buildTaskMonitorProto(fp))
                .setSegmentStats(buildTaskMonitorProto(ss))
                .setIndexedQueryExecution(buildTaskMonitorProto(iqe))
                .build())
            .build();

        DataFusionPluginStats decoded = DataFusionPluginStats.decode(proto.toByteArray());
        assertNull(decoded.getCpuRuntime(), "cpuRuntime should be null when absent from protobuf");
        assertNotNull(decoded.getIoRuntime(), "ioRuntime should still be present");
    }

    // --- Edge case: invalid bytes → IllegalArgumentException ---

    /**
     * Edge case: Passing invalid (non-protobuf) bytes to decode() should throw
     * IllegalArgumentException.
     *
     * **Validates: Requirements 4.4**
     */
    @Property(tries = 1)
    void invalidBytesThrowsIllegalArgumentException() {
        // A varint that signals "more bytes follow" (high bit set) but is truncated.
        // Field 1, wire type 0 (varint), then 0x80 means "continuation" with no following byte.
        byte[] truncatedVarint = new byte[]{0x08, (byte) 0x80};
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(truncatedVarint),
            "decode() should throw IllegalArgumentException for invalid protobuf bytes");
    }

    // --- Helper methods ---

    private TokioMetricsProto.RuntimeMetrics buildRuntimeMetricsProto(long[] fields) {
        return TokioMetricsProto.RuntimeMetrics.newBuilder()
            .setWorkersCount(fields[0])
            .setTotalPollsCount(fields[1])
            .setTotalBusyDurationMs(fields[2])
            .setTotalOverflowCount(fields[3])
            .setGlobalQueueDepth(fields[4])
            .setBlockingQueueDepth(fields[5])
            .build();
    }

    private TokioMetricsProto.TaskMonitorMetrics buildTaskMonitorProto(
            DataFusionPluginStats.TaskMonitorValues tm) {
        return TokioMetricsProto.TaskMonitorMetrics.newBuilder()
            .setTotalPollDurationMs(tm.getTotalPollDurationMs())
            .setTotalScheduledDurationMs(tm.getTotalScheduledDurationMs())
            .setTotalIdleDurationMs(tm.getTotalIdleDurationMs())
            .build();
    }

    private void assertRuntimeValuesMatch(DataFusionPluginStats.RuntimeValues rv, long[] fields) {
        assertEquals(fields[0], rv.getWorkersCount(), "workersCount mismatch");
        assertEquals(fields[1], rv.getTotalPollsCount(), "totalPollsCount mismatch");
        assertEquals(fields[2], rv.getTotalBusyDurationMs(), "totalBusyDurationMs mismatch");
        assertEquals(fields[3], rv.getTotalOverflowCount(), "totalOverflowCount mismatch");
        assertEquals(fields[4], rv.getGlobalQueueDepth(), "globalQueueDepth mismatch");
        assertEquals(fields[5], rv.getBlockingQueueDepth(), "blockingQueueDepth mismatch");
    }

    private void assertTaskMonitorEquals(DataFusionPluginStats.TaskMonitorValues expected,
                                          DataFusionPluginStats.TaskMonitorValues actual) {
        assertEquals(expected.getTotalPollDurationMs(), actual.getTotalPollDurationMs());
        assertEquals(expected.getTotalScheduledDurationMs(), actual.getTotalScheduledDurationMs());
        assertEquals(expected.getTotalIdleDurationMs(), actual.getTotalIdleDurationMs());
    }
}
