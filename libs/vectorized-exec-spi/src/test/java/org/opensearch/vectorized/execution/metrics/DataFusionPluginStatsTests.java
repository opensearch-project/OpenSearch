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
    Arbitrary<long[]> longArray28() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(28);
    }

    @Provide
    Arbitrary<DataFusionPluginStats.TaskMonitorValues> taskMonitorValues() {
        Arbitrary<Long> posLong = Arbitraries.longs().between(0, Long.MAX_VALUE / 2);
        Arbitrary<Double> ratio = Arbitraries.doubles().between(0.0, 1.0);
        return Combinators.combine(posLong, posLong, posLong, posLong, posLong, ratio)
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
    void protobufDecodeRoundTrip(@ForAll("longArray28") long[] ioFields,
                                  @ForAll("longArray28") long[] cpuFields,
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
    void absentCpuRuntimeDecodesToNull(@ForAll("longArray28") long[] ioFields,
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
            .setTotalParkCount(fields[1])
            .setMaxParkCount(fields[2])
            .setMinParkCount(fields[3])
            .setTotalNoopCount(fields[4])
            .setMaxNoopCount(fields[5])
            .setMinNoopCount(fields[6])
            .setTotalStealCount(fields[7])
            .setMaxStealCount(fields[8])
            .setMinStealCount(fields[9])
            .setTotalStealOperations(fields[10])
            .setTotalLocalScheduleCount(fields[11])
            .setMaxLocalScheduleCount(fields[12])
            .setMinLocalScheduleCount(fields[13])
            .setTotalOverflowCount(fields[14])
            .setMaxOverflowCount(fields[15])
            .setMinOverflowCount(fields[16])
            .setTotalPollsCount(fields[17])
            .setMaxPollsCount(fields[18])
            .setMinPollsCount(fields[19])
            .setTotalBusyDurationMs(fields[20])
            .setMaxBusyDurationMs(fields[21])
            .setMinBusyDurationMs(fields[22])
            .setTotalLocalQueueDepth(fields[23])
            .setMaxLocalQueueDepth(fields[24])
            .setMinLocalQueueDepth(fields[25])
            .setGlobalQueueDepth(fields[26])
            .setBlockingQueueDepth(fields[27])
            .build();
    }

    private TokioMetricsProto.TaskMonitorMetrics buildTaskMonitorProto(
            DataFusionPluginStats.TaskMonitorValues tm) {
        return TokioMetricsProto.TaskMonitorMetrics.newBuilder()
            .setTotalPollDurationMs(tm.getTotalPollDurationMs())
            .setTotalScheduledDurationMs(tm.getTotalScheduledDurationMs())
            .setTotalIdleDurationMs(tm.getTotalIdleDurationMs())
            .setTotalSlowPollCount(tm.getTotalSlowPollCount())
            .setTotalLongDelayCount(tm.getTotalLongDelayCount())
            .setSlowPollRatio(tm.getSlowPollRatio())
            .build();
    }

    private void assertRuntimeValuesMatch(DataFusionPluginStats.RuntimeValues rv, long[] fields) {
        assertEquals(fields[0], rv.getWorkersCount(), "workersCount mismatch");
        assertEquals(fields[1], rv.getTotalParkCount(), "totalParkCount mismatch");
        assertEquals(fields[2], rv.getMaxParkCount(), "maxParkCount mismatch");
        assertEquals(fields[3], rv.getMinParkCount(), "minParkCount mismatch");
        assertEquals(fields[4], rv.getTotalNoopCount(), "totalNoopCount mismatch");
        assertEquals(fields[5], rv.getMaxNoopCount(), "maxNoopCount mismatch");
        assertEquals(fields[6], rv.getMinNoopCount(), "minNoopCount mismatch");
        assertEquals(fields[7], rv.getTotalStealCount(), "totalStealCount mismatch");
        assertEquals(fields[8], rv.getMaxStealCount(), "maxStealCount mismatch");
        assertEquals(fields[9], rv.getMinStealCount(), "minStealCount mismatch");
        assertEquals(fields[10], rv.getTotalStealOperations(), "totalStealOperations mismatch");
        assertEquals(fields[11], rv.getTotalLocalScheduleCount(), "totalLocalScheduleCount mismatch");
        assertEquals(fields[12], rv.getMaxLocalScheduleCount(), "maxLocalScheduleCount mismatch");
        assertEquals(fields[13], rv.getMinLocalScheduleCount(), "minLocalScheduleCount mismatch");
        assertEquals(fields[14], rv.getTotalOverflowCount(), "totalOverflowCount mismatch");
        assertEquals(fields[15], rv.getMaxOverflowCount(), "maxOverflowCount mismatch");
        assertEquals(fields[16], rv.getMinOverflowCount(), "minOverflowCount mismatch");
        assertEquals(fields[17], rv.getTotalPollsCount(), "totalPollsCount mismatch");
        assertEquals(fields[18], rv.getMaxPollsCount(), "maxPollsCount mismatch");
        assertEquals(fields[19], rv.getMinPollsCount(), "minPollsCount mismatch");
        assertEquals(fields[20], rv.getTotalBusyDurationMs(), "totalBusyDurationMs mismatch");
        assertEquals(fields[21], rv.getMaxBusyDurationMs(), "maxBusyDurationMs mismatch");
        assertEquals(fields[22], rv.getMinBusyDurationMs(), "minBusyDurationMs mismatch");
        assertEquals(fields[23], rv.getTotalLocalQueueDepth(), "totalLocalQueueDepth mismatch");
        assertEquals(fields[24], rv.getMaxLocalQueueDepth(), "maxLocalQueueDepth mismatch");
        assertEquals(fields[25], rv.getMinLocalQueueDepth(), "minLocalQueueDepth mismatch");
        assertEquals(fields[26], rv.getGlobalQueueDepth(), "globalQueueDepth mismatch");
        assertEquals(fields[27], rv.getBlockingQueueDepth(), "blockingQueueDepth mismatch");
    }

    private void assertTaskMonitorEquals(DataFusionPluginStats.TaskMonitorValues expected,
                                          DataFusionPluginStats.TaskMonitorValues actual) {
        assertEquals(expected.getTotalPollDurationMs(), actual.getTotalPollDurationMs());
        assertEquals(expected.getTotalScheduledDurationMs(), actual.getTotalScheduledDurationMs());
        assertEquals(expected.getTotalIdleDurationMs(), actual.getTotalIdleDurationMs());
        assertEquals(expected.getTotalSlowPollCount(), actual.getTotalSlowPollCount());
        assertEquals(expected.getTotalLongDelayCount(), actual.getTotalLongDelayCount());
        assertEquals(expected.getSlowPollRatio(), actual.getSlowPollRatio(), 1e-10);
    }
}
