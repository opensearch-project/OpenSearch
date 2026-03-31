/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.vectorized.execution.metrics;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Property-based tests for DataFusionPluginStats long[] decode using jqwik.
 *
 * Writeable round-trip and XContent tests have moved to NativeExecutorsStatsTests
 * in the server module, since DataFusionPluginStats is now a pure POJO.
 *
 * Feature: proto-to-longarray-migration
 */
public class DataFusionPluginStatsTests {

    // --- Arbitraries ---

    /**
     * Generates a valid long[27] array with non-zero cpu_runtime workers_count (index 6)
     * so that cpuRuntime is present after decode.
     */
    @Provide
    Arbitrary<long[]> validLong27WithCpu() {
        return Arbitraries.longs().between(1, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(27);
    }

    /**
     * Generates a valid long[27] array with cpu_runtime workers_count == 0 (index 6)
     * so that cpuRuntime is null after decode.
     */
    @Provide
    Arbitrary<long[]> validLong27WithoutCpu() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
            .array(long[].class).ofSize(27)
            .map(arr -> {
                // Zero out all cpu_runtime slots [6..11]
                for (int i = 6; i < 12; i++) {
                    arr[i] = 0;
                }
                return arr;
            });
    }

    // --- Property 1: long[] decode round-trip with CPU runtime present ---

    /**
     * Property 1: For any valid long[27] with non-zero cpu workers_count,
     * decode(long[]) produces an object whose field values match the input array.
     *
     * **Validates: Requirements 2.1**
     */
    @Property(tries = 100)
    void longArrayDecodeRoundTripWithCpu(@ForAll("validLong27WithCpu") long[] data) {
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(data);

        // IO runtime [0..5]
        assertNotNull(decoded.getIoRuntime());
        assertRuntimeValues(decoded.getIoRuntime(), data, 0);

        // CPU runtime [6..11] — present because workers_count > 0
        assertNotNull(decoded.getCpuRuntime(), "cpuRuntime should be present when workers_count > 0");
        assertRuntimeValues(decoded.getCpuRuntime(), data, 6);

        // Task monitors [12..26]
        assertTaskMonitorValues(decoded.getQueryExecution(), data, 12);
        assertTaskMonitorValues(decoded.getStreamNext(), data, 15);
        assertTaskMonitorValues(decoded.getFetchPhase(), data, 18);
        assertTaskMonitorValues(decoded.getSegmentStats(), data, 21);
        assertTaskMonitorValues(decoded.getIndexedQueryExecution(), data, 24);
    }

    // --- Property 2: absent CPU runtime when workers_count == 0 ---

    /**
     * Property 2: When cpu_runtime workers_count (index 6) is 0 and all cpu slots
     * are zero, decoded cpuRuntime is null while all other fields decode correctly.
     *
     * **Validates: Requirements 2.3**
     */
    @Property(tries = 50)
    void absentCpuRuntimeWhenWorkersCountZero(@ForAll("validLong27WithoutCpu") long[] data) {
        DataFusionPluginStats decoded = DataFusionPluginStats.decode(data);

        assertNull(decoded.getCpuRuntime(), "cpuRuntime should be null when workers_count == 0");
        assertNotNull(decoded.getIoRuntime(), "ioRuntime should still be present");
        assertRuntimeValues(decoded.getIoRuntime(), data, 0);

        // Task monitors still decode correctly
        assertTaskMonitorValues(decoded.getQueryExecution(), data, 12);
        assertTaskMonitorValues(decoded.getStreamNext(), data, 15);
        assertTaskMonitorValues(decoded.getFetchPhase(), data, 18);
        assertTaskMonitorValues(decoded.getSegmentStats(), data, 21);
        assertTaskMonitorValues(decoded.getIndexedQueryExecution(), data, 24);
    }

    // --- Property 3: invalid array length throws ---

    /**
     * Property 3: Passing an array whose length is not 27 to decode() throws
     * IllegalArgumentException.
     *
     * **Validates: Requirements 2.2**
     */
    @Property(tries = 50)
    void invalidArrayLengthThrows(@ForAll("invalidLength") long[] data) {
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(data),
            "decode() should throw IllegalArgumentException for array length != 27");
    }

    @Provide
    Arbitrary<long[]> invalidLength() {
        // Generate arrays of length 0..50 but exclude 27
        return Arbitraries.integers().between(0, 50)
            .filter(len -> len != 27)
            .flatMap(len -> Arbitraries.longs().between(0, Long.MAX_VALUE / 2)
                .array(long[].class).ofSize(len));
    }

    // --- Property 4: null array throws ---

    /**
     * Edge case: Passing null to decode() throws IllegalArgumentException.
     *
     * **Validates: Requirements 2.2**
     */
    @Property(tries = 1)
    void nullArrayThrowsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> DataFusionPluginStats.decode(null),
            "decode() should throw IllegalArgumentException for null array");
    }

    // --- Helper methods ---

    private void assertRuntimeValues(DataFusionPluginStats.RuntimeValues rv, long[] data, int offset) {
        assertEquals(data[offset], rv.getWorkersCount(), "workersCount mismatch at offset " + offset);
        assertEquals(data[offset + 1], rv.getTotalPollsCount(), "totalPollsCount mismatch");
        assertEquals(data[offset + 2], rv.getTotalBusyDurationMs(), "totalBusyDurationMs mismatch");
        assertEquals(data[offset + 3], rv.getTotalOverflowCount(), "totalOverflowCount mismatch");
        assertEquals(data[offset + 4], rv.getGlobalQueueDepth(), "globalQueueDepth mismatch");
        assertEquals(data[offset + 5], rv.getBlockingQueueDepth(), "blockingQueueDepth mismatch");
    }

    private void assertTaskMonitorValues(DataFusionPluginStats.TaskMonitorValues tm, long[] data, int offset) {
        assertEquals(data[offset], tm.getTotalPollDurationMs(), "totalPollDurationMs mismatch at offset " + offset);
        assertEquals(data[offset + 1], tm.getTotalScheduledDurationMs(), "totalScheduledDurationMs mismatch");
        assertEquals(data[offset + 2], tm.getTotalIdleDurationMs(), "totalIdleDurationMs mismatch");
    }
}
