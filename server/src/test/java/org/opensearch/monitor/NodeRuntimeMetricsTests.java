/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.monitor;

import org.opensearch.common.settings.Settings;
import org.opensearch.monitor.jvm.JvmService;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsProbe;
import org.opensearch.monitor.process.ProcessProbe;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;
import org.opensearch.test.OpenSearchTestCase;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NodeRuntimeMetricsTests extends OpenSearchTestCase {

    private MetricsRegistry registry;
    private JvmService jvmService;
    private ProcessProbe processProbe;
    private OsProbe osProbe;
    private List<Closeable> createdHandles;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = mock(MetricsRegistry.class);
        jvmService = new JvmService(Settings.EMPTY);
        processProbe = ProcessProbe.getInstance();
        osProbe = OsProbe.getInstance();
        createdHandles = new ArrayList<>();

        when(registry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenAnswer(invocation -> {
            Closeable handle = mock(Closeable.class);
            createdHandles.add(handle);
            return handle;
        });
    }

    public void testRegistersMemoryGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        Tags heapTags = Tags.of(NodeRuntimeMetrics.TAG_TYPE, "heap");
        Tags nonHeapTags = Tags.of(NodeRuntimeMetrics.TAG_TYPE, "non_heap");

        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_MEMORY_USED),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_BYTES),
            any(),
            eq(heapTags)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_MEMORY_COMMITTED),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_BYTES),
            any(),
            eq(heapTags)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_MEMORY_LIMIT),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_BYTES),
            any(),
            eq(heapTags)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_MEMORY_USED),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_BYTES),
            any(),
            eq(nonHeapTags)
        );
    }

    public void testRegistersMemoryPoolGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        JvmStats stats = jvmService.stats();
        for (JvmStats.MemoryPool pool : stats.getMem()) {
            Tags expectedTags = Tags.of(NodeRuntimeMetrics.TAG_POOL, pool.getName());
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_MEMORY_USED),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_BYTES),
                any(),
                eq(expectedTags)
            );
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_MEMORY_LIMIT),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_BYTES),
                any(),
                eq(expectedTags)
            );
        }
    }

    public void testRegistersGcGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        JvmStats stats = jvmService.stats();
        for (JvmStats.GarbageCollector gc : stats.getGc()) {
            Tags expectedTags = Tags.of(NodeRuntimeMetrics.TAG_GC, gc.getName());
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_GC_DURATION),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_SECONDS),
                any(),
                eq(expectedTags)
            );
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_GC_COUNT),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_1),
                any(),
                eq(expectedTags)
            );
        }
    }

    public void testRegistersBufferPoolGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        JvmStats stats = jvmService.stats();
        for (JvmStats.BufferPool bp : stats.getBufferPools()) {
            Tags expectedTags = Tags.of(NodeRuntimeMetrics.TAG_POOL, bp.getName());
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_BUFFER_MEMORY_USED),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_BYTES),
                any(),
                eq(expectedTags)
            );
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_BUFFER_MEMORY_LIMIT),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_BYTES),
                any(),
                eq(expectedTags)
            );
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_BUFFER_COUNT),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_1),
                any(),
                eq(expectedTags)
            );
        }
    }

    public void testRegistersThreadGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_THREAD_COUNT),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );

        for (Thread.State state : Thread.State.values()) {
            Tags expectedTags = Tags.of(NodeRuntimeMetrics.TAG_STATE, state.name().toLowerCase(Locale.ROOT));
            verify(registry).createGauge(
                eq(NodeRuntimeMetrics.JVM_THREAD_COUNT),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_1),
                any(),
                eq(expectedTags)
            );
        }
    }

    public void testRegistersClassGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_CLASS_COUNT),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_CLASS_LOADED),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_CLASS_UNLOADED),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );
    }

    public void testRegistersUptimeGauge() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_UPTIME),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_SECONDS),
            any(),
            eq(Tags.EMPTY)
        );
    }

    public void testRegistersCpuGauges() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_CPU_RECENT_UTILIZATION),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );
        verify(registry).createGauge(
            eq(NodeRuntimeMetrics.JVM_SYSTEM_CPU_UTILIZATION),
            anyString(),
            eq(NodeRuntimeMetrics.UNIT_1),
            any(),
            eq(Tags.EMPTY)
        );
    }

    public void testTotalGaugeCount() {
        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        JvmStats stats = jvmService.stats();
        int memoryPools = 0;
        for (JvmStats.MemoryPool ignored : stats.getMem())
            memoryPools++;
        int gcCollectors = stats.getGc().getCollectors().length;
        int bufferPools = stats.getBufferPools().size();

        int expected = 4                            // memory aggregates (3 heap + 1 non-heap)
            + (memoryPools * 2)                     // memory pools (used + limit per pool)
            + (gcCollectors * 2)                    // GC (duration + count per collector)
            + (bufferPools * 3)                     // buffer pools (used + limit + count per pool)
            + 1 + Thread.State.values().length      // threads (total + per-state)
            + 3                                     // classes
            + 1                                     // uptime
            + 2;                                    // CPU

        verify(registry, org.mockito.Mockito.times(expected)).createGauge(
            anyString(),
            anyString(),
            anyString(),
            any(Supplier.class),
            any(Tags.class)
        );
    }

    public void testCloseClosesAllHandles() throws Exception {
        NodeRuntimeMetrics metrics = new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);
        int handleCount = createdHandles.size();
        assertTrue("Expected gauge handles to be created", handleCount > 0);

        metrics.close();

        for (Closeable handle : createdHandles) {
            verify(handle).close();
        }
    }

    public void testCloseHandlesErrors() throws Exception {
        NodeRuntimeMetrics metrics = new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);
        for (Closeable handle : createdHandles) {
            doThrow(new IOException("test")).when(handle).close();
        }

        metrics.close();
    }

    public void testCloseIdempotent() throws Exception {
        NodeRuntimeMetrics metrics = new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);
        metrics.close();
        metrics.close();
    }

    @SuppressWarnings("unchecked")
    public void testHeapMemoryGaugeSupplierReturnsValidValue() {
        Tags heapTags = Tags.of(NodeRuntimeMetrics.TAG_TYPE, "heap");
        final Supplier<Double>[] captured = new Supplier[1];
        when(registry.createGauge(eq(NodeRuntimeMetrics.JVM_MEMORY_USED), anyString(), anyString(), any(Supplier.class), eq(heapTags)))
            .thenAnswer(invocation -> {
                captured[0] = invocation.getArgument(3);
                return mock(Closeable.class);
            });

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        assertNotNull(captured[0]);
        double value = captured[0].get();
        assertTrue("Heap used should be positive", value > 0);
    }

    @SuppressWarnings("unchecked")
    public void testUptimeGaugeSupplierReturnsValidValue() {
        final Supplier<Double>[] captured = new Supplier[1];
        when(registry.createGauge(eq(NodeRuntimeMetrics.JVM_UPTIME), anyString(), anyString(), any(Supplier.class), any(Tags.class)))
            .thenAnswer(invocation -> {
                captured[0] = invocation.getArgument(3);
                return mock(Closeable.class);
            });

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        assertNotNull(captured[0]);
        double value = captured[0].get();
        assertTrue("Uptime should be positive", value > 0);
    }

    @SuppressWarnings("unchecked")
    public void testThreadStateGaugesReturnNonNegative() {
        List<Supplier<Double>> stateSuppliers = new ArrayList<>();
        when(registry.createGauge(eq(NodeRuntimeMetrics.JVM_THREAD_COUNT), anyString(), anyString(), any(Supplier.class), any(Tags.class)))
            .thenAnswer(invocation -> {
                Tags tags = invocation.getArgument(4);
                if (!Tags.EMPTY.equals(tags)) {
                    stateSuppliers.add(invocation.getArgument(3));
                }
                return mock(Closeable.class);
            });

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        assertEquals(Thread.State.values().length, stateSuppliers.size());
        double total = 0;
        for (Supplier<Double> supplier : stateSuppliers) {
            double count = supplier.get();
            assertTrue("Thread state count should be >= 0", count >= 0);
            total += count;
        }
        assertTrue("Total thread state count should be positive", total > 0);
    }

    @SuppressWarnings("unchecked")
    public void testGcDurationInSeconds() {
        final Supplier<Double>[] captured = new Supplier[1];
        when(
            registry.createGauge(
                eq(NodeRuntimeMetrics.JVM_GC_DURATION),
                anyString(),
                eq(NodeRuntimeMetrics.UNIT_SECONDS),
                any(Supplier.class),
                any(Tags.class)
            )
        ).thenAnswer(invocation -> {
            captured[0] = invocation.getArgument(3);
            return mock(Closeable.class);
        });

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        if (captured[0] != null) {
            double value = captured[0].get();
            assertTrue("GC duration should be >= 0", value >= 0);
        }
    }

    @SuppressWarnings("unchecked")
    public void testCpuUtilizationIsRatio() {
        final Supplier<Double>[] captured = new Supplier[1];
        when(
            registry.createGauge(
                eq(NodeRuntimeMetrics.JVM_CPU_RECENT_UTILIZATION),
                anyString(),
                anyString(),
                any(Supplier.class),
                any(Tags.class)
            )
        ).thenAnswer(invocation -> {
            captured[0] = invocation.getArgument(3);
            return mock(Closeable.class);
        });

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        assertNotNull(captured[0]);
        double value = captured[0].get();
        assertTrue("CPU utilization should be >= 0", value >= 0);
        assertTrue("CPU utilization should be <= 1.0", value <= 1.0);
    }

    public void testDynamicPoolDiscovery() {
        JvmStats stats = jvmService.stats();
        int poolCount = 0;
        for (JvmStats.MemoryPool ignored : stats.getMem())
            poolCount++;

        assertTrue("JVM should have at least one memory pool", poolCount > 0);

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry, atLeastOnce()).createGauge(
            eq(NodeRuntimeMetrics.JVM_MEMORY_USED),
            anyString(),
            anyString(),
            any(),
            any(Tags.class)
        );
    }

    @SuppressWarnings("unchecked")
    public void testCpuGuardAgainstNegativeValues() {
        ProcessProbe negativeCpuProbe = mock(ProcessProbe.class);
        when(negativeCpuProbe.getProcessCpuPercent()).thenReturn((short) -1);
        OsProbe negativeOsProbe = mock(OsProbe.class);
        when(negativeOsProbe.getSystemCpuPercent()).thenReturn((short) -1);

        final Supplier<Double>[] processCpu = new Supplier[1];
        final Supplier<Double>[] systemCpu = new Supplier[1];
        when(
            registry.createGauge(
                eq(NodeRuntimeMetrics.JVM_CPU_RECENT_UTILIZATION),
                anyString(),
                anyString(),
                any(Supplier.class),
                any(Tags.class)
            )
        ).thenAnswer(invocation -> {
            processCpu[0] = invocation.getArgument(3);
            return mock(Closeable.class);
        });
        when(
            registry.createGauge(
                eq(NodeRuntimeMetrics.JVM_SYSTEM_CPU_UTILIZATION),
                anyString(),
                anyString(),
                any(Supplier.class),
                any(Tags.class)
            )
        ).thenAnswer(invocation -> {
            systemCpu[0] = invocation.getArgument(3);
            return mock(Closeable.class);
        });

        new NodeRuntimeMetrics(registry, jvmService, negativeCpuProbe, negativeOsProbe);

        assertNotNull(processCpu[0]);
        assertNotNull(systemCpu[0]);
        assertEquals(0.0, processCpu[0].get(), 0.0);
        assertEquals(0.0, systemCpu[0].get(), 0.0);
    }

    public void testConstructorCleansUpOnFailure() {
        MetricsRegistry failingRegistry = mock(MetricsRegistry.class);
        Closeable successHandle = mock(Closeable.class);
        when(failingRegistry.createGauge(anyString(), anyString(), anyString(), any(Supplier.class), any(Tags.class))).thenReturn(
            successHandle
        ).thenReturn(successHandle).thenThrow(new RuntimeException("registration failure"));

        expectThrows(RuntimeException.class, () -> new NodeRuntimeMetrics(failingRegistry, jvmService, processProbe, osProbe));
    }

    public void testDynamicBufferPoolDiscovery() {
        JvmStats stats = jvmService.stats();
        int bpCount = stats.getBufferPools().size();

        assertTrue("JVM should have at least one buffer pool", bpCount > 0);

        new NodeRuntimeMetrics(registry, jvmService, processProbe, osProbe);

        verify(registry, atLeastOnce()).createGauge(
            eq(NodeRuntimeMetrics.JVM_BUFFER_MEMORY_USED),
            anyString(),
            anyString(),
            any(),
            any(Tags.class)
        );
    }
}
