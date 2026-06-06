/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.health.SpillDirectoryHealthMonitor;
import org.opensearch.be.datafusion.stats.SpillStats;
import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Unit tests for {@link DataFusionService#getStats()}.
 *
 * Validates: Requirements 5.2, 5.3, 5.5
 *
 * Note: Cache TTL behavior (Requirement 5.3 — same instance within TTL window,
 * fresh instance after TTL expires) requires a running native runtime since
 * {@code doStart()} calls {@code NativeBridge.stats()} to seed the cache.
 * That behavior is verified in integration tests where the native library is loaded.
 */
public class DataFusionServiceStatsTests extends OpenSearchTestCase {

    /**
     * Validates Requirement 5.5: getStats() throws IllegalStateException before doStart().
     *
     * When the service is constructed but not started, the statsCache field is null.
     * Calling getStats() must throw IllegalStateException with a descriptive message.
     */
    public void testGetStatsBeforeStartThrowsIllegalStateException() {
        DataFusionService service = DataFusionService.builder().build();

        IllegalStateException ex = expectThrows(IllegalStateException.class, service::getStats);
        assertEquals("DataFusionService has not been started", ex.getMessage());
    }

    public void testGetSpillDirectoryReflectsBuilderValue() {
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(64L * 1024L * 1024L)
            .spillMemoryLimit(32L * 1024L * 1024L)
            .spillDirectory("/var/lib/opensearch/spill")
            .build();

        assertEquals("/var/lib/opensearch/spill", service.getSpillDirectory());
        assertEquals(32L * 1024L * 1024L, service.getSpillMemoryLimit());
    }

    public void testBuildSpillStatsDefaultsWritableTrueWhenMonitorAbsent() {
        DataFusionService service = DataFusionService.builder().memoryPoolLimit(1L).spillMemoryLimit(2L).spillDirectory("").build();
        // No monitor set; spill disabled -> isDirectoryWritable should be true vacuously.
        SpillStats spill = service.buildSpillStats();
        assertTrue(spill.isDirectoryWritable());
    }

    public void testBuildSpillStatsPropagatesMonitorWritability() throws Exception {
        Path realSpillDir = createTempDir();
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(1L)
            .spillMemoryLimit(2L)
            .spillDirectory(realSpillDir.toString())
            .build();

        // Force a monitor into the unwritable state by pointing it at a directory we
        // delete before probing. The monitor's probeOnce() will fail and flip writable to false.
        Path doomed = createTempDir();
        SpillDirectoryHealthMonitor unwritableMonitor = new SpillDirectoryHealthMonitor(doomed.toString());
        Files.delete(doomed);
        unwritableMonitor.probeOnce();
        assertFalse(unwritableMonitor.isWritable());

        service.setSpillDirectoryHealthMonitor(unwritableMonitor);
        SpillStats spill = service.buildSpillStats();
        assertFalse(spill.isDirectoryWritable());
    }

    public void testBuildSpillStatsPropagatesWritableTrueFromMonitor() throws Exception {
        Path realSpillDir = createTempDir();
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(1L)
            .spillMemoryLimit(2L)
            .spillDirectory(realSpillDir.toString())
            .build();

        SpillDirectoryHealthMonitor writableMonitor = new SpillDirectoryHealthMonitor(realSpillDir.toString());
        writableMonitor.probeOnce();
        assertTrue(writableMonitor.isWritable());

        service.setSpillDirectoryHealthMonitor(writableMonitor);
        SpillStats spill = service.buildSpillStats();
        assertTrue(spill.isDirectoryWritable());
    }
}
