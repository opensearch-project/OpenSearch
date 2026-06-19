/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.stats.SpillStats;
import org.opensearch.test.OpenSearchTestCase;

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

    public void testBuildSpillStatsWithDisabledSpillReturnsEmptyDirectory() {
        DataFusionService service = DataFusionService.builder().memoryPoolLimit(1L).spillMemoryLimit(2L).spillDirectory("").build();
        SpillStats spill = service.buildSpillStats();
        assertEquals("", spill.getDirectory());
        assertEquals(0L, spill.getDiskTotalBytes());
        assertEquals(0L, spill.getDiskReservedBytes());
    }

    public void testBuildSpillStatsReadsConfiguredDirectory() {
        Path realSpillDir = createTempDir();
        DataFusionService service = DataFusionService.builder()
            .memoryPoolLimit(1L)
            .spillMemoryLimit(2L)
            .spillDirectory(realSpillDir.toString())
            .build();

        SpillStats spill = service.buildSpillStats();
        assertEquals(realSpillDir.toString(), spill.getDirectory());
        assertEquals(2L, spill.getDiskReservedBytes());
        assertTrue(spill.getDiskTotalBytes() > 0L);
    }
}
