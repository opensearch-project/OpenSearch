/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.file.Files;
import java.nio.file.Path;

public class SpillStatsCollectorTests extends OpenSearchTestCase {

    public void testEmptyDirectoryReturnsAllZeroSnapshot() {
        SpillStats stats = SpillStatsCollector.collect("", 999_999L);

        assertEquals("", stats.getDirectory());
        assertEquals(0L, stats.getDiskTotalBytes());
        assertEquals(0L, stats.getDiskAvailableBytes());
        assertEquals(0L, stats.getDiskUsedBytes());
        assertEquals(0L, stats.getDiskReservedBytes());
    }

    public void testValidDirectoryReportsRealCapacityAndConfiguredReserve() throws Exception {
        Path tmp = createTempDir();

        SpillStats stats = SpillStatsCollector.collect(tmp.toString(), 1_000_000L);

        assertEquals(tmp.toString(), stats.getDirectory());
        assertTrue("disk_total_bytes should be > 0 on a real tmpdir", stats.getDiskTotalBytes() > 0);
        assertTrue("disk_available_bytes should be > 0 on a real tmpdir", stats.getDiskAvailableBytes() > 0);
        // total = available + used by definition (when using FS-level used)
        assertEquals(
            stats.getDiskTotalBytes(),
            stats.getDiskAvailableBytes() + stats.getDiskUsedBytes()
        );
        assertEquals(1_000_000L, stats.getDiskReservedBytes());
    }

    public void testMissingDirectoryReturnsZeroBytesButPreservesPathAndReserve() throws Exception {
        Path missing = createTempDir().resolve("does-not-exist");
        assertFalse(Files.exists(missing));

        SpillStats stats = SpillStatsCollector.collect(missing.toString(), 4242L);

        assertEquals(missing.toString(), stats.getDirectory());
        assertEquals(0L, stats.getDiskTotalBytes());
        assertEquals(0L, stats.getDiskAvailableBytes());
        assertEquals(0L, stats.getDiskUsedBytes());
        assertEquals(4242L, stats.getDiskReservedBytes());
    }
}
