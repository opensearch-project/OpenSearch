/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.stats;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class SpillStatsCollectorTests extends OpenSearchTestCase {

    public void testCollectReturnsZeroForEmptyDirectory() {
        SpillStats stats = SpillStatsCollector.collect("", 999_999L);
        assertEquals("", stats.getDirectory());
        assertEquals(0L, stats.getDiskTotalBytes());
        assertEquals(0L, stats.getDiskAvailableBytes());
        assertEquals(0L, stats.getDiskUsedBytes());
        assertEquals(0L, stats.getDiskReservedBytes());
    }

    public void testCollectReadsFilesystemForExistingDirectory() throws IOException {
        Path tmp = createTempDir();
        SpillStats stats = SpillStatsCollector.collect(tmp.toString(), 1_000_000L);
        assertEquals(tmp.toString(), stats.getDirectory());
        assertTrue(stats.getDiskTotalBytes() > 0L);
        assertTrue(stats.getDiskAvailableBytes() > 0L);
        assertEquals(1_000_000L, stats.getDiskReservedBytes());
        assertEquals(stats.getDiskTotalBytes(), stats.getDiskAvailableBytes() + stats.getDiskUsedBytes());
    }

    public void testCollectReturnsZeroBytesForMissingDirectoryButPreservesReserved() {
        Path missing = createTempDir().resolve("does-not-exist-" + UUID.randomUUID());
        assertFalse("precondition: path must not exist", Files.exists(missing));
        SpillStats stats = SpillStatsCollector.collect(missing.toString(), 4242L);
        assertEquals(missing.toString(), stats.getDirectory());
        assertEquals(0L, stats.getDiskTotalBytes());
        assertEquals(4242L, stats.getDiskReservedBytes());
    }
}
