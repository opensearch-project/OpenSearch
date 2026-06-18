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
        assertEquals(0L, stats.getStaleEntryCount());
    }

    public void testCollectReadsFilesystemForExistingDirectory() throws IOException {
        Path tmp = createTempDir();
        SpillStats stats = SpillStatsCollector.collect(tmp.toString(), 1_000_000L);
        assertEquals(tmp.toString(), stats.getDirectory());
        assertTrue(stats.getDiskTotalBytes() > 0L);
        assertTrue(stats.getDiskAvailableBytes() > 0L);
        assertEquals(1_000_000L, stats.getDiskReservedBytes());
        assertEquals(stats.getDiskTotalBytes(), stats.getDiskAvailableBytes() + stats.getDiskUsedBytes());
        assertEquals("empty spill dir has no .stale entries", 0L, stats.getStaleEntryCount());
    }

    public void testCollectReturnsZeroBytesForMissingDirectoryButPreservesReserved() {
        Path missing = createTempDir().resolve("does-not-exist-" + UUID.randomUUID());
        assertFalse("precondition: path must not exist", Files.exists(missing));
        SpillStats stats = SpillStatsCollector.collect(missing.toString(), 4242L);
        assertEquals(missing.toString(), stats.getDirectory());
        assertEquals(0L, stats.getDiskTotalBytes());
        assertEquals(4242L, stats.getDiskReservedBytes());
        assertEquals(0L, stats.getStaleEntryCount());
    }

    /**
     * Pre-seeds three {@code *.stale} entries (a dir, a regular file, and a symlink)
     * plus two non-stale entries that must NOT be counted (a live datafusion-* dir
     * and a regular file without the suffix). Verifies the count reports exactly 3.
     */
    public void testCollectCountsStaleEntries() throws IOException {
        Path tmp = createTempDir();
        // Three .stale entries — should be counted.
        Files.createDirectory(tmp.resolve("datafusion-aB3kF7.stale"));
        Files.writeString(tmp.resolve("loose.txt.stale"), "stale loose");
        // Symlinks may not be supported on every test platform; try and skip if unavailable.
        try {
            Files.createSymbolicLink(tmp.resolve("link.stale"), tmp.resolve("datafusion-aB3kF7.stale"));
        } catch (UnsupportedOperationException | IOException e) {
            // Symlinks unsupported here — accept the test on 2 entries instead of 3.
            SpillStats statsNoLink = SpillStatsCollector.collect(tmp.toString(), 0L);
            assertEquals("dir + file should be counted", 2L, statsNoLink.getStaleEntryCount());
            return;
        }

        // Two NON-.stale entries — must be ignored.
        Files.createDirectory(tmp.resolve("datafusion-newRandom"));
        Files.writeString(tmp.resolve("README.md"), "live");

        SpillStats stats = SpillStatsCollector.collect(tmp.toString(), 0L);
        assertEquals("only the three .stale entries should be counted", 3L, stats.getStaleEntryCount());
    }
}
