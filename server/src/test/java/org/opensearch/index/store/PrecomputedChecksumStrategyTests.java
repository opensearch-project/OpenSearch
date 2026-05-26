/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Unit tests for {@link PrecomputedChecksumStrategy} covering register/lookup, fallback
 * caching, generation-based overwrite semantics, and retain/evict behavior.
 */
public class PrecomputedChecksumStrategyTests extends OpenSearchTestCase {

    private Directory directory;
    private PrecomputedChecksumStrategy strategy;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        directory = new ByteBuffersDirectory();
        strategy = new PrecomputedChecksumStrategy();
    }

    @Override
    public void tearDown() throws Exception {
        directory.close();
        super.tearDown();
    }

    public void testRegisteredChecksumIsReturnedWithoutScanning() throws IOException {
        writeFile("a.parquet", "payload");
        strategy.registerChecksum("a.parquet", 12345L, 1L);

        // Strategy should return the registered value, not the scanned CRC32 of "payload".
        assertEquals(12345L, strategy.computeChecksum(directory, "a.parquet"));
    }

    public void testFallbackScanMatchesFullFileCrc32AndIsCached() throws IOException {
        byte[] payload = "hello-world".getBytes(StandardCharsets.UTF_8);
        writeFile("b.parquet", payload);

        long expected = fullFileCrc32(payload);

        // First call: cache miss — scans the file.
        long first = strategy.computeChecksum(directory, "b.parquet");
        assertEquals(expected, first);

        // Second call should hit the cache and return the same value without re-reading.
        long second = strategy.computeChecksum(directory, "b.parquet");
        assertEquals(expected, second);
    }

    public void testWriteRegistrationOverwritesFallbackEntry() throws IOException {
        writeFile("c.parquet", "payload");

        // Prime the cache with the fallback (generation 0).
        long scanned = strategy.computeChecksum(directory, "c.parquet");

        // Write path registers a different value with a real generation; it must overwrite.
        strategy.registerChecksum("c.parquet", 999L, 5L);

        assertEquals(999L, strategy.computeChecksum(directory, "c.parquet"));
        assertNotEquals(scanned, 999L);
    }

    public void testStaleRegistrationDoesNotOverwriteNewerEntry() {
        // Newer generation first.
        strategy.registerChecksum("d.parquet", 2000L, 10L);
        // Older generation arriving late must NOT clobber.
        strategy.registerChecksum("d.parquet", 1000L, 5L);

        try {
            assertEquals(2000L, strategy.computeChecksum(directory, "d.parquet"));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    public void testEqualGenerationRegistrationOverwrites() throws IOException {
        strategy.registerChecksum("e.parquet", 1000L, 5L);
        strategy.registerChecksum("e.parquet", 2000L, 5L);
        assertEquals(2000L, strategy.computeChecksum(directory, "e.parquet"));
    }

    public void testRegisterIgnoresZeroChecksumAndNullName() throws IOException {
        writeFile("f.parquet", "payload");

        strategy.registerChecksum("f.parquet", 0L, 1L); // zero treated as "unset"
        strategy.registerChecksum((String) null, 1234L, 1L);     // null name is a no-op

        // Since no real register happened, compute falls back to scanning the file.
        long expected = fullFileCrc32("payload".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, strategy.computeChecksum(directory, "f.parquet"));
    }

    public void testEvictChecksumRemovesEntry() throws IOException {
        writeFile("g.parquet", "data");
        strategy.registerChecksum("g.parquet", 42L, 1L);
        assertEquals(42L, strategy.computeChecksum(directory, "g.parquet"));

        strategy.evictChecksum("g.parquet");

        // After eviction, lookup falls back to the file contents.
        long expected = fullFileCrc32("data".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, strategy.computeChecksum(directory, "g.parquet"));
    }

    public void testRetainOnlyEvictsFilesNotInActiveSet() throws IOException {
        writeFile("h.parquet", "x");
        writeFile("i.parquet", "y");
        strategy.registerChecksum("h.parquet", 111L, 1L);
        strategy.registerChecksum("i.parquet", 222L, 1L);

        strategy.retainOnly(List.of("h.parquet"));

        // Retained file still returns the registered value.
        assertEquals(111L, strategy.computeChecksum(directory, "h.parquet"));
        // Evicted file falls back to a scan.
        long expectedI = fullFileCrc32("y".getBytes(StandardCharsets.UTF_8));
        assertEquals(expectedI, strategy.computeChecksum(directory, "i.parquet"));
    }

    public void testClearChecksumsEmptiesCache() throws IOException {
        writeFile("j.parquet", "z");
        strategy.registerChecksum("j.parquet", 777L, 1L);

        strategy.clearChecksums();

        // After clear, lookup falls back to a scan.
        long expected = fullFileCrc32("z".getBytes(StandardCharsets.UTF_8));
        assertEquals(expected, strategy.computeChecksum(directory, "j.parquet"));
    }

    // ─── helpers ─────────────────────────────────────────────────────────────

    private void writeFile(String name, String content) throws IOException {
        writeFile(name, content.getBytes(StandardCharsets.UTF_8));
    }

    private void writeFile(String name, byte[] bytes) throws IOException {
        try (IndexOutput out = directory.createOutput(name, IOContext.DEFAULT)) {
            out.writeBytes(bytes, bytes.length);
        }
    }

    private static long fullFileCrc32(byte[] bytes) {
        CRC32 crc = new CRC32();
        crc.update(bytes);
        return crc.getValue();
    }
}
