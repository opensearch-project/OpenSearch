/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link WriterFileSet}.
 */
public class WriterFileSetTests extends OpenSearchTestCase {

    public void testCopyWriteable() throws Exception {
        WriterFileSet original = randomWriterFileSet();
        String directory = original.directory();
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, directory, DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        assertEquals(original, copy);
    }

    public void testDirectoryNotSerialized() throws Exception {
        String originalDirectory = "/tmp/original";
        String differentDirectory = "/tmp/different";
        WriterFileSet original = new WriterFileSet(originalDirectory, 1L, Set.of("a.dat"), 10, 0L);

        WriterFileSet deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, differentDirectory, DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );

        assertEquals(differentDirectory, deserialized.directory());
        assertNotEquals(originalDirectory, deserialized.directory());
        assertEquals(original.writerGeneration(), deserialized.writerGeneration());
        assertEquals(original.files(), deserialized.files());
        assertEquals(original.numRows(), deserialized.numRows());
        assertEquals(original.perFileMetadata(), deserialized.perFileMetadata());
    }

    public void testPerFileMetadataRoundTrip() throws Exception {
        WriterFileSet original = new WriterFileSet(
            "/tmp/original",
            3L,
            Set.of("_0.parquet"),
            42L,
            Map.of("_0.parquet", Map.of("opensearch.pme.encrypted", "true", "opensearch.pme.kms.instance_id", "kms-1"))
        );

        WriterFileSet deserialized = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, "/tmp/other")
        );

        assertEquals(original.perFileMetadata(), deserialized.perFileMetadata());
        assertEquals("true", deserialized.metadataForFile("_0.parquet").get("opensearch.pme.encrypted"));
    }

    public void testStreamRoundTripPreservesFormatVersion() throws Exception {
        WriterFileSet original = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 10, 9_010_000L);
        WriterFileSet copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new WriterFileSet(in, "/tmp/dir", DataformatAwareCatalogSnapshot.CURRENT_SERIALIZATION_VERSION)
        );
        assertEquals(9_010_000L, copy.formatVersion());
    }

    public void testDefaultFormatVersionIsZero() {
        WriterFileSet wfs = new WriterFileSet("/tmp/dir", 1L, Set.of("a.dat"), 0, 0L);
        assertEquals(0L, wfs.formatVersion());
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet() {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom("cfs", "si", "dat", "parquet"));
        }
        Map<String, Map<String, String>> metadata = randomBoolean()
            ? Map.of()
            : Map.of(files.iterator().next(), Map.of("opensearch.pme.encrypted", "true"));
        return new WriterFileSet(directory, randomNonNegativeLong(), files, randomIntBetween(0, 10000), 0L, metadata);
    }
}
