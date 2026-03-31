/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link DataformatAwareCatalogSnapshot}.
 */
public class DataformatAwareCatalogSnapshotTests extends OpenSearchTestCase {

    public void testSnapshotFieldAccessConsistency() {
        for (int iter = 0; iter < 100; iter++) {
            long id = randomLong();
            long generation = randomNonNegativeLong();
            long version = randomNonNegativeLong();
            List<Segment> segments = randomSegments();
            long lastWriterGeneration = randomNonNegativeLong();
            Map<String, String> userData = randomUserData();

            DataformatAwareCatalogSnapshot snapshot = new DataformatAwareCatalogSnapshot(
                id,
                generation,
                version,
                segments,
                lastWriterGeneration,
                userData
            );

            assertEquals(id, snapshot.getId());
            assertEquals(generation, snapshot.getGeneration());
            assertEquals(version, snapshot.getVersion());
            assertEquals(segments, snapshot.getSegments());
            assertEquals(lastWriterGeneration, snapshot.getLastWriterGeneration());
            assertEquals(userData, snapshot.getUserData());

            Set<String> expectedFormats = new HashSet<>();
            for (Segment seg : segments) {
                expectedFormats.addAll(seg.dfGroupedSearchableFiles().keySet());
            }
            for (String format : expectedFormats) {
                List<WriterFileSet> expected = new ArrayList<>();
                for (Segment seg : segments) {
                    WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(format);
                    if (wfs != null) expected.add(wfs);
                }
                assertEquals(expected, new ArrayList<>(snapshot.getSearchableFiles(format)));
            }

            assertTrue(snapshot.getSearchableFiles("nonexistent_" + randomAlphaOfLength(5)).isEmpty());
            assertEquals(expectedFormats, snapshot.getDataFormats());
            expectThrows(UnsupportedOperationException.class, () -> snapshot.getSegments().add(randomSegment()));
        }
    }

    public void testSerializationRoundTrip() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            DataformatAwareCatalogSnapshot original = randomSnapshot();
            String serialized = original.serializeToString();

            DataformatAwareCatalogSnapshot deserialized = DataformatAwareCatalogSnapshot.deserializeFromString(serialized);
            assertSnapshotFieldsEqual("round-trip", original, deserialized);

            String reserialized = deserialized.serializeToString();
            DataformatAwareCatalogSnapshot deserialized2 = DataformatAwareCatalogSnapshot.deserializeFromString(reserialized);
            assertSnapshotFieldsEqual("double round-trip", deserialized, deserialized2);
        }
    }

    public void testCopyWriteable() throws Exception {
        DataformatAwareCatalogSnapshot original = randomSnapshot();
        DataformatAwareCatalogSnapshot copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            DataformatAwareCatalogSnapshot::new
        );
        assertSnapshotFieldsEqual("copyWriteable", original, copy);
    }

    public void testDeserializationRejectsInvalidInput() {
        for (int iter = 0; iter < 100; iter++) {
            String input = generateInvalidInput(iter);
            expectThrows(IOException.class, () -> DataformatAwareCatalogSnapshot.deserializeFromString(input));
        }
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet(String format) {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        long writerGeneration = randomNonNegativeLong();
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        String[] extensions = "lucene".equals(format) ? new String[] { "cfs", "si", "dat" } : new String[] { "parquet" };
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom(extensions));
        }
        return new WriterFileSet(directory, writerGeneration, files, randomIntBetween(0, 10000));
    }

    private Segment randomSegment() {
        long generation = randomNonNegativeLong();
        int formatCount = randomIntBetween(1, 2);
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < formatCount; i++) {
            String format = randomFrom("lucene", "parquet");
            dfGrouped.put(format, randomWriterFileSet(format));
        }
        return new Segment(generation, dfGrouped);
    }

    private List<Segment> randomSegments() {
        int count = randomIntBetween(0, 5);
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            segments.add(randomSegment());
        }
        return segments;
    }

    private Map<String, String> randomUserData() {
        int entries = randomIntBetween(0, 4);
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < entries; i++) {
            userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        return userData;
    }

    private DataformatAwareCatalogSnapshot randomSnapshot() {
        return new DataformatAwareCatalogSnapshot(
            randomLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomSegments(),
            randomNonNegativeLong(),
            randomUserData()
        );
    }

    private void assertSnapshotFieldsEqual(String context, DataformatAwareCatalogSnapshot expected, DataformatAwareCatalogSnapshot actual) {
        assertEquals(context + ": id", expected.getId(), actual.getId());
        assertEquals(context + ": generation", expected.getGeneration(), actual.getGeneration());
        assertEquals(context + ": version", expected.getVersion(), actual.getVersion());
        assertEquals(context + ": segments", expected.getSegments(), actual.getSegments());
        assertEquals(context + ": lastWriterGeneration", expected.getLastWriterGeneration(), actual.getLastWriterGeneration());
        assertEquals(context + ": userData", expected.getUserData(), actual.getUserData());
    }

    private String generateInvalidInput(int iter) {
        switch (iter % 6) {
            case 0:
                return randomAlphaOfLengthBetween(1, 200);
            case 1:
                byte[] randomBytes = new byte[randomIntBetween(1, 100)];
                random().nextBytes(randomBytes);
                return java.util.Base64.getEncoder().encodeToString(randomBytes);
            case 2:
                DataformatAwareCatalogSnapshot snap = randomSnapshot();
                try {
                    String validBase64 = snap.serializeToString();
                    return validBase64.substring(0, randomIntBetween(1, Math.max(1, validBase64.length() / 2)));
                } catch (IOException e) {
                    return "AAAA";
                }
            case 3:
                return "";
            case 4:
                return randomFrom("not-base64!!!", "===", "@@@@", "hello world", "{\"json\":true}");
            case 5:
                return randomFrom("null", "undefined", "None", "nil", "NaN");
            default:
                return randomAlphaOfLength(10);
        }
    }
}
