/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Property-based tests for {@link DataformatAwareCatalogSnapshot}.
 * Uses OpenSearch randomization utilities to generate random inputs across many iterations.
 */
public class DataformatAwareCatalogSnapshotTests extends OpenSearchTestCase {

    // Feature: catalog-snapshot-manager, Property 1: Snapshot field access consistency

    /**
     * Generates a random {@link WriterFileSet} with random directory, writer generation, files, and row count.
     */
    private WriterFileSet randomWriterFileSet() {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        long writerGeneration = randomNonNegativeLong();
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom("cfs", "si", "parquet", "dat"));
        }
        long numRows = randomIntBetween(0, 10000);
        return new WriterFileSet(directory, writerGeneration, files, numRows);
    }

    /**
     * Generates a random {@link Segment} with a random generation and random data format keys.
     */
    private Segment randomSegment() {
        long generation = randomNonNegativeLong();
        int formatCount = randomIntBetween(1, 4);
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < formatCount; i++) {
            String formatKey = randomFrom("lucene", "parquet", "arrow", "custom_" + randomAlphaOfLength(3));
            dfGrouped.put(formatKey, randomWriterFileSet());
        }
        return new Segment(generation, dfGrouped);
    }

    /**
     * Generates a random {@link DataformatAwareCatalogSnapshot} with random fields.
     */
    private DataformatAwareCatalogSnapshot randomSnapshot() {
        long id = randomLong();
        long generation = randomNonNegativeLong();
        long version = randomNonNegativeLong();
        int segmentCount = randomIntBetween(0, 5);
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segments.add(randomSegment());
        }
        long lastWriterGeneration = randomNonNegativeLong();
        int userDataEntries = randomIntBetween(0, 4);
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < userDataEntries; i++) {
            userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        return new DataformatAwareCatalogSnapshot(id, generation, version, segments, lastWriterGeneration, userData);
    }

    /**
     * Property 1: Snapshot field access consistency.
     * For any valid combination of inputs, constructing a DataformatAwareCatalogSnapshot and querying
     * its accessors should return consistent results.
     *
     * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.6
     */
    public void testSnapshotFieldAccessConsistency() {
        for (int iter = 0; iter < 100; iter++) {
            long id = randomLong();
            long generation = randomNonNegativeLong();
            long version = randomNonNegativeLong();
            int segmentCount = randomIntBetween(0, 5);
            List<Segment> segments = new ArrayList<>();
            for (int i = 0; i < segmentCount; i++) {
                segments.add(randomSegment());
            }
            long lastWriterGeneration = randomNonNegativeLong();
            int userDataEntries = randomIntBetween(0, 4);
            Map<String, String> userData = new HashMap<>();
            for (int i = 0; i < userDataEntries; i++) {
                userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
            }

            DataformatAwareCatalogSnapshot snapshot = new DataformatAwareCatalogSnapshot(
                id,
                generation,
                version,
                segments,
                lastWriterGeneration,
                userData
            );

            try {
                // Verify getId()
                assertEquals("getId() should return the snapshot id", id, snapshot.getId());

                // Verify getGeneration() and getVersion() from parent
                assertEquals("getGeneration() should return the generation", generation, snapshot.getGeneration());
                assertEquals("getVersion() should return the version", version, snapshot.getVersion());

                // Verify getSegments() returns equal content
                assertEquals("getSegments() should return segments equal to input", segments, snapshot.getSegments());

                // Verify getLastWriterGeneration()
                assertEquals(
                    "getLastWriterGeneration() should return the writer generation",
                    lastWriterGeneration,
                    snapshot.getLastWriterGeneration()
                );

                // Verify getUserData()
                assertEquals("getUserData() should return the user data", userData, snapshot.getUserData());

                // Verify getSearchableFiles() returns exactly the WriterFileSets matching the queried format
                Set<String> expectedFormats = new HashSet<>();
                for (Segment seg : segments) {
                    expectedFormats.addAll(seg.dfGroupedSearchableFiles().keySet());
                }
                for (String format : expectedFormats) {
                    Collection<WriterFileSet> searchableFiles = snapshot.getSearchableFiles(format);
                    List<WriterFileSet> expected = new ArrayList<>();
                    for (Segment seg : segments) {
                        WriterFileSet wfs = seg.dfGroupedSearchableFiles().get(format);
                        if (wfs != null) {
                            expected.add(wfs);
                        }
                    }
                    assertEquals(
                        "getSearchableFiles('" + format + "') should return matching WriterFileSets",
                        expected,
                        new ArrayList<>(searchableFiles)
                    );
                }

                // Verify getSearchableFiles() for a non-existent format returns empty
                Collection<WriterFileSet> emptyResult = snapshot.getSearchableFiles("nonexistent_format_" + randomAlphaOfLength(5));
                assertTrue("getSearchableFiles for unknown format should be empty", emptyResult.isEmpty());

                // Verify getDataFormats() returns the union of all format keys
                assertEquals("getDataFormats() should return union of all format keys", expectedFormats, snapshot.getDataFormats());

                // Verify getSegments() returns an unmodifiable list
                expectThrows(UnsupportedOperationException.class, () -> snapshot.getSegments().add(randomSegment()));
            } finally {
                snapshot.decRef();
            }
        }
    }

    // Feature: catalog-snapshot-manager, Property 2: Serialization round-trip

    /**
     * Property 2: Serialization round-trip.
     * For any valid DataformatAwareCatalogSnapshot, serializing via serializeToString(),
     * then deserializing via deserializeFromString(), then serializing again should produce
     * a binary-encoded string that, when deserialized, yields an equivalent snapshot.
     *
     * Validates: Requirements 2.1, 2.2, 2.3
     */
    public void testSerializationRoundTrip() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            DataformatAwareCatalogSnapshot original = randomSnapshot();
            try {
                // First serialization
                String json1 = original.serializeToString();

                // Deserialize from the first JSON
                DataformatAwareCatalogSnapshot deserialized1 = DataformatAwareCatalogSnapshot.deserializeFromString(json1);
                try {
                    // Second serialization from the deserialized snapshot
                    String json2 = deserialized1.serializeToString();

                    // Deserialize from the second JSON to verify round-trip equivalence
                    DataformatAwareCatalogSnapshot deserialized2 = DataformatAwareCatalogSnapshot.deserializeFromString(json2);
                    try {
                        // Verify deserialized1 fields match the original
                        assertSnapshotFieldsEqual("first deserialization", original, deserialized1);

                        // Verify deserialized2 fields match deserialized1 (round-trip stability)
                        assertSnapshotFieldsEqual("second deserialization", deserialized1, deserialized2);
                    } finally {
                        deserialized2.decRef();
                    }
                } finally {
                    deserialized1.decRef();
                }
            } finally {
                original.decRef();
            }
        }
    }

    /**
     * Asserts that two {@link DataformatAwareCatalogSnapshot} instances have equivalent fields.
     */
    private void assertSnapshotFieldsEqual(String context, DataformatAwareCatalogSnapshot expected, DataformatAwareCatalogSnapshot actual) {
        assertEquals(context + ": id should match", expected.getId(), actual.getId());
        assertEquals(context + ": generation should match", expected.getGeneration(), actual.getGeneration());
        assertEquals(context + ": version should match", expected.getVersion(), actual.getVersion());
        assertEquals(context + ": segments should match", expected.getSegments(), actual.getSegments());
        assertEquals(context + ": lastWriterGeneration should match", expected.getLastWriterGeneration(), actual.getLastWriterGeneration());
        assertEquals(context + ": userData should match", expected.getUserData(), actual.getUserData());
    }

    // Feature: catalog-snapshot-manager, Property 3: Deserialization rejects invalid input

    /**
     * Property 3: Deserialization rejects invalid input.
     * For any string that is not valid Base64 or is truncated/corrupted binary data,
     * calling deserializeFromString() should throw an IOException.
     *
     * Validates: Requirements 2.4
     */
    public void testDeserializationRejectsInvalidInput() {
        for (int iter = 0; iter < 100; iter++) {
            String input = generateInvalidInput(iter);
            expectThrows(IOException.class, () -> DataformatAwareCatalogSnapshot.deserializeFromString(input));
        }
    }

    /**
     * Generates an invalid input string for deserialization testing.
     * Mixes different categories of invalid input across iterations.
     */
    private String generateInvalidInput(int iter) {
        int category = iter % 6;
        switch (category) {
            case 0:
                // Completely random alphanumeric strings (not valid Base64 payload)
                return randomAlphaOfLengthBetween(1, 200);
            case 1:
                // Valid Base64 but random bytes (not a valid serialized snapshot)
                byte[] randomBytes = new byte[randomIntBetween(1, 100)];
                random().nextBytes(randomBytes);
                return java.util.Base64.getEncoder().encodeToString(randomBytes);
            case 2:
                // Truncated valid Base64: serialize a valid snapshot, then truncate the Base64 string
                DataformatAwareCatalogSnapshot snap = randomSnapshot();
                try {
                    String validBase64 = snap.serializeToString();
                    int truncateAt = randomIntBetween(1, Math.max(1, validBase64.length() / 2));
                    return validBase64.substring(0, truncateAt);
                } catch (IOException e) {
                    return "AAAA";
                } finally {
                    snap.decRef();
                }
            case 3:
                // Empty string
                return "";
            case 4:
                // Strings with invalid Base64 characters
                return randomFrom("not-base64!!!", "===", "@@@@", "hello world", "{\"json\":true}");
            case 5:
                // Null-like strings
                return randomFrom("null", "undefined", "None", "nil", "NaN");
            default:
                return randomAlphaOfLength(10);
        }
    }

}
