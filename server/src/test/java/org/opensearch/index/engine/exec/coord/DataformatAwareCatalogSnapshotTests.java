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

            // Directory is not serialized; pass a placeholder for deserialization
            String directory = "/tmp/deserialized";
            DataformatAwareCatalogSnapshot deserialized = DataformatAwareCatalogSnapshot.deserializeFromString(
                serialized,
                key -> directory
            );
            assertSnapshotMetadataEqual("round-trip", original, deserialized);

            String reserialized = deserialized.serializeToString();
            DataformatAwareCatalogSnapshot deserialized2 = DataformatAwareCatalogSnapshot.deserializeFromString(
                reserialized,
                key -> directory
            );
            assertSnapshotFieldsEqual("double round-trip", deserialized, deserialized2);
        }
    }

    public void testCopyWriteable() throws Exception {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        DataformatAwareCatalogSnapshot original = randomSnapshotWithDirectory(directory);
        DataformatAwareCatalogSnapshot copy = copyWriteable(
            original,
            new NamedWriteableRegistry(Collections.emptyList()),
            in -> new DataformatAwareCatalogSnapshot(in, key -> directory)
        );
        assertSnapshotFieldsEqual("copyWriteable", original, copy);
    }

    public void testDeserializationRejectsInvalidInput() {
        for (int iter = 0; iter < 100; iter++) {
            String input = generateInvalidInput(iter);
            expectThrows(IOException.class, () -> DataformatAwareCatalogSnapshot.deserializeFromString(input, key -> "/tmp/test"));
        }
    }

    public void testInitialRefCountIsOne() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertEquals(1, snapshot.refCount());
    }

    public void testAcquireRefIncrementsCount() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertEquals(1, snapshot.refCount());

        snapshot.tryIncRef();
        assertEquals(2, snapshot.refCount());

        snapshot.tryIncRef();
        assertEquals(3, snapshot.refCount());

        snapshot.decRef();
        snapshot.decRef();
        snapshot.decRef();
    }

    public void testReleaseRefDecrementsAndTriggersCloseAtZero() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertEquals(1, snapshot.refCount());

        snapshot.tryIncRef();
        assertEquals(2, snapshot.refCount());

        assertFalse(snapshot.decRef());
        assertEquals(1, snapshot.refCount());

        assertTrue(snapshot.decRef());
        assertEquals(0, snapshot.refCount());
    }

    public void testTryAcquireRefSucceedsWhenOpen() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertTrue(snapshot.tryIncRef());
        assertEquals(2, snapshot.refCount());

        snapshot.decRef();
        snapshot.decRef();
    }

    public void testTryAcquireRefFailsWhenClosed() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertTrue(snapshot.decRef());
        assertEquals(0, snapshot.refCount());

        assertFalse(snapshot.tryIncRef());
    }

    public void testCloseInternalCalledOnceAtZeroRefCount() {
        final java.util.concurrent.atomic.AtomicInteger closeCount = new java.util.concurrent.atomic.AtomicInteger(0);
        DataformatAwareCatalogSnapshot snapshot = new DataformatAwareCatalogSnapshot(1L, 1L, 1L, List.of(), 0L, Map.of()) {
            @Override
            protected void closeInternal() {
                closeCount.incrementAndGet();
            }
        };

        snapshot.tryIncRef();
        snapshot.tryIncRef();
        assertEquals(0, closeCount.get());

        snapshot.decRef();
        assertEquals(0, closeCount.get());

        snapshot.decRef();
        assertEquals(0, closeCount.get());

        snapshot.decRef();
        assertEquals(1, closeCount.get());
    }

    public void testClonedSnapshotHasFreshRefCount() {
        DataformatAwareCatalogSnapshot original = randomSnapshot();
        original.tryIncRef();
        assertEquals(2, original.refCount());

        DataformatAwareCatalogSnapshot cloned = original.clone();
        assertEquals(1, cloned.refCount());
        assertEquals(2, original.refCount());

        original.decRef();
        original.decRef();
        cloned.decRef();
    }

    public void testRefCounterDelegatesCloseInternalToSubclass() {
        // Verifies the anonymous AbstractRefCounted bridge calls the subclass closeInternal, not a default
        final List<String> events = new ArrayList<>();
        DataformatAwareCatalogSnapshot snapshot = new DataformatAwareCatalogSnapshot(1L, 1L, 1L, List.of(), 0L, Map.of()) {
            @Override
            protected void closeInternal() {
                events.add("subclass-closed");
            }
        };

        assertTrue(events.isEmpty());
        snapshot.decRef();
        assertEquals(List.of("subclass-closed"), events);
    }

    public void testEachSnapshotHasIndependentRefCounter() {
        DataformatAwareCatalogSnapshot snap1 = randomSnapshot();
        DataformatAwareCatalogSnapshot snap2 = randomSnapshot();

        snap1.tryIncRef();
        assertEquals(2, snap1.refCount());
        assertEquals(1, snap2.refCount());

        snap2.decRef();
        assertEquals(0, snap2.refCount());
        assertEquals(2, snap1.refCount());

        snap1.decRef();
        snap1.decRef();
    }

    public void testRefCounterSurvivesMultipleIncDecCycles() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();

        for (int cycle = 0; cycle < 10; cycle++) {
            int refs = randomIntBetween(1, 20);
            for (int i = 0; i < refs; i++) {
                snapshot.tryIncRef();
            }
            assertEquals(1 + refs, snapshot.refCount());
            for (int i = 0; i < refs; i++) {
                assertFalse(snapshot.decRef());
            }
            assertEquals(1, snapshot.refCount());
        }

        assertTrue(snapshot.decRef());
        assertEquals(0, snapshot.refCount());
        assertFalse(snapshot.tryIncRef());
    }

    public void testCloseInternalNotCalledOnIntermediateDecRef() {
        final java.util.concurrent.atomic.AtomicBoolean closed = new java.util.concurrent.atomic.AtomicBoolean(false);
        DataformatAwareCatalogSnapshot snapshot = new DataformatAwareCatalogSnapshot(1L, 1L, 1L, List.of(), 0L, Map.of()) {
            @Override
            protected void closeInternal() {
                closed.set(true);
            }
        };

        // Acquire several refs
        int extraRefs = randomIntBetween(2, 10);
        for (int i = 0; i < extraRefs; i++) {
            snapshot.tryIncRef();
        }

        // Release all but one — closeInternal must NOT fire
        for (int i = 0; i < extraRefs; i++) {
            snapshot.decRef();
            assertFalse("closeInternal should not fire while refs remain", closed.get());
        }

        // Release the last ref — now it fires
        snapshot.decRef();
        assertTrue("closeInternal should fire when last ref is released", closed.get());
    }

    public void testDeserializedSnapshotHasIndependentRefCounter() throws Exception {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        DataformatAwareCatalogSnapshot original = randomSnapshotWithDirectory(directory);
        String serialized = original.serializeToString();

        DataformatAwareCatalogSnapshot deserialized = DataformatAwareCatalogSnapshot.deserializeFromString(serialized, key -> directory);

        // Each has its own ref counter starting at 1
        assertEquals(1, original.refCount());
        assertEquals(1, deserialized.refCount());

        original.tryIncRef();
        assertEquals(2, original.refCount());
        assertEquals(1, deserialized.refCount());

        original.decRef();
        original.decRef();
        deserialized.decRef();
    }

    public void testIsClosedInitiallyFalse() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertFalse(snapshot.isClosed());
        snapshot.decRef();
    }

    public void testIsClosedTrueAfterLastDecRef() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        assertFalse(snapshot.isClosed());

        snapshot.decRef();
        assertTrue(snapshot.isClosed());
    }

    public void testIsClosedFalseWhileRefsRemain() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        int extraRefs = randomIntBetween(1, 10);
        for (int i = 0; i < extraRefs; i++) {
            snapshot.tryIncRef();
        }

        for (int i = 0; i < extraRefs; i++) {
            snapshot.decRef();
            assertFalse("isClosed should be false while refs remain", snapshot.isClosed());
        }

        snapshot.decRef();
        assertTrue(snapshot.isClosed());
    }

    public void testIsClosedAfterCloneIndependent() {
        DataformatAwareCatalogSnapshot original = randomSnapshot();
        DataformatAwareCatalogSnapshot cloned = original.clone();

        original.decRef();
        assertTrue(original.isClosed());
        assertFalse(cloned.isClosed());

        cloned.decRef();
        assertTrue(cloned.isClosed());
    }

    public void testTryIncRefFailsAfterClosed() {
        DataformatAwareCatalogSnapshot snapshot = randomSnapshot();
        snapshot.decRef();
        assertTrue(snapshot.isClosed());
        assertFalse(snapshot.tryIncRef());
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

    private DataformatAwareCatalogSnapshot randomSnapshotWithDirectory(String directory) {
        return new DataformatAwareCatalogSnapshot(
            randomLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomSegmentsWithDirectory(directory),
            randomNonNegativeLong(),
            randomUserData()
        );
    }

    private List<Segment> randomSegmentsWithDirectory(String directory) {
        int count = randomIntBetween(0, 5);
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            segments.add(randomSegmentWithDirectory(directory));
        }
        return segments;
    }

    private Segment randomSegmentWithDirectory(String directory) {
        long generation = randomNonNegativeLong();
        int formatCount = randomIntBetween(1, 2);
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < formatCount; i++) {
            String format = randomFrom("lucene", "parquet");
            dfGrouped.put(format, randomWriterFileSetWithDirectory(format, directory));
        }
        return new Segment(generation, dfGrouped);
    }

    private WriterFileSet randomWriterFileSetWithDirectory(String format, String directory) {
        long writerGeneration = randomNonNegativeLong();
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        String[] extensions = "lucene".equals(format) ? new String[] { "cfs", "si", "dat" } : new String[] { "parquet" };
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom(extensions));
        }
        return new WriterFileSet(directory, writerGeneration, files, randomIntBetween(0, 10000));
    }

    private void assertSnapshotFieldsEqual(String context, DataformatAwareCatalogSnapshot expected, DataformatAwareCatalogSnapshot actual) {
        assertEquals(context + ": id", expected.getId(), actual.getId());
        assertEquals(context + ": generation", expected.getGeneration(), actual.getGeneration());
        assertEquals(context + ": version", expected.getVersion(), actual.getVersion());
        assertEquals(context + ": segments", expected.getSegments(), actual.getSegments());
        assertEquals(context + ": lastWriterGeneration", expected.getLastWriterGeneration(), actual.getLastWriterGeneration());
        assertEquals(context + ": userData", expected.getUserData(), actual.getUserData());
    }

    /**
     * Asserts metadata equality between two snapshots, ignoring directory (which is not serialized).
     */
    private void assertSnapshotMetadataEqual(
        String context,
        DataformatAwareCatalogSnapshot expected,
        DataformatAwareCatalogSnapshot actual
    ) {
        assertEquals(context + ": id", expected.getId(), actual.getId());
        assertEquals(context + ": generation", expected.getGeneration(), actual.getGeneration());
        assertEquals(context + ": version", expected.getVersion(), actual.getVersion());
        assertEquals(context + ": segment count", expected.getSegments().size(), actual.getSegments().size());
        for (int i = 0; i < expected.getSegments().size(); i++) {
            Segment expectedSeg = expected.getSegments().get(i);
            Segment actualSeg = actual.getSegments().get(i);
            assertEquals(context + ": segment[" + i + "].generation", expectedSeg.generation(), actualSeg.generation());
            assertEquals(
                context + ": segment[" + i + "].formats",
                expectedSeg.dfGroupedSearchableFiles().keySet(),
                actualSeg.dfGroupedSearchableFiles().keySet()
            );
            for (String format : expectedSeg.dfGroupedSearchableFiles().keySet()) {
                WriterFileSet expectedWfs = expectedSeg.dfGroupedSearchableFiles().get(format);
                WriterFileSet actualWfs = actualSeg.dfGroupedSearchableFiles().get(format);
                assertEquals(context + ": writerGeneration", expectedWfs.writerGeneration(), actualWfs.writerGeneration());
                assertEquals(context + ": files", expectedWfs.files(), actualWfs.files());
                assertEquals(context + ": numRows", expectedWfs.numRows(), actualWfs.numRows());
            }
        }
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
