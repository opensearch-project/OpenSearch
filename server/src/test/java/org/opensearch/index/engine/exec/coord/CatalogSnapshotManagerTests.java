/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link CatalogSnapshotManager}.
 */
public class CatalogSnapshotManagerTests extends OpenSearchTestCase {

    public void testCommitProducesCorrectNewSnapshot() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                long previousGeneration;
                Set<Long> seenIds = new HashSet<>();
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    previousGeneration = ref.get().getGeneration();
                    seenIds.add(ref.get().getId());
                }

                int numCommits = randomIntBetween(1, 10);
                for (int c = 0; c < numCommits; c++) {
                    List<Segment> newSegments = randomSegments();
                    manager.commitNewSnapshot(newSegments);

                    try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                        assertEquals(previousGeneration + 1, ref.get().getGeneration());
                        assertTrue(seenIds.add(ref.get().getId()));
                        assertEquals(newSegments, ref.get().getSegments());
                        previousGeneration = ref.get().getGeneration();
                    }
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testUserDataPreservationOnCommit() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            Map<String, String> initialUserData = randomUserData(randomIntBetween(1, 5));
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                randomNonNegativeLong(),
                initGen,
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                initialUserData
            );
            try {
                manager.commitNewSnapshot(randomSegments());
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    assertEquals(initialUserData, ref.get().getUserData());
                }

                manager.commitNewSnapshot(randomSegments());
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    assertEquals(initialUserData, ref.get().getUserData());
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testReferenceCountingLifecycle() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                randomNonNegativeLong(),
                initGen,
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                Collections.emptyMap()
            );

            CatalogSnapshot initialSnapshot;
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                initialSnapshot = ref.get();
                assertEquals(2, initialSnapshot.refCount());
            }
            assertEquals(1, initialSnapshot.refCount());

            manager.commitNewSnapshot(randomSegments());
            assertEquals(0, initialSnapshot.refCount());

            int numCommits = randomIntBetween(1, 8);
            for (int c = 0; c < numCommits; c++) {
                CatalogSnapshot prev;
                try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                    prev = ref.get();
                    assertEquals(2, prev.refCount());
                }
                assertEquals(1, prev.refCount());
                manager.commitNewSnapshot(randomSegments());
                assertEquals(0, prev.refCount());
            }

            CatalogSnapshot finalSnapshot;
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                finalSnapshot = ref.get();
                assertEquals(2, finalSnapshot.refCount());
            }
            assertEquals(1, finalSnapshot.refCount());
            manager.close();
            assertEquals(0, finalSnapshot.refCount());
        }
    }

    public void testAcquireAndReleaseViaGatedCloseable() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                CatalogSnapshot currentSnap;
                try (GatedCloseable<CatalogSnapshot> initialRef = manager.acquireSnapshot()) {
                    currentSnap = initialRef.get();
                    assertEquals(2, currentSnap.refCount());
                }
                assertEquals(1, currentSnap.refCount());

                int numAcquires = randomIntBetween(1, 5);
                List<GatedCloseable<CatalogSnapshot>> refs = new ArrayList<>();
                for (int a = 0; a < numAcquires; a++) {
                    refs.add(manager.acquireSnapshot());
                    assertEquals(1 + (a + 1), currentSnap.refCount());
                }
                for (int r = 0; r < numAcquires; r++) {
                    refs.get(r).close();
                    assertEquals(1 + numAcquires - r - 1, currentSnap.refCount());
                }
                assertEquals(1, currentSnap.refCount());

                GatedCloseable<CatalogSnapshot> heldRef = manager.acquireSnapshot();
                CatalogSnapshot heldSnapshot = heldRef.get();
                assertEquals(2, heldSnapshot.refCount());

                manager.commitNewSnapshot(randomSegments());
                assertEquals(1, heldSnapshot.refCount());

                heldRef.close();
                assertEquals(0, heldSnapshot.refCount());
            } finally {
                manager.close();
            }
        }
    }

    public void testClosedManagerRejectsAcquisition() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            for (int c = 0; c < randomIntBetween(0, 5); c++) {
                manager.commitNewSnapshot(randomSegments());
            }
            manager.close();
            expectThrows(IllegalStateException.class, manager::acquireSnapshot);
        }
    }

    public void testInitialSnapshotRecovery() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            long id = randomNonNegativeLong();
            long generation = randomIntBetween(0, 100);
            long version = randomNonNegativeLong();
            long lastWriterGeneration = randomNonNegativeLong();
            List<Segment> segments = randomIntBetween(1, 5) == 1 ? Collections.emptyList() : randomSegments();
            Map<String, String> userData = randomUserData(randomIntBetween(0, 4));

            CatalogSnapshotManager manager = new CatalogSnapshotManager(id, generation, version, segments, lastWriterGeneration, userData);
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                CatalogSnapshot acquired = ref.get();
                assertEquals(id, acquired.getId());
                assertEquals(generation, acquired.getGeneration());
                assertEquals(segments, acquired.getSegments());
                assertEquals(userData, acquired.getUserData());
                assertEquals(lastWriterGeneration, acquired.getLastWriterGeneration());
            } finally {
                manager.close();
            }
        }
    }

    public void testCloseInternalInvokedOnCommit() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        CatalogSnapshot initialSnapshot;
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            initialSnapshot = ref.get();
        }
        assertFalse(((DataformatAwareCatalogSnapshot) initialSnapshot).isClosed());

        manager.commitNewSnapshot(randomSegments());
        assertTrue(
            "snapshot should be closed when commit replaces the last ref",
            ((DataformatAwareCatalogSnapshot) initialSnapshot).isClosed()
        );
        manager.close();
    }

    public void testCloseInternalInvokedOnManagerClose() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        CatalogSnapshot snapshot;
        try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
            snapshot = ref.get();
        }
        assertFalse(((DataformatAwareCatalogSnapshot) snapshot).isClosed());

        manager.close();
        assertTrue("snapshot should be closed when manager releases the last ref", ((DataformatAwareCatalogSnapshot) snapshot).isClosed());
    }

    public void testCloseInternalNotInvokedWhileRefsHeld() throws Exception {
        CatalogSnapshotManager manager = createRandomManager();

        GatedCloseable<CatalogSnapshot> heldRef = manager.acquireSnapshot();
        CatalogSnapshot heldSnapshot = heldRef.get();
        assertFalse(((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        manager.commitNewSnapshot(randomSegments());
        assertFalse("snapshot should not be closed while a ref is still held", ((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        heldRef.close();
        assertTrue("snapshot should be closed after the last ref is released", ((DataformatAwareCatalogSnapshot) heldSnapshot).isClosed());

        manager.close();
    }

    // --- helpers ---

    private WriterFileSet randomWriterFileSet(String format) {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        String[] extensions = "lucene".equals(format) ? new String[] { "cfs", "si", "dat" } : new String[] { "parquet" };
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom(extensions));
        }
        return new WriterFileSet(directory, randomNonNegativeLong(), files, randomIntBetween(0, 10000));
    }

    private Segment randomSegment() {
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 2); i++) {
            String format = randomFrom("lucene", "parquet");
            dfGrouped.put(format, randomWriterFileSet(format));
        }
        return new Segment(randomNonNegativeLong(), dfGrouped);
    }

    private List<Segment> randomSegments() {
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            segments.add(randomSegment());
        }
        return segments;
    }

    private Map<String, String> randomUserData(int entries) {
        Map<String, String> userData = new HashMap<>();
        for (int i = 0; i < entries; i++) {
            userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        return userData;
    }

    private CatalogSnapshotManager createRandomManager() {
        return new CatalogSnapshotManager(
            randomNonNegativeLong(),
            randomIntBetween(0, 100),
            randomNonNegativeLong(),
            randomSegments(),
            randomNonNegativeLong(),
            Map.of()
        );
    }
}
