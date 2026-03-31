/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.CatalogSnapshot;
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
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for {@link CatalogSnapshotManager}.
 */
public class CatalogSnapshotManagerTests extends OpenSearchTestCase {

    public void testCommitProducesCorrectNewSnapshot() {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                long previousGeneration = manager.getCurrentSnapshot().getGeneration();
                Set<Long> seenIds = new HashSet<>();
                seenIds.add(manager.getCurrentSnapshot().getId());

                int numCommits = randomIntBetween(1, 10);
                for (int c = 0; c < numCommits; c++) {
                    List<Segment> newSegments = randomSegments();
                    long newGeneration = previousGeneration + 1;

                    manager.commitNewSnapshot(buildSnapshot(newGeneration, newSegments, randomNonNegativeLong(), Map.of()));

                    assertEquals(previousGeneration + 1, manager.getCurrentSnapshot().getGeneration());
                    assertTrue(seenIds.add(manager.getCurrentSnapshot().getId()));
                    assertEquals(newSegments, manager.getCurrentSnapshot().getSegments());

                    previousGeneration = manager.getCurrentSnapshot().getGeneration();
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testUserDataPreservationOnCommit() {
        for (int iter = 0; iter < 100; iter++) {
            Map<String, String> initialUserData = randomUserData(randomIntBetween(1, 5));
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                () -> new DataformatAwareCatalogSnapshot(
                    randomNonNegativeLong(),
                    initGen,
                    randomNonNegativeLong(),
                    randomSegments(),
                    randomNonNegativeLong(),
                    initialUserData
                )
            );
            try {
                long gen1 = initGen + 1;
                manager.commitNewSnapshot(
                    buildSnapshot(gen1, randomSegments(), randomNonNegativeLong(), manager.getCurrentSnapshot().getUserData())
                );
                assertEquals(initialUserData, manager.getCurrentSnapshot().getUserData());

                Map<String, String> newUserData = randomUserData(randomIntBetween(1, 5));
                long gen2 = gen1 + 1;
                manager.commitNewSnapshot(buildSnapshot(gen2, randomSegments(), randomNonNegativeLong(), newUserData));
                assertEquals(newUserData, manager.getCurrentSnapshot().getUserData());
            } finally {
                manager.close();
            }
        }
    }

    public void testReferenceCountingLifecycle() {
        for (int iter = 0; iter < 100; iter++) {
            AtomicBoolean closeInternalCalled = new AtomicBoolean(false);
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                () -> new TrackableSnapshot(
                    randomNonNegativeLong(),
                    initGen,
                    randomNonNegativeLong(),
                    randomSegments(),
                    randomNonNegativeLong(),
                    Collections.emptyMap(),
                    closeInternalCalled
                )
            );

            CatalogSnapshot initialSnapshot = manager.getCurrentSnapshot();
            assertEquals(1, initialSnapshot.refCount());

            manager.commitNewSnapshot(buildSnapshot(initGen + 1, randomSegments(), randomNonNegativeLong(), Map.of()));
            assertEquals(0, initialSnapshot.refCount());
            assertTrue(closeInternalCalled.get());

            int numCommits = randomIntBetween(1, 8);
            for (int c = 0; c < numCommits; c++) {
                CatalogSnapshot prev = manager.getCurrentSnapshot();
                assertEquals(1, prev.refCount());
                manager.commitNewSnapshot(
                    buildSnapshot(manager.getCurrentSnapshot().getGeneration() + 1, randomSegments(), randomNonNegativeLong(), Map.of())
                );
                assertEquals(0, prev.refCount());
            }

            CatalogSnapshot finalSnapshot = manager.getCurrentSnapshot();
            assertEquals(1, finalSnapshot.refCount());
            manager.close();
            assertEquals(0, finalSnapshot.refCount());
        }
    }

    public void testAcquireAndReleaseViaGatedCloseable() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                CatalogSnapshot currentSnap = manager.getCurrentSnapshot();
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

                manager.commitNewSnapshot(
                    buildSnapshot(manager.getCurrentSnapshot().getGeneration() + 1, randomSegments(), randomNonNegativeLong(), Map.of())
                );
                assertEquals(1, heldSnapshot.refCount());

                heldRef.close();
                assertEquals(0, heldSnapshot.refCount());
            } finally {
                manager.close();
            }
        }
    }

    public void testClosedManagerRejectsAcquisition() {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            for (int c = 0; c < randomIntBetween(0, 5); c++) {
                manager.commitNewSnapshot(
                    buildSnapshot(manager.getCurrentSnapshot().getGeneration() + 1, randomSegments(), randomNonNegativeLong(), Map.of())
                );
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

            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                () -> new DataformatAwareCatalogSnapshot(id, generation, version, segments, lastWriterGeneration, userData)
            );
            try (GatedCloseable<CatalogSnapshot> ref = manager.acquireSnapshot()) {
                CatalogSnapshot acquired = ref.get();
                assertEquals(id, acquired.getId());
                assertEquals(generation, acquired.getGeneration());
                assertEquals(segments, acquired.getSegments());
                assertEquals(userData, acquired.getUserData());
                assertEquals(lastWriterGeneration, acquired.getLastWriterGeneration());
                assertSame(acquired, manager.getCurrentSnapshot());
            } finally {
                manager.close();
            }
        }
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

    private DataformatAwareCatalogSnapshot buildSnapshot(long gen, List<Segment> segments, long writerGen, Map<String, String> userData) {
        return new DataformatAwareCatalogSnapshot(gen, gen, 0L, segments, writerGen, userData);
    }

    private CatalogSnapshotManager createRandomManager() {
        return new CatalogSnapshotManager(
            () -> new DataformatAwareCatalogSnapshot(
                randomNonNegativeLong(),
                randomIntBetween(0, 100),
                randomNonNegativeLong(),
                randomSegments(),
                randomNonNegativeLong(),
                Map.of()
            )
        );
    }

    private static class TrackableSnapshot extends DataformatAwareCatalogSnapshot {
        private final AtomicBoolean closeInternalCalled;

        TrackableSnapshot(
            long id,
            long gen,
            long version,
            List<Segment> segments,
            long writerGen,
            Map<String, String> userData,
            AtomicBoolean closeInternalCalled
        ) {
            super(id, gen, version, segments, writerGen, userData);
            this.closeInternalCalled = closeInternalCalled;
        }

        @Override
        protected void closeInternal() {
            closeInternalCalled.set(true);
        }
    }
}
