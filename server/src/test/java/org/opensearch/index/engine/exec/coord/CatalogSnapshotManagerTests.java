/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

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
 * Property-based tests for {@link CatalogSnapshotManager}.
 */
public class CatalogSnapshotManagerTests extends OpenSearchTestCase {

    private WriterFileSet randomWriterFileSet() {
        String directory = "/tmp/" + randomAlphaOfLength(8);
        long writerGeneration = randomNonNegativeLong();
        int fileCount = randomIntBetween(1, 5);
        Set<String> files = new HashSet<>();
        for (int i = 0; i < fileCount; i++) {
            files.add(randomAlphaOfLength(6) + "." + randomFrom("cfs", "si", "parquet", "dat"));
        }
        return new WriterFileSet(directory, writerGeneration, files, randomIntBetween(0, 10000));
    }

    private Segment randomSegment() {
        long generation = randomNonNegativeLong();
        int formatCount = randomIntBetween(1, 4);
        Map<String, WriterFileSet> dfGrouped = new HashMap<>();
        for (int i = 0; i < formatCount; i++) {
            dfGrouped.put(randomFrom("lucene", "parquet", "arrow", "custom_" + randomAlphaOfLength(3)), randomWriterFileSet());
        }
        return new Segment(generation, dfGrouped);
    }

    private List<Segment> randomSegments() {
        int segmentCount = randomIntBetween(0, 5);
        List<Segment> segments = new ArrayList<>();
        for (int i = 0; i < segmentCount; i++) {
            segments.add(randomSegment());
        }
        return segments;
    }

    private DataformatAwareCatalogSnapshot buildSnapshot(
        long generation,
        List<Segment> segments,
        long lastWriterGeneration,
        Map<String, String> userData
    ) {
        return new DataformatAwareCatalogSnapshot(generation, generation, 0L, segments, lastWriterGeneration, userData);
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

    public void testCommitProducesCorrectNewSnapshot() {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                long previousGeneration = manager.getCurrentGeneration();
                Set<Long> seenIds = new HashSet<>();
                seenIds.add(manager.getCurrentSnapshot().getId());

                int numCommits = randomIntBetween(1, 10);
                for (int c = 0; c < numCommits; c++) {
                    List<Segment> newSegments = randomSegments();
                    long newWriterGeneration = randomNonNegativeLong();
                    long newGeneration = previousGeneration + 1;

                    manager.commitNewSnapshot(buildSnapshot(newGeneration, newSegments, newWriterGeneration, Map.of()));

                    assertEquals(previousGeneration + 1, manager.getCurrentGeneration());
                    assertTrue(seenIds.add(manager.getCurrentSnapshot().getId()));
                    assertEquals(newSegments, manager.getCurrentSnapshot().getSegments());
                    assertTrue(manager.getCatalogSnapshotMap().containsKey(newGeneration));

                    previousGeneration = manager.getCurrentGeneration();
                }
            } finally {
                manager.close();
            }
        }
    }

    public void testUserDataPreservationOnCommit() {
        for (int iter = 0; iter < 100; iter++) {
            int initialEntries = randomIntBetween(1, 5);
            Map<String, String> initialUserData = new HashMap<>();
            for (int i = 0; i < initialEntries; i++) {
                initialUserData.put("init_" + randomAlphaOfLength(4), randomAlphaOfLength(8));
            }
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

                Map<String, String> newUserData = new HashMap<>();
                for (int i = 0; i < randomIntBetween(1, 5); i++) {
                    newUserData.put("new_" + randomAlphaOfLength(4), randomAlphaOfLength(8));
                }
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
            AtomicBoolean initialCloseInternalCalled = new AtomicBoolean(false);
            long initGen = randomIntBetween(0, 100);
            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                () -> new TrackableSnapshot(
                    randomNonNegativeLong(),
                    initGen,
                    randomNonNegativeLong(),
                    randomSegments(),
                    randomNonNegativeLong(),
                    Collections.emptyMap(),
                    initialCloseInternalCalled
                )
            );

            CatalogSnapshot initialSnapshot = manager.getCurrentSnapshot();
            assertEquals(1, initialSnapshot.refCount());

            long gen1 = initGen + 1;
            manager.commitNewSnapshot(buildSnapshot(gen1, randomSegments(), randomNonNegativeLong(), Map.of()));

            assertEquals(0, initialSnapshot.refCount());
            assertTrue(initialCloseInternalCalled.get());
            assertFalse(initialSnapshot.tryIncRef());

            int numCommits = randomIntBetween(1, 8);
            for (int c = 0; c < numCommits; c++) {
                CatalogSnapshot prev = manager.getCurrentSnapshot();
                assertEquals(1, prev.refCount());
                long nextGen = manager.getCurrentGeneration() + 1;
                manager.commitNewSnapshot(buildSnapshot(nextGen, randomSegments(), randomNonNegativeLong(), Map.of()));
                assertEquals(0, prev.refCount());
            }

            CatalogSnapshot finalSnapshot = manager.getCurrentSnapshot();
            assertEquals(1, finalSnapshot.refCount());
            manager.close();
            assertEquals(0, finalSnapshot.refCount());
        }
    }

    public void testAcquireAndReleaseViaReleasableRef() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            try {
                CatalogSnapshot currentSnap = manager.getCurrentSnapshot();
                assertEquals(1, currentSnap.refCount());

                int numAcquires = randomIntBetween(1, 5);
                List<CatalogSnapshotManager.ReleasableRef<CatalogSnapshot>> refs = new ArrayList<>();
                for (int a = 0; a < numAcquires; a++) {
                    refs.add(manager.acquireSnapshot());
                    assertEquals(1 + (a + 1), currentSnap.refCount());
                }

                for (int r = 0; r < numAcquires; r++) {
                    refs.get(r).close();
                    assertEquals(1 + numAcquires - r - 1, currentSnap.refCount());
                }
                assertEquals(1, currentSnap.refCount());

                CatalogSnapshotManager.ReleasableRef<CatalogSnapshot> heldRef = manager.acquireSnapshot();
                CatalogSnapshot heldSnapshot = heldRef.getRef();
                assertEquals(2, heldSnapshot.refCount());

                long nextGen = manager.getCurrentGeneration() + 1;
                manager.commitNewSnapshot(buildSnapshot(nextGen, randomSegments(), randomNonNegativeLong(), Map.of()));
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
            int numCommits = randomIntBetween(0, 5);
            for (int c = 0; c < numCommits; c++) {
                long nextGen = manager.getCurrentGeneration() + 1;
                manager.commitNewSnapshot(buildSnapshot(nextGen, randomSegments(), randomNonNegativeLong(), Map.of()));
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
            Map<String, String> userData = new HashMap<>();
            for (int i = 0; i < randomIntBetween(0, 4); i++) {
                userData.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
            }

            CatalogSnapshotManager manager = new CatalogSnapshotManager(
                () -> new DataformatAwareCatalogSnapshot(id, generation, version, segments, lastWriterGeneration, userData)
            );
            CatalogSnapshotManager.ReleasableRef<CatalogSnapshot> ref = null;
            try {
                ref = manager.acquireSnapshot();
                CatalogSnapshot acquired = ref.getRef();
                assertEquals(id, acquired.getId());
                assertEquals(generation, acquired.getGeneration());
                assertEquals(segments, acquired.getSegments());
                assertEquals(userData, acquired.getUserData());
                assertEquals(lastWriterGeneration, acquired.getLastWriterGeneration());
                assertSame(acquired, manager.getCurrentSnapshot());
            } finally {
                if (ref != null) ref.close();
                manager.close();
            }
        }
    }

    public void testDecRefAndRemoveFromMap() throws Exception {
        for (int iter = 0; iter < 100; iter++) {
            CatalogSnapshotManager manager = createRandomManager();
            long initGen = manager.getCurrentGeneration();
            assertEquals(1, manager.getCatalogSnapshotMap().size());

            long gen1 = initGen + 1;
            manager.commitNewSnapshot(buildSnapshot(gen1, randomSegments(), randomNonNegativeLong(), Map.of()));
            // Old snapshot had no extra refs, so decRef brought it to 0 and it was removed
            assertEquals(1, manager.getCatalogSnapshotMap().size());
            assertFalse(manager.getCatalogSnapshotMap().containsKey(initGen));
            assertTrue(manager.getCatalogSnapshotMap().containsKey(gen1));

            // Acquire a ref, commit, then the old snapshot stays in the map until the ref is released
            CatalogSnapshotManager.ReleasableRef<CatalogSnapshot> ref = manager.acquireSnapshot();
            long gen2 = gen1 + 1;
            manager.commitNewSnapshot(buildSnapshot(gen2, randomSegments(), randomNonNegativeLong(), Map.of()));
            assertEquals(2, manager.getCatalogSnapshotMap().size());
            assertTrue(manager.getCatalogSnapshotMap().containsKey(gen1));

            ref.close();
            assertEquals(1, manager.getCatalogSnapshotMap().size());
            assertFalse(manager.getCatalogSnapshotMap().containsKey(gen1));

            manager.close();
            assertEquals(0, manager.getCatalogSnapshotMap().size());
        }
    }

    private static class TrackableSnapshot extends DataformatAwareCatalogSnapshot {
        private final AtomicBoolean closeInternalCalled;

        TrackableSnapshot(
            long id,
            long generation,
            long version,
            List<Segment> segments,
            long lastWriterGeneration,
            Map<String, String> userData,
            AtomicBoolean closeInternalCalled
        ) {
            super(id, generation, version, segments, lastWriterGeneration, userData);
            this.closeInternalCalled = closeInternalCalled;
        }

        @Override
        protected void closeInternal() {
            closeInternalCalled.set(true);
        }
    }
}
