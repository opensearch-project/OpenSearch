/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link CombinedCatalogSnapshotDeletionPolicy}.
 */
public class CombinedCatalogSnapshotDeletionPolicyTests extends OpenSearchTestCase {

    private static CatalogSnapshot snapshot(long gen, long maxSeqNo, long localCP, String translogUUID) {
        Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(localCP));
        userData.put(Translog.TRANSLOG_UUID_KEY, translogUUID);
        return new DataformatAwareCatalogSnapshot(gen, gen, 0L, List.of(), 0L, userData);
    }

    private CombinedCatalogSnapshotDeletionPolicy createPolicy(AtomicLong globalCP) {
        return new CombinedCatalogSnapshotDeletionPolicy(logger, new DefaultTranslogDeletionPolicy(-1, -1, 0), globalCP::get);
    }

    public void testOnInitSetsSafeAndLastCommit() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));

        List<CatalogSnapshot> toDelete = policy.onInit(commits);
        assertTrue(toDelete.isEmpty());
        assertSame(cs1, policy.getSafeCommit());
        assertSame(cs1, policy.getLastCommit());
    }

    public void testOnInitThrowsWhenLastCommitIsNotSafe() {
        AtomicLong globalCP = new AtomicLong(50);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        // cs1 is safe (maxSeqNo=50 ≤ globalCP=50), cs2 is not (maxSeqNo=100 > 50)
        CatalogSnapshot cs1 = snapshot(1, 50, 50, "uuid1");
        CatalogSnapshot cs2 = snapshot(2, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1, cs2));

        // safe=cs1, last=cs2, safe != last → should throw
        expectThrows(IllegalStateException.class, () -> policy.onInit(commits));
    }

    public void testOnCommitIdentifiesSafeAndLastCommit() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        // New commit with maxSeqNo=200, globalCP still 100
        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);

        List<CatalogSnapshot> toDelete = policy.onCommit(commits);
        assertTrue(toDelete.isEmpty()); // cs1 is safe, cs2 is last, both kept
        assertSame(cs1, policy.getSafeCommit());
        assertSame(cs2, policy.getLastCommit());
    }

    public void testOnCommitDeletesOldCommitsBeforeSafe() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);
        policy.onCommit(commits);

        // Advance global checkpoint, add cs3
        globalCP.set(200);
        CatalogSnapshot cs3 = snapshot(3, 300, 300, "uuid1");
        commits.add(cs3);

        List<CatalogSnapshot> toDelete = policy.onCommit(commits);
        // cs2 is now safe (maxSeqNo=200 ≤ globalCP=200), cs1 should be deleted
        assertEquals(1, toDelete.size());
        assertSame(cs1, toDelete.get(0));
        assertSame(cs2, policy.getSafeCommit());
        assertSame(cs3, policy.getLastCommit());
    }

    public void testSnapshotProtectionPreventsCommitDeletion() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        // Acquire snapshot on cs1
        GatedCloseable<CatalogSnapshot> held = policy.acquireCommittedSnapshot(false);
        assertSame(cs1, held.get());

        // Add cs2, advance globalCP so cs1 would normally be deleted
        globalCP.set(200);
        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);

        List<CatalogSnapshot> toDelete = policy.onCommit(commits);
        // cs1 is snapshotted — should NOT be deleted
        assertTrue(toDelete.isEmpty());

        // Release snapshot
        held.close();

        // Now cs1 should be deletable
        toDelete = policy.onCommit(commits);
        assertEquals(1, toDelete.size());
        assertSame(cs1, toDelete.get(0));
    }

    public void testAcquireSafeVsLastCommit() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);
        policy.onCommit(commits);

        // safe=cs1, last=cs2
        try (GatedCloseable<CatalogSnapshot> safe = policy.acquireCommittedSnapshot(true)) {
            assertSame(cs1, safe.get());
        }
        try (GatedCloseable<CatalogSnapshot> last = policy.acquireCommittedSnapshot(false)) {
            assertSame(cs2, last.get());
        }
    }

    public void testMultipleSnapshotHoldsOnSameCommit() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        GatedCloseable<CatalogSnapshot> hold1 = policy.acquireCommittedSnapshot(false);
        GatedCloseable<CatalogSnapshot> hold2 = policy.acquireCommittedSnapshot(false);

        globalCP.set(200);
        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);

        // cs1 held twice — not deletable
        assertTrue(policy.onCommit(commits).isEmpty());

        hold1.close();
        // Still held once
        assertTrue(policy.onCommit(commits).isEmpty());

        hold2.close();
        // Now deletable
        assertEquals(1, policy.onCommit(commits).size());
    }

    public void testCommitsWithDifferentTranslogUUIDFiltered() throws IOException {
        AtomicLong globalCP = new AtomicLong(200);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 50, 50, "old-uuid");
        CatalogSnapshot cs2 = snapshot(2, 100, 100, "new-uuid");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1, cs2));

        // cs2 is safe and last. cs1 has different UUID and is before safe → deleted
        List<CatalogSnapshot> toDelete = policy.onInit(commits);
        assertEquals(1, toDelete.size());
        assertSame(cs1, toDelete.get(0));
        assertSame(cs2, policy.getSafeCommit());
        assertSame(cs2, policy.getLastCommit());
    }

    public void testHasUnreferencedCommits() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = createPolicy(globalCP);

        CatalogSnapshot cs1 = snapshot(1, 100, 100, "uuid1");
        List<CatalogSnapshot> commits = new ArrayList<>(List.of(cs1));
        policy.onInit(commits);

        CatalogSnapshot cs2 = snapshot(2, 200, 200, "uuid1");
        commits.add(cs2);
        policy.onCommit(commits);

        // maxSeqNoOfNextSafeCommit = 200 (cs2), globalCP = 100
        assertFalse(policy.hasUnreferencedCommits());

        // Advance globalCP past cs2's maxSeqNo
        globalCP.set(200);
        assertTrue(policy.hasUnreferencedCommits());
    }
}
