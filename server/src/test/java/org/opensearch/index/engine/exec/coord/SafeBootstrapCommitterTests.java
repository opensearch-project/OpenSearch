/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.CombinedCatalogSnapshotDeletionPolicy;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.commit.SafeBootstrapCommitter;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.translog.DefaultTranslogDeletionPolicy;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for {@link SafeBootstrapCommitter}.
 * In exec.coord package to access DataformatAwareCatalogSnapshot's package-private constructor.
 */
public class SafeBootstrapCommitterTests extends OpenSearchTestCase {

    /**
     * Concrete test subclass. Since instance fields are not initialized when super() calls
     * the abstract methods, we use static commits/returnNull fields set before each test.
     * Tests are single-threaded per instance so static fields are safe here.
     */
    private static List<CatalogSnapshot> commits = List.of();
    private static boolean returnNull = false;
    private static boolean rewriteCalled = false;

    private static void reset(List<CatalogSnapshot> c, boolean rn) {
        commits = c;
        returnNull = rn;
        rewriteCalled = false;
    }

    private static class TestCommitter extends SafeBootstrapCommitter {

        TestCommitter(CommitterConfig config) throws IOException {
            super(config);
        }

        @Override
        protected List<CatalogSnapshot> discoverCommits(CommitterConfig config) {
            if (returnNull) return null;
            return commits;
        }

        @Override
        protected void rewriteAtSafeCommit(CommitterConfig config, List<CatalogSnapshot> c, CatalogSnapshot safe) {
            rewriteCalled = true;
        }

        @Override
        public void commit(Map<String, String> commitData) {}

        @Override
        public Map<String, String> getLastCommittedData() {
            return Map.of();
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public SafeCommitInfo getSafeCommitInfo() {
            return SafeCommitInfo.EMPTY;
        }

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void close() {}
    }

    private static CatalogSnapshot snapshot(long gen, long maxSeqNo) {
        Map<String, String> userData = new HashMap<>();
        userData.put(SequenceNumbers.MAX_SEQ_NO, Long.toString(maxSeqNo));
        userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(maxSeqNo));
        userData.put(Translog.TRANSLOG_UUID_KEY, "test-uuid");
        return new DataformatAwareCatalogSnapshot(gen, gen, 0L, List.of(), 0L, userData);
    }

    private CommitterConfig configWithPolicy(CatalogSnapshotDeletionPolicy policy) {
        return new CommitterConfig(null, null, null, Optional.ofNullable(policy));
    }

    public void testThrowsWhenNoDeletionPolicy() {
        reset(List.of(snapshot(1, 100)), false);
        expectThrows(IllegalArgumentException.class, () -> new TestCommitter(configWithPolicy(null)));
    }

    public void testThrowsWhenNullCommitList() {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );
        reset(null, true);
        expectThrows(IllegalStateException.class, () -> new TestCommitter(configWithPolicy(policy)));
    }

    public void testNoTrimWhenEmptyCommitList() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );
        reset(List.of(), false);
        new TestCommitter(configWithPolicy(policy));
        assertFalse(rewriteCalled);
    }

    public void testNoTrimWhenSingleCommit() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );
        reset(List.of(snapshot(1, 100)), false);
        new TestCommitter(configWithPolicy(policy));
        assertFalse(rewriteCalled);
    }

    public void testTrimsWhenMultipleCommitsEvenIfSafeEqualsLast() throws IOException {
        AtomicLong globalCP = new AtomicLong(200);
        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );
        CatalogSnapshot cs1 = snapshot(1, 100);
        CatalogSnapshot cs2 = snapshot(2, 200);
        reset(new ArrayList<>(List.of(cs1, cs2)), false);
        new TestCommitter(configWithPolicy(policy));
        assertTrue(rewriteCalled);
    }

    public void testTrimsWhenSafeNotEqualToLast() throws IOException {
        AtomicLong globalCP = new AtomicLong(100);
        CombinedCatalogSnapshotDeletionPolicy policy = new CombinedCatalogSnapshotDeletionPolicy(
            logger,
            new DefaultTranslogDeletionPolicy(-1, -1, 0),
            globalCP::get
        );
        CatalogSnapshot cs1 = snapshot(1, 100);
        CatalogSnapshot cs2 = snapshot(2, 200);
        reset(new ArrayList<>(List.of(cs1, cs2)), false);
        new TestCommitter(configWithPolicy(policy));
        assertTrue(rewriteCalled);
    }
}
