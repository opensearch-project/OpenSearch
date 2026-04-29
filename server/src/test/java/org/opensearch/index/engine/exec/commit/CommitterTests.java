/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Tests for the {@link Committer} interface contract.
 */
public class CommitterTests extends OpenSearchTestCase {

    private static Committer noOpCommitter() {
        return new Committer() {
            @Override
            public void commit(Map<String, String> commitData) {}

            @Override
            public void close() {}

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
            public java.util.List<org.opensearch.index.engine.exec.coord.CatalogSnapshot> listCommittedSnapshots() {
                return java.util.List.of();
            }

            @Override
            public void deleteCommit(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {}

            @Override
            public boolean isCommitManagedFile(String fileName) {
                return false;
            }
        };
    }

    public void testMinimalImplementationCanBeInstantiated() {
        assertNotNull(noOpCommitter());
    }

    public void testCloseFromCloseableIsCallable() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        Committer committer = new Committer() {
            @Override
            public void commit(Map<String, String> commitData) {}

            @Override
            public void close() {
                closed.set(true);
            }

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
            public java.util.List<org.opensearch.index.engine.exec.coord.CatalogSnapshot> listCommittedSnapshots() {
                return java.util.List.of();
            }

            @Override
            public void deleteCommit(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {}

            @Override
            public boolean isCommitManagedFile(String fileName) {
                return false;
            }
        };
        committer.close();
        assertTrue("close() should have been called", closed.get());
    }

    public void testCommitIsCallable() throws IOException {
        AtomicBoolean committed = new AtomicBoolean(false);
        Committer committer = new Committer() {
            @Override
            public void commit(Map<String, String> commitData) {
                committed.set(true);
            }

            @Override
            public void close() {}

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
            public java.util.List<org.opensearch.index.engine.exec.coord.CatalogSnapshot> listCommittedSnapshots() {
                return java.util.List.of();
            }

            @Override
            public void deleteCommit(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {}

            @Override
            public boolean isCommitManagedFile(String fileName) {
                return false;
            }
        };
        committer.commit(Map.of());
        assertTrue("commit() should have been called", committed.get());
    }
}
