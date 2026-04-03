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

    /** Creates a minimal Committer with no-op implementations for all methods. */
    private static Committer noOpCommitter() {
        return new Committer() {
            @Override
            public void init(CommitterSettings settings) throws IOException {}

            @Override
            public void commit(Map<String, String> commitData) throws IOException {}

            @Override
            public void close() throws IOException {}

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
        };
    }

    public void testMinimalImplementationCanBeInstantiated() {
        Committer committer = noOpCommitter();
        assertNotNull(committer);
    }

    public void testCloseFromCloseableIsCallable() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        Committer committer = new Committer() {
            @Override
            public void init(CommitterSettings settings) {}

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
        };

        committer.close();
        assertTrue("close() should have been called", closed.get());
    }

    public void testCommitterWorksWithTryWithResources() throws IOException {
        AtomicBoolean closed = new AtomicBoolean(false);
        try (Committer committer = new Committer() {
            @Override
            public void init(CommitterSettings settings) {}

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
        }) {
            assertNotNull(committer);
        }
        assertTrue("close() should have been called by try-with-resources", closed.get());
    }

    public void testInitIsCallable() throws IOException {
        AtomicBoolean initialized = new AtomicBoolean(false);
        Committer committer = new Committer() {
            @Override
            public void init(CommitterSettings settings) {
                initialized.set(true);
            }

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
        };

        committer.init(null);
        assertTrue("init() should have been called", initialized.get());
    }

    public void testCommitIsCallable() throws IOException {
        AtomicBoolean committed = new AtomicBoolean(false);
        Committer committer = new Committer() {
            @Override
            public void init(CommitterSettings settings) {}

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
        };

        committer.commit(Map.of());
        assertTrue("commit() should have been called", committed.get());
    }
}
