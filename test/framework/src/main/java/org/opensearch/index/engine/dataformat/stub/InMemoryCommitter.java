/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * In-memory Committer for testing. Reads initial commit data from the store's
 * Lucene commit (bootstrapped in setUp), then stores subsequent commits in memory.
 */
public class InMemoryCommitter implements Committer {
    private volatile Map<String, String> committedData;
    private final AtomicReference<Exception> tragicException = new AtomicReference<>();
    private volatile Supplier<Exception> commitFailure;
    private volatile boolean markCorruptedCalled;

    public InMemoryCommitter(Store store) throws IOException {
        this.committedData = Map.copyOf(store.readLastCommittedSegmentsInfo().getUserData());
    }

    public void setTragicException(Exception e) {
        this.tragicException.set(e);
    }

    public void setCommitFailure(Supplier<Exception> supplier) {
        this.commitFailure = supplier;
    }

    public boolean isMarkCorruptedCalled() {
        return markCorruptedCalled;
    }

    public void setMarkCorruptedCalled(boolean v) {
        this.markCorruptedCalled = v;
    }

    @Override
    public Exception getTragicException() {
        return tragicException.get();
    }

    @Override
    public void commit(Map<String, String> commitData) throws IOException {
        Supplier<Exception> supplier = commitFailure;
        if (supplier != null) {
            Exception failure = supplier.get();
            if (failure != null) {
                if (failure instanceof IOException) throw (IOException) failure;
                throw new IOException(failure);
            }
        }
        this.committedData = Map.copyOf(commitData);
    }

    @Override
    public Map<String, String> getLastCommittedData() {
        return committedData;
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
    public void close() {}

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
}
