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
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * In-memory Committer for testing. Reads initial commit data from the store's
 * Lucene commit (bootstrapped in setUp), then stores subsequent commits in memory.
 */
public class InMemoryCommitter implements Committer {
    private volatile Map<String, String> committedData;

    public InMemoryCommitter(Store store) throws IOException {
        this.committedData = Map.copyOf(store.readLastCommittedSegmentsInfo().getUserData());
    }

    @Override
    public CommitResult commit(CommitInput commitData) {
        this.committedData = StreamSupport.stream(commitData.userData().spliterator(), false)
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                Map.Entry::getValue,
                (existing, replacement) -> replacement, // Merge function for duplicate keys
                HashMap::new
            ));
        return null;
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

    @Override
    public byte[] serializeToCommitFormat(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {
        // Test stub does not upload to remote store.
        throw new UnsupportedOperationException("InMemoryCommitter does not serialize commits");
    }
}
