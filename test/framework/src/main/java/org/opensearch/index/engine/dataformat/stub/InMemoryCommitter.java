/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat.stub;

import org.apache.lucene.index.SegmentInfos;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * In-memory Committer for testing. Reads initial commit data from the store's
 * Lucene commit (bootstrapped in setUp), then stores subsequent commits in memory.
 */
public class InMemoryCommitter implements Committer {
    private volatile Map<String, String> committedData;
    private final long initialCommitGeneration;
    private final Store store;

    public InMemoryCommitter(Store store) throws IOException {
        SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
        this.committedData = Map.copyOf(segmentInfos.getUserData());
        this.initialCommitGeneration = segmentInfos.getGeneration();
        this.store = store;
    }

    @Override
    public CommitResult commit(CommitInput commitData) {
        this.committedData = StreamSupport.stream(commitData.userData().spliterator(), false)
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (existing, replacement) -> replacement, // Merge function for duplicate keys
                    HashMap::new
                )
            );
        return null;
    }

    @Override
    public Map<String, String> getLastCommittedData() {
        return committedData;
    }

    @Override
    public CommitStats getCommitStats() {
        try {
            SegmentInfos segmentInfos = store.readLastCommittedSegmentsInfo();
            return new CommitStats(segmentInfos);
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return SafeCommitInfo.EMPTY;
    }

    @Override
    public void close() {}

    @Override
    public List<CatalogSnapshot> listCommittedSnapshots() {
        DataformatAwareCatalogSnapshot snapshot = (DataformatAwareCatalogSnapshot) CatalogSnapshotManager.createInitialSnapshot(
            0L,
            0L,
            0L,
            List.of(),
            -1L,
            committedData
        );
        snapshot.setLastCommitInfo("segments_" + initialCommitGeneration, initialCommitGeneration, 0L);
        return List.of(snapshot);
    }

    @Override
    public void deleteCommit(CatalogSnapshot snapshot) {}

    @Override
    public boolean isCommitManagedFile(String fileName) {
        return fileName.startsWith("segments_") || fileName.equals("write.lock");
    }

    @Override
    public byte[] serializeToCommitFormat(CatalogSnapshot snapshot) {
        // Test stub does not upload to remote store.
        throw new UnsupportedOperationException("InMemoryCommitter does not serialize commits");
    }
}
