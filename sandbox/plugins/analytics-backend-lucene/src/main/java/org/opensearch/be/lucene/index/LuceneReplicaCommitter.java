/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.engine.exec.coord.LuceneVersionConverter;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.opensearch.be.lucene.index.LuceneCommitter.loadCommittedSnapshots;

/**
 * Replica-side {@link Committer} implementation backed by Lucene's {@link SegmentInfos} commit mechanism.
 * <p>
 * Unlike the primary's {@link LuceneCommitter} which owns an {@link IndexWriter}, this committer
 * operates without a writer — it directly writes {@code segments_N} files to the store directory
 * using {@link Store#commitSegmentInfos(SegmentInfos, long, long)}. This is appropriate for replicas
 * that receive segments via segment replication and only need to persist the commit point.
 * <p>
 * The commit path clones the primary's {@link SegmentInfos} (received during replication), injects
 * the user data (containing the serialized {@link CatalogSnapshot}), and writes it as a new commit.
 * The {@link #listCommittedSnapshots()} method discovers existing commits by reading all
 * {@code segments_N} files from the store directory.
 *
 * @opensearch.experimental
 */
public class LuceneReplicaCommitter implements Committer {

    private final Store store;
    private final AtomicBoolean isClosed = new AtomicBoolean();

    private volatile SegmentInfos lastCommittedSegmentInfos;

    public LuceneReplicaCommitter(CommitterConfig committerConfig) throws IOException {
        this.store = committerConfig.engineConfig().getStore();
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
    }

    @Override
    public synchronized CommitResult commit(CommitInput commitInput) throws IOException {
        // Build the SegmentInfos to commit. If the primary's SegmentInfos is available
        // (set by SegmentReplicationTarget during replication), clone it so the commit on
        // disk contains the real Lucene segment entries. This is critical for primary
        // promotion: when this replica is later promoted, the new IndexWriter must open on
        // a commit that includes all Lucene segments. Without this, the IndexWriter sees an
        // empty commit and any Lucene secondary-format files become orphans.
        SegmentInfos sis = getSegmentInfos(commitInput.catalogSnapshot());
        if (sis == null) {
            sis = lastCommittedSegmentInfos;
        } else {
            sis = sis.clone();
            sis.updateGeneration(lastCommittedSegmentInfos);
        }
        Map<String, String> userData = new HashMap<>(sis.userData);
        for (Map.Entry<String, String> entries : commitInput.userData()) {
            userData.put(entries.getKey(), entries.getValue());
        }
        sis.setUserData(userData, false);

        if (commitInput.bumpCounter() > 0) {
            sis.counter = store.readLastCommittedSegmentsInfo().counter + commitInput.bumpCounter();
            sis.changed();
        }

        store.commitSegmentInfos(sis);
        store.directory().sync(commitInput.catalogSnapshot().getFiles(true));

        SegmentInfos committed = SegmentInfos.readLatestCommit(store.directory());
        assert sis.getGeneration() == committed.getGeneration() : "Committed generation is different from requested generation";
        assert sis.userData.equals(committed.getUserData()) : "Committer user data is different from requested user data";

        // Encode writer's Lucene version as a long — keeps CatalogSnapshot Lucene-type-agnostic.
        long version = LuceneVersionConverter.encode(committed.getCommitLuceneVersion());
        this.lastCommittedSegmentInfos = committed;
        return new CommitResult(committed.getSegmentsFileName(), committed.getGeneration(), version);
    }

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        return lastCommittedSegmentInfos.getUserData();
    }

    @Override
    public CommitStats getCommitStats() {
        try {
            return new CommitStats(store.readLastCommittedSegmentsInfo());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return null;
    }

    @Override
    public List<CatalogSnapshot> listCommittedSnapshots() throws IOException {
        ensureOpen();
        return loadCommittedSnapshots(store).values().stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    @Override
    public void close() throws IOException {
        if (isClosed.compareAndSet(false, true)) {
            store.decRef();
        }
    }

    @Override
    public void deleteCommit(CatalogSnapshot snapshot) throws IOException {
        store.deleteQuiet(snapshot.getLastCommitFileName());
    }

    @Override
    public boolean isCommitManagedFile(String fileName) {
        return fileName.startsWith(IndexFileNames.SEGMENTS) || IndexWriter.WRITE_LOCK_NAME.equals(fileName);
    }

    @Override
    public byte[] serializeToCommitFormat(CatalogSnapshot catalogSnapshot) throws IOException {
        throw new UnsupportedEncodingException("serialization is only for uploads. store committer is for replicas");
    }

    private void ensureOpen() {
        if (isClosed.get()) {
            throw new IllegalStateException("LuceneCommitter is closed");
        }
    }

    public static SegmentInfos getSegmentInfos(CatalogSnapshot catalogSnapshot) {
        if (catalogSnapshot instanceof DataformatAwareCatalogSnapshot dfacs) {
            // We got a catalog snapshot as part of finalize replication
            return (SegmentInfos) dfacs.getReplicatingCommitData();
        }
        // use last committed one only
        return null;
    }
}
