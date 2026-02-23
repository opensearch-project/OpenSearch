/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.CombinedDeletionPolicy;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogDeletionPolicy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

public class LuceneCommitEngine implements Committer {

    private final Logger logger;
    private IndexWriter indexWriter;
    private final Store store;
    private final CombinedDeletionPolicy combinedDeletionPolicy;
    private volatile SegmentInfos lastCommittedSegmentInfos;

    public LuceneCommitEngine(
        Store store,
        TranslogDeletionPolicy translogDeletionPolicy,
        LongSupplier globalCheckpointSupplier,
        boolean primaryMode
    ) throws IOException {
        this.logger = Loggers.getLogger(LuceneCommitEngine.class, store.shardId());
        this.combinedDeletionPolicy = new CombinedDeletionPolicy(logger, translogDeletionPolicy, null, globalCheckpointSupplier);
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setIndexDeletionPolicy(combinedDeletionPolicy);
        indexWriterConfig.setMergePolicy(NoMergePolicy.INSTANCE);
        this.store = store;
        this.lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        if (primaryMode) {
            this.indexWriter = new IndexWriter(store.directory(), indexWriterConfig);
        }
    }

    @Override
    public synchronized void addLuceneIndexes(List<Segment> segments) {
        for (Segment segment : segments) {
            WriterFileSet writerFileSet = segment.getDFGroupedSearchableFiles().get(DataFormat.LUCENE.name());
            if (writerFileSet == null || writerFileSet.isRefreshed()) {
                continue;
            }
            try {
                indexWriter.addIndexes(NIOFSDirectory.open(Path.of(writerFileSet.getDirectory())));
                writerFileSet.setRefreshed();
            } catch (IOException e) {
                throw new RuntimeException("Failed to add Lucene index at " + writerFileSet.getDirectory(), e);
            }
        }

        final Map<Long, Segment> segmentByGeneration =
            segments.stream().collect(Collectors.toMap(Segment::getGeneration, Function.identity()));

        try (DirectoryReader reader = DirectoryReader.open(indexWriter)) {
            for (LeafReaderContext leaf : reader.getContext().leaves()) {
                SegmentCommitInfo info = Lucene.segmentReader(leaf.reader()).getSegmentInfo();

                String generationAttr = info.info.getAttribute("writer_generation");
                if (generationAttr == null) {
                    throw new RuntimeException("Failed to fetch writer generation from Lucene writer.");
                }

                long writerGeneration = Long.parseLong(generationAttr);
                if (segmentByGeneration.containsKey(writerGeneration)) {
                    WriterFileSet writerFileSet =
                        segmentByGeneration.get(writerGeneration).getDFGroupedSearchableFiles().get(DataFormat.LUCENE.name());
                    segmentByGeneration.get(writerGeneration).addSearchableFiles(
                        DataFormat.LUCENE.name(),
                        writerFileSet.withDirectoryAndFiles(indexWriter.getDirectory().toString(), new HashSet<>(info.files()))
                    );
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to reconcile Lucene segments.", e);
        }
    }

    @Override
    public synchronized CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot) {
        indexWriter.setLiveCommitData(commitData);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = combinedDeletionPolicy.getLastCommit();
            refreshLastCommittedSegmentInfos();
            return CommitPoint.builder()
                .commitFileName(indexCommit.getSegmentsFileName())
                .fileNames(indexCommit.getFileNames())
                .commitData(indexCommit.getUserData())
                .generation(indexCommit.getGeneration())
                .directory(Path.of(indexCommit.getSegmentsFileName()).getParent())
                .build();
        } catch (IOException e) {
            throw new RuntimeException("lucene commit engine failed", e);
        }
    }

    private void refreshLastCommittedSegmentInfos() {
        store.incRef();
        try {
            lastCommittedSegmentInfos = store.readLastCommittedSegmentsInfo();
        } catch (Exception e) {
            throw new RuntimeException("failed to read latest segment infos on commit", e);
        } finally {
            store.decRef();
        }
    }

    @Override
    public Map<String, String> getLastCommittedData() {
        return MapBuilder.<String, String>newMapBuilder().putAll(lastCommittedSegmentInfos.getUserData()).immutableMap();
    }

    @Override
    public CommitStats getCommitStats() {
        String segmentId = Base64.getEncoder().encodeToString(lastCommittedSegmentInfos.getId());
        // TODO: Implement numDocs
        return new CommitStats(lastCommittedSegmentInfos.getUserData(), lastCommittedSegmentInfos.getLastGeneration(), segmentId, 0);
    }

    @Override
    public SafeCommitInfo getSafeCommitInfo() {
        return this.combinedDeletionPolicy.getSafeCommitInfo();
    }

    /**
     * Acquires the most recent safe index commit snapshot.
     * All index files referenced by this commit won't be freed until the commit/snapshot is closed.
     * This method is required for replica recovery operations.
     */
    public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
        try {
            // Use CombinedDeletionPolicy to acquire safe commit
            IndexCommit safeCommit = combinedDeletionPolicy.acquireIndexCommit(true);
            return new GatedCloseable<>(
                safeCommit, () -> {
                try {
                    combinedDeletionPolicy.releaseCommit(safeCommit);
                } catch (Exception e) {
                    logger.warn("Failed to release safe commit", e);
                }
            }
            );
        } catch (Exception e) {
            throw new EngineException(store.shardId(), "Failed to acquire safe index commit", e);
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indexWriter);
    }
}
