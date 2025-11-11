/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.CATALOG_SNAPSHOT_KEY;

public class LuceneCommitEngine implements Committer {

    private final IndexWriter indexWriter;
    private final LuceneIndexDeletionPolicy indexDeletionPolicy;
    private final Store store;

    public LuceneCommitEngine(Store store) throws IOException {
        indexDeletionPolicy = new LuceneIndexDeletionPolicy();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setIndexDeletionPolicy(indexDeletionPolicy);
        this.store = store;
        this.indexWriter = new IndexWriter(store.directory(), indexWriterConfig);
    }

    @Override
    public void addLuceneIndexes(CatalogSnapshot catalogSnapshot) {
        Collection<WriterFileSet> luceneFileCollection = catalogSnapshot.getSearchableFiles(DataFormat.LUCENE.name());
        luceneFileCollection.forEach(writerFileSet -> {
            try {
                indexWriter.addIndexes(new NIOFSDirectory(Path.of(writerFileSet.getDirectory())));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot) {
        addLuceneIndexes(catalogSnapshot);
        indexWriter.setLiveCommitData(commitData);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = indexDeletionPolicy.getLatestIndexCommit();
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

    @Override
    public Map<String, String> getLastCommittedData() throws IOException {
        return store.readLastCommittedSegmentsInfo().getUserData();
    }

    @Override
    public Optional<CatalogSnapshot> readLastCommittedCatalogSnapshot() throws IOException {
        Map<String, String> lastCommittedData = getLastCommittedData();
        if (lastCommittedData.containsKey(CATALOG_SNAPSHOT_KEY)) {
            return Optional.of(CatalogSnapshot.deserializeFromString(lastCommittedData.get(CATALOG_SNAPSHOT_KEY)));
        }
        return Optional.empty();
    }

    @Override
    public SequenceNumbers.CommitInfo loadSeqNoInfoFromLastCommit() throws IOException {
        return SequenceNumbers.loadSeqNoInfoFromLuceneCommit(getLastCommittedData().entrySet());
    }

    @Override
    public void close() throws IOException {
        this.indexWriter.close();
    }
}
