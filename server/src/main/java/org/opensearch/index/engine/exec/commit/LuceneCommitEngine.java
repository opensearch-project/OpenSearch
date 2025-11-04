/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

public class LuceneCommitEngine implements Committer {

    private final IndexWriter indexWriter;
    private final LuceneIndexDeletionPolicy indexDeletionPolicy;

    public LuceneCommitEngine(Path commitPath) throws IOException {
        Directory directory = new NIOFSDirectory(commitPath);
        indexDeletionPolicy = new LuceneIndexDeletionPolicy();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig();
        indexWriterConfig.setIndexDeletionPolicy(indexDeletionPolicy);
        this.indexWriter = new IndexWriter(directory, indexWriterConfig);
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

        Map<String, String> userData = null;
        try {
            userData = catalogSnapshot.toCommitUserData();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        indexWriter.setLiveCommitData(userData.entrySet());
    }

    public CatalogSnapshot readCatalogSnapshot() throws IOException {
        if(indexWriter.getLiveCommitData().iterator().hasNext()) {
            Map.Entry<String, String> entry = indexWriter.getLiveCommitData().iterator().next();
            return CatalogSnapshot.fromCommitUserData(entry.getValue());
        }
        return null;
    }
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    @Override
    public CommitPoint commit(CatalogSnapshot catalogSnapshot) {
        addLuceneIndexes(catalogSnapshot);
        try {
            indexWriter.commit();
            IndexCommit indexCommit = indexDeletionPolicy.getLatestIndexCommit();
            return CommitPoint.builder().commitFileName(indexCommit.getSegmentsFileName())
                .fileNames(indexCommit.getFileNames()).commitData(indexCommit.getUserData())
                .generation(indexCommit.getGeneration())
                .directory(Path.of(indexCommit.getSegmentsFileName()).getParent()).build();
        } catch (IOException e) {
            throw new RuntimeException("lucene commit engine failed", e);
        }
    }
}
