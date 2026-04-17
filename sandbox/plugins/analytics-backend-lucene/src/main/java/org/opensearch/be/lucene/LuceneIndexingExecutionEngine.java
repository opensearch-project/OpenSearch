/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Lucene-specific {@link IndexingExecutionEngine} that incorporates flushed segments
 * during refresh via {@code IndexWriter.addIndexes(Directory...)}.
 * <p>
 * Does not own the {@link IndexWriter} — it receives an optional parent writer
 * from the {@link LuceneCommitter} which owns the writer lifecycle. This separation allows
 * the committer to be used standalone (committer-only scenario) while the indexing engine
 * is only created when Lucene participates as a per-format engine in the composite engine.
 * <p>
 * When no parent writer is provided, refresh is a no-op (no segments to incorporate).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexingExecutionEngine implements IndexingExecutionEngine<DataFormat, DocumentInput<?>>, IndexStoreProvider {

    private static final Logger logger = LogManager.getLogger(LuceneIndexingExecutionEngine.class);

    // TODO: This will go once we implement the DataFormat plugin in Lucene
    private static final String LUCENE_FORMAT_NAME = "lucene";

    private final LuceneCommitter luceneCommitter;
    private final Store store;

    /**
     * Creates a new LuceneIndexingExecutionEngine.
     *
     * @param luceneCommitter the LuceneCommitter that owns the IndexWriter lifecycle, must not be null
     * @param store the shard's store, or null if not available
     */
    public LuceneIndexingExecutionEngine(LuceneCommitter luceneCommitter, Store store) {
        if (luceneCommitter == null) {
            throw new IllegalArgumentException("LuceneCommitter must not be null");
        }
        this.luceneCommitter = luceneCommitter;
        this.store = store;
    }

    // --- IndexWriter access (used by LuceneSearchBackEnd for NRT readers) ---

    /**
     * Returns the underlying IndexWriter.
     *
     * @return the index writer, or null if not available
     */
    IndexWriter getWriter() {
        return luceneCommitter.getIndexWriter();
    }

    // --- IndexStoreProvider ---

    @Override
    public IndexStoreProvider getProvider() {
        return this;
    }

    @Override
    public Store getStore() {
        return store;
    }

    // --- IndexingExecutionEngine ---

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        if (refreshInput == null || luceneCommitter.getIndexWriter() == null) {
            return new RefreshResult(List.of());
        }

        List<Directory> directories = new ArrayList<>();
        List<Path> sourcePaths = new ArrayList<>();
        try {
            for (Segment segment : refreshInput.writerFiles()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(LUCENE_FORMAT_NAME);
                if (wfs == null) {
                    continue;
                }
                Path dirPath = Path.of(wfs.directory());
                if (Files.isDirectory(dirPath)) {
                    directories.add(new HardlinkCopyDirectoryWrapper(new NIOFSDirectory(dirPath)));
                    sourcePaths.add(dirPath);
                }
            }
            if (directories.isEmpty() == false) {
                luceneCommitter.getIndexWriter().addIndexes(directories.toArray(new Directory[0]));
            }
        } finally {
            for (Directory dir : directories) {
                try {
                    dir.close();
                } catch (IOException e) {
                    logger.warn("Failed to close directory after addIndexes", e);
                }
            }
            for (Path sourcePath : sourcePaths) {
                try {
                    IOUtils.rm(sourcePath);
                } catch (IOException e) {
                    logger.warn(() -> new ParameterizedMessage("Failed to delete source directory [{}] after addIndexes", sourcePath), e);
                }
            }
        }
        return new RefreshResult(List.of());
    }

    @Override
    public Writer<DocumentInput<?>> createWriter(long writerGeneration) {
        throw new UnsupportedOperationException("createWriter not yet implemented for Lucene engine");
    }

    @Override
    public Merger getMerger() {
        return null;
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("getNextWriterGeneration not yet implemented for Lucene engine");
    }

    @Override
    public DataFormat getDataFormat() {
        throw new UnsupportedOperationException("getDataFormat not yet implemented — LuceneDataFormat deferred to future PR");
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        // Stub: file deletion to be implemented in a future iteration
    }

    @Override
    public DocumentInput<?> newDocumentInput() {
        throw new UnsupportedOperationException("newDocumentInput not yet implemented — LuceneDocumentInput deferred to future PR");
    }

    @Override
    public void close() throws IOException {
        // LuceneCommitter owns the IndexWriter lifecycle; nothing to close here.
    }
}
