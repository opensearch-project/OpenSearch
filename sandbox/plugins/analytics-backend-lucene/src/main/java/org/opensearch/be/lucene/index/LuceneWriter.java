/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Per-generation Lucene writer that creates segments in an isolated temporary directory.
 *
 * Each instance owns its own {@link IndexWriter} and {@link Directory}. Documents are
 * added via {@link #addDoc(LuceneDocumentInput)}, and on {@link #flush()}, the writer
 * performs a force merge to exactly 1 segment to maintain a 1:1 mapping between the
 * Lucene segment and the corresponding Parquet file for the same writer generation.
 *
 * The writer uses a large RAM buffer (256 MB by default) to minimize the chance of
 * intermediate flushes, since all writes to a single writer come from a single thread
 * (the writer pool in {@code DataFormatAwareEngine} locks the writer during use).
 *
 * After flush, the returned {@link FileInfos} contains the temp directory path so that
 * {@link LuceneIndexingExecutionEngine#refresh} can incorporate the segment into the
 * shared {@link LuceneCommitter} writer via {@code addIndexes}.
 *
 * Row ID invariant: Each document must have a {@code __row_id__} field set via
 * {@link LuceneDocumentInput#setRowId}. The row ID must equal the Lucene doc ID
 * (0-based sequential within this writer). This is asserted during flush to ensure
 * the 1:1 offset correspondence with Parquet.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneWriter implements Writer<LuceneDocumentInput> {

    private static final Logger logger = LogManager.getLogger(LuceneWriter.class);

    /** Segment info attribute key storing the writer generation for post-addIndexes correlation. */
    public static final String WRITER_GENERATION_ATTRIBUTE = "writer_generation";

    /** Large RAM buffer to avoid intermediate segment flushes within a single writer. */
    private static final double RAM_BUFFER_SIZE_MB = 256.0;

    private final long writerGeneration;
    private final LuceneDataFormat dataFormat;
    private final Path tempDirectory;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private volatile long docCount;
    private volatile boolean flushed;

    /**
     * Creates a new LuceneWriter for the given generation.
     *
     * @param writerGeneration the writer generation number
     * @param dataFormat       the Lucene data format descriptor
     * @param baseDirectory    the base directory under which to create the temp directory
     * @param analyzer         the analyzer to use for tokenized fields, or null for default
     * @param codec            the codec to use, or null for default
     * @param indexSort        the index sort to apply to segments, or null for no sort
     * @throws IOException if directory creation or IndexWriter opening fails
     */
    public LuceneWriter(
        long writerGeneration,
        LuceneDataFormat dataFormat,
        Path baseDirectory,
        Analyzer analyzer,
        Codec codec,
        Sort indexSort
    ) throws IOException {
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.docCount = 0;

        // Create an isolated temp directory for this writer's segment
        this.tempDirectory = baseDirectory.resolve("lucene_gen_" + writerGeneration);
        logger.info("Creating directory for temp lucene writer: " + tempDirectory);
        Files.createDirectories(tempDirectory);
        this.directory = new MMapDirectory(tempDirectory);

        IndexWriterConfig iwc = analyzer != null ? new IndexWriterConfig(analyzer) : new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB);
        if (indexSort != null) {
            iwc.setIndexSort(indexSort);
        }

        iwc.setCodec(new LuceneWriterCodec(codec, writerGeneration));
        this.indexWriter = new IndexWriter(directory, iwc);
    }

    /**
     * Adds a document to this writer's isolated IndexWriter.
     * The document is obtained from the input's {@link LuceneDocumentInput#getFinalInput()}.
     *
     * @param input the document input containing the Lucene document to index
     * @return a success result containing the current doc ID (0-based sequential)
     * @throws IOException if the underlying IndexWriter fails to add the document
     */
    @Override
    public WriteResult addDoc(LuceneDocumentInput input) throws IOException {
        Document doc = input.getFinalInput();
        assert doc.getField(LuceneDocumentInput.ROW_ID_FIELD) != null : "Document missing required "
            + LuceneDocumentInput.ROW_ID_FIELD
            + " field at doc position "
            + docCount;
        assert doc.getField(LuceneDocumentInput.ROW_ID_FIELD).numericValue().longValue() == docCount : "Row ID mismatch: expected "
            + docCount
            + " but got "
            + doc.getField(LuceneDocumentInput.ROW_ID_FIELD).numericValue().longValue();
        indexWriter.addDocument(doc);
        long currentDocId = docCount;
        docCount++;
        return new WriteResult.Success(1L, 1L, currentDocId);
    }

    /**
     * Rolls back the last document by soft-deleting it via a point query on the row ID field.
     * The deleted doc is physically removed on {@link #flush()} when {@code forceMerge(1)} runs.
     *
     * @throws IOException if the delete operation fails
     * @throws IllegalStateException if no documents have been added
     */
    @Override
    public void rollbackLastDoc() throws IOException {
        if (docCount == 0) {
            throw new IllegalStateException("Cannot rollback: no documents have been added");
        }
        long lastRowId = docCount - 1;
        indexWriter.deleteDocuments(NumericDocValuesField.newSlowExactQuery(DocumentInput.ROW_ID_FIELD, lastRowId));
        docCount--;
    }

    /**
     * Force-merges all buffered documents into exactly one segment, commits the IndexWriter,
     * and returns a {@link FileInfos} describing the resulting segment files in the temp directory.
     * <p>
     * After flush, the IndexWriter and Directory are closed. The temp directory files remain
     * on disk for {@link LuceneIndexingExecutionEngine#refresh} to incorporate via
     * {@code addIndexes}.
     *
     * @return file infos containing the temp directory path and segment file names,
     *         or {@link FileInfos#empty()} if no documents were added
     * @throws IOException if force merge, commit, or file listing fails
     */
    @Override
    public FileInfos flush() throws IOException {
        if (docCount == 0) {
            return FileInfos.empty();
        }

        // Force merge to exactly 1 segment to maintain 1:1 mapping with other formats.
        indexWriter.forceMerge(1, true);
        indexWriter.commit();

        // Verify the invariant: exactly 1 segment with docCount documents
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        assert segmentInfos.size() == 1 : "Expected exactly 1 segment after force merge, got " + segmentInfos.size();

        SegmentCommitInfo segmentInfo = segmentInfos.info(0);
        assert segmentInfo.info.maxDoc() == docCount : "Expected " + docCount + " docs in segment, got " + segmentInfo.info.maxDoc();

        // Build the WriterFileSet pointing to the temp directory
        WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
            .directory(tempDirectory)
            .writerGeneration(writerGeneration)
            .addNumRows(docCount);

        // Add all files in the segment
        for (String file : directory.listAll()) {
            if (file.startsWith("segments") == false && file.equals("write.lock") == false) {
                wfsBuilder.addFile(file);
            }
        }

        // Since flush is once only, close the IndexWriter but keep directory open for close()
        indexWriter.close();
        directory.close();
        flushed = true;

        return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
    }

    /**
     * Syncs all files in the temp directory to durable storage.
     *
     * @throws IOException if the sync fails
     */
    @Override
    public void sync() throws IOException {
        directory.sync(Arrays.asList(directory.listAll()));
        directory.syncMetaData();
    }

    /** {@inheritDoc} Returns the writer generation number assigned at construction. */
    @Override
    public long generation() {
        return writerGeneration;
    }

    /**
     * Closes this writer, rolling back the IndexWriter if still open, closing the directory,
     * and deleting the temp directory. Safe to call multiple times.
     *
     * @throws IOException if cleanup fails
     */
    @Override
    public void close() throws IOException {
        // Close the IndexWriter and Directory if they haven't been closed by flush()
        try {
            if (indexWriter.isOpen()) {
                indexWriter.rollback();
            }
        } catch (Exception e) {
            logger.warn("Failed to rollback IndexWriter for generation[{}]: {}", writerGeneration, e);
        }
        try {
            directory.close();
        } catch (Exception e) {
            logger.warn("Failed to close directory for generation[{}]: {}", writerGeneration, e);
        }
        if (flushed == false) {
            IOUtils.rm(tempDirectory);
        }
    }
}
