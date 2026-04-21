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
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.IdentityHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Per-generation Lucene writer that creates segments in an isolated temporary directory.
 *
 * Each instance owns its own {@link IndexWriter} and {@link Directory}. Documents are
 * added via {@link #addDoc(LuceneDocumentInput)}, and on {@link #flush(FlushInput)}, the writer
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

    private static final String ROW_ID = "___row_id";

    private final long writerGeneration;
    private final LuceneDataFormat dataFormat;
    private final Path tempDirectory;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private final ReentrantLock lock;
    private final Codec codec;
    private volatile long docCount;

    /**
     * Creates a new LuceneWriter for the given generation.
     *
     * @param writerGeneration the writer generation number
     * @param dataFormat       the Lucene data format descriptor
     * @param baseDirectory    the base directory under which to create the temp directory
     * @param analyzer         the analyzer to use for tokenized fields, or null for default
     * @throws IOException if directory creation or IndexWriter opening fails
     */
    public LuceneWriter(long writerGeneration, LuceneDataFormat dataFormat, Path baseDirectory, Analyzer analyzer, Codec codec)
        throws IOException {
        this.writerGeneration = writerGeneration;
        this.dataFormat = dataFormat;
        this.lock = new ReentrantLock();
        this.docCount = 0;
        this.codec = codec;

        // Create an isolated temp directory for this writer's segment
        this.tempDirectory = baseDirectory.resolve("lucene_gen_" + writerGeneration);
        logger.info("Creating directory for temp lucene writer: " + tempDirectory);
        Files.createDirectory(tempDirectory);
        this.directory = new MMapDirectory(tempDirectory);

        IndexWriterConfig iwc = analyzer != null ? new IndexWriterConfig(analyzer) : new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB);

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
        indexWriter.addDocument(doc);
        long currentDocId = docCount;
        docCount++;
        return new WriteResult.Success(1L, 1L, currentDocId);
    }

    /**
     * Force-merges all buffered documents into exactly one segment, commits the IndexWriter,
     * and returns a {@link FileInfos} describing the resulting segment files in the temp directory.
     * <p>
     * After flush, the IndexWriter and Directory are closed. The temp directory files remain
     * on disk for {@link LuceneIndexingExecutionEngine#refresh} to incorporate via
     * {@code addIndexes}.
     *
     * <p>If the {@link FlushInput} carries a sort permutation from the primary data format
     * (e.g., Parquet sort-on-close), the Lucene segment is reordered using Lucene's IndexSort
     * mechanism with a custom SortField that remaps {@code ___row_id} values through the
     * permutation. This ensures the Lucene doc order matches the sorted Parquet row order.
     *
     * @param flushInput optional context; if it carries a sort permutation, the segment is sorted
     * @return file infos containing the temp directory path and segment file names,
     *         or {@link FileInfos#empty()} if no documents were added
     * @throws IOException if force merge, commit, or file listing fails
     */
    @Override
    public FileInfos flush(FlushInput flushInput) throws IOException {
        if (docCount == 0) {
            return FileInfos.empty();
        }

        if (flushInput.hasSortPermutation()) {
            return flushSorted(flushInput.sortPermutation());
        }
        return flushUnsorted();
    }

    /**
     * Standard unsorted flush: force merge to 1 segment, commit, return file infos.
     */
    private FileInfos flushUnsorted() throws IOException {
        indexWriter.forceMerge(1, true);
        indexWriter.commit();

        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        assert segmentInfos.size() == 1 : "Expected exactly 1 segment after force merge, got " + segmentInfos.size();

        SegmentCommitInfo segmentInfo = segmentInfos.info(0);
        assert segmentInfo.info.maxDoc() == docCount : "Expected " + docCount + " docs in segment, got " + segmentInfo.info.maxDoc();

        WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
            .directory(tempDirectory)
            .writerGeneration(writerGeneration)
            .addNumRows(docCount);

        for (String file : directory.listAll()) {
            if (file.startsWith("segments") == false && file.equals("write.lock") == false) {
                wfsBuilder.addFile(file);
            }
        }

        indexWriter.close();
        directory.close();

        return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
    }

    /**
     * Sorted flush using the sort permutation from the primary data format.
     * <ol>
     *   <li>Force merge + commit the unsorted segment</li>
     *   <li>Open a DirectoryReader on the committed segment</li>
     *   <li>Create a new IndexWriter with a custom IndexSort that remaps ___row_id</li>
     *   <li>Use addIndexes(CodecReader...) to copy the segment with the sort applied</li>
     *   <li>Return the sorted segment's file infos</li>
     * </ol>
     */
    private FileInfos flushSorted(long[][] sortPermutation) throws IOException {
        // Step 1: Force merge + commit the unsorted segment
        indexWriter.forceMerge(1, true);
        indexWriter.commit();
        indexWriter.close();

        // Step 2: Open a reader on the unsorted segment
        Path sortedDir = tempDirectory.resolveSibling(tempDirectory.getFileName() + "_sorted");
        Files.createDirectory(sortedDir);

        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            // Build the old_row_id → new_row_id lookup array
            long[] oldRowIds = sortPermutation[0];
            long[] newRowIds = sortPermutation[1];
            long maxOldId = 0;
            for (long id : oldRowIds) {
                if (id > maxOldId) maxOldId = id;
            }
            long[] oldToNew = new long[(int) maxOldId + 1];
            Arrays.fill(oldToNew, -1);
            for (int i = 0; i < oldRowIds.length; i++) {
                oldToNew[(int) oldRowIds[i]] = newRowIds[i];
            }

            // Step 3: Build CodecReaders with remapped ___row_id doc values
            IdentityHashMap<LeafReader, String> readerToFileId = new IdentityHashMap<>();
            CodecReader[] codecReaders = new CodecReader[reader.leaves().size()];
            for (int i = 0; i < reader.leaves().size(); i++) {
                LeafReaderContext ctx = reader.leaves().get(i);
                LeafReader leaf = ctx.reader();
                String fileId = String.valueOf(writerGeneration);
                readerToFileId.put(leaf, fileId);
                codecReaders[i] = new RowIdRemappingCodecReader((CodecReader) leaf, oldToNew);
                readerToFileId.put(codecReaders[i], fileId);
            }

            // Step 4: Create a new IndexWriter with IndexSort on remapped ___row_id
            RowIdMappingSortField sortField = new RowIdMappingSortField(ROW_ID, oldToNew, readerToFileId);

            try (Directory sortedDirectory = new MMapDirectory(sortedDir)) {
                IndexWriterConfig iwc = new IndexWriterConfig();
                iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
                iwc.setMergeScheduler(new SerialMergeScheduler());
                iwc.setIndexSort(new Sort(sortField));
                iwc.setCodec(new LuceneWriterCodec(this.codec, writerGeneration));

                try (IndexWriter sortedWriter = new IndexWriter(sortedDirectory, iwc)) {
                    sortedWriter.addIndexes(codecReaders);
                    sortedWriter.commit();
                }

                // Step 5: Build FileInfos from the sorted directory
                WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
                    .directory(sortedDir)
                    .writerGeneration(writerGeneration)
                    .addNumRows(docCount);

                for (String file : sortedDirectory.listAll()) {
                    if (file.startsWith("segments") == false && file.equals("write.lock") == false) {
                        wfsBuilder.addFile(file);
                    }
                }

                return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
            }
        } finally {
            directory.close();
        }
    }

    /**
     * Custom SortedNumericSortField that remaps ___row_id values through the sort permutation
     * during Lucene's IndexSort merge. This causes Lucene to reorder documents according to
     * the Parquet sort order.
     */
    static class RowIdMappingSortField extends SortedNumericSortField {
        private final long[] oldToNew;
        private final IdentityHashMap<LeafReader, String> readerToFileId;

        RowIdMappingSortField(String field, long[] oldToNew, IdentityHashMap<LeafReader, String> readerToFileId) {
            super(field, SortField.Type.LONG);
            this.oldToNew = oldToNew;
            this.readerToFileId = readerToFileId;
        }

        @Override
        public IndexSorter getIndexSorter() {
            return new IndexSorter.LongSorter(
                SortedNumericSortField.Provider.NAME,
                (Long) getMissingValue(),
                getReverse(),
                reader -> new RemappedNumericDocValues(reader, ROW_ID, oldToNew)
            );
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj instanceof SortedNumericSortField == false) return false;
            SortedNumericSortField other = (SortedNumericSortField) obj;
            return getField().equals(other.getField())
                && getType() == other.getType()
                && getReverse() == other.getReverse();
        }

        @Override
        public int hashCode() {
            return super.hashCode();
        }
    }

    /**
     * NumericDocValues that reads original ___row_id and returns the remapped value.
     */
    static class RemappedNumericDocValues extends NumericDocValues {
        private final SortedNumericDocValues delegate;
        private final long[] oldToNew;

        RemappedNumericDocValues(LeafReader reader, String field, long[] oldToNew) throws IOException {
            this.delegate = reader.getSortedNumericDocValues(field);
            this.oldToNew = oldToNew;
        }

        @Override
        public long longValue() throws IOException {
            long originalRowId = delegate.nextValue();
            if (originalRowId >= 0 && originalRowId < oldToNew.length && oldToNew[(int) originalRowId] >= 0) {
                return oldToNew[(int) originalRowId];
            }
            return originalRowId;
        }

        @Override public boolean advanceExact(int target) throws IOException { return delegate.advanceExact(target); }
        @Override public int docID() { return delegate.docID(); }
        @Override public int nextDoc() throws IOException { return delegate.nextDoc(); }
        @Override public int advance(int target) throws IOException { return delegate.advance(target); }
        @Override public long cost() { return delegate.cost(); }
    }

    /**
     * Wraps a CodecReader to replace ___row_id SortedNumericDocValues with
     * remapped values from the sort permutation.
     */
    static class RowIdRemappingCodecReader extends FilterCodecReader {
        private final long[] oldToNew;

        RowIdRemappingCodecReader(CodecReader in, long[] oldToNew) {
            super(in);
            this.oldToNew = oldToNew;
        }

        @Override
        public DocValuesProducer getDocValuesReader() {
            DocValuesProducer delegate = in.getDocValuesReader();
            if (delegate == null) return null;
            return new RowIdRemappingDocValuesProducer(delegate, oldToNew, in.maxDoc());
        }

        @Override public CacheHelper getCoreCacheHelper() { return in.getCoreCacheHelper(); }
        @Override public CacheHelper getReaderCacheHelper() { return in.getReaderCacheHelper(); }
    }

    /**
     * DocValuesProducer that intercepts ___row_id and returns remapped values.
     */
    static class RowIdRemappingDocValuesProducer extends DocValuesProducer {
        private final DocValuesProducer delegate;
        private final long[] oldToNew;
        private final int maxDoc;

        RowIdRemappingDocValuesProducer(DocValuesProducer delegate, long[] oldToNew, int maxDoc) {
            this.delegate = delegate;
            this.oldToNew = oldToNew;
            this.maxDoc = maxDoc;
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) throws IOException {
            return delegate.getNumeric(field);
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            if (ROW_ID.equals(field.name)) {
                return new SortedNumericDocValues() {
                    private int docID = -1;

                    @Override
                    public long nextValue() {
                        // docID is the original position; return the remapped new_row_id
                        if (docID >= 0 && docID < oldToNew.length && oldToNew[docID] >= 0) {
                            return oldToNew[docID];
                        }
                        return docID;
                    }

                    @Override public int docValueCount() { return 1; }
                    @Override public boolean advanceExact(int target) { docID = target; return true; }
                    @Override public int docID() { return docID; }
                    @Override public int nextDoc() { return ++docID < maxDoc ? docID : NO_MORE_DOCS; }
                    @Override public int advance(int target) { docID = target; return docID < maxDoc ? docID : NO_MORE_DOCS; }
                    @Override public long cost() { return maxDoc; }
                };
            }
            return delegate.getSortedNumeric(field);
        }

        @Override public BinaryDocValues getBinary(FieldInfo field) throws IOException { return delegate.getBinary(field); }
        @Override public SortedDocValues getSorted(FieldInfo field) throws IOException { return delegate.getSorted(field); }
        @Override public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException { return delegate.getSortedSet(field); }
        @Override public DocValuesSkipper getSkipper(FieldInfo field) throws IOException { return delegate.getSkipper(field); }
        @Override public void checkIntegrity() throws IOException { delegate.checkIntegrity(); }
        @Override public void close() throws IOException { delegate.close(); }
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

    /** Acquires the writer's reentrant lock. Used by the writer pool to serialize access. */
    @Override
    public void lock() {
        lock.lock();
    }

    /** Attempts to acquire the writer's reentrant lock without blocking. */
    @Override
    public boolean tryLock() {
        return lock.tryLock();
    }

    /** Releases the writer's reentrant lock. */
    @Override
    public void unlock() {
        lock.unlock();
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
            // Best effort — may already be closed
        }
        try {
            directory.close();
        } catch (Exception e) {
            // Best effort — may already be closed
        }
        IOUtils.rm(tempDirectory);
        // Also clean up the sorted directory if it was created by flushWithSortPermutation
        Path sortedDir = tempDirectory.resolveSibling(tempDirectory.getFileName() + "_sorted");
        if (Files.exists(sortedDir)) {
            IOUtils.rm(sortedDir);
        }
    }
}
