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
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.DeleteInput;
import org.opensearch.index.engine.dataformat.DeleteResult;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.WriteResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
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

    private final long writerGeneration;
    private final LuceneDataFormat dataFormat;
    private final Path tempDirectory;
    private final Directory directory;
    private final IndexWriter indexWriter;
    private long mappingVersion;
    private volatile long docCount;

    /**
     * Creates a new LuceneWriter for the given generation.
     *
     * @param writerGeneration the writer generation number
     * @param mappingVersion   the initial mapping version
     * @param dataFormat       the Lucene data format descriptor
     * @param baseDirectory    the base directory under which to create the temp directory
     * @param analyzer         the analyzer to use for tokenized fields, or null for default
     * @param codec            the codec to use, or null for default
     * @param indexSort        the index sort to apply (null when Lucene is secondary format)
     * @throws IOException if directory creation or IndexWriter opening fails
     */
    public LuceneWriter(
        long writerGeneration,
        long mappingVersion,
        LuceneDataFormat dataFormat,
        Path baseDirectory,
        Analyzer analyzer,
        Codec codec,
        Sort indexSort
    ) throws IOException {
        this.writerGeneration = writerGeneration;
        this.mappingVersion = mappingVersion;
        this.dataFormat = dataFormat;
        this.docCount = 0;

        // Create an isolated temp directory for this writer's segment
        this.tempDirectory = baseDirectory.resolve("lucene_gen_" + writerGeneration);
        logger.info("Creating directory for temp lucene writer: " + tempDirectory);
        Files.createDirectory(tempDirectory);
        this.directory = new MMapDirectory(tempDirectory);

        IndexWriterConfig iwc = analyzer != null ? new IndexWriterConfig(analyzer) : new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setRAMBufferSizeMB(RAM_BUFFER_SIZE_MB);
        // When Lucene is primary, apply the customer's IndexSort so segments
        // are natively sorted and compatible with the shared writer's IndexSort.
        // When Lucene is secondary, no IndexSort — reorder is done via
        // ReorderingOneMerge.reorder() in configureSortedMerge().
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

        // If sort permutation is provided, configure the reorder merge policy
        if (flushInput.hasRowIdMapping()) {
            configureSortedMerge(flushInput.rowIdMapping());
        }

        // Common path: forceMerge to 1 segment, commit, build FileInfos
        indexWriter.forceMerge(1, true);
        indexWriter.commit();

        // Close the IndexWriter before rewriting segment metadata.
        // This prevents IndexFileDeleter from removing our rewritten segments_N
        // file (which it wouldn't recognize as its own commit).
        indexWriter.close();

        // Verify the invariant: exactly 1 segment with docCount documents
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        assert segmentInfos.size() == 1 : "Expected exactly 1 segment after force merge, got " + segmentInfos.size();

        SegmentCommitInfo segmentInfo = segmentInfos.info(0);
        assert segmentInfo.info.maxDoc() == docCount : "Expected " + docCount + " docs in segment, got " + segmentInfo.info.maxDoc();

        // Stamp the IndexSort on the segment metadata post-commit so that
        // addIndexes(Directory...) on the shared writer sees matching sort.
        // The segment is always sorted by __row_id__ — either naturally (docs
        // written sequentially) or via OneMerge.reorder() + row ID rewrite.
        if (segmentInfo.info.getIndexSort() == null) {
            rewriteSegmentInfoWithSort(segmentInfos, segmentInfo);
        }

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


        return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
    }

    /**
     * Configures the child writer for sorted flush: sets a ReorderingMergePolicy
     * that physically reorders docs via OneMerge.reorder(), and enables sequential
     * __row_id__ rewrite on the codec so the merge writes 0..N in one pass.
     */
    private void configureSortedMerge(RowIdMapping mapping) {
        indexWriter.getConfig().setMergePolicy(new ReorderingMergePolicy(mapping));
        Codec currentCodec = indexWriter.getConfig().getCodec();
        if (currentCodec instanceof LuceneWriterCodec lwc) {
            lwc.enableRowIdRewrite();
        }
    }

    /**
     * Rewrites the segment's .si file and segments_N commit to declare the IndexSort.
     * <p>
     * After the child writer commits, the segment on disk has no IndexSort metadata
     * (because the writer operates without IndexSort to allow OneMerge.reorder()).
     * However, the segment is logically sorted by __row_id__ (either naturally sequential
     * or via reorder + row ID rewrite). This method reconstructs the SegmentInfo with
     * the expected sort, rewrites the .si file, and re-commits the SegmentInfos so that
     * addIndexes(Directory...) on the shared writer sees matching sort metadata.
     *
     * @param segmentInfos the current committed SegmentInfos
     * @param segmentCommitInfo the single segment's commit info
     * @throws IOException if rewriting fails
     */
    private void rewriteSegmentInfoWithSort(SegmentInfos segmentInfos, SegmentCommitInfo segmentCommitInfo) throws IOException {
        SegmentInfo originalInfo = segmentCommitInfo.info;
        Sort sort = new Sort(new SortedNumericSortField(LuceneDocumentInput.ROW_ID_FIELD, SortField.Type.LONG));

        // Reconstruct SegmentInfo with the IndexSort declared
        SegmentInfo sortedInfo = new SegmentInfo(
            originalInfo.dir,
            originalInfo.getVersion(),
            originalInfo.getMinVersion(),
            originalInfo.name,
            originalInfo.maxDoc(),
            originalInfo.getUseCompoundFile(),
            originalInfo.getHasBlocks(),
            originalInfo.getCodec(),
            originalInfo.getDiagnostics(),
            originalInfo.getId(),
            originalInfo.getAttributes(),
            sort
        );
        sortedInfo.setFiles(originalInfo.files());

        // Delete the existing .si file before rewriting — Lucene's createOutput
        // does not overwrite existing files.
        String siFileName = originalInfo.name + ".si";
        directory.deleteFile(siFileName);

        // Rewrite the .si file with sort metadata
        originalInfo.getCodec().segmentInfoFormat().write(directory, sortedInfo, IOContext.DEFAULT);

        // Replace the segment in SegmentInfos and re-commit so segments_N is consistent
        SegmentCommitInfo newCommitInfo = new SegmentCommitInfo(
            sortedInfo,
            segmentCommitInfo.getDelCount(),
            segmentCommitInfo.getSoftDelCount(),
            segmentCommitInfo.getDelGen(),
            segmentCommitInfo.getFieldInfosGen(),
            segmentCommitInfo.getDocValuesGen(),
            segmentCommitInfo.getId()
        );
        segmentInfos.clear();
        segmentInfos.add(newCommitInfo);
        segmentInfos.commit(directory);
    }

    /**
     * MergePolicy that wraps the standard merge selection but returns
     * ReorderingOneMerge instances that override reorder() with our DocMap.
     */
    static class ReorderingMergePolicy extends MergePolicy {
        private final RowIdMapping mapping;
        private volatile boolean reorderDone = false;

        ReorderingMergePolicy(RowIdMapping mapping) {
            this.mapping = mapping;
        }

        @Override
        public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
            return null; // no automatic merges
        }

        @Override
        public MergeSpecification findForcedMerges(SegmentInfos segmentInfos, int maxSegmentCount, Map<SegmentCommitInfo, Boolean> segmentsToMerge, MergeContext mergeContext) {
            if (reorderDone) {
                return null; // already reordered, stop the loop
            }
            reorderDone = true;

            List<SegmentCommitInfo> segments = new ArrayList<>();
            for (int i = 0; i < segmentInfos.size(); i++) {
                segments.add(segmentInfos.info(i));
            }
            if (segments.isEmpty()) {
                return null;
            }
            MergeSpecification spec = new MergeSpecification();
            spec.add(new ReorderingOneMerge(segments, mapping));
            return spec;
        }

        @Override
        public MergeSpecification findForcedDeletesMerges(SegmentInfos segmentInfos, MergeContext mergeContext) {
            return null;
        }
    }

    /**
     * Custom OneMerge that overrides {@code reorder()} to provide the sort permutation
     * as a {@link Sorter.DocMap}. This causes Lucene to physically reorder docs during
     * the merge according to the Parquet sort order.
     */
    static class ReorderingOneMerge extends MergePolicy.OneMerge {
        private final RowIdMapping mapping;

        ReorderingOneMerge(List<SegmentCommitInfo> segments, RowIdMapping mapping) {
            super(segments);
            this.mapping = mapping;
        }

        @Override
        public Sorter.DocMap reorder(CodecReader reader, Directory dir, Executor executor) throws IOException {
            return new Sorter.DocMap() {
                @Override
                public int oldToNew(int docID) {
                    return mapping.oldToNew(docID);
                }

                @Override
                public int newToOld(int docID) {
                    return mapping.newToOld(docID);
                }

                @Override
                public int size() {
                    return mapping.size();
                }
            };
        }

        @Override
        public void setMergeInfo(SegmentCommitInfo info) {
            super.setMergeInfo(info);
            if (info != null) {
                info.info.putAttribute(WRITER_GENERATION_ATTRIBUTE, String.valueOf(0));
            }
        }
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

    @Override
    public boolean isSchemaMutable() {
        return true;
    }

    @Override
    public long mappingVersion() {
        return mappingVersion;
    }

    @Override
    public void updateMappingVersion(long newVersion) {
        if (newVersion > this.mappingVersion) {
            this.mappingVersion = newVersion;
        }
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
        IOUtils.rm(tempDirectory);
    }

    /**
     * Deletes all documents containing the given term from this writer's {@link IndexWriter}.
     *
     * @param deleteInput the {@code _id} term identifying the document(s) to delete
     * @return the result of the delete operation
     * @throws IOException if a low-level I/O error occurs
     */
    @Override
    public DeleteResult deleteDocument(DeleteInput deleteInput) throws IOException {
        Term uid = new Term(deleteInput.fieldName(), deleteInput.value());
        indexWriter.deleteDocuments(uid);
        return new DeleteResult.Success(1L, 1L, 1L);
    }

    @Override
    public Optional<Writer<?>> getWriterForFormat(String formatName) {
        if (LuceneDataFormat.LUCENE_FORMAT_NAME.equals(formatName)) {
            return Optional.of(this);
        }
        return Optional.empty();
    }
}
