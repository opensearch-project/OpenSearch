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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.unit.TimeValue;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

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

    /** Large RAM buffer to avoid intermediate segment flushes within a single writer in production. */
    private static final double RAM_BUFFER_SIZE_MB = 256.0;

    private final long writerGeneration;
    private final LuceneDataFormat dataFormat;
    private final LuceneShardStatsTracker stats;
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
     * @param stats            the shard-level stats collector
     * @throws IOException if directory creation or IndexWriter opening fails
     */
    public LuceneWriter(
        long writerGeneration,
        long mappingVersion,
        LuceneDataFormat dataFormat,
        Path baseDirectory,
        Analyzer analyzer,
        Codec codec,
        Sort indexSort,
        LuceneShardStatsTracker stats
    ) throws IOException {
        this.writerGeneration = writerGeneration;
        this.mappingVersion = mappingVersion;
        this.dataFormat = dataFormat;
        this.stats = stats;
        this.docCount = 0;

        // Create an isolated temp directory for this writer's segment
        this.tempDirectory = baseDirectory.resolve("lucene_gen_" + writerGeneration);
        logger.info("Creating directory for temp lucene writer: " + tempDirectory);
        Files.createDirectory(tempDirectory);
        this.directory = new MMapDirectory(tempDirectory);

        IndexWriterConfig iwc = analyzer != null ? new IndexWriterConfig(analyzer) : new IndexWriterConfig();
        iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        // Two flush triggers wired in via overridable hooks:
        // - ramBufferSizeMB(): RAM-buffer based flushing. Default 256 MB so production
        // accumulates docs into a single in-memory segment before flush.
        // - maxBufferedDocs(): doc-count based flushing. Default DISABLE_AUTO_FLUSH so
        // production never flushes by doc count. Tests that need deterministic
        // multi-segment paths override ramBufferSizeMB() to Double.MAX_VALUE and
        // maxBufferedDocs() to a small value (e.g. 20) to force exact spill points
        // independent of JVM/Lucene version.
        iwc.setRAMBufferSizeMB(ramBufferSizeMB());
        iwc.setMaxBufferedDocs(maxBufferedDocs());
        if (indexSort != null) {
            iwc.setMergePolicy(new LogByteSizeMergePolicy());
            iwc.setIndexSort(indexSort);
        } else {
            // We are taking control here hence not allowing any merge to happen automatically.
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        }
        iwc.setCodec(new LuceneWriterCodec(codec, writerGeneration));
        this.indexWriter = new IndexWriter(directory, iwc);
    }

    /**
     * Hook for tests to override the IndexWriter RAM buffer size. Default is
     * {@link #RAM_BUFFER_SIZE_MB}, large enough that production accumulates all
     * docs into a single in-memory segment before flush.
     *
     * <p>For deterministic multi-segment tests, override this to a very large
     * value (e.g. {@code 1024.0}) together with a small
     * {@link #maxBufferedDocs()} value, so doc count alone drives spilling
     * independent of JVM/Lucene version and per-doc encoding size.
     *
     * <p>This method is invoked from the constructor, so overrides must be
     * stateless (return a constant) — they cannot depend on any instance fields.
     */
    double ramBufferSizeMB() {
        return RAM_BUFFER_SIZE_MB;
    }

    /**
     * Hook for tests to override the IndexWriter max-buffered-docs threshold.
     * Default is {@link IndexWriterConfig#DISABLE_AUTO_FLUSH}, so production
     * never flushes based on doc count and relies on {@link #ramBufferSizeMB()}.
     *
     * <p>Tests exercising the multi-segment merge path should override
     * {@link #ramBufferSizeMB()} to a very large value (e.g. {@code 1024.0})
     * and return a small value (e.g. 20) here to force exact, version-independent
     * spill points.
     *
     * <p>This method is invoked from the constructor, so overrides must be
     * stateless (return a constant) — they cannot depend on any instance fields.
     */
    int maxBufferedDocs() {
        return IndexWriterConfig.DISABLE_AUTO_FLUSH;
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
        long start = System.nanoTime();
        try {
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
            stats.addDocsIndexed(1);
            return new WriteResult.Success(1L, 1L, currentDocId);
        } catch (IOException e) {
            stats.incDocsIndexedFailures();
            throw e;
        } finally {
            stats.addIndexTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
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

        long flushStart = System.nanoTime();
        try {
            long flushStartNanos = System.nanoTime();
            logger.info(
                "flush: START generation={}, docCount={}, hasRowIdMapping={}",
                writerGeneration,
                docCount,
                flushInput.hasRowIdMapping()
            );
            indexWriter.flush();

            // If sort permutation is provided, configure the reorder merge policy
            if (flushInput.hasRowIdMapping()) {
                // RowIdMapping shouldn't be available if index has sort configurations.
                Sort configuredIndexSort = indexWriter.getConfig().getIndexSort();
                if (configuredIndexSort != null) {
                    throw new IllegalStateException(
                        "RowIdMapping should not be available when child IndexWriter is configured with IndexSort ["
                            + configuredIndexSort
                            + "] for writer generation ["
                            + writerGeneration
                            + "]"
                    );
                }
                RowIdMapping mapping = flushInput.rowIdMapping();
                if (mapping.size() != docCount) {
                    throw new IllegalStateException(
                        "RowIdMapping size ["
                            + mapping.size()
                            + "] does not match document count ["
                            + docCount
                            + "] for writer generation ["
                            + writerGeneration
                            + "]"
                    );
                }
                configureSortedMerge(mapping);
            } else if (indexWriter.getConfig().getIndexSort() != null) {
                // Lucene is primary with IndexSort: Lucene natively reorders docs during
                // forceMerge, but __row_id__ values were assigned at insertion time and will
                // be scrambled after sort. Force a merge so the codec rewrites row IDs to
                // sequential 0..N-1 in the final doc order.
                if (flushInput.hasRowIdMapping()) {
                    throw new IllegalStateException(
                        "RowIdMapping must not be provided when IndexSort is configured for writer generation [" + writerGeneration + "]"
                    );
                }
            }

            // Common path: forceMerge to 1 segment, commit, build FileInfos
            long forceMergeStartNanos = System.nanoTime();
            long forceMergeStart = System.nanoTime();
            try {
                indexWriter.forceMerge(1, true);
            } finally {
                stats.addFlushForceMergeTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - forceMergeStart));
            }
            long forceMergeDurationMs = TimeValue.nsecToMSec(System.nanoTime() - forceMergeStartNanos);
            logger.info(
                "flush: forceMerge complete: generation={}, docCount={}, duration={}ms",
                writerGeneration,
                docCount,
                forceMergeDurationMs
            );

            long commitStartNanos = System.nanoTime();
            indexWriter.commit();
            long commitDurationMs = TimeValue.nsecToMSec(System.nanoTime() - commitStartNanos);
            logger.info("flush: commit complete: generation={}, duration={}ms", writerGeneration, commitDurationMs);

            // Close the IndexWriter before rewriting segment metadata.
            // This prevents IndexFileDeleter from removing our rewritten segments_N
            // file (which it wouldn't recognize as its own commit).
            indexWriter.close();

            // Verify the invariant: exactly 1 segment with docCount documents
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
            assert segmentInfos.size() == 1 : "Expected exactly 1 segment after force merge, got " + segmentInfos.size();

            SegmentCommitInfo segmentInfo = segmentInfos.info(0);
            assert segmentInfo.info.maxDoc() == docCount : "Expected " + docCount + " docs in segment, got " + segmentInfo.info.maxDoc();

            // Invariant: ___row_id__ doc values must be sequential 0..maxDoc-1 after forceMerge.
            // This holds in all cases:
            // - Lucene secondary: docs reordered via OneMerge.reorder() + row ID rewrite
            // - Lucene primary with IndexSort: Lucene sorts natively + row ID rewrite
            // - No sort: docs added sequentially, row IDs naturally sequential
            // Wrapped in `assert` so the I/O cost is paid only when assertions are enabled.
            assert assertRowIdsSequential(directory) : "___row_id__ doc values not sequential after forceMerge for writer generation ["
                + writerGeneration
                + "]";

            // Stamp the IndexSort on the segment metadata post-commit so that
            // addIndexes(Directory...) on the shared writer sees matching sort.
            // The segment is always sorted by __row_id__ — either naturally (docs
            // written sequentially) or via OneMerge.reorder() + row ID rewrite.
            if (flushInput.hasRowIdMapping()) {
                logger.debug("Overriding segment info manually");
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

            long totalFlushDurationMs = TimeValue.nsecToMSec(System.nanoTime() - flushStartNanos);
            logger.info(
                "flush: DONE generation={}, totalRows={}, forceMerge={}ms, commit={}ms, total={}ms",
                writerGeneration,
                docCount,
                forceMergeDurationMs,
                commitDurationMs,
                totalFlushDurationMs
            );

            return FileInfos.builder().putWriterFileSet(dataFormat, wfsBuilder.build()).build();
        } finally {
            stats.incFlushTotal();
            stats.addFlushTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - flushStart));
        }
    }

    /**
     * Configures the child writer for sorted flush: sets a ReorderingMergePolicy
     * that forces a merge of all segments (with optional doc reordering via the mapping).
     * Row ID rewrite is already enabled on the codec at construction time.
     */
    private void configureSortedMerge(RowIdMapping mapping) {
        indexWriter.getConfig().setMergePolicy(new ReorderingMergePolicy(mapping));
        Codec currentCodec = indexWriter.getConfig().getCodec();
        if (currentCodec instanceof LuceneWriterCodec lwc) {
            lwc.enableRowIdRewrite();
        }
    }

    /**
     * Asserts that the {@code ___row_id} doc values in the freshly committed segment
     * are sequential 0..maxDoc-1 in doc order. This invariant must hold whether the
     * segment was produced naturally (docs added sequentially) or via reorder + row ID
     * rewrite during a sorted flush. It guards the 1:1 offset correspondence with the
     * Parquet file for the same writer generation.
     *
     * <p>This method is intended to be invoked from inside an {@code assert} statement
     * so the I/O cost is paid only when assertions are enabled (tests, dev, CI).
     *
     * @return {@code true} if all row IDs are sequential, {@code false} otherwise
     * @throws AssertionError with a descriptive message if I/O fails or the field is missing
     */
    private boolean assertRowIdsSequential(Directory directory) {
        try (DirectoryReader reader = DirectoryReader.open(directory)) {
            assert reader.leaves().size() == 1 : "Expected exactly 1 leaf reader, got " + reader.leaves().size();
            LeafReader leaf = reader.leaves().get(0).reader();
            SortedNumericDocValues rowIdDV = leaf.getSortedNumericDocValues(LuceneDocumentInput.ROW_ID_FIELD);
            if (rowIdDV == null) {
                throw new AssertionError(
                    "Field [" + LuceneDocumentInput.ROW_ID_FIELD + "] missing from segment for writer generation [" + writerGeneration + "]"
                );
            }
            int maxDoc = leaf.maxDoc();
            for (int docId = 0; docId < maxDoc; docId++) {
                if (rowIdDV.advanceExact(docId) == false) {
                    throw new AssertionError(
                        "Doc ["
                            + docId
                            + "] missing "
                            + LuceneDocumentInput.ROW_ID_FIELD
                            + " for writer generation ["
                            + writerGeneration
                            + "]"
                    );
                }
                long rowId = rowIdDV.nextValue();
                if (rowId != docId) {
                    throw new AssertionError(
                        "Non-sequential "
                            + LuceneDocumentInput.ROW_ID_FIELD
                            + " at docId="
                            + docId
                            + " (got "
                            + rowId
                            + ", expected "
                            + docId
                            + ") for writer generation ["
                            + writerGeneration
                            + "]"
                    );
                }
            }
            return true;
        } catch (IOException e) {
            throw new AssertionError("Failed to verify ___row_id__ invariant for writer generation [" + writerGeneration + "]", e);
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

        // Replace the segment in SegmentInfos and re-commit so segments_N is consistent.
        // The flush() caller has already asserted segmentInfos.size() == 1, so removing
        // the original SegmentCommitInfo expresses intent better than clear() + add().
        SegmentCommitInfo newCommitInfo = new SegmentCommitInfo(
            sortedInfo,
            segmentCommitInfo.getDelCount(),
            segmentCommitInfo.getSoftDelCount(),
            segmentCommitInfo.getDelGen(),
            segmentCommitInfo.getFieldInfosGen(),
            segmentCommitInfo.getDocValuesGen(),
            segmentCommitInfo.getId()
        );
        segmentInfos.remove(segmentCommitInfo);
        segmentInfos.add(newCommitInfo);
        segmentInfos.commit(directory);
    }

    /**
     * MergePolicy that unconditionally forces a merge of all segments on {@code forceMerge},
     * even if there is only one segment. When a {@link RowIdMapping} is provided, the merge
     * also reorders docs via {@link ReorderingOneMerge}. When mapping is null, a plain
     * {@code OneMerge} is used (the merge still rewrites the segment, enabling the codec's
     * row ID rewrite to fire).
     *
     * <p>This policy is used in two scenarios:
     * <ul>
     *   <li>Lucene secondary with sort permutation from Parquet: mapping is non-null,
     *       docs are reordered to match Parquet's sort order.</li>
     *   <li>Lucene primary with IndexSort: mapping is null, Lucene's native sort already
     *       reordered docs, but the merge is needed to trigger the codec's row ID rewrite.</li>
     * </ul>
     */
    static class ReorderingMergePolicy extends MergePolicy {
        private final RowIdMapping mapping; // nullable
        private volatile boolean mergeDone = false;

        /**
         * @param mapping the sort permutation to apply during merge, or null for a plain
         *                rewrite merge (used when Lucene's IndexSort already sorted the docs)
         */
        ReorderingMergePolicy(RowIdMapping mapping) {
            if (mapping != null && mapping.isNewToOldSupported() == false) {
                throw new IllegalArgumentException("RowIdMapping must support reverse lookup (newToOld) for sorted flush reordering");
            }
            this.mapping = mapping;
        }

        @Override
        public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos segmentInfos, MergeContext mergeContext) {
            return null; // no automatic merges
        }

        @Override
        public MergeSpecification findForcedMerges(
            SegmentInfos segmentInfos,
            int maxSegmentCount,
            Map<SegmentCommitInfo, Boolean> segmentsToMerge,
            MergeContext mergeContext
        ) {
            if (mergeDone) {
                return null; // already done, stop the loop
            }
            mergeDone = true;

            List<SegmentCommitInfo> segments = new ArrayList<>();
            for (int i = 0; i < segmentInfos.size(); i++) {
                segments.add(segmentInfos.info(i));
            }
            if (segments.isEmpty()) {
                return null;
            }
            MergeSpecification spec = new MergeSpecification();
            if (mapping != null) {
                spec.add(new ReorderingOneMerge(segments, mapping));
            } else {
                spec.add(new MergePolicy.OneMerge(segments));
            }
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
                    return (int) mapping.getNewRowId(docID, RowIdMapping.SINGLE_GEN);
                }

                @Override
                public int newToOld(int docID) {
                    return (int) mapping.getOldRowId(docID);
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
