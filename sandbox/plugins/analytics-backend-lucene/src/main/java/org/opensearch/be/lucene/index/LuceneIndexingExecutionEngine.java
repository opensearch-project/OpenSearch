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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.misc.store.HardlinkCopyDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LuceneFieldFactoryRegistry;
import org.opensearch.be.lucene.merge.LuceneMerger;
import org.opensearch.be.lucene.merge.RowIdRemappingSortField;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene-specific {@link IndexingExecutionEngine} that manages per-writer Lucene segments
 * and incorporates them into the shared {@link LuceneCommitter} writer during refresh.
 *
 * Write path: Each call to {@link #createWriter(long)} creates a {@link LuceneWriter} with its own
 * {@link IndexWriter} in an isolated temp directory. Documents are indexed into this
 * per-writer segment. On flush, the writer force-merges to exactly 1 segment.
 *
 * Refresh path: During refresh, flushed segments from temp directories are incorporated into the
 * shared {@link LuceneCommitter} writer via {@code IndexWriter.addIndexes(Directory...)}.
 * The shared writer uses {@link NoMergePolicy} to preserve the 1:1 segment-to-Parquet-file
 * mapping. After addIndexes, the temp directories are cleaned up.
 *
 * Segment correlation: Each per-writer segment stores the writer generation as a segment info attribute
 * ({@link LuceneWriter#WRITER_GENERATION_ATTRIBUTE}). After addIndexes, the catalog
 * snapshot records the final directory (the shared writer's directory) so that readers
 * can find the segments.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneIndexingExecutionEngine implements IndexingExecutionEngine<LuceneDataFormat, LuceneDocumentInput>, IndexStoreProvider {

    private static final Logger logger = LogManager.getLogger(LuceneIndexingExecutionEngine.class);

    private final LuceneDataFormat dataFormat;
    private final IndexWriter sharedWriter;
    private final Store store;
    private final Path baseDirectory;
    private final Analyzer analyzer;
    private final Codec codec;
    private final LuceneMerger luceneMerger;
    private final LuceneFieldFactoryRegistry fieldFactoryRegistry;

    /**
     * Creates a new LuceneIndexingExecutionEngine with a specific analyzer.
     *
     * @param dataFormat      the Lucene data format descriptor
     * @param luceneCommitter the committer that owns the shared IndexWriter
     * @param store           the shard's store
     */
    public LuceneIndexingExecutionEngine(
        LuceneDataFormat dataFormat,
        LuceneCommitter luceneCommitter,
        MapperService mapperService,
        Store store
    ) {
        if (luceneCommitter == null) {
            throw new IllegalArgumentException("LuceneCommitter must not be null");
        }
        this.dataFormat = dataFormat;
        this.sharedWriter = luceneCommitter.getIndexWriter();
        this.store = store;
        this.baseDirectory = store.shardPath().resolve(LuceneDataFormat.LUCENE_FORMAT_NAME);
        this.analyzer = sharedWriter.getAnalyzer();
        this.codec = sharedWriter.getConfig().getCodec();
        this.fieldFactoryRegistry = new LuceneFieldFactoryRegistry();

        // Extract the RowIdRemappingSortField from the writer's IndexSort for the merger
        RowIdRemappingSortField rowIdSortField = null;
        if (sharedWriter.getConfig().getIndexSort() != null) {
            for (var sf : sharedWriter.getConfig().getIndexSort().getSort()) {
                if (sf instanceof RowIdRemappingSortField rmsf) {
                    rowIdSortField = rmsf;
                    break;
                }
            }
        }
        this.luceneMerger = new LuceneMerger(sharedWriter, rowIdSortField, dataFormat, store.shardPath().resolveIndex());

        // Create the lucene subdirectory if it doesn't exist
        try {
            Files.createDirectories(baseDirectory);
        } catch (FileAlreadyExistsException ex) {
            logger.warn("Directory already exists: {}", baseDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the underlying shared IndexWriter from the committer.
     *
     * @return the index writer
     */
    public IndexWriter getWriter() {
        return sharedWriter;
    }

    /** {@inheritDoc} Returns this engine as the {@link IndexStoreProvider}. */
    @Override
    public IndexStoreProvider getProvider() {
        return this;
    }

    /**
     * Returns a {@link LuceneFormatStore} wrapping the shard's store and the shared IndexWriter.
     *
     * @param dataFormat the data format (ignored — always returns the Lucene store)
     * @return the format store
     */
    @Override
    public FormatStore getStore(DataFormat dataFormat) {
        return new LuceneFormatStore(store, sharedWriter);
    }

    /**
     * Creates a new {@link LuceneWriter} for the given generation in an isolated temp directory
     * under the shard's Lucene base directory.
     *
     * @param writerGeneration the generation number for the new writer
     * @return a new writer
     * @throws RuntimeException wrapping an {@link IOException} if writer creation fails
     */
    @Override
    public Writer<LuceneDocumentInput> createWriter(long writerGeneration) {
        assert sharedWriter.isOpen() : "Cannot create writer — shared IndexWriter is closed";
        try {
            return new LuceneWriter(writerGeneration, dataFormat, baseDirectory, analyzer, codec, sharedWriter.getConfig().getIndexSort());
        } catch (IOException e) {
            throw new RuntimeException("Failed to create LuceneWriter for generation " + writerGeneration, e);
        }
    }

    /**
     * Creates a new empty {@link LuceneDocumentInput} using the default field factory registry.
     *
     * @return a new document input
     */
    @Override
    public LuceneDocumentInput newDocumentInput() {
        return new LuceneDocumentInput(fieldFactoryRegistry);
    }

    /** {@inheritDoc} Returns the {@link LuceneDataFormat} descriptor. */
    @Override
    public LuceneDataFormat getDataFormat() {
        return dataFormat;
    }

    /**
     * Incorporates flushed per-writer segments into the shared IndexWriter via
     * {@code addIndexes}, then opens an NRT reader to discover the final file names
     * assigned by Lucene after the merge.
     * <p>
     * Existing segments from the catalog snapshot are preserved. New segments from
     * writer temp directories are batched into a single {@code addIndexes} call for
     * efficiency. After incorporation, the writer generation attribute on each segment
     * is used to correlate back to the originating writer.
     *
     * @param refreshInput contains existing catalog segments and newly flushed writer segments
     * @return the combined list of existing and newly incorporated segments
     * @throws IOException if addIndexes or reader opening fails
     */
    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        if (refreshInput == null || sharedWriter == null) {
            return new RefreshResult(List.of());
        }

        List<Segment> resultSegments = new ArrayList<>(refreshInput.existingSegments());

        // Collect all source directories and their paths for a single batched addIndexes call
        List<Directory> sourceDirectories = new ArrayList<>();
        Set<Long> writerGenerations = new HashSet<>();

        for (Segment segment : refreshInput.writerFiles()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(LuceneDataFormat.LUCENE_FORMAT_NAME);
            if (wfs == null) {
                continue;
            }

            Path dirPath = Path.of(wfs.directory());
            if (Files.isDirectory(dirPath) == false) {
                logger.warn("Lucene writer directory does not exist: {}", dirPath);
                continue;
            }

            sourceDirectories.add(new HardlinkCopyDirectoryWrapper(new MMapDirectory(dirPath)));
            writerGenerations.add(wfs.writerGeneration());
        }

        // Single batched addIndexes call for all source directories
        if (sourceDirectories.isEmpty() == false) {
            try {
                sharedWriter.addIndexes(sourceDirectories.toArray(new Directory[0]));
                logger.debug("Incorporated {} Lucene segments into shared writer in a single addIndexes call", sourceDirectories.size());
            } finally {
                // Close all source directories
                for (Directory dir : sourceDirectories) {
                    try {
                        dir.close();
                    } catch (IOException e) {
                        logger.warn("Failed to close source directory after addIndexes", e);
                    }
                }
            }

            // After addIndexes, open an NRT reader to discover the actual file names
            // for the newly added segments. Lucene renames files during addIndexes,
            // so the original temp directory file names are no longer valid.
            Path sharedDir = store.shardPath().resolveIndex();

            try (DirectoryReader reader = DirectoryReader.open(sharedWriter)) {
                List<LeafReaderContext> leaves = reader.leaves();

                for (int i = 0; i < leaves.size(); i++) {
                    LeafReaderContext ctx = leaves.get(i);
                    if (ctx.reader() instanceof SegmentReader segReader) {
                        SegmentCommitInfo segInfo = segReader.getSegmentInfo();
                        String genAttr = segInfo.info.getAttribute(LuceneWriter.WRITER_GENERATION_ATTRIBUTE);
                        if (genAttr == null) {
                            continue;
                        }

                        long writerGen = Long.parseLong(genAttr);
                        if (!writerGenerations.contains(writerGen)) {
                            continue;
                        }
                        long numDocs = segInfo.info.maxDoc();

                        WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
                            .directory(sharedDir)
                            .writerGeneration(writerGen)
                            .addNumRows(numDocs);

                        for (String file : segInfo.files()) {
                            wfsBuilder.addFile(file);
                        }

                        resultSegments.add(Segment.builder(writerGen).addSearchableFiles(dataFormat, wfsBuilder.build()).build());
                        writerGenerations.remove(writerGen);
                    }
                }
            }
            assert writerGenerations.isEmpty() : "Could not get segments from all writers";
        }

        return new RefreshResult(List.copyOf(resultSegments));
    }

    /** Returns {@code null} — merge scheduling is not yet implemented for the Lucene format. */
    @Override
    public Merger getMerger() {
        return this.luceneMerger;
    }

    /**
     * Not supported — writer generation is managed by the {@code DataFormatAwareEngine}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("getNextWriterGeneration managed by DataFormatAwareEngine");
    }

    @Override
    public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        // No-op: Lucene's internal IndexFileDeleter handles segment file cleanup when
        // IndexCommit.delete() is called inside LuceneCommitDeletionPolicy.onCommit(),
        // triggered by IndexWriter.deleteUnusedFiles() from LuceneCommitter.deleteCommit().
        return Map.of();
    }

    /** No-op — the {@link LuceneCommitter} owns the shared IndexWriter lifecycle. */
    @Override
    public void close() throws IOException {
        // LuceneCommitter owns the shared IndexWriter lifecycle
    }

    /**
     * A record combining the shard's {@link Store} and the shared {@link IndexWriter},
     * used by the search back-end to open NRT readers.
     *
     * @param store  the shard store
     * @param writer the shared index writer
     */
    public static record LuceneFormatStore(Store store, IndexWriter writer) implements FormatStore {
    }
}
