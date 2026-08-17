/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.dataformat.AuxiliaryDataFormat;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Merges Engine-4 <em>element index</em> segments (auxiliary format {@code aux__lucene__nested}).
 *
 * <p>Unlike {@link LuceneMerger}, which merges within the shard's single shared {@code IndexWriter},
 * the element index lives in its own per-generation {@code lucene_gen_<auxGeneration>} directories. So
 * this merger opens those directories directly and re-indexes them into a fresh merged directory via
 * {@code IndexWriter.addIndexes(CodecReader...)}.
 *
 * <p>The one rewrite that matters: each source element segment's {@code __parent_row__} doc-values are
 * parent rows <em>local to that segment's parent generation</em>. The document (parquet) merge renumbers
 * parent rows and produces a {@link RowIdMapping} keyed by {@code (oldParentRow, parentGeneration)}; each
 * source reader is wrapped in {@link NestedParentRowRemappingCodecReader} so the merged element index
 * points at the merged parent rows. The {@code attributes.*} postings carry through unchanged; the
 * element's own {@code __row_id__} is not read on the query path and is left as merged by addIndexes.
 *
 * <p>The parent generation of a source element segment is derived from its auxiliary generation via
 * {@link AuxiliaryDataFormat#writerGenerationOf} — the same offset that paired the two at write time.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ElementIndexMerger implements Merger {

    private static final Logger logger = LogManager.getLogger(ElementIndexMerger.class);

    private final DataFormat luceneFormat;

    /** @param luceneFormat the concrete Lucene data format the element index's files are stored as */
    public ElementIndexMerger(DataFormat luceneFormat) {
        this.luceneFormat = luceneFormat;
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        List<Segment> inputs = mergeInput.segments();
        if (inputs.isEmpty()) {
            return new MergeResult(Map.of());
        }
        RowIdMapping documentMapping = mergeInput.rowIdMapping();
        if (documentMapping == null) {
            throw new IllegalStateException(
                "Element index merge at generation ["
                    + mergeInput.newWriterGeneration()
                    + "] requires the document merge's RowIdMapping to remap __parent_row__, but none was provided"
            );
        }
        long mergedAuxGeneration = mergeInput.newWriterGeneration();

        // The merged element index is written beside its sources: <shard>/lucene/lucene_gen_<mergedAuxGen>.
        Path baseDirectory = elementDirectory(inputs.get(0)).getParent();
        Path mergedDirectory = baseDirectory.resolve("lucene_gen_" + mergedAuxGeneration);
        Files.createDirectories(mergedDirectory);

        List<Directory> sourceDirs = new ArrayList<>(inputs.size());
        List<DirectoryReader> sourceReaders = new ArrayList<>(inputs.size());
        try (Directory mergedDir = new MMapDirectory(mergedDirectory)) {
            IndexWriterConfig iwc = new IndexWriterConfig(new KeywordAnalyzer());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setMergePolicy(NoMergePolicy.INSTANCE);
            try (IndexWriter writer = new IndexWriter(mergedDir, iwc)) {
                List<CodecReader> wrapped = new ArrayList<>();
                for (Segment input : inputs) {
                    long parentGeneration = AuxiliaryDataFormat.writerGenerationOf(input.generation());
                    Path sourcePath = elementDirectory(input);
                    Directory sourceDir = new MMapDirectory(sourcePath);
                    sourceDirs.add(sourceDir);
                    DirectoryReader reader = DirectoryReader.open(sourceDir);
                    sourceReaders.add(reader);
                    for (LeafReaderContext ctx : reader.leaves()) {
                        CodecReader codecReader = asCodecReader(ctx.reader());
                        wrapped.add(new NestedParentRowRemappingCodecReader(codecReader, documentMapping, parentGeneration));
                    }
                }
                writer.addIndexes(wrapped.toArray(new CodecReader[0]));
                // One segment for the merged element index, mirroring the write path's single-segment flush.
                writer.forceMerge(1, true);
                writer.commit();
            }

            WriterFileSet.Builder wfs = WriterFileSet.builder().directory(mergedDirectory).writerGeneration(mergedAuxGeneration);
            long rows = 0L;
            try (Directory readBack = new MMapDirectory(mergedDirectory); DirectoryReader dr = DirectoryReader.open(readBack)) {
                rows = dr.maxDoc();
            }
            wfs.addNumRows(rows);
            for (String file : mergedDir.listAll()) {
                if (file.startsWith("segments") == false && file.equals("write.lock") == false) {
                    wfs.addFile(file);
                }
            }
            logger.info(
                "Merged element index: {} source segments -> generation [{}] ({} elements) in [{}]",
                inputs.size(),
                mergedAuxGeneration,
                rows,
                mergedDirectory
            );
            return new MergeResult(Map.of(luceneFormat, wfs.build()));
        } finally {
            IOUtils.closeWhileHandlingException(sourceReaders);
            IOUtils.closeWhileHandlingException(sourceDirs);
        }
    }

    /** The on-disk directory holding a source element segment's files. */
    private Path elementDirectory(Segment segment) {
        WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(luceneFormat.name());
        if (wfs == null) {
            throw new IllegalStateException(
                "Element segment at generation ["
                    + segment.generation()
                    + "] has no files for storage format ["
                    + luceneFormat.name()
                    + "]; cannot merge"
            );
        }
        return Path.of(wfs.directory());
    }

    /** Unwraps any {@link FilterLeafReader}s down to the {@link SegmentReader} (a {@link CodecReader}). */
    private static CodecReader asCodecReader(LeafReader reader) {
        LeafReader current = reader;
        while (current instanceof FilterLeafReader filter) {
            current = filter.getDelegate();
        }
        return (CodecReader) current;
    }
}
