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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeIndexWriter;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Lucene-specific {@link Merger} that merges segments using Lucene's internal
 * {@code merge(OneMerge)} path with a {@link RowIdRemappingSortField} on the writer's
 * IndexSort for document reordering.
 *
 * <h2>How it works</h2>
 *
 * <ol>
 *   <li><b>Value rewriting</b> — {@link RowIdRemappingOneMerge#wrapForMerge} wraps each
 *       CodecReader with {@link RowIdRemappingCodecReader} to remap {@code ___row_id}
 *       doc values for the output.</li>
 *   <li><b>Document ordering</b> — The writer's {@link RowIdRemappingSortField} has the
 *       {@link RowIdMapping} set before merge. Its {@code getIndexSorter()} reads original
 *       (unwrapped) row IDs and remaps them. {@code MultiSorter.sort()} uses these to build
 *       DocMaps that reorder all data (stored fields, doc values, postings).</li>
 *   <li><b>Segment lifecycle</b> — Lucene's internal merge path handles reference-counted
 *       file cleanup via {@code IndexFileDeleter}. If the merge fails, old segments are
 *       preserved and the partially-written merged segment is cleaned up.</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneMerger implements Merger {

    private static final Logger logger = LogManager.getLogger(LuceneMerger.class);

    static final String ROW_ID_FIELD = "___row_id";
    static final String WRITER_GENERATION_ATTR = "writer_generation";

    private static final Field SEGMENT_INFOS_FIELD = initSegmentInfosField();

    @SuppressForbidden(reason = "Need live SegmentInfos reference for post-merge segment removal; cloneSegmentInfos() returns a copy")
    private static Field initSegmentInfosField() {
        try {
            Field field = IndexWriter.class.getDeclaredField("segmentInfos");
            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final MergeIndexWriter indexWriter;
    private final RowIdRemappingSortField sortField;
    private final DataFormat dataFormat;
    private final Path storeDirectory;

    public LuceneMerger(IndexWriter indexWriter, RowIdRemappingSortField sortField, DataFormat dataFormat, Path storeDirectory) {
        if (indexWriter == null) {
            throw new IllegalArgumentException("IndexWriter must not be null");
        }
        if (indexWriter instanceof MergeIndexWriter == false) {
            throw new IllegalArgumentException("IndexWriter must be a MergeIndexWriter, got " + indexWriter.getClass().getName());
        }
        this.indexWriter = (MergeIndexWriter) indexWriter;
        this.sortField = sortField;
        this.dataFormat = dataFormat;
        this.storeDirectory = storeDirectory;
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        RowIdMapping rowIdMapping = mergeInput.rowIdMapping();
        List<Segment> segments = mergeInput.segments();

        if (segments.isEmpty()) {
            return new MergeResult(Map.of());
        }

        Set<Long> generationsToMerge = new HashSet<>();
        for (Segment segment : segments) {
            generationsToMerge.add(segment.generation());
        }

        SegmentInfos segmentInfos;
        try {
            segmentInfos = (SegmentInfos) SEGMENT_INFOS_FIELD.get(indexWriter);
        } catch (IllegalAccessException e) {
            throw new IOException("Failed to access IndexWriter segmentInfos via reflection", e);
        }

        if (segmentInfos.size() == 0) {
            logger.warn("No segments in IndexWriter — skipping merge");
            return new MergeResult(Map.of());
        }

        List<SegmentCommitInfo> matchingSegments = new ArrayList<>();
        for (SegmentCommitInfo sci : segmentInfos.asList()) {
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTR);
            if (genAttr != null && generationsToMerge.contains(Long.parseLong(genAttr))) {
                matchingSegments.add(sci);
            }
        }

        if (matchingSegments.isEmpty()) {
            logger.warn("No segments found matching writer generations {} — skipping merge", generationsToMerge);
            return new MergeResult(Map.of());
        }

        logger.info(
            "LuceneMerger: merging {} segments (generations {}) using merge(OneMerge) + IndexSort",
            matchingSegments.size(),
            generationsToMerge
        );

        // Set the RowIdMapping on the sort field so getIndexSorter() returns remapped
        // values for MultiSorter to build DocMaps
        if (rowIdMapping != null && sortField != null) {
            sortField.setRowIdMapping(rowIdMapping);
        }

        try {
            // wrapForMerge remaps ___row_id values for the output,
            // MultiSorter uses getIndexSorter() to reorder documents,
            // merge(OneMerge) handles segment lifecycle and file cleanup
            RowIdRemappingOneMerge oneMerge = new RowIdRemappingOneMerge(matchingSegments, rowIdMapping);
            indexWriter.executeMerge(oneMerge, mergeInput.newWriterGeneration());

            // Build the merged WriterFileSet from the output segment info
            SegmentCommitInfo mergedInfo = oneMerge.getMergeInfo();
            long mergedDocCount = mergedInfo.info.maxDoc();

            WriterFileSet.Builder wfsBuilder = WriterFileSet.builder()
                .directory(storeDirectory)
                .writerGeneration(mergeInput.newWriterGeneration())
                .addNumRows(mergedDocCount);

            for (String file : mergedInfo.files()) {
                wfsBuilder.addFile(file);
            }

            WriterFileSet mergedFileSet = wfsBuilder.build();

            logger.info(
                "LuceneMerger: completed merge of {} segments at generation {} ({} docs, {} files)",
                matchingSegments.size(),
                mergeInput.newWriterGeneration(),
                mergedDocCount,
                mergedInfo.files().size()
            );

            return new MergeResult(Map.of(dataFormat, mergedFileSet), rowIdMapping);
        } finally {
            if (sortField != null) {
                sortField.clearRowIdMapping();
            }
        }
    }
}
