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
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.be.lucene.stats.LuceneShardStatsTracker;
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
import java.util.concurrent.TimeUnit;

import static org.opensearch.be.lucene.index.LuceneWriter.WRITER_GENERATION_ATTRIBUTE;

/**
 * Lucene-specific {@link Merger} that merges segments using Lucene's internal
 * {@code merge(OneMerge)} path with IndexSort-based document reordering.
 *
 * <h2>How it works</h2>
 *
 * <ol>
 *   <li><b>Value rewriting</b> — {@link RowIdRemappingOneMerge#wrapForMerge} wraps each
 *       CodecReader with {@link RowIdRemappingCodecReader} to remap row ID
 *       doc values for the output.</li>
 *   <li><b>Document ordering</b> — The writer's IndexSort (a {@code SortedNumericSortField}
 *       on the row ID field) reads the already-remapped values from the wrapped readers.
 *       {@code MultiSorter.sort()} uses these to build DocMaps that reorder all data
 *       (stored fields, doc values, postings).</li>
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
    private final DataFormat dataFormat;
    private final Path storeDirectory;
    private final LuceneMergeStrategy strategy;
    private final LuceneShardStatsTracker stats;

    public LuceneMerger(MergeIndexWriter indexWriter, DataFormat dataFormat, Path storeDirectory, LuceneShardStatsTracker stats) {
        if (indexWriter == null) {
            throw new IllegalArgumentException("IndexWriter must not be null");
        }
        this.indexWriter = indexWriter;
        this.dataFormat = dataFormat;
        this.storeDirectory = storeDirectory;
        this.stats = stats;
        // TODO implement primary and integrate the same here
        this.strategy = new SecondaryLuceneMergeStrategy();
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        long start = System.nanoTime();
        try {
            Map<Long, RowIdMapping> rowIdMappings = mergeInput.rowIdMappings();
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
                throw new IOException(
                    "IndexWriter has no segments — cannot proceed with Lucene merge for generations "
                        + generationsToMerge
                        + ". This may indicate a concurrent commit cleared the segment list."
                );
            }

            List<SegmentCommitInfo> matchingSegments = findMatchingSegments(segmentInfos, generationsToMerge);

            if (matchingSegments.isEmpty()) {
                throw new IOException(
                    "No Lucene segments found matching writer generations "
                        + generationsToMerge
                        + " — segments may have been consumed by a concurrent merge"
                );
            }

            if (matchingSegments.size() != generationsToMerge.size()) {
                throw new IllegalStateException(
                    "Expected "
                        + generationsToMerge.size()
                        + " Lucene segments for generations "
                        + generationsToMerge
                        + " but found only "
                        + matchingSegments.size()
                        + ". Missing segments may have been consumed by a concurrent merge. "
                        + "Found generations: "
                        + matchingSegments.stream()
                            .map(sci -> sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE))
                            .collect(java.util.stream.Collectors.toList())
                );
            }

            logger.debug(
                "LuceneMerger: merging {} segments (generations {}) using merge(OneMerge) + IndexSort",
                matchingSegments.size(),
                generationsToMerge
            );

            // Delegate OneMerge creation to the strategy (primary vs secondary behavior).
            // For the secondary path, the returned RowIdRemappingOneMerge stamps the
            // writer_generation attribute onto the merged SegmentInfo via setMergeInfo, which
            // Lucene invokes immediately before codec.segmentInfoFormat().write(...) — so the
            // attribute is persisted to the .si file and survives a writer reopen.
            MergePolicy.OneMerge oneMerge = strategy.createOneMerge(matchingSegments, rowIdMappings, mergeInput.newWriterGeneration());
            indexWriter.executeMerge(oneMerge, mergeInput.newWriterGeneration());

            // Build the merged WriterFileSet from the output segment info
            SegmentCommitInfo mergedInfo = oneMerge.getMergeInfo();
            WriterFileSet mergedFileSet = buildMergedFileSet(mergedInfo, mergeInput.newWriterGeneration());

            // Delegate RowIdMapping production to the strategy
            Map<Long, RowIdMapping> outputMappings = strategy.buildRowIdMappings(oneMerge, mergeInput);

            logger.debug(
                "LuceneMerger: completed merge of {} segments at generation {} ({} docs, {} files)",
                matchingSegments.size(),
                mergeInput.newWriterGeneration(),
                oneMerge.getMergeInfo().info.maxDoc(),
                oneMerge.getMergeInfo().files().size()
            );

            stats.incMergeTotal();
            return new MergeResult(Map.of(dataFormat, mergedFileSet), outputMappings);
        } catch (IOException e) {
            stats.incMergeFailures();
            throw e;
        } finally {
            stats.addMergeTimeMillis(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        }
    }

    /**
     * Finds segments in the IndexWriter whose writer generation matches the requested generations.
     */
    private List<SegmentCommitInfo> findMatchingSegments(SegmentInfos segmentInfos, Set<Long> generations) {
        List<SegmentCommitInfo> matching = new ArrayList<>();
        for (SegmentCommitInfo sci : segmentInfos) {
            String genAttr = sci.info.getAttribute(WRITER_GENERATION_ATTRIBUTE);
            if (genAttr != null && generations.contains(Long.parseLong(genAttr))) {
                matching.add(sci);
            }
        }
        return matching;
    }

    /**
     * Builds a {@link WriterFileSet} from the merged segment info.
     */
    private WriterFileSet buildMergedFileSet(SegmentCommitInfo mergedInfo, long writerGeneration) throws IOException {
        WriterFileSet.Builder builder = WriterFileSet.builder()
            .directory(storeDirectory)
            .writerGeneration(writerGeneration)
            .addNumRows(mergedInfo.info.maxDoc());
        for (String file : mergedInfo.files()) {
            builder.addFile(file);
        }
        return builder.build();
    }
}
