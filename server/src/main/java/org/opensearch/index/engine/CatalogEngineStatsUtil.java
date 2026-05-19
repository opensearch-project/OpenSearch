/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.Sort;
import org.apache.lucene.util.Version;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for building segment-related stats and metadata from catalog-snapshot-based engines.
 * <p>
 * Encapsulates logic that is common to any non-Lucene engine (e.g. Parquet, ORC) so that
 * individual engine implementations do not duplicate file-size resolution, stats assembly,
 * or segment list construction.
 */
public final class CatalogEngineStatsUtil {

    private CatalogEngineStatsUtil() {}

    /**
     * Builds a {@link SegmentsStats} from catalog snapshot segments.
     *
     * @param segmentCount             total number of segments
     * @param nativeBytesUsed          native/off-heap memory used by the engine
     * @param maxUnsafeAutoIdTimestamp max unsafe auto-id timestamp
     * @param includeSegmentFileSizes  if true, resolves and includes per-file sizes
     * @param fileSets                 all {@link WriterFileSet}s across all segments and formats
     * @param dataPath                 base data path used to resolve file paths
     * @param logger                   logger for warnings on file size resolution failures
     * @return populated {@link SegmentsStats}
     */
    public static SegmentsStats buildSegmentsStats(
        int segmentCount,
        long nativeBytesUsed,
        long maxUnsafeAutoIdTimestamp,
        boolean includeSegmentFileSizes,
        Collection<WriterFileSet> fileSets,
        Path dataPath,
        Logger logger
    ) {
        SegmentsStats stats = new SegmentsStats();
        stats.add(segmentCount);
        stats.addIndexWriterMemoryInBytes(nativeBytesUsed);
        stats.updateMaxUnsafeAutoIdTimestamp(maxUnsafeAutoIdTimestamp);
        if (includeSegmentFileSizes && dataPath != null) {
            for (WriterFileSet wfs : fileSets) {
                Map<String, Long> fileSizes = new HashMap<>();
                for (String file : wfs.files()) {
                    Path filePath = dataPath.resolve(wfs.directory()).resolve(file);
                    try {
                        fileSizes.put(file, Files.size(filePath));
                    } catch (IOException e) {
                        logger.warn(() -> "Failed to read size for file [" + filePath + "]; reporting 0", e);
                        fileSizes.put(file, 0L);
                    }
                }
                stats.addFileSizes(fileSizes);
            }
        }
        return stats;
    }

    /**
     * Builds a list of {@link Segment} from catalog snapshot segments.
     * <p>
     * All segments are marked as searchable. Committed state is determined by the caller
     * (i.e. whether the current snapshot ID matches the last committed snapshot ID).
     *
     * @param catalogSegments catalog segments from the current snapshot
     * @param isCommitted     whether the current snapshot has been flushed/committed
     * @param indexSort       the index sort, or null if unsorted
     * @param logger          logger for warnings on empty or oversized segments
     * @return list of engine segments sorted by generation
     */
    public static List<Segment> buildSegments(
        List<org.opensearch.index.engine.exec.Segment> catalogSegments,
        boolean isCommitted,
        Sort indexSort,
        Logger logger
    ) {
        List<Segment> result = new ArrayList<>(catalogSegments.size());
        for (org.opensearch.index.engine.exec.Segment dfSeg : catalogSegments) {
            Segment seg = new Segment("_" + Long.toString(dfSeg.generation(), Character.MAX_RADIX));
            seg.search = true;
            seg.committed = isCommitted;
            seg.version = Version.LATEST;
            if (dfSeg.dfGroupedSearchableFiles().isEmpty()) {
                logger.warn("Segment [{}] has no searchable files; reporting 0 doc count", dfSeg.generation());
            }
            long numRows = dfSeg.dfGroupedSearchableFiles().values().stream().findFirst().map(WriterFileSet::numRows).orElse(0L);
            if (numRows > Integer.MAX_VALUE) {
                logger.warn("Segment [{}] has {} rows exceeding Integer.MAX_VALUE; clamping docCount", dfSeg.generation(), numRows);
            }
            seg.docCount = (int) Math.min(numRows, Integer.MAX_VALUE);
            seg.delDocCount = 0;
            seg.sizeInBytes = dfSeg.dfGroupedSearchableFiles().values().stream().mapToLong(WriterFileSet::getTotalSize).sum();
            seg.segmentSort = indexSort;
            result.add(seg);
        }
        result.sort(Comparator.comparingLong(Segment::getGeneration));
        return result;
    }

    /**
     * Resolves whether the given snapshot has been committed by comparing its ID
     * against the last committed snapshot ID stored in commit data.
     *
     * @param snapshot       the current catalog snapshot
     * @param lastCommitData commit data map from the committer
     * @param logger         logger for warnings on invalid snapshot ID format
     * @return true if the snapshot ID matches the last committed snapshot ID
     */
    public static boolean resolveIsCommitted(CatalogSnapshot snapshot, Map<String, String> lastCommitData, Logger logger) {
        if (snapshot == null) {
            return false;
        }
        String lastCommittedIdStr = lastCommitData.get(CatalogSnapshot.CATALOG_SNAPSHOT_ID);
        if (lastCommittedIdStr == null) {
            return false;
        }
        try {
            return snapshot.getId() == Long.parseLong(lastCommittedIdStr);
        } catch (NumberFormatException e) {
            logger.warn("Invalid catalog snapshot ID in committed data: {}", lastCommittedIdStr);
            return false;
        }
    }
}
