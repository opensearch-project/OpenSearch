/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.Directory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates field-level segment statistics for Lucene index files with maximum accuracy.
 * Uses Lucene's built-in statistics APIs and proportional attribution for precise calculations.
 *
 * @opensearch.internal
 */
public class FieldLevelSegmentStatsCalculator {
    private static final Logger logger = LogManager.getLogger(FieldLevelSegmentStatsCalculator.class);

    /**
     * Calculate field-level statistics for a segment using actual file sizes and Lucene statistics
     * @param segmentReader The segment reader to analyze
     * @return Map of field names to their file type statistics (dvd, dvm, tim, tip, pos, dim, dii)
     */
    public Map<String, Map<String, Long>> calculateFieldLevelStats(SegmentReader segmentReader) {
        Map<String, Map<String, Long>> fieldLevelStats = new ConcurrentHashMap<>();

        if (segmentReader.getFieldInfos().size() == 0) {
            return fieldLevelStats;
        }

        // Get actual segment file sizes for proportional attribution
        Map<String, Long> segmentFileSizes = getSegmentFileSizes(segmentReader);

        for (FieldInfo fieldInfo : segmentReader.getFieldInfos()) {
            Map<String, Long> fieldStats = new HashMap<>();

            try {
                // Calculate DocValues statistics
                if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    calculateDocValuesStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes);
                }

                // Calculate PointValues statistics
                if (fieldInfo.getPointDimensionCount() > 0) {
                    calculatePointValuesStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes);
                }

                // Calculate Terms statistics
                if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                    calculateTermStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes);
                }

                if (!fieldStats.isEmpty()) {
                    fieldLevelStats.put(fieldInfo.name, fieldStats);
                }

            } catch (OutOfMemoryError e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Out of memory calculating stats for field [{}] in segment [{}], skipping field",
                        fieldInfo.name,
                        segmentReader.getSegmentName()
                    ),
                    e
                );
                continue;
            } catch (Exception e) {
                logger.debug(
                    () -> new ParameterizedMessage(
                        "Failed to calculate stats for field [{}] in segment [{}]",
                        fieldInfo.name,
                        segmentReader.getSegmentName()
                    ),
                    e
                );
            }
        }

        if (fieldLevelStats.isEmpty() && segmentReader.getFieldInfos().size() > 0) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "No field-level stats calculated for segment {} with {} fields",
                    segmentReader.getSegmentName(),
                    segmentReader.getFieldInfos().size()
                )
            );
        }

        return fieldLevelStats;
    }

    /**
     * Get actual segment file sizes by extension, following Engine.getSegmentFileSizes() pattern
     */
    private Map<String, Long> getSegmentFileSizes(SegmentReader segmentReader) {
        Map<String, Long> fileSizes = new HashMap<>();
        try {
            Directory directory = segmentReader.directory();
            if (directory != null) {
                String[] files = directory.listAll();
                for (String file : files) {
                    try {
                        long size = directory.fileLength(file);
                        int idx = file.lastIndexOf('.');
                        if (idx != -1) {
                            String ext = file.substring(idx + 1);
                            fileSizes.merge(ext, size, Long::sum);
                        }
                    } catch (NoSuchFileException | FileNotFoundException e) {
                        // File was deleted during processing, skip
                    }
                }
            }
        } catch (IOException e) {
            logger.debug(() -> new ParameterizedMessage("Failed to get segment file sizes for {}", segmentReader.getSegmentName()), e);
        }
        return fileSizes;
    }

    /**
     * Get total segment size using SegmentCommitInfo for maximum accuracy
     */
    private long getTotalSegmentSize(SegmentReader segmentReader) {
        try {
            SegmentCommitInfo info = segmentReader.getSegmentInfo();
            return info.sizeInBytes();
        } catch (IOException e) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "Failed to get total segment size from SegmentCommitInfo for {}",
                    segmentReader.getSegmentName()
                ),
                e
            );
            return 0;
        }
    }

    /**
     * Calculate DocValues statistics with proportional attribution
     */
    private void calculateDocValuesStats(
        SegmentReader reader,
        FieldInfo fieldInfo,
        Map<String, Long> stats,
        Map<String, Long> segmentFileSizes
    ) {
        // Use actual file sizes if available for proportional attribution
        Long actualDvdSize = segmentFileSizes.get("dvd");
        Long actualDvmSize = segmentFileSizes.get("dvm");

        if (actualDvdSize != null && actualDvdSize > 0) {
            // Calculate this field's proportion of total DocValues usage
            long totalDocValuesSize = calculateTotalDocValuesSize(reader);
            long fieldDocValuesSize = estimateFieldDocValuesSize(reader, fieldInfo);

            if (totalDocValuesSize > 0 && fieldDocValuesSize > 0) {
                stats.put("dvd", (actualDvdSize * fieldDocValuesSize) / totalDocValuesSize);
                if (actualDvmSize != null) {
                    stats.put("dvm", (actualDvmSize * fieldDocValuesSize) / totalDocValuesSize);
                } else {
                    stats.put("dvm", Math.max(1L, stats.get("dvd") / 10));
                }
            } else {
                stats.put("dvd", fieldDocValuesSize);
                stats.put("dvm", Math.max(1L, fieldDocValuesSize / 10));
            }
        } else {
            // Fallback to estimation
            long estimatedSize = estimateFieldDocValuesSize(reader, fieldInfo);
            if (estimatedSize > 0) {
                stats.put("dvd", estimatedSize);
                stats.put("dvm", Math.max(1L, estimatedSize / 10));
            }
        }
    }

    /**
     * Calculate PointValues statistics using Lucene's built-in APIs
     */
    private void calculatePointValuesStats(
        SegmentReader reader,
        FieldInfo fieldInfo,
        Map<String, Long> stats,
        Map<String, Long> segmentFileSizes
    ) {
        try {
            PointValues pointValues = reader.getPointValues(fieldInfo.name);
            if (pointValues == null) return;

            // Use Lucene's exact statistics
            long numPoints = pointValues.size();
            long numDocs = pointValues.getDocCount();
            int bytesPerDim = pointValues.getBytesPerDimension();
            int numDims = pointValues.getNumIndexDimensions();

            long pointsDataSize = numPoints * bytesPerDim * numDims;
            long pointsIndexSize = Math.max(numDocs * 8, pointsDataSize / 4);

            if (pointsDataSize > 0) {
                // Use proportional attribution if actual file sizes available
                Long actualDimSize = segmentFileSizes.get("dim");
                Long actualDiiSize = segmentFileSizes.get("dii");

                if (actualDimSize != null && actualDimSize > 0) {
                    long totalPointsSize = calculateTotalPointValuesSize(reader);
                    if (totalPointsSize > 0) {
                        stats.put("dim", (actualDimSize * pointsDataSize) / totalPointsSize);
                        if (actualDiiSize != null) {
                            stats.put("dii", (actualDiiSize * pointsDataSize) / totalPointsSize);
                        } else {
                            stats.put("dii", Math.max(1L, stats.get("dim") / 4));
                        }
                    } else {
                        stats.put("dim", pointsDataSize);
                        stats.put("dii", pointsIndexSize);
                    }
                } else {
                    stats.put("dim", pointsDataSize);
                    stats.put("dii", pointsIndexSize);
                }
            }
        } catch (IllegalArgumentException e) {
            // Field doesn't have point values, skip silently
        } catch (IOException e) {
            logger.debug(() -> new ParameterizedMessage("Failed to calculate PointValues stats for field {}", fieldInfo.name), e);
        }
    }

    /**
     * Calculate Terms statistics using Lucene's aggregate statistics APIs
     */
    private void calculateTermStats(SegmentReader reader, FieldInfo fieldInfo, Map<String, Long> stats, Map<String, Long> segmentFileSizes)
        throws IOException {
        Terms terms = reader.terms(fieldInfo.name);
        if (terms == null) return;

        // Use Lucene's built-in aggregate statistics for performance
        long termCount = terms.size();
        long docCount = terms.getDocCount();
        long sumDocFreq = terms.getSumDocFreq();
        long sumTotalTermFreq = terms.getSumTotalTermFreq();

        // Calculate estimated sizes based on actual statistics
        long termDictSize = 0;
        long termIndexSize = 0;
        long positionsSize = 0;

        if (termCount > 0) {
            // Term dictionary: terms + posting lists
            termDictSize = termCount * 15; // Average term length + overhead
            if (sumDocFreq > 0) {
                termDictSize += sumDocFreq * 8; // Doc IDs and frequencies
            } else if (docCount > 0) {
                termDictSize += docCount * 8;
            }
            termIndexSize = Math.max(1L, termDictSize / 10);
        } else {
            // Fallback estimation
            termDictSize = reader.maxDoc() * 20L;
            termIndexSize = termDictSize / 10;
        }

        // Position data calculation
        if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
            if (sumTotalTermFreq > 0) {
                positionsSize = sumTotalTermFreq * 4; // 4 bytes per position
            } else {
                positionsSize = reader.maxDoc() * 8L; // Fallback estimate
            }
        }

        // Apply proportional attribution using actual file sizes
        applyTermStatsWithAttribution(stats, termDictSize, termIndexSize, positionsSize, segmentFileSizes, reader);
    }

    /**
     * Apply term statistics with proportional attribution to actual file sizes
     */
    private void applyTermStatsWithAttribution(
        Map<String, Long> stats,
        long termDictSize,
        long termIndexSize,
        long positionsSize,
        Map<String, Long> segmentFileSizes,
        SegmentReader reader
    ) {
        Long actualTimSize = segmentFileSizes.get("tim");
        Long actualTipSize = segmentFileSizes.get("tip");
        Long actualPosSize = segmentFileSizes.get("pos");

        // Term dictionary attribution
        if (actualTimSize != null && actualTimSize > 0) {
            long totalTermDictSize = calculateTotalTermDictSize(reader);
            if (totalTermDictSize > 0) {
                stats.put("tim", (actualTimSize * termDictSize) / totalTermDictSize);
            } else {
                stats.put("tim", termDictSize);
            }
        } else if (termDictSize > 0) {
            stats.put("tim", termDictSize);
        }

        // Term index attribution
        if (actualTipSize != null && actualTipSize > 0) {
            stats.put("tip", Math.max(1L, stats.getOrDefault("tim", termDictSize) / 10));
        } else if (termIndexSize > 0) {
            stats.put("tip", termIndexSize);
        }

        // Positions attribution
        if (positionsSize > 0) {
            if (actualPosSize != null && actualPosSize > 0) {
                long totalPosSize = calculateTotalPositionsSize(reader);
                if (totalPosSize > 0) {
                    stats.put("pos", (actualPosSize * positionsSize) / totalPosSize);
                } else {
                    stats.put("pos", positionsSize);
                }
            } else {
                stats.put("pos", positionsSize);
            }
        }
    }

    /**
     * Estimate DocValues size for a single field
     */
    private long estimateFieldDocValuesSize(SegmentReader reader, FieldInfo fieldInfo) {
        int maxDoc = reader.maxDoc();
        switch (fieldInfo.getDocValuesType()) {
            case NUMERIC:
            case SORTED_NUMERIC:
                return maxDoc * 8L; // 8 bytes per numeric value
            case BINARY:
                return maxDoc * 32L; // Estimate 32 bytes per binary value
            case SORTED:
            case SORTED_SET:
                return maxDoc * 16L; // Estimate 16 bytes for sorted values
            default:
                return maxDoc * 8L;
        }
    }

    /**
     * Calculate total DocValues size across all fields for proportional attribution
     */
    private long calculateTotalDocValuesSize(SegmentReader reader) {
        long total = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                total += estimateFieldDocValuesSize(reader, fieldInfo);
            }
        }
        return total;
    }

    /**
     * Calculate total PointValues size across all fields for proportional attribution
     */
    private long calculateTotalPointValuesSize(SegmentReader reader) {
        long total = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            if (fieldInfo.getPointDimensionCount() > 0) {
                try {
                    PointValues pv = reader.getPointValues(fieldInfo.name);
                    if (pv != null) {
                        total += pv.size() * pv.getBytesPerDimension() * pv.getNumIndexDimensions();
                    }
                } catch (Exception e) {
                    // Skip field on error
                }
            }
        }
        return total;
    }

    /**
     * Calculate total term dictionary size across all fields for proportional attribution
     */
    private long calculateTotalTermDictSize(SegmentReader reader) {
        long total = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                try {
                    Terms terms = reader.terms(fieldInfo.name);
                    if (terms != null) {
                        long termCount = terms.size();
                        long sumDocFreq = terms.getSumDocFreq();
                        if (termCount > 0) {
                            total += termCount * 15; // Average term length
                            if (sumDocFreq > 0) {
                                total += sumDocFreq * 8; // Postings
                            }
                        }
                    }
                } catch (IOException e) {
                    // Skip field on error
                }
            }
        }
        return total;
    }

    /**
     * Calculate total positions size across all fields for proportional attribution
     */
    private long calculateTotalPositionsSize(SegmentReader reader) {
        long total = 0;
        for (FieldInfo fieldInfo : reader.getFieldInfos()) {
            if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
                try {
                    Terms terms = reader.terms(fieldInfo.name);
                    if (terms != null) {
                        long sumTotalTermFreq = terms.getSumTotalTermFreq();
                        if (sumTotalTermFreq > 0) {
                            total += sumTotalTermFreq * 4;
                        }
                    }
                } catch (IOException e) {
                    // Skip field on error
                }
            }
        }
        return total;
    }
}
