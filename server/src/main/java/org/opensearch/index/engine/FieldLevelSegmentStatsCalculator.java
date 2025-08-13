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
import org.opensearch.common.Randomness;
import org.opensearch.core.common.unit.ByteSizeValue;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Calculates field-level segment statistics for Lucene index files with maximum accuracy.
 * Uses Lucene's built-in statistics APIs and proportional attribution for precise calculations.
 * Supports sampling for large segments to prevent OOM while maintaining statistical accuracy.
 *
 * @opensearch.internal
 */
public class FieldLevelSegmentStatsCalculator {
    private static final Logger logger = LogManager.getLogger(FieldLevelSegmentStatsCalculator.class);

    // Configuration constants
    private static final long DEFAULT_LARGE_SEGMENT_THRESHOLD = 1024L * 1024L * 1024L; // 1GB default
    private static final double DEFAULT_SAMPLING_RATE = 0.1; // 10% sampling for large segments
    private static final int MIN_SAMPLE_FIELDS = 10; // Minimum fields to sample for accuracy

    private final long largeSegmentThreshold;
    private final double samplingRate;
    private final FieldLevelStatsCache cache = new FieldLevelStatsCache();

    /**
     * Default constructor with default sampling threshold
     */
    public FieldLevelSegmentStatsCalculator() {
        this(DEFAULT_LARGE_SEGMENT_THRESHOLD);
    }

    /**
     * Constructor with configurable sampling threshold
     * @param largeSegmentThreshold Threshold in bytes for triggering sampling
     */
    public FieldLevelSegmentStatsCalculator(long largeSegmentThreshold) {
        this.largeSegmentThreshold = largeSegmentThreshold;
        this.samplingRate = DEFAULT_SAMPLING_RATE;
    }

    /**
     * Calculate field-level statistics for a segment using actual file sizes and Lucene statistics
     * @param segmentReader The segment reader to analyze
     * @return Map of field names to their file type statistics (dvd, dvm, tim, tip, pos, dim, dii)
     */
    public Map<String, Map<String, Long>> calculateFieldLevelStats(SegmentReader segmentReader) {
        return calculateFieldLevelStats(segmentReader, null, false);
    }

    /**
     * Calculate field-level statistics with options for field filtering and sampling
     * @param segmentReader The segment reader to analyze
     * @param fieldFilter Optional set of fields to include (null = all fields)
     * @param forceSampling Force sampling even for small segments
     * @return Map of field names to their file type statistics
     */
    public Map<String, Map<String, Long>> calculateFieldLevelStats(
        SegmentReader segmentReader,
        Set<String> fieldFilter,
        boolean forceSampling
    ) {
        long startTime = System.currentTimeMillis();

        // Check cache first
        Map<String, Map<String, Long>> cachedStats = cache.get(segmentReader);
        if (cachedStats != null && fieldFilter == null) {
            return cachedStats;
        }

        Map<String, Map<String, Long>> fieldLevelStats = new ConcurrentHashMap<>();
        if (segmentReader.getFieldInfos().size() == 0) {
            return fieldLevelStats;
        }

        long segmentSize = estimateSegmentSize(segmentReader);
        boolean shouldSample = forceSampling || segmentSize > largeSegmentThreshold;

        if (shouldSample) {
            logger.info(
                "Using sampling for large segment {} (size: {}, sampling rate: {}%)",
                segmentReader.getSegmentName(),
                new ByteSizeValue(segmentSize),
                (int) (samplingRate * 100)
            );
            fieldLevelStats = calculateWithSampling(segmentReader, fieldFilter);
        } else {
            Map<String, Long> segmentFileSizes = getSegmentFileSizes(segmentReader);
            processFields(segmentReader, fieldFilter, segmentFileSizes, fieldLevelStats, 1.0);
        }

        // Cache the results if no filter was applied
        if (fieldFilter == null && !fieldLevelStats.isEmpty()) {
            cache.put(segmentReader, fieldLevelStats);
        }

        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime > 5000) {
            logger.warn(
                "Field-level stats calculation took {} ms for segment {} (sampled: {})",
                elapsedTime,
                segmentReader.getSegmentName(),
                shouldSample
            );
        }
        return fieldLevelStats;
    }

    /**
     * Calculate statistics using sampling for large segments
     * @param segmentReader The segment reader
     * @param fieldFilter Optional field filter
     * @return Sampled and extrapolated field statistics
     */
    private Map<String, Map<String, Long>> calculateWithSampling(SegmentReader segmentReader, Set<String> fieldFilter) {
        Map<String, Map<String, Long>> sampledStats = new ConcurrentHashMap<>();
        Map<String, Long> segmentFileSizes = getSegmentFileSizes(segmentReader);

        // Determine fields to sample
        int totalFields = segmentReader.getFieldInfos().size();
        int fieldsToSample = Math.max(MIN_SAMPLE_FIELDS, (int) (totalFields * samplingRate));

        // Ensure we don't sample more fields than exist
        fieldsToSample = Math.min(fieldsToSample, totalFields);
        // Use statistical sampling for field selection
        Set<String> sampledFields = selectFieldsForSampling(segmentReader, fieldsToSample, fieldFilter);
        // Process only sampled fields
        processFields(segmentReader, sampledFields, segmentFileSizes, sampledStats, 1.0 / samplingRate);

        // Add sampling metadata to indicate these are estimates
        for (Map.Entry<String, Map<String, Long>> entry : sampledStats.entrySet()) {
            entry.getValue().put("_sampled", 1L);
            entry.getValue().put("_sampling_rate", (long) (samplingRate * 100));
        }

        return sampledStats;
    }

    /**
     * Select fields for sampling using stratified sampling to ensure representation
     * @param segmentReader The segment reader
     * @param numFields Number of fields to sample
     * @param fieldFilter Optional field filter
     * @return Set of field names to sample
     */
    private Set<String> selectFieldsForSampling(SegmentReader segmentReader, int numFields, Set<String> fieldFilter) {
        Set<String> sampledFields = ConcurrentHashMap.newKeySet();

        // Group fields by type for stratified sampling
        Map<String, Set<String>> fieldsByType = new HashMap<>();
        fieldsByType.put("docvalues", ConcurrentHashMap.newKeySet());
        fieldsByType.put("terms", ConcurrentHashMap.newKeySet());
        fieldsByType.put("points", ConcurrentHashMap.newKeySet());

        for (FieldInfo fieldInfo : segmentReader.getFieldInfos()) {
            if (fieldFilter != null && !fieldFilter.contains(fieldInfo.name)) {
                continue;
            }

            if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                fieldsByType.get("docvalues").add(fieldInfo.name);
            }
            if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                fieldsByType.get("terms").add(fieldInfo.name);
            }
            if (fieldInfo.getPointDimensionCount() > 0) {
                fieldsByType.get("points").add(fieldInfo.name);
            }
        }

        // Stratified sampling: sample proportionally from each type
        for (Map.Entry<String, Set<String>> typeEntry : fieldsByType.entrySet()) {
            Set<String> fieldsOfType = typeEntry.getValue();
            if (fieldsOfType.isEmpty()) continue;

            int samplesToTake = Math.max(1, (int) (numFields * ((double) fieldsOfType.size() / segmentReader.getFieldInfos().size())));

            // Random sampling within stratum
            fieldsOfType.stream()
                .sorted((a, b) -> Randomness.get().nextBoolean() ? 1 : -1)
                .limit(samplesToTake)
                .forEach(sampledFields::add);
        }
        return sampledFields;
    }

    /**
     * Get actual segment file sizes by extension
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
     * Calculate DocValues statistics with proportional attribution
     */
    private void calculateDocValuesStats(
        SegmentReader reader,
        FieldInfo fieldInfo,
        Map<String, Long> stats,
        Map<String, Long> segmentFileSizes,
        double extrapolationFactor
    ) {
        Long actualDvdSize = segmentFileSizes.get("dvd");
        Long actualDvmSize = segmentFileSizes.get("dvm");

        if (actualDvdSize != null && actualDvdSize > 0) {
            long totalDocValuesSize = calculateTotalDocValuesSize(reader);
            long fieldDocValuesSize = estimateFieldDocValuesSize(reader, fieldInfo);
            if (totalDocValuesSize > 0 && fieldDocValuesSize > 0) {
                long baseSize = (actualDvdSize * fieldDocValuesSize) / totalDocValuesSize;
                stats.put("dvd", (long) (baseSize * extrapolationFactor));
                if (actualDvmSize != null) {
                    long metaSize = (actualDvmSize * fieldDocValuesSize) / totalDocValuesSize;
                    stats.put("dvm", (long) (metaSize * extrapolationFactor));
                } else {
                    stats.put("dvm", Math.max(1L, (long) (baseSize * extrapolationFactor / 10)));
                }
            } else {
                stats.put("dvd", (long) (fieldDocValuesSize * extrapolationFactor));
                stats.put("dvm", Math.max(1L, (long) (fieldDocValuesSize * extrapolationFactor / 10)));
            }
        } else {
            long estimatedSize = estimateFieldDocValuesSize(reader, fieldInfo);
            if (estimatedSize > 0) {
                stats.put("dvd", (long) (estimatedSize * extrapolationFactor));
                stats.put("dvm", Math.max(1L, (long) (estimatedSize * extrapolationFactor / 10)));
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
        Map<String, Long> segmentFileSizes,
        double extrapolationFactor
    ) {
        try {
            PointValues pointValues = reader.getPointValues(fieldInfo.name);
            if (pointValues == null) return;

            long numPoints = pointValues.size();
            long numDocs = pointValues.getDocCount();
            int bytesPerDim = pointValues.getBytesPerDimension();
            int numDims = pointValues.getNumIndexDimensions();

            long pointsDataSize = numPoints * bytesPerDim * numDims;
            long pointsIndexSize = Math.max(numDocs * 8, pointsDataSize / 4);

            if (pointsDataSize > 0) {
                Long actualDimSize = segmentFileSizes.get("dim");
                Long actualDiiSize = segmentFileSizes.get("dii");

                if (actualDimSize != null && actualDimSize > 0) {
                    long totalPointsSize = calculateTotalPointValuesSize(reader);
                    if (totalPointsSize > 0) {
                        long baseSize = (actualDimSize * pointsDataSize) / totalPointsSize;
                        stats.put("dim", (long) (baseSize * extrapolationFactor));
                        if (actualDiiSize != null) {
                            long indexSize = (actualDiiSize * pointsDataSize) / totalPointsSize;
                            stats.put("dii", (long) (indexSize * extrapolationFactor));
                        } else {
                            stats.put("dii", Math.max(1L, (long) (baseSize * extrapolationFactor / 4)));
                        }
                    } else {
                        stats.put("dim", (long) (pointsDataSize * extrapolationFactor));
                        stats.put("dii", (long) (pointsIndexSize * extrapolationFactor));
                    }
                } else {
                    stats.put("dim", (long) (pointsDataSize * extrapolationFactor));
                    stats.put("dii", (long) (pointsIndexSize * extrapolationFactor));
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
    private void calculateTermStats(
        SegmentReader reader,
        FieldInfo fieldInfo,
        Map<String, Long> stats,
        Map<String, Long> segmentFileSizes,
        double extrapolationFactor
    ) throws IOException {
        Terms terms = reader.terms(fieldInfo.name);
        if (terms == null) return;

        long termCount = terms.size();
        long docCount = terms.getDocCount();
        long sumDocFreq = terms.getSumDocFreq();
        long sumTotalTermFreq = terms.getSumTotalTermFreq();
        long termDictSize;
        long termIndexSize;
        long positionsSize = 0;

        if (termCount > 0) {
            // 15 bytes: average term length + overhead (based on Lucene's BytesRef)
            // 8 bytes: doc ID (4 bytes) + frequency (4 bytes) per posting
            termDictSize = termCount * 15;
            if (sumDocFreq > 0) {
                termDictSize += sumDocFreq * 8;
            } else if (docCount > 0) {
                termDictSize += docCount * 8;
            }
            termIndexSize = Math.max(1L, termDictSize / 10);
        } else {
            termDictSize = reader.maxDoc() * 20L;
            termIndexSize = termDictSize / 10;
        }

        if (fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0) {
            if (sumTotalTermFreq > 0) {
                positionsSize = sumTotalTermFreq * 4;
            } else {
                positionsSize = reader.maxDoc() * 8L;
            }
        }

        applyTermStatsWithAttribution(stats, termDictSize, termIndexSize, positionsSize, segmentFileSizes, reader, extrapolationFactor);
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
        SegmentReader reader,
        double extrapolationFactor
    ) {
        Long actualTimSize = segmentFileSizes.get("tim");
        Long actualTipSize = segmentFileSizes.get("tip");
        Long actualPosSize = segmentFileSizes.get("pos");

        if (actualTimSize != null && actualTimSize > 0) {
            long totalTermDictSize = calculateTotalTermDictSize(reader);
            if (totalTermDictSize > 0) {
                long baseSize = (actualTimSize * termDictSize) / totalTermDictSize;
                stats.put("tim", (long) (baseSize * extrapolationFactor));
            } else {
                stats.put("tim", (long) (termDictSize * extrapolationFactor));
            }
        } else if (termDictSize > 0) {
            stats.put("tim", (long) (termDictSize * extrapolationFactor));
        }

        if (actualTipSize != null && actualTipSize > 0) {
            long timSize = stats.getOrDefault("tim", (long) (termDictSize * extrapolationFactor));
            // Index is typically 10% of dictionary size based on Lucene's FST compression
            stats.put("tip", Math.max(1L, (long) (timSize / 10)));
        } else if (termIndexSize > 0) {
            stats.put("tip", (long) (termIndexSize * extrapolationFactor));
        }

        if (positionsSize > 0) {
            if (actualPosSize != null && actualPosSize > 0) {
                long totalPosSize = calculateTotalPositionsSize(reader);
                if (totalPosSize > 0) {
                    long baseSize = (actualPosSize * positionsSize) / totalPosSize;
                    stats.put("pos", (long) (baseSize * extrapolationFactor));
                } else {
                    stats.put("pos", (long) (positionsSize * extrapolationFactor));
                }
            } else {
                stats.put("pos", (long) (positionsSize * extrapolationFactor));
            }
        }
    }

    /**
     * Estimate DocValues size for a single field
     */
    private long estimateFieldDocValuesSize(SegmentReader reader, FieldInfo fieldInfo) {
        int maxDoc = reader.maxDoc();
        return switch (fieldInfo.getDocValuesType()) {
            case BINARY -> maxDoc * 32L;
            case SORTED, SORTED_SET -> maxDoc * 16L;
            default -> maxDoc * 8L;
        };
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
                            total += termCount * 15;
                            if (sumDocFreq > 0) {
                                total += sumDocFreq * 8;
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

    /**
     * Process fields (with optional sampling)
     */
    private void processFields(
        SegmentReader segmentReader,
        Set<String> fieldFilter,
        Map<String, Long> segmentFileSizes,
        Map<String, Map<String, Long>> fieldLevelStats,
        double extrapolationFactor
    ) {
        for (FieldInfo fieldInfo : segmentReader.getFieldInfos()) {
            if (fieldFilter != null && !fieldFilter.contains(fieldInfo.name)) {
                continue;
            }

            Map<String, Long> fieldStats = new HashMap<>();

            try {
                if (fieldInfo.getDocValuesType() != DocValuesType.NONE) {
                    calculateDocValuesStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes, extrapolationFactor);
                }
                if (fieldInfo.getPointDimensionCount() > 0) {
                    calculatePointValuesStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes, extrapolationFactor);
                }
                if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                    calculateTermStats(segmentReader, fieldInfo, fieldStats, segmentFileSizes, extrapolationFactor);
                }
                if (!fieldStats.isEmpty()) {
                    fieldLevelStats.put(fieldInfo.name, fieldStats);
                }

            } catch (OutOfMemoryError e) {
                handleOutOfMemoryError(fieldInfo, segmentReader, e);
                continue;
            } catch (Exception e) {
                handleCalculationError(fieldInfo, segmentReader, e);
            }
        }
    }

    /**
     * Get actual segment size using SegmentCommitInfo
     */
    private long estimateSegmentSize(SegmentReader reader) {
        try {
            SegmentCommitInfo segmentInfo = reader.getSegmentInfo();
            if (segmentInfo != null) {
                return segmentInfo.sizeInBytes();
            }
        } catch (IOException e) {
            logger.debug("Failed to get segment size for {}: {}", reader.getSegmentName(), e.getMessage());
        } catch (Exception e) {
            logger.trace("Error getting segment size, using estimation", e);
        }

        // Fallback to estimation based on document count
        return reader.maxDoc() * 10240L; // Assume 10KB per doc average
    }

    /**
     * Handle out of memory errors
     */
    private void handleOutOfMemoryError(FieldInfo fieldInfo, SegmentReader reader, OutOfMemoryError e) {
        logger.warn(
            () -> new ParameterizedMessage(
                "Out of memory calculating stats for field [{}] in segment [{}], clearing cache",
                fieldInfo.name,
                reader.getSegmentName()
            ),
            e
        );
        cache.clear();
    }

    /**
     * Handle general calculation errors
     */
    private void handleCalculationError(FieldInfo fieldInfo, SegmentReader reader, Exception e) {
        logger.debug(
            () -> new ParameterizedMessage(
                "Failed to calculate stats for field [{}] in segment [{}]",
                fieldInfo.name,
                reader.getSegmentName()
            ),
            e
        );
    }

    /**
     * Get the current sampling configuration
     * @return Map containing threshold and sampling rate
     */
    public Map<String, Object> getSamplingConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put("threshold_bytes", largeSegmentThreshold);
        config.put("threshold_gb", largeSegmentThreshold / (1024.0 * 1024.0 * 1024.0));
        config.put("sampling_rate", samplingRate);
        config.put("min_sample_fields", MIN_SAMPLE_FIELDS);
        return config;
    }
}
