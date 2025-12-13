/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.AdaptiveMergePolicyCalculator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Analyzes segment topology to identify problematic distributions that cause
 * benchmark variance and provides recommendations for optimal merge settings.
 *
 * This addresses the problem described in:
 * https://github.com/opensearch-project/OpenSearch/issues/11163
 *
 * Implementation notes:
 * - Recommendations use smooth interpolation across shard-size decades to avoid
 *   stepwise jumps at category thresholds.
 * - Max segment size recommendations are capped at 5GB to align with Lucene.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.3.0")
public class SegmentTopologyAnalyzer implements Writeable {

    private static final Logger logger = LogManager.getLogger(SegmentTopologyAnalyzer.class);

    private final List<Segment> segments;
    private final SegmentMetrics metrics;
    private final String indexName;
    private final long totalSizeBytes;
    private final int totalDocs;

    public SegmentTopologyAnalyzer(String indexName, SegmentInfos segmentInfos) {
        this.indexName = indexName;
        List<Segment> tempSegments = new ArrayList<>();

        long totalSize = 0;
        int totalDocCount = 0;
        int failedSegments = 0;
        int totalSegments = segmentInfos.size();

        for (SegmentCommitInfo segmentInfo : segmentInfos) {
            try {
                long segmentSize = segmentInfo.sizeInBytes();
                int docCount = segmentInfo.info.maxDoc();

                tempSegments.add(
                    new Segment(
                        segmentInfo.info.name,
                        segmentSize,
                        docCount,
                        segmentInfo.getDelCount(),
                        segmentInfo.info.getVersion().toString()
                    )
                );

                totalSize += segmentSize;
                totalDocCount += docCount;
            } catch (IOException e) {
                failedSegments++;
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "Failed to analyze segment [{}] for index [{}], skipping: {}",
                            segmentInfo.info.name,
                            indexName,
                            e.getMessage()
                        ),
                        e
                    );
                }
            }
        }

        // Log warning if significant number of segments failed or all segments failed
        if (failedSegments > 0) {
            double failureRate = (double) failedSegments / totalSegments;
            if (tempSegments.isEmpty()) {
                // All segments failed - analysis will be based on empty data
                logger.warn(
                    "All {} segments failed to analyze for index [{}]. "
                        + "Segment topology analysis will be based on empty data, which may lead to incorrect recommendations.",
                    totalSegments,
                    indexName
                );
            } else if (failureRate > 0.5) {
                // More than half failed - significant data loss
                logger.warn(
                    "{} of {} segments ({}%) failed to analyze for index [{}]. "
                        + "Segment topology analysis may be based on incomplete data, which could lead to suboptimal recommendations.",
                    failedSegments,
                    totalSegments,
                    String.format(Locale.ROOT, "%.1f", failureRate * 100.0),
                    indexName
                );
            } else if (logger.isTraceEnabled()) {
                // Log at trace level for minor failures
                logger.trace(
                    "{} of {} segments failed to analyze for index [{}] (analysis may be slightly incomplete)",
                    failedSegments,
                    totalSegments,
                    indexName
                );
            }
        }

        this.segments = Collections.unmodifiableList(tempSegments);
        this.totalSizeBytes = totalSize;
        this.totalDocs = totalDocCount;
        this.metrics = calculateMetrics();
    }

    public SegmentTopologyAnalyzer(StreamInput in) throws IOException {
        this.indexName = in.readString();
        this.totalSizeBytes = in.readVLong();
        this.totalDocs = in.readVInt();

        int segmentCount = in.readVInt();
        List<Segment> tempSegments = new ArrayList<>(segmentCount);
        for (int i = 0; i < segmentCount; i++) {
            tempSegments.add(new Segment(in));
        }
        this.segments = Collections.unmodifiableList(tempSegments);
        this.metrics = calculateMetrics();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(indexName);
        out.writeVLong(totalSizeBytes);
        out.writeVInt(totalDocs);
        out.writeVInt(segments.size());
        for (Segment segment : segments) {
            segment.writeTo(out);
        }
    }

    private SegmentMetrics calculateMetrics() {
        if (segments.isEmpty()) {
            return new SegmentMetrics(0, 0L, 0, 0L, 0L, 0L, 0L, 0.0, 0L);
        }

        // Calculate basic statistics
        int segmentCount = segments.size();
        long totalSize = totalSizeBytes;
        int totalDocs = this.totalDocs;

        // Calculate size statistics
        List<Long> sizes = new ArrayList<>();
        for (Segment segment : segments) {
            sizes.add(segment.sizeBytes);
        }
        Collections.sort(sizes);

        long minSize = sizes.get(0);
        long maxSize = sizes.get(sizes.size() - 1);
        long medianSize;
        int mid = sizes.size() / 2;
        if (sizes.size() % 2 == 0) {
            // For even-sized lists, median is the average of the two middle values
            medianSize = (sizes.get(mid - 1) + sizes.get(mid)) / 2;
        } else {
            // For odd-sized lists, median is the middle value
            medianSize = sizes.get(mid);
        }
        long meanSize = totalSize / segmentCount;

        // Calculate variance and skew
        double varianceSum = 0.0;
        for (long size : sizes) {
            double diff = (double) size - meanSize;
            varianceSum += diff * diff;
        }
        double variance = varianceSum / segmentCount;

        // Calculate skew (simplified measure)
        double skew = maxSize > 0 ? (double) maxSize / meanSize : 0;

        return new SegmentMetrics(
            segmentCount,
            totalSize,
            totalDocs,
            minSize,
            maxSize,
            medianSize,
            meanSize,
            variance,
            (long) (skew * 1000)
        );
    }

    public MergePolicyRecommendations analyzeAndRecommend() {
        boolean hasVarianceIssue = metrics.coefficientOfVariation > 0.5; // High variance
        boolean hasSkewIssue = metrics.skew > 3000; // One segment dominates (skew stored as skew * 1000)
        boolean hasTooManySegments = segments.size() > 20; // Too many small segments
        boolean hasTooFewSegments = segments.size() < 3; // Too few segments

        // Calculate recommended settings (smooth interpolation, capped at 5GB)
        long recommendedMaxSegmentSize = AdaptiveMergePolicyCalculator.calculateSmoothMaxSegmentSize(totalSizeBytes);
        long recommendedFloorSegmentSize = AdaptiveMergePolicyCalculator.calculateSmoothFloorSegmentSize(totalSizeBytes);
        int optimalSegmentCount = (int) Math.round(AdaptiveMergePolicyCalculator.calculateSmoothSegmentsPerTier(totalSizeBytes));

        return new MergePolicyRecommendations(
            hasVarianceIssue,
            hasSkewIssue,
            hasTooManySegments,
            hasTooFewSegments,
            metrics.variance,
            // Expose skew as an unscaled double instead of the internal ×1000 representation
            (double) metrics.skew / 1000.0,
            recommendedMaxSegmentSize,
            recommendedFloorSegmentSize,
            optimalSegmentCount
        );
    }

    // Getters
    /**
     * Returns the list of segments. The returned list is unmodifiable to preserve
     * internal invariants between segments, metrics, and recommendations.
     */
    public List<Segment> getSegments() {
        return segments;
    }

    public SegmentMetrics getMetrics() {
        return metrics;
    }

    public String getIndexName() {
        return indexName;
    }

    public long getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public int getTotalDocs() {
        return totalDocs;
    }

    // Inner classes
    /**
     * Represents a single segment within an index, containing metadata about its size,
     * document count, and other characteristics.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.3.0")
    public static class Segment implements Writeable {
        public final String name;
        public final long sizeBytes;
        public final int docCount;
        public final int deletedDocs;
        public final String version;

        public Segment(String name, long sizeBytes, int docCount, int deletedDocs, String version) {
            this.name = name;
            this.sizeBytes = sizeBytes;
            this.docCount = docCount;
            this.deletedDocs = deletedDocs;
            this.version = version;
        }

        public Segment(StreamInput in) throws IOException {
            this.name = in.readString();
            this.sizeBytes = in.readVLong();
            this.docCount = in.readVInt();
            this.deletedDocs = in.readVInt();
            this.version = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(sizeBytes);
            out.writeVInt(docCount);
            out.writeVInt(deletedDocs);
            out.writeString(version);
        }
    }

    /**
     * Contains calculated metrics about the segment topology of an index,
     * including statistical measures like variance, skew, and distribution characteristics.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.3.0")
    public static class SegmentMetrics {
        public final int segmentCount;
        public final long totalSizeBytes;
        public final int totalDocs;
        public final long minSizeBytes;
        public final long maxSizeBytes;
        public final long medianSizeBytes;
        public final long meanSizeBytes;
        public final double variance;
        public final long skew; // multiplied by 1000 for precision

        // Calculated fields
        public final double coefficientOfVariation;

        public SegmentMetrics(
            int segmentCount,
            long totalSizeBytes,
            int totalDocs,
            long minSizeBytes,
            long maxSizeBytes,
            long medianSizeBytes,
            long meanSizeBytes,
            double variance,
            long skew
        ) {
            this.segmentCount = segmentCount;
            this.totalSizeBytes = totalSizeBytes;
            this.totalDocs = totalDocs;
            this.minSizeBytes = minSizeBytes;
            this.maxSizeBytes = maxSizeBytes;
            this.medianSizeBytes = medianSizeBytes;
            this.meanSizeBytes = meanSizeBytes;
            this.variance = variance;
            this.skew = skew;

            // Calculate derived metrics
            this.coefficientOfVariation = meanSizeBytes > 0 ? Math.sqrt(variance) / meanSizeBytes : 0;
        }
    }

    /**
     * Contains recommendations for optimal merge policy settings based on segment topology analysis,
     * including flags for identified issues and suggested parameter values.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.3.0")
    public static class MergePolicyRecommendations {
        public final boolean hasVarianceIssue;
        public final boolean hasSkewIssue;
        public final boolean hasTooManySegments;
        public final boolean hasTooFewSegments;
        public final double sizeVariance;
        public final double sizeSkew;
        public final long recommendedMaxSegmentSize;
        public final long recommendedFloorSegmentSize;
        public final int optimalSegmentCount;

        public MergePolicyRecommendations(
            boolean hasVarianceIssue,
            boolean hasSkewIssue,
            boolean hasTooManySegments,
            boolean hasTooFewSegments,
            double sizeVariance,
            double sizeSkew,
            long recommendedMaxSegmentSize,
            long recommendedFloorSegmentSize,
            int optimalSegmentCount
        ) {
            this.hasVarianceIssue = hasVarianceIssue;
            this.hasSkewIssue = hasSkewIssue;
            this.hasTooManySegments = hasTooManySegments;
            this.hasTooFewSegments = hasTooFewSegments;
            this.sizeVariance = sizeVariance;
            this.sizeSkew = sizeSkew;
            this.recommendedMaxSegmentSize = recommendedMaxSegmentSize;
            this.recommendedFloorSegmentSize = recommendedFloorSegmentSize;
            this.optimalSegmentCount = optimalSegmentCount;
        }
    }
}
