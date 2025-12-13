/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SegmentTopologyAnalyzerTests extends OpenSearchTestCase {

    public void testSegmentCreation() {
        // Test that the Segment inner class can be created
        SegmentTopologyAnalyzer.Segment segment = new SegmentTopologyAnalyzer.Segment(
            "test-segment",
            1024 * 1024, // 1MB
            1000, // 1000 docs
            0, // 0 deleted
            "9.12.2" // version
        );

        assertEquals("test-segment", segment.name);
        assertEquals(1024 * 1024, segment.sizeBytes);
        assertEquals(1000, segment.docCount);
        assertEquals(0, segment.deletedDocs);
        assertEquals("9.12.2", segment.version);
    }

    public void testSegmentMetricsCreation() {
        // Test SegmentMetrics creation
        SegmentTopologyAnalyzer.SegmentMetrics metrics = new SegmentTopologyAnalyzer.SegmentMetrics(
            5, // segmentCount
            5 * 1024 * 1024, // totalSizeBytes (5MB)
            5000, // totalDocs
            1024 * 1024, // minSizeBytes (1MB)
            2 * 1024 * 1024, // maxSizeBytes (2MB)
            1024 * 1024, // medianSizeBytes (1MB)
            1024 * 1024, // meanSizeBytes (1MB)
            1024 * 1024, // variance
            2000 // skew
        );

        assertEquals(5, metrics.segmentCount);
        assertEquals(5 * 1024 * 1024, metrics.totalSizeBytes);
        assertEquals(5000, metrics.totalDocs);
        assertEquals(1024 * 1024, metrics.minSizeBytes);
        assertEquals(2 * 1024 * 1024, metrics.maxSizeBytes);
        assertEquals(1024 * 1024, metrics.medianSizeBytes);
        assertEquals(1024 * 1024, metrics.meanSizeBytes);
        assertEquals(1024 * 1024, metrics.variance, 0.0);
        assertEquals(2000, metrics.skew);
        // CV = stddev / mean = sqrt(variance) / meanSizeBytes = 1024 / (1024*1024) ≈
        // 0.000976
        double expectedCV = Math.sqrt(1024 * 1024) / (1024 * 1024);
        assertEquals("Coefficient of variation should match expected calculation", expectedCV, metrics.coefficientOfVariation, 0.0001);
    }

    public void testMergePolicyRecommendationsCreation() {
        // Test MergePolicyRecommendations creation
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = new SegmentTopologyAnalyzer.MergePolicyRecommendations(
            true, // hasVarianceIssue
            false, // hasSkewIssue
            true, // hasTooManySegments
            false, // hasTooFewSegments
            0.8, // sizeVariance
            2.5, // sizeSkew
            1024 * 1024 * 1024, // recommendedMaxSegmentSize (1GB)
            50 * 1024 * 1024, // recommendedFloorSegmentSize (50MB)
            10 // optimalSegmentCount
        );

        assertTrue("Should have variance issue", recommendations.hasVarianceIssue);
        assertFalse("Should not have skew issue", recommendations.hasSkewIssue);
        assertTrue("Should have too many segments", recommendations.hasTooManySegments);
        assertFalse("Should not have too few segments", recommendations.hasTooFewSegments);
        assertEquals(0.8, recommendations.sizeVariance, 0.01);
        assertEquals(2.5, recommendations.sizeSkew, 0.01);
        assertEquals(1024 * 1024 * 1024, recommendations.recommendedMaxSegmentSize);
        assertEquals(50 * 1024 * 1024, recommendations.recommendedFloorSegmentSize);
        assertEquals(10, recommendations.optimalSegmentCount);
    }

    public void testAnalysisWithEmptySegments() {
        SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
        SegmentTopologyAnalyzer analyzer = new SegmentTopologyAnalyzer("test-index", infos);
        SegmentTopologyAnalyzer.SegmentMetrics metrics = analyzer.getMetrics();
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = analyzer.analyzeAndRecommend();

        assertEquals(0, metrics.segmentCount);
        assertEquals(0L, metrics.totalSizeBytes);
        assertEquals(0.0, metrics.variance, 0.001);

        assertFalse("Should not have variance issue", recommendations.hasVarianceIssue);
        assertTrue("Should have too few segments", recommendations.hasTooFewSegments);
    }

    public void testAnalysisWithSingleSegment() throws IOException {
        SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
        long size = 100L * 1024 * 1024; // 100 MB
        infos.add(createSegmentCommitInfo("_0", size));

        SegmentTopologyAnalyzer analyzer = new SegmentTopologyAnalyzer("test-index", infos);
        SegmentTopologyAnalyzer.SegmentMetrics metrics = analyzer.getMetrics();
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = analyzer.analyzeAndRecommend();

        assertEquals(1, metrics.segmentCount);
        assertEquals(size, metrics.totalSizeBytes);
        assertEquals(size, metrics.medianSizeBytes);
        assertEquals(size, metrics.meanSizeBytes);
        assertEquals(0.0, metrics.variance, 0.001);

        assertFalse("Should not have variance issue", recommendations.hasVarianceIssue);
        assertTrue("Should have too few segments", recommendations.hasTooFewSegments);
    }

    public void testAnalysisWithRealisticDistribution() throws IOException {
        SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
        // Create a distribution: 10 segments of varying sizes around 100MB
        long[] sizes = new long[] {
            90,
            95,
            100,
            105,
            110, // ~100MB
            98,
            102,
            99,
            101,
            100 // ~100MB
        };

        long totalSize = 0;
        for (int i = 0; i < sizes.length; i++) {
            long sizeBytes = sizes[i] * 1024 * 1024;
            infos.add(createSegmentCommitInfo("_" + i, sizeBytes));
            totalSize += sizeBytes;
        }

        SegmentTopologyAnalyzer analyzer = new SegmentTopologyAnalyzer("test-index", infos);
        SegmentTopologyAnalyzer.SegmentMetrics metrics = analyzer.getMetrics();
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = analyzer.analyzeAndRecommend();

        assertEquals(10, metrics.segmentCount);
        assertEquals(totalSize, metrics.totalSizeBytes);
        assertEquals(100L * 1024 * 1024, metrics.meanSizeBytes);

        // Variance should be low
        assertTrue("Variance should be low", metrics.coefficientOfVariation < 0.2);
        assertFalse("Should not have variance issue", recommendations.hasVarianceIssue);
        assertFalse("Should not have too few segments", recommendations.hasTooFewSegments);
        assertFalse("Should not have too many segments", recommendations.hasTooManySegments);
    }

    public void testAnalysisWithExtremeVariance() throws IOException {
        SegmentInfos infos = new SegmentInfos(Version.LATEST.major);
        // One huge segment (5GB) and many tiny ones (1MB)
        infos.add(createSegmentCommitInfo("_huge", 5L * 1024 * 1024 * 1024));
        for (int i = 0; i < 20; i++) {
            infos.add(createSegmentCommitInfo("_small_" + i, 1L * 1024 * 1024));
        }

        SegmentTopologyAnalyzer analyzer = new SegmentTopologyAnalyzer("test-index", infos);
        SegmentTopologyAnalyzer.SegmentMetrics metrics = analyzer.getMetrics();
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = analyzer.analyzeAndRecommend();

        assertEquals(21, metrics.segmentCount);
        assertTrue("Variance should be high", metrics.coefficientOfVariation > 1.0);
        assertTrue("Skew should be high", metrics.skew > 3000); // > 3.0 ratio

        assertTrue("Should have variance issue", recommendations.hasVarianceIssue);
        assertTrue("Should have skew issue", recommendations.hasSkewIssue);
        assertTrue("Should have too many segments", recommendations.hasTooManySegments);
    }

    private SegmentCommitInfo createSegmentCommitInfo(String name, long sizeInBytes) throws IOException {
        Directory directory = mock(Directory.class);
        when(directory.fileLength(any())).thenAnswer(invocation -> {
            String fileName = invocation.getArgument(0);
            if (fileName.equals(name + ".cfe")) {
                return sizeInBytes;
            }
            return 0L;
        });

        SegmentInfo info = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LATEST,
            name,
            1,
            false,
            false,
            org.apache.lucene.codecs.Codec.getDefault(),
            java.util.Collections.emptyMap(),
            StringHelper.randomId(),
            java.util.Collections.emptyMap(),
            null
        );
        info.setFiles(java.util.Collections.singleton(name + ".cfe"));

        return new SegmentCommitInfo(info, 0, 0, 0, 0, 0, null);
    }
}
