/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.merge;

import org.opensearch.test.OpenSearchTestCase;

public class SegmentTopologyAnalyzerTests extends OpenSearchTestCase {

    public void testSegmentCreation() {
        // Test that the Segment inner class can be created
        SegmentTopologyAnalyzer.Segment segment = new SegmentTopologyAnalyzer.Segment(
            "test-segment",
            1024 * 1024, // 1MB
            1000,        // 1000 docs
            0,           // 0 deleted
            "9.12.2"     // version
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
            5,                    // segmentCount
            5 * 1024 * 1024,      // totalSizeBytes (5MB)
            5000,                 // totalDocs
            1024 * 1024,         // minSizeBytes (1MB)
            2 * 1024 * 1024,      // maxSizeBytes (2MB)
            1024 * 1024,          // medianSizeBytes (1MB)
            1024 * 1024,          // meanSizeBytes (1MB)
            1024 * 1024,          // variance
            2000                  // skew
        );

        assertEquals(5, metrics.segmentCount);
        assertEquals(5 * 1024 * 1024, metrics.totalSizeBytes);
        assertEquals(5000, metrics.totalDocs);
        assertEquals(1024 * 1024, metrics.minSizeBytes);
        assertEquals(2 * 1024 * 1024, metrics.maxSizeBytes);
        assertTrue("Coefficient of variation should be calculated", metrics.coefficientOfVariation >= 0);
    }

    public void testMergePolicyRecommendationsCreation() {
        // Test MergePolicyRecommendations creation
        SegmentTopologyAnalyzer.MergePolicyRecommendations recommendations = new SegmentTopologyAnalyzer.MergePolicyRecommendations(
            true,  // hasVarianceIssue
            false, // hasSkewIssue
            true,  // hasTooManySegments
            false, // hasTooFewSegments
            0.8,   // sizeVariance
            2.5,   // sizeSkew
            1024 * 1024 * 1024, // recommendedMaxSegmentSize (1GB)
            50 * 1024 * 1024,   // recommendedFloorSegmentSize (50MB)
            10                  // optimalSegmentCount
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
}
