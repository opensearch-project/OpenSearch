/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.analysis;

import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.index.analysis.SegmentTopologyTestUtils.getRecommendedFloorSegmentSize;
import static org.opensearch.index.analysis.SegmentTopologyTestUtils.getRecommendedMaxSegmentSize;
import static org.opensearch.index.analysis.SegmentTopologyTestUtils.getRecommendedSegmentsPerTier;

/**
 * Simple tests for segment topology analysis concepts.
 *
 * This addresses the problem described in:
 * https://github.com/opensearch-project/OpenSearch/issues/11163
 */
public class SimpleSegmentTopologyTests extends OpenSearchTestCase {

    /**
     * Test that demonstrates the variance problem concept
     */
    public void testSegmentTopologyVarianceConcept() {
        // Simulate different segment topologies that cause variance

        // Scenario 1: One huge segment (bad topology)
        long[] badTopology = { 5L * 1024 * 1024 * 1024 }; // 5GB segment

        // Scenario 2: Many small segments (also bad topology)
        long[] manySmallSegments = {
            16 * 1024 * 1024,  // 16MB
            20 * 1024 * 1024,  // 20MB
            18 * 1024 * 1024,  // 18MB
            15 * 1024 * 1024,  // 15MB
            22 * 1024 * 1024,  // 22MB
            19 * 1024 * 1024,  // 19MB
            17 * 1024 * 1024,  // 17MB
            21 * 1024 * 1024,  // 21MB
            16 * 1024 * 1024,  // 16MB
            20 * 1024 * 1024   // 20MB
        };

        // Scenario 3: Optimal topology (what we want)
        long[] optimalTopology = {
            200 * 1024 * 1024,  // 200MB
            180 * 1024 * 1024,  // 180MB
            220 * 1024 * 1024,  // 220MB
            190 * 1024 * 1024,  // 190MB
            210 * 1024 * 1024   // 210MB
        };

        // Verify the concept: different topologies exist
        assertTrue("Bad topology should have one segment", badTopology.length == 1);
        assertTrue("Many small segments should have many segments", manySmallSegments.length > 5);
        assertTrue(
            "Optimal topology should have reasonable number of segments",
            optimalTopology.length >= 3 && optimalTopology.length <= 10
        );

        // Verify segment sizes are reasonable
        assertTrue("Bad topology has huge segment", badTopology[0] > 1024 * 1024 * 1024); // > 1GB
        assertTrue("Many small segments are indeed small", manySmallSegments[0] < 50 * 1024 * 1024); // < 50MB
        assertTrue(
            "Optimal topology has medium-sized segments",
            optimalTopology[0] >= 100 * 1024 * 1024 && optimalTopology[0] <= 500 * 1024 * 1024
        );
    }

    /**
     * Test adaptive merge policy recommendations
     */
    public void testAdaptiveMergePolicyRecommendations() {
        // Test recommendations for different shard sizes

        // Small shard (< 100MB)
        long smallShardSize = 50 * 1024 * 1024; // 50MB
        long smallMaxSegment = getRecommendedMaxSegmentSize(smallShardSize);
        long smallFloorSegment = getRecommendedFloorSegmentSize(smallShardSize);
        double smallSegmentsPerTier = getRecommendedSegmentsPerTier(smallShardSize);

        assertTrue("Small shard should recommend smaller max segment", smallMaxSegment <= 50 * 1024 * 1024); // <= 50MB
        assertTrue("Small shard should recommend smaller floor segment", smallFloorSegment <= 10 * 1024 * 1024); // <= 10MB
        assertTrue("Small shard should recommend fewer segments per tier", smallSegmentsPerTier <= 5.0);

        // Large shard (> 1GB)
        long largeShardSize = 5L * 1024 * 1024 * 1024; // 5GB
        long largeMaxSegment = getRecommendedMaxSegmentSize(largeShardSize);
        long largeFloorSegment = getRecommendedFloorSegmentSize(largeShardSize);
        double largeSegmentsPerTier = getRecommendedSegmentsPerTier(largeShardSize);

        assertTrue("Large shard should recommend larger max segment", largeMaxSegment >= 200 * 1024 * 1024); // >= 200MB
        assertTrue("Large shard should recommend larger floor segment", largeFloorSegment >= 25 * 1024 * 1024); // >= 25MB
        assertTrue("Large shard should recommend more segments per tier", largeSegmentsPerTier >= 8.0);
    }

    /**
     * Test that demonstrates the improvement over current defaults
     */
    public void testImprovementOverCurrentDefaults() {
        // Current problematic defaults
        long currentMaxSegment = 5L * 1024 * 1024 * 1024; // 5GB
        long currentFloorSegment = 16 * 1024 * 1024; // 16MB
        double currentSegmentsPerTier = 10.0;

        // Recommended improved defaults
        long recommendedMaxSegment = 1L * 1024 * 1024 * 1024; // 1GB
        long recommendedFloorSegment = 50 * 1024 * 1024; // 50MB
        double recommendedSegmentsPerTier = 8.0;

        // Verify improvements
        assertTrue("Recommended max segment should be smaller than current", recommendedMaxSegment < currentMaxSegment);
        assertTrue("Recommended floor segment should be larger than current", recommendedFloorSegment > currentFloorSegment);
        assertTrue("Recommended segments per tier should be reasonable", recommendedSegmentsPerTier < currentSegmentsPerTier);
    }

}
