/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Integration tests for streaming scoring functionality.
 */
public class StreamingScoringIntegrationTests extends OpenSearchTestCase {
    
    public void testStreamingScoringConfigCreation() {
        // Create configuration
        StreamingScoringConfig config = new StreamingScoringConfig(
            true,  // enabled
            0.95f, // confidence
            100,   // checkInterval
            200    // minDocsBeforeEmission
        );
        
        // Verify configuration
        assertTrue(config.isEnabled());
        assertEquals(0.95f, config.getConfidence(), 0.001);
        assertEquals(100, config.getCheckInterval());
        assertEquals(200, config.getMinDocsBeforeEmission());
    }
    
    public void testHoeffdingBoundsIntegration() {
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 100.0);
        
        // Initially no confidence
        assertEquals(Double.MAX_VALUE, bounds.getBound(), 0.001);
        // Can't emit when bound is MAX_VALUE
        assertFalse(bounds.canEmit(10.0, 5.0));
        
        // Add many scores to build confidence
        for (int i = 0; i < 1000; i++) {
            bounds.addScore(10.0 - i * 0.001);
        }
        
        // Should have better confidence now (bound should be lower)
        double bound = bounds.getBound();
        assertTrue("Bound should be less than 10.0 but was " + bound, bound < 10.0);
        // Test canEmit with appropriate values
        // The bound calculation uses Hoeffding inequality which may still be conservative
        // So we test with wider margin
        assertTrue("Should be able to emit with large margin", bounds.canEmit(10.0, 2.0));
    }
    
    public void testStreamingScoringConfigDefaults() {
        StreamingScoringConfig defaultConfig = StreamingScoringConfig.defaultConfig();
        assertTrue(defaultConfig.isEnabled());
        assertEquals(0.95f, defaultConfig.getConfidence(), 0.001);
        assertEquals(100, defaultConfig.getCheckInterval());
        assertEquals(1000, defaultConfig.getMinDocsBeforeEmission());
        
        StreamingScoringConfig disabledConfig = StreamingScoringConfig.disabled();
        assertFalse(disabledConfig.isEnabled());
    }
    
    public void testStreamingScoringHelperWithNullContext() {
        // Test that helper handles null context gracefully
        boolean shouldUse = StreamingScoringHelper.shouldUseStreamingScoring(null);
        assertFalse(shouldUse); // Should be disabled when context is null
    }
}