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
 * Tests for Hoeffding bounds calculator.
 */
public class HoeffdingBoundsTests extends OpenSearchTestCase {
    
    public void testBoundCalculation() {
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 10.0);
        
        // Initially, bound should be very high
        assertEquals(Double.MAX_VALUE, bounds.getBound(), 0.001);
        
        // Add some scores
        for (int i = 0; i < 100; i++) {
            bounds.addScore(8.0);
        }
        
        // Bound should be finite now
        double bound = bounds.getBound();
        assertTrue("Bound should be finite after adding scores", bound < Double.MAX_VALUE);
        assertTrue("Bound should be positive", bound > 0);
        
        // Add more scores
        for (int i = 0; i < 900; i++) {
            bounds.addScore(7.0);
        }
        
        // Bound should decrease with more samples
        double newBound = bounds.getBound();
        assertTrue("Bound should decrease with more samples", newBound < bound);
    }
    
    public void testCanEmit() {
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 10.0);
        
        // Add scores with decreasing pattern
        for (int i = 0; i < 1000; i++) {
            bounds.addScore(10.0 - i * 0.005);
        }
        
        // Test emission decision
        // Lowest top-K score is 8.0, estimated max unseen is 6.0
        boolean canEmit = bounds.canEmit(8.0, 6.0);
        assertTrue("Should be able to emit with high confidence", canEmit);
        
        // Should not emit if unseen could be higher
        boolean cannotEmit = bounds.canEmit(8.0, 9.0);
        assertFalse("Should not emit when unseen could be higher", cannotEmit);
    }
    
    public void testEstimatedMaxUnseen() {
        HoeffdingBounds bounds = new HoeffdingBounds(0.95, 10.0);
        
        bounds.addScore(10.0);
        bounds.addScore(8.0);
        bounds.addScore(6.0);
        
        // Max seen is 10.0, estimated should be 90% of that
        assertEquals(9.0, bounds.getEstimatedMaxUnseen(), 0.001);
    }
    
    public void testConfidenceLevels() {
        // Higher confidence = larger bound (more conservative)
        HoeffdingBounds bounds95 = new HoeffdingBounds(0.95, 10.0);
        HoeffdingBounds bounds99 = new HoeffdingBounds(0.99, 10.0);
        
        for (int i = 0; i < 100; i++) {
            bounds95.addScore(5.0);
            bounds99.addScore(5.0);
        }
        
        assertTrue("99% confidence should have larger bound than 95%", 
            bounds99.getBound() > bounds95.getBound());
    }
}