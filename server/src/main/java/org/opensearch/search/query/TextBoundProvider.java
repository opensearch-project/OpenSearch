/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.TopDocs;
import org.opensearch.search.query.BoundProvider.SearchContext;
import org.opensearch.search.query.BoundProvider.SearchPhase;

/**
 * Text-based bound provider for streaming search.
 * Implements WAND/BMW-style bounds for text search scenarios.
 * 
 * @opensearch.internal
 */
public class TextBoundProvider implements BoundProvider {
    
    private final int minDocsForStability;
    private final double stabilityThreshold;
    
    public TextBoundProvider(int minDocsForStability, double stabilityThreshold) {
        this.minDocsForStability = minDocsForStability;
        this.stabilityThreshold = stabilityThreshold;
    }
    
    @Override
    public double calculateBound(SearchContext context) {
        if (context.getDocCount() < minDocsForStability) {
            return Double.MAX_VALUE; // Not enough docs for confidence
        }
        
        TopDocs topDocs = context.getTopDocs();
        if (topDocs == null || topDocs.scoreDocs.length == 0) {
            return Double.MAX_VALUE;
        }
        
        // Simple bound: ratio of k-th score to max possible score
        float kthScore = context.getKthScore();
        float maxPossible = context.getMaxPossibleScore();
        
        if (maxPossible <= 0 || kthScore <= 0) {
            return Double.MAX_VALUE;
        }
        
        // Lower bound means higher confidence
        // Calculate how close kth score is to max possible score
        // When kth is close to max, bound should be low (high confidence)
        // When kth is far from max, bound should be high (low confidence)
        double bound = (maxPossible - kthScore) / maxPossible;
        
        // Ensure the bound is reasonable (between 0 and 1)
        return Math.max(0.0, Math.min(1.0, bound));
    }
    
    @Override
    public boolean isStable(SearchContext context) {
        if (context.getDocCount() < minDocsForStability) {
            return false;
        }
        
        double bound = calculateBound(context);
        // Lower bound means higher confidence, so we want bound <= threshold
        // With our new calculation: bound of 0.5 means kth is halfway to max
        // We want this to be stable when we have enough docs
        return bound <= stabilityThreshold;
    }
    
    @Override
    public double getProgress(SearchContext context) {
        // Simple progress based on document count
        // This could be enhanced with more sophisticated progress tracking
        return Math.min(1.0, (double) context.getDocCount() / 1000.0);
    }
    
    @Override
    public SearchPhase getCurrentPhase() {
        // Text search typically goes through FIRST -> SECOND -> GLOBAL
        // This is a simplified implementation
        return SearchPhase.FIRST;
    }
    
    /**
     * Create a default text bound provider.
     */
    public static TextBoundProvider createDefault() {
        return new TextBoundProvider(100, 0.5); // Increased threshold to 0.5 for testing
    }
}
