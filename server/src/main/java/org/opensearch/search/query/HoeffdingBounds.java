/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

/**
 * Hoeffding confidence bounds calculator for streaming search.
 * Uses Hoeffding's inequality to determine statistical confidence in partial search results.
 * 
 * @opensearch.internal
 */
public class HoeffdingBounds {
    
    private final double confidence;
    private final double scoreRange;
    
    private volatile double sumScores = 0;
    private volatile double sumSquaredScores = 0;
    private volatile int docCount = 0;
    private volatile double maxScoreSeen = 0;
    
    /**
     * Creates a new Hoeffding bounds calculator.
     * 
     * @param confidence The confidence level (between 0 and 1)
     * @param scoreRange The expected score range
     */
    public HoeffdingBounds(double confidence, double scoreRange) {
        this.confidence = confidence;
        this.scoreRange = scoreRange;
    }
    
    /**
     * Add a score observation.
     * Thread-safe using synchronized for concurrent access.
     * 
     * @param score The score to add
     */
    public synchronized void addScore(double score) {
        sumScores += score;
        sumSquaredScores += score * score;
        docCount++;
        maxScoreSeen = Math.max(maxScoreSeen, score);
    }
    
    /**
     * Get the Hoeffding bound for current observations.
     * Lower bound means higher confidence.
     * 
     * @return The calculated Hoeffding bound
     */
    public double getBound() {
        if (docCount == 0) {
            return Double.MAX_VALUE;
        }
        
        double delta = 1.0 - confidence;
        return Math.sqrt((scoreRange * scoreRange * Math.log(2.0 / delta)) / (2.0 * docCount));
    }
    
    /**
     * Check if we can emit results with confidence.
     * 
     * @param lowestTopKScore The lowest score in current top-K
     * @param estimatedMaxUnseen Estimated maximum score for unseen documents
     * @return true if results are stable enough to emit
     */
    public boolean canEmit(double lowestTopKScore, double estimatedMaxUnseen) {
        double bound = getBound();
        // We can emit if the lowest score in top-K minus the bound
        // is still higher than the estimated max unseen score
        return (lowestTopKScore - bound) > estimatedMaxUnseen;
    }
    
    /**
     * Get estimated maximum score for unseen documents.
     * Simple decay model: assume scores decay over time.
     * 
     * @return The estimated maximum score for unseen documents
     */
    public double getEstimatedMaxUnseen() {
        // Simple heuristic: assume 10% decay from max seen
        return maxScoreSeen * 0.9;
    }
    
    /**
     * Get the number of documents processed.
     * 
     * @return The document count
     */
    public int getDocCount() {
        return docCount;
    }
    
    /**
     * Get the maximum score observed.
     * 
     * @return The maximum score seen
     */
    public double getMaxScoreSeen() {
        return maxScoreSeen;
    }
}