/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopScoreDocCollector;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Streaming collector implementation that wraps TopScoreDocCollector
 * and tracks scores for confidence-based emission.
 * 
 * @opensearch.internal
 */
public class SimpleStreamingScoringCollector implements Collector {
    
    private final TopScoreDocCollector delegate;
    private final StreamingScoringConfig config;
    private final Consumer<TopDocs> emissionCallback;
    private final HoeffdingBounds confidenceBounds;
    
    private int docsCollected = 0;
    private int emissionCount = 0;
    private Scorable currentScorer = null;
    
    public SimpleStreamingScoringCollector(
        TopScoreDocCollector delegate,
        StreamingScoringConfig config,
        Consumer<TopDocs> emissionCallback
    ) {
        this.delegate = delegate;
        this.config = config;
        this.emissionCallback = emissionCallback;
        // Initialize Hoeffding bounds with configured confidence
        // Assume max score range of 100 (can be refined)
        this.confidenceBounds = new HoeffdingBounds(config.getConfidence(), 100.0);
    }
    
    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final LeafCollector delegateLeafCollector = delegate.getLeafCollector(context);
        
        return new LeafCollector() {
            private Scorable leafScorer;
            
            @Override
            public void setScorer(Scorable scorer) throws IOException {
                delegateLeafCollector.setScorer(scorer);
                this.leafScorer = scorer;
            }
            
            @Override
            public void collect(int doc) throws IOException {
                delegateLeafCollector.collect(doc);
                docsCollected++;
                
                // Track score for Hoeffding bounds
                if (leafScorer != null) {
                    try {
                        float score = leafScorer.score();
                        confidenceBounds.addScore(score);
                    } catch (IOException e) {
                        // Ignore scoring errors for MVP
                    }
                }
                
                // Check if we should emit partial results
                if (config.isEnabled() && shouldEmit()) {
                    tryEmit();
                }
            }
        };
    }
    
    private boolean shouldEmit() {
        return docsCollected >= config.getMinDocsBeforeEmission() &&
               docsCollected % config.getCheckInterval() == 0;
    }
    
    private void tryEmit() {
        TopDocs topDocs = delegate.topDocs();
        if (topDocs != null && topDocs.scoreDocs.length > 0) {
            // Check Hoeffding bounds for confidence
            float lowestTopKScore = topDocs.scoreDocs[topDocs.scoreDocs.length - 1].score;
            double estimatedMaxUnseen = confidenceBounds.getEstimatedMaxUnseen();
            
            if (confidenceBounds.canEmit(lowestTopKScore, estimatedMaxUnseen)) {
                if (emissionCallback != null) {
                    emissionCallback.accept(topDocs);
                    emissionCount++;
                }
            }
        }
    }
    
    @Override
    public ScoreMode scoreMode() {
        return delegate.scoreMode();
    }
    
    public TopDocs getTopDocs() {
        return delegate.topDocs();
    }
    
    public int getEmissionCount() {
        return emissionCount;
    }
}