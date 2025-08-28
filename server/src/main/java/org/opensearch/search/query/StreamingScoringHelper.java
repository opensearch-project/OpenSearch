/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.query.ReduceableSearchResult;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.action.support.StreamSearchChannelListener;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;

import java.io.IOException;

/**
 * Helper class for streaming scoring integration with QueryPhase.
 * 
 * @opensearch.internal
 */
public class StreamingScoringHelper {
    
    /**
     * Check if streaming scoring is enabled for this search context.
     * 
     * @param searchContext The search context to check
     * @return true if streaming is enabled for this context
     */
    public static boolean isStreamingEnabled(SearchContext searchContext) {
        // Handle null context
        if (searchContext == null) {
            return false;
        }
        
        // Check if streaming is enabled on the context
        if (!searchContext.isStreamSearch()) {
            return false;
        }
        
        // Don't use streaming for scroll requests
        if (searchContext.scrollContext() != null) {
            return false;
        }
        
        // Don't use streaming with aggregations (for now)
        if (searchContext.aggregations() != null) {
            return false;
        }
        
        // Additional check: only stream if this is a scoring query (has size > 0)
        if (searchContext.size() <= 0) {
            return false;
        }
        
        return true;
    }
    
    /**
     * Get streaming configuration for the search context.
     * 
     * @param searchContext The search context
     * @return The appropriate streaming configuration
     */
    public static StreamingScoringConfig getConfig(SearchContext searchContext) {
        if (!isStreamingEnabled(searchContext)) {
            return StreamingScoringConfig.disabled();
        }
        
        // Default to confidence-based mode
        // The actual mode is determined at the coordinator level in StreamQueryPhaseResultConsumer
        // This is just for shard-level configuration
        return StreamingScoringConfig.defaultConfig();
    }
    
    /**
     * Wrap a collector with streaming capabilities if appropriate.
     * 
     * @param collector The collector to wrap
     * @param searchContext The search context
     * @return The wrapped collector or original if streaming not applicable
     */
    public static Collector wrapWithStreaming(
        Collector collector,
        SearchContext searchContext
    ) {
        if (!isStreamingEnabled(searchContext)) {
            return collector;
        }
        
        // Only wrap TopScoreDocCollector for now
        if (!(collector instanceof TopScoreDocCollector)) {
            return collector;
        }
        
        StreamingScoringConfig config = getConfig(searchContext);
        TopScoreDocCollector topDocsCollector = (TopScoreDocCollector) collector;
        
        // Create emission callback that sends via streaming infrastructure
        return new SimpleStreamingScoringCollector(
            topDocsCollector,
            config,
            (TopDocs topDocs) -> {
                // Send partial results via streaming channel if available
                if (searchContext.isStreamSearch()) {
                    try {
                        // Get the streaming listener that was set up by StreamSearchTransportService
                        var streamListener = searchContext.getStreamChannelListener();
                        if (streamListener != null) {
                            // Create a partial result with current top docs
                            QuerySearchResult partialResult = new QuerySearchResult();
                            
                            // Calculate max score from top docs
                            float maxScore = 0;
                            if (topDocs.scoreDocs.length > 0) {
                                maxScore = topDocs.scoreDocs[0].score;
                            }
                            
                            // Wrap TopDocs in TopDocsAndMaxScore
                            TopDocsAndMaxScore topDocsAndMaxScore = new TopDocsAndMaxScore(topDocs, maxScore);
                            partialResult.topDocs(topDocsAndMaxScore, null);
                            
                            // Set shard information
                            partialResult.setShardIndex(0); // For MVP, use index 0
                            partialResult.setSearchShardTarget(searchContext.shardTarget());
                            
                            // Send as streaming response (not final)
                            streamListener.onStreamResponse(partialResult, false);
                        }
                    } catch (Exception e) {
                        // Log error but don't fail the search
                        // In production, would use proper logging
                    }
                }
            }
        );
    }
    
    /**
     * Create a QueryCollectorContext that supports streaming scoring.
     * 
     * @param searchContext The search context
     * @param hasFilterCollector Whether a filter collector is present
     * @return A QueryCollectorContext configured for streaming
     * @throws IOException If an I/O error occurs
     */
    public static QueryCollectorContext createStreamingContext(
        SearchContext searchContext,
        boolean hasFilterCollector
    ) throws IOException {
        // Get the default context
        QueryCollectorContext defaultContext = TopDocsCollectorContext.createTopDocsCollectorContext(
            searchContext,
            hasFilterCollector
        );
        
        if (!isStreamingEnabled(searchContext)) {
            return defaultContext;
        }
        
        // Wrap with streaming
        return new QueryCollectorContext("streaming_top_docs") {
            @Override
            Collector create(Collector in) throws IOException {
                Collector defaultCollector = defaultContext.create(in);
                return wrapWithStreaming(defaultCollector, searchContext);
            }
            
            @Override
            CollectorManager<?, ReduceableSearchResult> createManager(
                CollectorManager<?, ReduceableSearchResult> in
            ) throws IOException {
                // For MVP, delegate to default implementation
                return defaultContext.createManager(in);
            }
        };
    }
}