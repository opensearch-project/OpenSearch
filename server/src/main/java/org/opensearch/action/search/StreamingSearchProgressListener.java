/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.action.ActionListener;
import org.opensearch.common.lucene.search.TopDocsAndMaxScore;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.query.QuerySearchResult;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SearchProgressListener implementation for streaming search with scoring.
 * Computes partial search results when confidence thresholds are met.
 * 
 * @opensearch.internal
 */
public class StreamingSearchProgressListener extends SearchProgressListener {
    private static final Logger logger = LogManager.getLogger(StreamingSearchProgressListener.class);
    
    private final ActionListener<SearchResponse> responseListener;
    private final AtomicInteger streamEmissions = new AtomicInteger(0);
    private final SearchPhaseController searchPhaseController;
    private final SearchRequest searchRequest;
    
    public StreamingSearchProgressListener(
        ActionListener<SearchResponse> responseListener,
        SearchPhaseController searchPhaseController,
        SearchRequest searchRequest
    ) {
        this.responseListener = responseListener;
        this.searchPhaseController = searchPhaseController;
        this.searchRequest = searchRequest;
    }
    
    @Override
    protected void onPartialReduceWithTopDocs(
        List<SearchShard> shards, 
        TotalHits totalHits, 
        TopDocs topDocs,
        InternalAggregations aggs, 
        int reducePhase
    ) {
        if (topDocs == null || topDocs.scoreDocs.length == 0) {
            // No docs to emit
            return;
        }
        
        try {
            // Convert TopDocs to SearchHits
            // Simplified conversion of TopDocs to SearchHits
            SearchHit[] hits = new SearchHit[topDocs.scoreDocs.length];
            for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                hits[i] = new SearchHit(topDocs.scoreDocs[i].doc);
                hits[i].score(topDocs.scoreDocs[i].score);
            }
            
            float maxScore = hits.length > 0 ? hits[0].getScore() : Float.NaN;
            SearchHits searchHits = new SearchHits(hits, totalHits, maxScore);
            
            // Create a partial search response with the current TopDocs
            SearchResponseSections sections = new SearchResponseSections(
                searchHits,
                aggs,
                null, // no suggestions in partial results
                false, // not timed out
                false, // not terminated early
                null, // no profile results
                reducePhase
            );
            
            // Create partial response
            SearchResponse partialResponse = new SearchResponse(
                sections,
                null, // no scroll ID for streaming
                shards.size(), // total shards
                shards.size(), // successful shards
                0, // no skipped shards
                0, // took time (will be set later)
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null // no phase took
            );
            
            collectPartialResponse(partialResponse);
            
            int count = streamEmissions.incrementAndGet();
            logger.info("Computed streaming partial #{} with {} docs from {} shards", 
                count, topDocs.scoreDocs.length, shards.size());
            
        } catch (Exception e) {
            logger.error("Failed to send partial TopDocs", e);
        }
    }
    
    private void collectPartialResponse(SearchResponse partialResponse) {
        if (responseListener instanceof StreamingSearchResponseListener) {
            ((StreamingSearchResponseListener) responseListener).onPartialResponse(partialResponse);
        } else {
            logger.debug("Partial result computed, listener type: {}", responseListener.getClass().getSimpleName());
        }
    }
    
    @Override
    protected void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
        logger.info("Final reduce: {} total hits from {} shards, {} partial computations",
            totalHits.value(), shards.size(), streamEmissions.get());
    }
    
    public int getStreamEmissions() {
        return streamEmissions.get();
    }
}