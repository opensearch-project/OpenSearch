/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Message sent from shard to coordinator with streaming search progress updates.
 * Implements milestone-based streaming at 25%, 50%, 75%, and 100% completion.
 */
public class StreamingSearchProgressMessage extends TransportRequest {
    
    /**
     * Milestone types for streaming progress
     */
    public enum Milestone {
        INITIAL(0.0f),      // Initial results available
        QUARTER(0.25f),     // 25% complete
        HALF(0.50f),        // 50% complete
        THREE_QUARTER(0.75f), // 75% complete
        COMPLETE(1.0f);     // 100% complete
        
        private final float progress;
        
        Milestone(float progress) {
            this.progress = progress;
        }
        
        public float getProgress() {
            return progress;
        }
        
        public static Milestone fromProgress(float progress) {
            if (progress <= 0.0f) return INITIAL;
            if (progress <= 0.25f) return QUARTER;
            if (progress <= 0.50f) return HALF;
            if (progress <= 0.75f) return THREE_QUARTER;
            return COMPLETE;
        }
    }
    
    private final SearchShardTarget shardTarget;
    private final int shardIndex;
    private final long requestId;
    private final Milestone milestone;
    private final TopDocs topDocs;
    private final float confidence;
    private final ProgressStatistics statistics;
    private final boolean isFinal;
    
    public StreamingSearchProgressMessage() {
        super();
        this.shardTarget = null;
        this.shardIndex = -1;
        this.requestId = -1;
        this.milestone = Milestone.INITIAL;
        this.topDocs = null;
        this.confidence = 0.0f;
        this.statistics = null;
        this.isFinal = false;
    }
    
    public StreamingSearchProgressMessage(
        SearchShardTarget shardTarget,
        int shardIndex,
        long requestId,
        Milestone milestone,
        TopDocs topDocs,
        float confidence,
        ProgressStatistics statistics,
        boolean isFinal
    ) {
        this.shardTarget = shardTarget;
        this.shardIndex = shardIndex;
        this.requestId = requestId;
        this.milestone = milestone;
        this.topDocs = topDocs;
        this.confidence = confidence;
        this.statistics = statistics;
        this.isFinal = isFinal;
    }
    
    public StreamingSearchProgressMessage(StreamInput in) throws IOException {
        super(in);
        this.shardTarget = new SearchShardTarget(in);
        this.shardIndex = in.readVInt();
        this.requestId = in.readLong();
        this.milestone = in.readEnum(Milestone.class);
        
        // Read TopDocs
        int totalHits = in.readVInt();
        int numDocs = in.readVInt();
        ScoreDoc[] scoreDocs = new ScoreDoc[numDocs];
        for (int i = 0; i < numDocs; i++) {
            int doc = in.readVInt();
            float score = in.readFloat();
            scoreDocs[i] = new ScoreDoc(doc, score);
        }
        this.topDocs = new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), scoreDocs);
        
        this.confidence = in.readFloat();
        this.statistics = new ProgressStatistics(in);
        this.isFinal = in.readBoolean();
    }
    
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardTarget.writeTo(out);
        out.writeVInt(shardIndex);
        out.writeLong(requestId);
        out.writeEnum(milestone);
        
        // Write TopDocs
        out.writeVLong(topDocs.totalHits != null ? topDocs.totalHits.value() : 0);
        out.writeVInt(topDocs.scoreDocs.length);
        for (ScoreDoc doc : topDocs.scoreDocs) {
            out.writeVInt(doc.doc);
            out.writeFloat(doc.score);
        }
        
        out.writeFloat(confidence);
        statistics.writeTo(out);
        out.writeBoolean(isFinal);
    }
    
    public SearchShardTarget getShardTarget() {
        return shardTarget;
    }
    
    public int getShardIndex() {
        return shardIndex;
    }
    
    public long getRequestId() {
        return requestId;
    }
    
    public Milestone getMilestone() {
        return milestone;
    }
    
    public TopDocs getTopDocs() {
        return topDocs;
    }
    
    public float getConfidence() {
        return confidence;
    }
    
    public ProgressStatistics getStatistics() {
        return statistics;
    }
    
    public boolean isFinal() {
        return isFinal;
    }
    
    /**
     * Statistics about the search progress
     */
    public static class ProgressStatistics implements Writeable {
        private final int docsCollected;
        private final long docsEvaluated;
        private final long docsSkipped;
        private final long blocksProcessed;
        private final long blocksSkipped;
        private final float minCompetitiveScore;
        private final float maxSeenScore;
        private final long elapsedTimeMillis;
        
        public ProgressStatistics(
            int docsCollected,
            long docsEvaluated,
            long docsSkipped,
            long blocksProcessed,
            long blocksSkipped,
            float minCompetitiveScore,
            float maxSeenScore,
            long elapsedTimeMillis
        ) {
            this.docsCollected = docsCollected;
            this.docsEvaluated = docsEvaluated;
            this.docsSkipped = docsSkipped;
            this.blocksProcessed = blocksProcessed;
            this.blocksSkipped = blocksSkipped;
            this.minCompetitiveScore = minCompetitiveScore;
            this.maxSeenScore = maxSeenScore;
            this.elapsedTimeMillis = elapsedTimeMillis;
        }
        
        public ProgressStatistics(StreamInput in) throws IOException {
            this.docsCollected = in.readVInt();
            this.docsEvaluated = in.readVLong();
            this.docsSkipped = in.readVLong();
            this.blocksProcessed = in.readVLong();
            this.blocksSkipped = in.readVLong();
            this.minCompetitiveScore = in.readFloat();
            this.maxSeenScore = in.readFloat();
            this.elapsedTimeMillis = in.readVLong();
        }
        
        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(docsCollected);
            out.writeVLong(docsEvaluated);
            out.writeVLong(docsSkipped);
            out.writeVLong(blocksProcessed);
            out.writeVLong(blocksSkipped);
            out.writeFloat(minCompetitiveScore);
            out.writeFloat(maxSeenScore);
            out.writeVLong(elapsedTimeMillis);
        }
        
        public int getDocsCollected() {
            return docsCollected;
        }
        
        public long getDocsEvaluated() {
            return docsEvaluated;
        }
        
        public long getDocsSkipped() {
            return docsSkipped;
        }
        
        public double getSkipRatio() {
            long total = docsEvaluated + docsSkipped;
            return total > 0 ? (double) docsSkipped / total : 0.0;
        }
        
        public double getBlockSkipRatio() {
            long total = blocksProcessed + blocksSkipped;
            return total > 0 ? (double) blocksSkipped / total : 0.0;
        }
        
        public float getMinCompetitiveScore() {
            return minCompetitiveScore;
        }
        
        public float getMaxSeenScore() {
            return maxSeenScore;
        }
        
        public long getElapsedTimeMillis() {
            return elapsedTimeMillis;
        }
    }
    
    /**
     * Builder for creating progress messages
     */
    public static class Builder {
        private SearchShardTarget shardTarget;
        private int shardIndex;
        private long requestId;
        private Milestone milestone;
        private TopDocs topDocs;
        private float confidence = 0.0f;
        private ProgressStatistics statistics;
        private boolean isFinal = false;
        
        public Builder shardTarget(SearchShardTarget shardTarget) {
            this.shardTarget = shardTarget;
            return this;
        }
        
        public Builder shardIndex(int shardIndex) {
            this.shardIndex = shardIndex;
            return this;
        }
        
        public Builder requestId(long requestId) {
            this.requestId = requestId;
            return this;
        }
        
        public Builder milestone(Milestone milestone) {
            this.milestone = milestone;
            return this;
        }
        
        public Builder topDocs(TopDocs topDocs) {
            this.topDocs = topDocs;
            return this;
        }
        
        public Builder confidence(float confidence) {
            this.confidence = confidence;
            return this;
        }
        
        public Builder statistics(ProgressStatistics statistics) {
            this.statistics = statistics;
            return this;
        }
        
        public Builder isFinal(boolean isFinal) {
            this.isFinal = isFinal;
            return this;
        }
        
        public StreamingSearchProgressMessage build() {
            return new StreamingSearchProgressMessage(
                shardTarget,
                shardIndex,
                requestId,
                milestone,
                topDocs,
                confidence,
                statistics,
                isFinal
            );
        }
    }
}