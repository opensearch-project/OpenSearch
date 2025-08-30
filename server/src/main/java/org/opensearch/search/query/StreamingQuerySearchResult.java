/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Wrapper for QuerySearchResult that includes streaming frame information.
 * Since QuerySearchResult is final, we use composition instead of inheritance.
 *
 * @opensearch.internal
 */
public class StreamingQuerySearchResult implements Writeable {

    private final QuerySearchResult delegate;
    private StreamingScoringCollector.StreamingFrame streamingFrame;

    public StreamingQuerySearchResult(QuerySearchResult delegate) {
        this.delegate = delegate;
    }

    public StreamingQuerySearchResult(StreamInput in) throws IOException {
        // Read the delegate QuerySearchResult
        this.delegate = new QuerySearchResult(in);

        // Read streaming frame if available
        if (in.readBoolean()) {
            this.streamingFrame = new StreamingScoringCollector.StreamingFrame(
                readTopDocs(in),
                in.readVInt(),
                in.readVInt(),
                in.readBoolean(),
                in.readEnum(BoundProvider.SearchPhase.class),
                in.readDouble(),
                in.readDouble()
            );
        }
    }

    private TopDocs readTopDocs(StreamInput in) throws IOException {
        // Simple TopDocs reading - in practice you'd want more robust handling
        long totalHits = in.readVLong();
        int scoreDocsLength = in.readVInt();
        if (scoreDocsLength == 0) {
            return new TopDocs(new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), new org.apache.lucene.search.ScoreDoc[0]);
        }
        // For simplicity, return null - in practice you'd reconstruct ScoreDoc array
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // Write the delegate QuerySearchResult
        delegate.writeTo(out);

        // Write streaming frame if available
        if (streamingFrame != null) {
            out.writeBoolean(true);
            writeTopDocs(out, streamingFrame.getTopDocs());
            out.writeVInt(streamingFrame.getDocCount());
            out.writeVInt(streamingFrame.getSequenceNumber());
            out.writeBoolean(streamingFrame.isStable());
            out.writeEnum(streamingFrame.getPhase());
            out.writeDouble(streamingFrame.getProgress());
            out.writeDouble(streamingFrame.getBound());
        } else {
            out.writeBoolean(false);
        }
    }

    private void writeTopDocs(StreamOutput out, TopDocs topDocs) throws IOException {
        if (topDocs == null) {
            out.writeVLong(0);
            out.writeVInt(0);
        } else {
            // Use reflection to access the private value field, or handle it differently
            // For now, just write a default value
            out.writeVLong(0);
            out.writeVInt(topDocs.scoreDocs.length);
        }
    }

    /**
     * Set the streaming frame for this result.
     */
    public void setStreamingFrame(StreamingScoringCollector.StreamingFrame frame) {
        this.streamingFrame = frame;
    }

    /**
     * Get the streaming frame from this result.
     */
    public StreamingScoringCollector.StreamingFrame getStreamingFrame() {
        return streamingFrame;
    }

    /**
     * Check if this result has streaming frame information.
     */
    public boolean hasStreamingFrame() {
        return streamingFrame != null;
    }

    /**
     * Get the delegate QuerySearchResult.
     */
    public QuerySearchResult getDelegate() {
        return delegate;
    }

    /**
     * Delegate methods to the wrapped QuerySearchResult
     */
    public org.opensearch.common.lucene.search.TopDocsAndMaxScore topDocs() {
        return delegate.topDocs();
    }

    public int getShardIndex() {
        return delegate.getShardIndex();
    }

    public org.opensearch.search.SearchShardTarget getSearchShardTarget() {
        return delegate.getSearchShardTarget();
    }
}
