package org.opensearch.search.query;

import org.apache.lucene.search.ScoreDoc;

import java.util.List;

/**
 * Interface for streaming collectors to emit batches of results.
 * This enables progressive emission of search results during collection.
 */
public interface StreamingCollectorContext {

    /**
     * Emit a batch of documents to the streaming channel.
     *
     * @param docs The documents to emit
     * @param isFinal Whether this is the final batch
     */
    void emitBatch(List<ScoreDoc> docs, boolean isFinal);

    /**
     * Get the configured batch size for this collector.
     *
     * @return The batch size in number of documents
     */
    int getBatchSize();

    /**
     * Check if a batch should be emitted based on current state.
     *
     * @return true if a batch should be emitted now
     */
    boolean shouldEmitBatch();
}
