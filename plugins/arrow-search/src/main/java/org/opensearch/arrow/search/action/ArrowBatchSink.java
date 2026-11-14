/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.action.search.SearchResponse;

/**
 * Receives the results of an Arrow search on the coordinator: the schema, then batches, then a summary or a failure.
 * Implementations are thread-safe, because batches of different shards arrive concurrently.
 */
public interface ArrowBatchSink {

    /**
     * Sends the schema of every batch, once and before any batch. Like {@link #sendBatch}, it may wait for the client.
     *
     * @param schema the schema of every batch
     * @throws Exception if the client went away or stopped reading for too long
     */
    void sendSchema(Schema schema) throws Exception;

    /**
     * Sends a batch, waiting while the client is not ready for more data. The caller should be a virtual thread. The sink
     * takes ownership of {@code batch} in every case, including when it throws, and closes it once the client has it.
     *
     * @param batch the batch to send
     * @throws Exception if the client went away or stopped reading for too long
     */
    void sendBatch(VectorSchemaRoot batch) throws Exception;

    /**
     * Ends the stream with the search summary. Only the first call to this method or {@link #fail} has an effect.
     *
     * @param summary the search summary
     */
    void complete(SearchResponse summary);

    /**
     * Ends the stream with a failure and wakes up callers waiting in {@link #sendBatch}.
     *
     * @param failure why the search failed
     */
    void fail(Exception failure);

    /**
     * Discards, and so closes, the batches the transport still holds for a client that stopped reading after the stream
     * ended. Ends the stream like {@link #fail} if it has not ended yet.
     */
    void reset();

    /**
     * Sets the callback to run once, never on the caller's thread, when the client goes away.
     *
     * @param onCancel the callback
     */
    void setOnCancel(Runnable onCancel);
}
