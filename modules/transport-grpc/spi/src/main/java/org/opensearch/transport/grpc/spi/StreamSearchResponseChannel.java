/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.spi;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Response side of a {@code ServerStreamSearch} call. All methods are thread-safe.
 * <p>
 * Every payload is sent to the client as its own {@code SearchResponse} message carrying only field
 * {@value #PAYLOAD_FIELD_NUMBER} (bytes), whose content is encoded in the format the client requested. The stream ends
 * with a regular {@code SearchResponse} built from the summary passed to {@link #complete}. Clients that know the format
 * can therefore read payloads without protobuf decoding, while generic protobuf clients still parse every message.
 */
@ExperimentalApi
public interface StreamSearchResponseChannel {

    /** Protobuf field number of {@code SearchResponse} that carries a payload. */
    int PAYLOAD_FIELD_NUMBER = 100;

    /**
     * Returns whether the transport can accept another payload without buffering beyond its flow-control window.
     */
    boolean isReady();

    /**
     * Registers a callback invoked, on a transport thread, whenever {@link #isReady()} may have turned true. The callback
     * must not block.
     *
     * @param onReady the callback, replacing any previously registered one
     */
    void setOnReadyHandler(Runnable onReady);

    /**
     * Registers a callback invoked, on a transport thread, once when the client cancels the call or disconnects. The
     * callback must not block. It runs immediately if the call is already cancelled.
     *
     * @param onCancel the callback, replacing any previously registered one
     */
    void setOnCancelHandler(Runnable onCancel);

    /**
     * Returns whether the client has cancelled the call.
     */
    boolean isCancelled();

    /**
     * Sends a payload. Ownership of {@code payload} transfers to the channel, which releases it exactly once: after the
     * transport has written its memory, or immediately if the payload cannot be sent.
     *
     * @param payload the payload to send
     */
    void sendPayload(StreamPayload payload);

    /**
     * Sends {@code summary} as the last message and closes the call successfully. The hits of the summary are not sent.
     * Messages sent earlier stay queued, holding their payloads, until the client reads them or the call is {@link #reset}.
     *
     * @param summary the search summary: took, shard statistics, total hits and termination flags
     */
    void complete(SearchResponse summary);

    /**
     * Closes the call with the gRPC status that corresponds to {@code failure}. Messages sent earlier stay queued, holding
     * their payloads, until the client reads them or the call is {@link #reset}.
     *
     * @param failure the failure
     */
    void fail(Exception failure);

    /**
     * Resets the call's stream for a client that stopped reading. The transport discards the messages it has not written,
     * which releases their payloads, and the client sees the call cancelled. Ends the call like {@link #fail} if it has not
     * ended. Does nothing once the transport has closed the call.
     */
    void reset();
}
