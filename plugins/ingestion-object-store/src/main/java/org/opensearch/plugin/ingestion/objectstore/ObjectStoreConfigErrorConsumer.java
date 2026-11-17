/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;

import java.util.List;
import java.util.Objects;

/**
 * A consumer that stands in for {@link ObjectStoreShardConsumer} when shard-consumer creation fails on a deterministic
 * configuration error, such as a validation failure, a malformed manifest, or a cost cap exceeded during discovery.
 *
 * <p>If the factory threw instead, the poller would log a warning and retry consumer creation on a fixed interval
 * forever, and under {@code discovery=list} every retry would repeat the billed listing. Returning this consumer lets
 * creation succeed so that the first poll fails instead: both {@code readNext} methods throw an
 * {@link IllegalStateException} carrying the original error message, which the poller logs at error level, counts as
 * a consumer error, and answers by pausing ingestion. Every read after a resume fails and pauses again, until the
 * source params are updated, which makes the poller create a new consumer from the updated configuration.
 *
 * <p>The consumer performs no IO. The pointer methods throw the same error: the poller calls them only while
 * resolving the configured reset, inside the same error handling that pauses ingestion, and the ingestion engine calls
 * {@link #pointerFromOffset(String)} and {@link #pointerFromTimestampMillis(long)} only for a reset request, which then
 * fails with this error. Returning a placeholder
 * pointer instead would let the poller record it as the batch start pointer and force it onto the real consumer once
 * the configuration is fixed. The pointer-based lag, which the poller polls on its own schedule, is {@code -1}, the
 * value the poller itself uses for an unknown lag, so a misconfigured shard is never reported as fully ingested.
 */
final class ObjectStoreConfigErrorConsumer implements IngestionShardConsumer<ObjectStorePointer, ObjectStoreMessage> {

    /**
     * The pointer-based lag reported by this consumer, matching the poller's own sentinel for an unknown lag.
     */
    static final long UNKNOWN_LAG = -1;

    private final int shardId;
    private final Exception error;

    /**
     * Creates a consumer that reports the given configuration error on every read.
     *
     * @param shardId the shard id this consumer serves
     * @param error   the deterministic configuration error that prevented the real consumer from being created
     */
    ObjectStoreConfigErrorConsumer(int shardId, Exception error) {
        this.shardId = shardId;
        this.error = Objects.requireNonNull(error, "error must not be null");
    }

    @Override
    public List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> readNext(
        ObjectStorePointer pointer,
        boolean includeStart,
        long maxMessages,
        int timeoutMillis
    ) {
        throw configurationError();
    }

    @Override
    public List<ReadResult<ObjectStorePointer, ObjectStoreMessage>> readNext(long maxMessages, int timeoutMillis) {
        throw configurationError();
    }

    @Override
    public IngestionShardPointer earliestPointer() {
        throw configurationError();
    }

    @Override
    public IngestionShardPointer latestPointer() {
        throw configurationError();
    }

    @Override
    public IngestionShardPointer pointerFromTimestampMillis(long timestampMillis) {
        throw configurationError();
    }

    @Override
    public IngestionShardPointer pointerFromOffset(String offset) {
        throw configurationError();
    }

    @Override
    public int getShardId() {
        return shardId;
    }

    @Override
    public long getPointerBasedLag(IngestionShardPointer expectedStartPointer) {
        return UNKNOWN_LAG;
    }

    @Override
    public void close() {}

    private IllegalStateException configurationError() {
        return new IllegalStateException(
            "object_store source configuration for shard ["
                + shardId
                + "] is invalid, so ingestion is paused; fix the configuration and update the ingestion source params or "
                + "recreate the index: "
                + error.getMessage(),
            error
        );
    }
}
