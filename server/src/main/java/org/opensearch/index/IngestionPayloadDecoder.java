/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.Map;

/**
 * Converts a raw ingestion message into a field map that the message mapper consumes.
 *
 * <p>A decoder is created once per poller initialization by its {@link IngestionPayloadDecoderFactory}.
 * Multiple poller instances may exist per shard (e.g. for concurrent ingestion support), so each
 * poller owns its own decoder instance. Implementations must not share mutable state across poller
 * instances without external synchronization; thread-local or per-instance state is safe.
 *
 * <p>The returned map must contain XContent-compatible Java values and be freshly allocated on
 * every call so the mapper can mutate it without side effects.
 *
 * <p>The decoder is closed when its poller shuts down (including when the poller was never started);
 * implementations should release any resources (connections, caches, etc.) in {@link #close()}.
 *
 * @see IngestionPayloadDecoderFactory
 */
@ExperimentalApi
public interface IngestionPayloadDecoder extends Closeable {

    /**
     * Decodes the payload of {@code message} into a field map.
     *
     * <p>Implementations may cast {@code message.getPayload()} to {@code byte[]} since all
     * current ingestion consumers (Kafka, Kinesis, etc.) produce byte-array payloads.
     *
     * @param message the raw ingestion message
     * @return a fresh, mutable, XContent-compatible field map
     * @throws IngestionPayloadDecodingException if the payload cannot be decoded
     */
    Map<String, Object> decode(Message<?> message);

    /**
     * Releases resources held by this decoder. Called once when the shard poller shuts down,
     * even if the poller was never started. The default implementation is a no-op.
     */
    @Override
    default void close() {}
}
