/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.Closeable;
import java.util.Map;

/**
 * Converts a raw ingestion message payload into a field map that the message mapper consumes.
 *
 * <p>A decoder is created once per shard poller by its {@link IngestionPayloadDecoderFactory}
 * and is called exclusively from the poller's single consumer thread. Implementations are
 * therefore NOT required to be thread-safe; they may freely use thread-local or instance-level
 * mutable state (e.g. reusable Avro binary-decoder objects, per-thread reader maps).
 *
 * <p>The returned map must contain XContent-compatible Java values and be freshly allocated on
 * every call so the mapper can mutate it without side effects.
 *
 * <p>The decoder is closed when its poller shuts down; implementations should release any
 * resources (open connections, file handles, etc.) in {@link #close()}.
 *
 * @see IngestionPayloadDecoderFactory
 */
@ExperimentalApi
public interface IngestionPayloadDecoder extends Closeable {

    /**
     * Decodes {@code payload} into a field map.
     *
     * @param payload the raw message payload
     * @return a fresh, mutable, XContent-compatible field map
     * @throws IngestionPayloadDecodingException if the payload cannot be decoded
     */
    Map<String, Object> decode(BytesReference payload);

    /**
     * Releases resources held by this decoder. Called once when the shard poller shuts down.
     * The default implementation is a no-op.
     */
    @Override
    default void close() {}
}
