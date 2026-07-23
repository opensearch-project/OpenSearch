/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.Closeable;
import java.util.Map;

/**
 * Node-scoped factory that creates per-poller {@link IngestionPayloadDecoder} instances.
 *
 * <p>A factory is registered once at node startup (via
 * {@link org.opensearch.plugins.IngestionConsumerPlugin#getIngestionPayloadDecoderFactories()})
 * and may own long-lived, shared resources such as a bounded schema cache or an HTTP client.
 * It must be thread-safe because {@link #create} may be called from multiple threads when
 * multiple shards (or multiple pollers per shard) initialize concurrently.
 *
 * <p>The factory itself is {@link Closeable}; node shutdown will call {@link #close()}
 * so implementations can release shared resources (connections, caches, etc.).
 */
@ExperimentalApi
public interface IngestionPayloadDecoderFactory extends Closeable {

    /**
     * Validates {@code decoderSettings}
     *
     * @param settings the {@code index.ingestion_source.decoder_settings.*} map
     */
    default void validate(Map<String, Object> settings) {}

    /**
     * Creates a new decoder for the given poller.
     *
     * <p>Called once per poller initialization. Multiple pollers may exist per shard
     * (e.g. for concurrent ingestion). Each returned decoder is owned by one poller,
     * used from that poller's consumer thread, and closed when the poller shuts down.
     *
     * @param indexMetadata metadata of the index this shard belongs to
     * @param shardId       the shard id
     * @param settings      the {@code index.ingestion_source.decoder_settings.*} map
     * @return a new decoder instance
     */
    IngestionPayloadDecoder create(IndexMetadata indexMetadata, int shardId, Map<String, Object> settings);

    /**
     * Releases shared resources held by this factory.
     */
    @Override
    default void close() {}
}
