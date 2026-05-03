/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.Closeable;
import java.io.IOException;

/**
 * SPI for catalog plugins. Publish is a three-phase operation:
 * {@link #startPublishForIndex} (once), {@link #copyShard} (per primary), {@link #finalizePublish} (once).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CatalogMetadataClient extends Closeable {

    /** Idempotent setup for the index. Called once on the cluster manager. */
    void startPublishForIndex(String indexName, IndexMetadata indexMetadata) throws IOException;

    /**
     * Copies one primary shard's files into the catalog warehouse and registers them.
     * Requires {@link #startPublishForIndex} to have been called for the index first.
     *
     * @param publishId        correlation id shared by all shard invocations of this publish
     * @param shardId          primary shard being copied
     * @param remoteDirectory  remote segment store directory for the shard
     */
    void copyShard(
        String publishId,
        ShardId shardId,
        RemoteSegmentStoreDirectory remoteDirectory
    ) throws IOException;

    /**
     * Final bookkeeping. {@code success=false} is the rollback path; it also covers cancellation.
     * Default is no-op.
     */
    default void finalizePublish(String indexName, boolean success) throws IOException {}

    /** Returns persisted metadata, or {@code null} if not published. */
    IndexMetadata getMetadata(String indexName) throws IOException;

    @Override
    void close() throws IOException;
}
