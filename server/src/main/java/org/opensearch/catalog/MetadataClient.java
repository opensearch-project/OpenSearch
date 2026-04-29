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
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.Closeable;
import java.io.IOException;

/**
 * Client for publishing index data and metadata to an external catalog.
 * <p>
 * Implementations are provided by plugins via {@link org.opensearch.plugins.CatalogPlugin}.
 * A publish proceeds in three phases:
 * <ol>
 *   <li>{@link #initialize} — once on the cluster manager to set up catalog state.</li>
 *   <li>{@link #publish} — once per primary shard to copy data files and register them.</li>
 *   <li>{@link #finalizePublish} — once after all shards finish, for post-publish bookkeeping.</li>
 * </ol>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface MetadataClient extends Closeable {

    /**
     * Prepares the catalog for publishing an index. Creates or loads the catalog table,
     * infers schema from {@code indexMetadata} on first create, and persists the metadata.
     * Called once on the cluster manager before any shard publish. Must be idempotent.
     *
     * @param indexName      name of the OpenSearch index being published
     * @param indexMetadata  index metadata (mappings, settings) to persist in the catalog
     * @return the catalog snapshot id observed before this publish began, or {@code null}
     *         if the table was freshly created and has no snapshots yet. The orchestrator
     *         threads this back into {@link #finalizePublish} to support rollback.
     * @throws IOException   if catalog setup fails
     */
    String initialize(String indexName, IndexMetadata indexMetadata) throws IOException;

    /**
     * Publishes data files for a single shard to the catalog warehouse. The plugin
     * discovers committed files from {@code remoteDirectory}, streams the ones it needs
     * into the warehouse, and registers them with the catalog.
     * <p>
     * {@link #initialize} must have been called for {@code indexName} first.
     *
     * @param indexName        name of the OpenSearch index being published
     * @param remoteDirectory  remote segment store directory for the shard
     * @param shardId          primary shard id being published
     * @throws IOException     if file read, warehouse write, or catalog registration fails
     */
    void publish(
        String indexName,
        RemoteSegmentStoreDirectory remoteDirectory,
        int shardId
    ) throws IOException;

    /**
     * Called once after all shards complete. {@code success} is true when every shard's
     * {@link #publish} returned without error. Implementations use this to stamp a
     * completion marker on success, or rollback partial commits on failure. Default is
     * no-op.
     *
     * @param indexName         name of the index that was published
     * @param success           whether all shards published successfully
     * @param savedSnapshotId   the snapshot id captured by {@link #initialize} before this
     *                          publish began. On failure, the implementation rolls the table
     *                          back to this snapshot. May be {@code null} if the table had no
     *                          snapshots prior to this publish (first-ever publish).
     * @throws IOException if the finalization fails
     */
    default void finalizePublish(String indexName, boolean success, String savedSnapshotId) throws IOException {}

    /**
     * Reads index metadata previously persisted by {@link #initialize}.
     *
     * @param indexName  name of the index
     * @return the persisted index metadata, or {@code null} if not published
     * @throws IOException if the catalog read fails
     */
    IndexMetadata getMetadata(String indexName) throws IOException;

    /**
     * Returns {@code true} if the index has been initialized in the catalog.
     *
     * @param indexName  name of the index
     * @return whether the catalog has a record of this index
     * @throws IOException if the catalog lookup fails
     */
    boolean indexExists(String indexName) throws IOException;

    /**
     * Releases resources held by the client.
     */
    @Override
    void close() throws IOException;
}
