/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.catalog.CatalogMetadataClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.env.Environment;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;

/**
 * {@link CatalogMetadataClient} implementation backed by an Apache Iceberg REST catalog.
 * <p>
 * Bound to the catalog repository metadata registered at node startup from
 * {@code catalog.repository.*} node settings.
 * <p>
 * This is a skeleton. The locking, idempotency, and recovery logic described in the design
 * document lands across follow-up PRs:
 * <ul>
 *   <li>PR 5 — {@code startPublishForIndex}, {@code finalizePublish}, {@code getMetadata},
 *       {@code close}; stale-lock recovery and rollback.</li>
 *   <li>PR 6 — {@code copyShard}; content-addressed warehouse paths, idempotent
 *       {@code AppendFiles} with refresh-and-retry on {@code CommitFailedException}.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public class IcebergMetadataClient implements CatalogMetadataClient {

    private final RepositoryMetadata repositoryMetadata;
    private final Environment environment;

    /**
     * Creates a new client bound to the given catalog repository metadata.
     *
     * @param repositoryMetadata  metadata for the catalog repository configured on this node
     * @param environment         node environment, for resolving keystore-backed secrets and
     *                            relative paths against the OpenSearch config directory
     */
    public IcebergMetadataClient(RepositoryMetadata repositoryMetadata, Environment environment) {
        this.repositoryMetadata = repositoryMetadata;
        this.environment = environment;
    }

    @Override
    public void startPublishForIndex(String indexName, IndexMetadata indexMetadata) throws IOException {
        throw new UnsupportedOperationException("startPublishForIndex is not yet implemented");
    }

    @Override
    public void copyShard(String publishId, ShardId shardId, RemoteSegmentStoreDirectory remoteDirectory) throws IOException {
        throw new UnsupportedOperationException("copyShard is not yet implemented");
    }

    @Override
    public void finalizePublish(String indexName, boolean success) throws IOException {
        throw new UnsupportedOperationException("finalizePublish is not yet implemented");
    }

    @Override
    public IndexMetadata getMetadata(String indexName) throws IOException {
        throw new UnsupportedOperationException("getMetadata is not yet implemented");
    }

    @Override
    public void close() throws IOException {
        // No resources to release yet.
    }
}
