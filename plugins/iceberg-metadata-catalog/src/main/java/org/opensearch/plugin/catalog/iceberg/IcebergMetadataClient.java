/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import org.opensearch.catalog.MetadataClient;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;

/**
 * {@link MetadataClient} implementation backed by an Apache Iceberg REST catalog.
 * <p>
 * Bound to a single {@link IcebergCatalogRepository} registered at node startup from
 * {@code catalog.repository.*} node settings.
 * <p>
 * This is a skeleton. The locking, idempotency, and recovery logic described in the design
 * document lands across follow-up PRs:
 * <ul>
 *   <li>PR 5 — {@code initialize}, {@code finalizePublish}, {@code getMetadata},
 *       {@code indexExists}, {@code close}; stale-lock recovery and rollback.</li>
 *   <li>PR 6 — {@code publish}; content-addressed warehouse paths, idempotent
 *       {@code AppendFiles} with refresh-and-retry on {@code CommitFailedException}.</li>
 * </ul>
 *
 * @opensearch.experimental
 */
public class IcebergMetadataClient implements MetadataClient {

    private final IcebergCatalogRepository repository;

    /**
     * Creates a new client bound to the given repository.
     *
     * @param repository  catalog repository holding the warehouse settings
     */
    public IcebergMetadataClient(IcebergCatalogRepository repository) {
        this.repository = repository;
    }

    @Override
    public void initialize(String indexName, IndexMetadata indexMetadata) throws IOException {
        throw new UnsupportedOperationException("initialize is not yet implemented");
    }

    @Override
    public void publish(String indexName, RemoteSegmentStoreDirectory remoteDirectory, int shardId) throws IOException {
        throw new UnsupportedOperationException("publish is not yet implemented");
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
    public boolean indexExists(String indexName) throws IOException {
        throw new UnsupportedOperationException("indexExists is not yet implemented");
    }

    @Override
    public void close() throws IOException {
        // No resources to release yet.
    }
}
