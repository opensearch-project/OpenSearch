/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.catalog;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** In-memory, test-only {@link CatalogMetadataClient}. Records every call; can inject failures. */
public class MockCatalogMetadataClient implements CatalogMetadataClient {

    private final Map<String, String> indexToSnapshotId = new ConcurrentHashMap<>();
    private final Map<String, List<ShardId>> indexToCopiedShards = new ConcurrentHashMap<>();
    private final Map<String, List<FinalizeCall>> indexToFinalizeCalls = new ConcurrentHashMap<>();
    private final Map<ShardId, AtomicInteger> copyShardFailures = new ConcurrentHashMap<>();

    private volatile Exception finalizeFailure;

    @Override
    public void startPublishForIndex(String indexName, IndexMetadata indexMetadata) throws IOException {
        Objects.requireNonNull(indexName, "indexName");
        Objects.requireNonNull(indexMetadata, "indexMetadata");
        indexToSnapshotId.put(indexName, "snap-" + System.nanoTime());
    }

    @Override
    public void copyShard(String publishId, ShardId shardId, RemoteSegmentStoreDirectory remoteDirectory) throws IOException {
        Objects.requireNonNull(publishId, "publishId");
        Objects.requireNonNull(shardId, "shardId");

        AtomicInteger remaining = copyShardFailures.get(shardId);
        if (remaining != null && remaining.get() > 0) {
            remaining.decrementAndGet();
            throw new IOException("configured mock failure for shard " + shardId);
        }
        indexToCopiedShards.computeIfAbsent(shardId.getIndexName(), k -> new ArrayList<>()).add(shardId);
    }

    @Override
    public void finalizePublish(String indexName, boolean success) throws IOException {
        if (finalizeFailure != null) {
            if (finalizeFailure instanceof IOException io) throw io;
            throw new IOException("configured mock finalize failure", finalizeFailure);
        }
        indexToFinalizeCalls
            .computeIfAbsent(indexName, k -> new ArrayList<>())
            .add(new FinalizeCall(success, indexToSnapshotId.get(indexName)));
    }

    @Override
    public IndexMetadata getMetadata(String indexName) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {}

    public MockCatalogMetadataClient failOnCopyShard(ShardId shardId, int count) {
        copyShardFailures.put(shardId, new AtomicInteger(count));
        return this;
    }

    public MockCatalogMetadataClient failOnFinalize(Exception ex) {
        this.finalizeFailure = ex;
        return this;
    }

    public MockCatalogMetadataClient clearFailures() {
        this.finalizeFailure = null;
        this.copyShardFailures.clear();
        return this;
    }

    public Map<String, String> indexToSnapshotId() { return indexToSnapshotId; }
    public Map<String, List<ShardId>> indexToCopiedShards() { return indexToCopiedShards; }
    public Map<String, List<FinalizeCall>> indexToFinalizeCalls() { return indexToFinalizeCalls; }

    public static final class FinalizeCall {
        private final boolean success;
        private final String savedSnapshotId;

        public FinalizeCall(boolean success, String savedSnapshotId) {
            this.success = success;
            this.savedSnapshotId = savedSnapshotId;
        }

        public boolean success() { return success; }
        public String savedSnapshotId() { return savedSnapshotId; }

        @Override
        public String toString() {
            return "FinalizeCall{success=" + success + ", snapshotId=" + savedSnapshotId + "}";
        }
    }
}
