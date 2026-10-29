/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.SearchableDirectoryReaderProvider;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Test-only reader manager whose format reader exposes one {@link OpenSearchDirectoryReader} over the store's
 * latest commit. Like {@code LuceneReaderManager}, {@link #close()} drops its reference whether or not
 * searchers are still open.
 */
final class SearchableStubReaderManager implements EngineReaderManager<SearchableDirectoryReaderProvider> {
    private final Store store;
    private final ShardId shardId;
    private volatile OpenSearchDirectoryReader reader;
    final Set<Long> deletedSnapshotIds = ConcurrentHashMap.newKeySet();

    SearchableStubReaderManager(Store store, ShardId shardId) {
        this.store = store;
        this.shardId = shardId;
    }

    /** The reader handed to searchers, or {@code null} before the first {@link #getReader}. */
    OpenSearchDirectoryReader currentReader() {
        return reader;
    }

    @Override
    public synchronized SearchableDirectoryReaderProvider getReader(CatalogSnapshot catalogSnapshot) throws IOException {
        if (reader == null) {
            reader = OpenSearchDirectoryReader.wrap(DirectoryReader.open(store.directory()), shardId);
        }
        final OpenSearchDirectoryReader current = reader;
        return () -> current;
    }

    @Override
    public void beforeRefresh() {}

    @Override
    public void afterRefresh(boolean didRefresh, CatalogSnapshot catalogSnapshot) {}

    @Override
    public void onDeleted(CatalogSnapshot catalogSnapshot) {
        deletedSnapshotIds.add(catalogSnapshot.getId());
    }

    @Override
    public void onFilesDeleted(Collection<String> files) {}

    @Override
    public void onFilesAdded(Collection<String> files) {}

    @Override
    public synchronized void close() throws IOException {
        if (reader != null) {
            reader.decRef();
            reader = null;
        }
    }
}
