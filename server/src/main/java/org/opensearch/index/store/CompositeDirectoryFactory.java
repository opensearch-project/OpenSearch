/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;

import java.io.IOException;
import java.util.function.Supplier;

public class CompositeDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private final Supplier<RepositoriesService> repositoriesService;
    private final FileCache remoteStoreFileCache;

    public CompositeDirectoryFactory(Supplier<RepositoriesService> repositoriesService, FileCache remoteStoreFileCache) {
        this.repositoriesService = repositoriesService;
        this.remoteStoreFileCache = remoteStoreFileCache;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath shardPath) throws IOException {
        final FSDirectory primaryDirectory = FSDirectory.open(shardPath.resolveIndex());
        return new CompositeDirectory(primaryDirectory, remoteStoreFileCache);
    }
}
