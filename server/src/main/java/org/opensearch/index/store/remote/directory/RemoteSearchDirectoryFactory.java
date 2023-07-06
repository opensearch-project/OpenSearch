/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

/**
 * Factory for a remote search directory
 *
 * @opensearch.internal
 */
public class RemoteSearchDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private final Supplier<RepositoriesService> repositoriesService;
    private final FileCache remoteStoreFileCache;

    public RemoteSearchDirectoryFactory(Supplier<RepositoriesService> repositoriesService,
                                        FileCache remoteStoreFileCache) {
        this.repositoriesService = repositoriesService;
        this.remoteStoreFileCache = remoteStoreFileCache;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {
        assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
        BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        return createRemoteSearchDirectory(indexSettings, path, blobStoreRepository);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_search enabled setting", e);
        }
    }

    private RemoteSearchDirectory createRemoteSearchDirectory(
        IndexSettings indexSettings,
        ShardPath localShardPath,
        BlobStoreRepository blobStoreRepository
    ) throws IOException {
        BlobPath commonBlobPath = blobStoreRepository.basePath()
            .add(indexSettings.getIndex().getUUID())
            .add(String.valueOf(localShardPath.getShardId().getId()))
            .add("segments");

        // these directories are initialized again as the composite directory implementation is not yet implemented
        // and there is no way to pass the remote segment directory info to this directory
        RemoteDirectory dataDirectory = createRemoteDirectory(blobStoreRepository, commonBlobPath, "data");
        RemoteDirectory metadataDirectory = createRemoteDirectory(blobStoreRepository, commonBlobPath, "metadata");

        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(dataDirectory, metadataDirectory);
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> segmentsUploadedToRemoteStore = remoteSegmentStoreDirectory.getSegmentsUploadedToRemoteStore();

        Path localStorePath = localShardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(localStorePath));
        // make sure directory is flushed to persistent storage
        localStoreDir.syncMetaData();

        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(commonBlobPath.add("data"));
        TransferManager transferManager = new TransferManager(blobContainer, remoteStoreFileCache);
        return new RemoteSearchDirectory(segmentsUploadedToRemoteStore, localStoreDir, transferManager);
    }

    private RemoteDirectory createRemoteDirectory(Repository repository, BlobPath commonBlobPath, String extention) {
        BlobPath extendedPath = commonBlobPath.add(extention);
        BlobContainer dataBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(extendedPath);
        return new RemoteDirectory(dataBlobContainer);
    }
}
