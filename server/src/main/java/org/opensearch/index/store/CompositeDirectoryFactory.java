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
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.lockmanager.RemoteStoreLockManagerFactory;
import org.opensearch.index.store.lockmanager.RemoteStoreMetadataLockManager;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryMissingException;
import org.opensearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Supplier;

import static org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION;

/**
 * Factory for a composite directory
 *
 * @opensearch.internal
 */
public class CompositeDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    private final Supplier<RepositoriesService> repositoriesService;
    private final FileCache remoteStoreFileCache;
    private FileTrackerImp fileTrackerImp;

    public CompositeDirectoryFactory(
        Supplier<RepositoriesService> repositoriesService,
        FileCache remoteStoreFileCache,
        FileTrackerImp fileTrackerImp
    ) {
        this.repositoriesService = repositoriesService;
        this.remoteStoreFileCache = remoteStoreFileCache;
        this.fileTrackerImp = fileTrackerImp;
    }

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        String repositoryName = indexSettings.getRemoteStoreRepository();
        try (Repository repository = repositoriesService.get().repository(repositoryName)) {
            assert repository instanceof BlobStoreRepository : "repository should be instance of BlobStoreRepository";
            BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
            return createRemoteSearchDirectory(indexSettings, path, blobStoreRepository, repositoryName);
        } catch (RepositoryMissingException e) {
            throw new IllegalArgumentException("Repository should be created before creating index with remote_search enabled setting", e);
        }
    }

    private CompositeDirectory createRemoteSearchDirectory(
        IndexSettings indexSettings,
        ShardPath localShardPath,
        BlobStoreRepository blobStoreRepository,
        String repositoryName
    ) throws IOException {

        final Path location = localShardPath.resolveIndex();
        final FSDirectory primaryDirectory = FSDirectory.open(location);
        String shardId = String.valueOf(localShardPath.getShardId().getId());
        String indexUUID = indexSettings.getIndex().getUUID();
        BlobPath commonBlobPath = blobStoreRepository.basePath().add(indexUUID).add(shardId).add("segments");

        RemoteDirectory dataDirectory = createRemoteDirectory(blobStoreRepository, commonBlobPath, "data");
        RemoteDirectory metadataDirectory = createRemoteDirectory(blobStoreRepository, commonBlobPath, "metadata");
        RemoteStoreMetadataLockManager mdLockManager = RemoteStoreLockManagerFactory.newLockManager(
            repositoriesService.get(),
            repositoryName,
            indexUUID,
            shardId
        );

        RemoteSegmentStoreDirectory remoteSegmentStoreDirectory = new RemoteSegmentStoreDirectory(
            dataDirectory,
            metadataDirectory,
            mdLockManager
        );
        Map<String, RemoteSegmentStoreDirectory.UploadedSegmentMetadata> segmentsUploadedToRemoteStore = remoteSegmentStoreDirectory
            .getSegmentsUploadedToRemoteStore();

        Path localStorePath = localShardPath.getDataPath().resolve(LOCAL_STORE_LOCATION);
        FSDirectory localStoreDir = FSDirectory.open(Files.createDirectories(localStorePath));
        // make sure directory is flushed to persistent storage
        localStoreDir.syncMetaData();

        final BlobContainer blobContainer = blobStoreRepository.blobStore().blobContainer(commonBlobPath.add("data"));
        TransferManager transferManager = new TransferManager(blobContainer, remoteStoreFileCache);

        return new CompositeDirectory(
            primaryDirectory,
            remoteSegmentStoreDirectory,
            transferManager,
            remoteStoreFileCache,
            localStoreDir,
            fileTrackerImp
        );
    }

    private RemoteDirectory createRemoteDirectory(Repository repository, BlobPath commonBlobPath, String extention) {
        BlobPath extendedPath = commonBlobPath.add(extention);
        BlobContainer dataBlobContainer = ((BlobStoreRepository) repository).blobStore().blobContainer(extendedPath);
        return new RemoteDirectory(dataBlobContainer);
    }
}
