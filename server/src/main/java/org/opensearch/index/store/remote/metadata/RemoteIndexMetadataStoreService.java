/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.metadata;

import org.opensearch.cluster.ClusterStateObserver;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.Index;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.indices.cluster.IndicesClusterStateService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The class is responsible for uploading the latest index metadata if changed to the remote store location
 *
 */
public class RemoteIndexMetadataStoreService implements IndexEventListener {

    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private static final String INDEX_METADATA_PATH = "index-metadata";
    private final Object mutex = new Object();

    public RemoteIndexMetadataStoreService(ClusterService clusterService, ThreadPool threadPool, Settings settings,
                                           Supplier<RepositoriesService> repositoriesServiceSupplier) {
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.settings = settings;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
    }

    @Override
    public void indexMetadataChanged(IndexMetadata previousIndexMetadata,
                                     IndexMetadata currentIndexMetadata) {
        if (isLocalNodeElectedClusterManager()) {
            persistIndexMetadata(currentIndexMetadata);
        }
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        if (isLocalNodeElectedClusterManager()) {
            persistIndexMetadata(indexService.getMetadata());
        }
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings,
                                  IndicesClusterStateService.AllocatedIndices.IndexRemovalReason reason) {
        if (isLocalNodeElectedClusterManager()) {
            //TODO add deletion logic for blob store
        }
    }

    private boolean isLocalNodeElectedClusterManager() {
        return clusterService.getClusterApplierService().applierState().nodes().isLocalNodeElectedClusterManager();
    }

    private long getClusterStateTerm() {
        return clusterService.getClusterApplierService().applierState().term();
    }

    private void persistIndexMetadata(IndexMetadata indexMetaData) {
        assertCalledFromClusterStateApplier("index metadata upload should occur as a part of cluster state application");
        synchronized (mutex) {
            if (isLocalNodeElectedClusterManager()) {
                String repositoryName = indexMetaData.getSettings().get(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY);
                Repository repository = repositoriesServiceSupplier.get().repository(repositoryName);
                BlobPath commonBlobPath = ((BlobStoreRepository) repository).basePath();
                final BlobPath indexMetadataBlobPath = commonBlobPath.add(indexMetaData.getIndexUUID()).add(INDEX_METADATA_PATH);
                threadPool.executor(ThreadPool.Names.GENERIC).execute((() -> {
                    String metaUUID = getClusterStateTerm() + "__" + UUIDs.base64UUID();
                    try {
                        BlobStoreRepository.INDEX_METADATA_FORMAT.write(indexMetaData,
                            ((BlobStoreRepository)repository).blobStore().blobContainer(indexMetadataBlobPath), metaUUID, null);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }));
            }
        }
    }

    /** asserts that the current stack trace involves a cluster state applier */
    private static boolean assertCalledFromClusterStateApplier(String reason) {
        if (Thread.currentThread().getName().contains(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME)) {
            for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
                final String className = element.getClassName();
                final String methodName = element.getMethodName();
                if (className.equals(ClusterStateObserver.class.getName())) {
                    // people may start an observer from an applier
                    throw new AssertionError("should not be called by a cluster state applier. reason [" + reason + "]");
                } else if (className.equals(ClusterApplierService.class.getName()) && methodName.equals("callClusterStateAppliers")) {
                    return true;
                }
            }
        }
        return false;
    }
}
