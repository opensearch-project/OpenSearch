/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing.remote;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.threadpool.ThreadPool;

import java.util.function.Supplier;

import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteRoutingTableEnabled;

/**
 * Factory to provide impl for RemoteRoutingTableService based on settings.
 */
public class RemoteRoutingTableServiceFactory {

    /**
     * Returns {@code DefaultRemoteRoutingTableService} if the feature is enabled, otherwise {@code NoopRemoteRoutingTableService}
     * @param repositoriesService repositoriesService
     * @param settings settings
     * @param clusterSettings clusterSettings
     * @param threadPool threadPool
     * @return RemoteRoutingTableService
     */
    public static RemoteRoutingTableService getService(
        Supplier<RepositoriesService> repositoriesService,
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        Compressor compressor,
        BlobStoreTransferService blobStoreTransferService,
        BlobStoreRepository blobStoreRepository,
        String clusterName
    ) {
        if (isRemoteRoutingTableEnabled(settings)) {
            return new InternalRemoteRoutingTableService(
                repositoriesService,
                settings,
                clusterSettings,
                threadPool,
                compressor,
                blobStoreTransferService,
                blobStoreRepository,
                clusterName
            );
        }
        return new NoopRemoteRoutingTableService();
    }
}
