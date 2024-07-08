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
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;

import java.util.function.Supplier;

import static org.opensearch.common.util.FeatureFlags.REMOTE_PUBLICATION_EXPERIMENTAL;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.mockito.Mockito.mock;

public class RemoteRoutingTableServiceFactoryTests extends OpenSearchTestCase {

    Supplier<RepositoriesService> repositoriesService;
    private ThreadPool threadPool = new TestThreadPool(getClass().getName());

    @After
    public void teardown() throws Exception {
        super.tearDown();
        threadPool.shutdown();
    }

    public void testGetServiceWhenRemoteRoutingDisabled() {
        Settings settings = Settings.builder().build();
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        Compressor compressor = new NoneCompressor();
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            compressor,
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster"
        );
        assertTrue(service instanceof NoopRemoteRoutingTableService);
    }

    public void testGetServiceWhenRemoteRoutingEnabled() {
        Settings settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, "routing_repository")
            .put(FsRepository.REPOSITORIES_COMPRESS_SETTING.getKey(), false)
            .build();
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        Compressor compressor = new NoneCompressor();
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        Settings nodeSettings = Settings.builder().put(REMOTE_PUBLICATION_EXPERIMENTAL, "true").build();
        FeatureFlags.initializeFeatureFlags(nodeSettings);
        RemoteRoutingTableService service = RemoteRoutingTableServiceFactory.getService(
            repositoriesService,
            settings,
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            compressor,
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster"
        );
        assertTrue(service instanceof InternalRemoteRoutingTableService);
    }

}
