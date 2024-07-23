/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.gateway.remote.RemoteClusterStateUtils;
import org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.translog.transfer.BlobStoreTransferService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.opensearch.gateway.remote.RemoteClusterStateUtils.CLUSTER_STATE_PATH_TOKEN;
import static org.opensearch.gateway.remote.routingtable.RemoteIndexRoutingTable.INDEX_ROUTING_TABLE;
import static org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteRoutingTableBlobStoreTests extends OpenSearchTestCase {

    private RemoteRoutingTableBlobStore<IndexRoutingTable, RemoteIndexRoutingTable> remoteIndexRoutingTableStore;
    ClusterSettings clusterSettings;
    ThreadPool threadPool;

    @Before
    public void setup() {
        clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BlobStoreTransferService blobStoreTransferService = mock(BlobStoreTransferService.class);
        BlobStoreRepository blobStoreRepository = mock(BlobStoreRepository.class);
        BlobPath blobPath = new BlobPath().add("base-path");
        when(blobStoreRepository.basePath()).thenReturn(blobPath);

        threadPool = new TestThreadPool(getClass().getName());
        this.remoteIndexRoutingTableStore = new RemoteRoutingTableBlobStore<>(
            blobStoreTransferService,
            blobStoreRepository,
            "test-cluster",
            threadPool,
            ThreadPool.Names.REMOTE_STATE_READ,
            clusterSettings
        );
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdown();

    }

    public void testRemoteRoutingTablePathTypeSetting() {
        // Assert the default is HASHED_PREFIX
        assertEquals(HASHED_PREFIX.toString(), remoteIndexRoutingTableStore.getPathTypeSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.routing_table.path_type", RemoteStoreEnums.PathType.FIXED.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(RemoteStoreEnums.PathType.FIXED.toString(), remoteIndexRoutingTableStore.getPathTypeSetting().toString());
    }

    public void testRemoteRoutingTableHashAlgoSetting() {
        // Assert the default is FNV_1A_BASE64
        assertEquals(FNV_1A_BASE64.toString(), remoteIndexRoutingTableStore.getPathHashAlgoSetting().toString());

        Settings newSettings = Settings.builder()
            .put("cluster.remote_store.routing_table.path_hash_algo", RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString())
            .build();
        clusterSettings.applySettings(newSettings);
        assertEquals(
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1.toString(),
            remoteIndexRoutingTableStore.getPathHashAlgoSetting().toString()
        );
    }

    public void testGetBlobPathForUpload() {

        Index index = new Index("test-idx", "index-uuid");
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, index.getUUID())
            .build();

        IndexMetadata indexMetadata = new IndexMetadata.Builder(index.getName()).settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        IndexRoutingTable indexRoutingTable = new IndexRoutingTable.Builder(index).initializeAsNew(indexMetadata).build();

        RemoteIndexRoutingTable remoteObjectForUpload = new RemoteIndexRoutingTable(
            indexRoutingTable,
            "cluster-uuid",
            new DeflateCompressor(),
            2L,
            3L
        );
        BlobPath blobPath = remoteIndexRoutingTableStore.getBlobPathForUpload(remoteObjectForUpload);
        BlobPath expectedPath = HASHED_PREFIX.path(
            RemoteStorePathStrategy.BasePathInput.builder()
                .basePath(
                    new BlobPath().add("base-path")
                        .add(RemoteClusterStateUtils.encodeString("test-cluster"))
                        .add(CLUSTER_STATE_PATH_TOKEN)
                        .add("cluster-uuid")
                        .add(INDEX_ROUTING_TABLE)
                )
                .indexUUID(index.getUUID())
                .build(),
            FNV_1A_BASE64
        );
        assertEquals(expectedPath, blobPath);
    }
}
