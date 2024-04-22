/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SetOnce;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.RemoteClusterStateService.RemoteStateTransferException;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.node.Node;
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.mockito.Mockito;

import static org.opensearch.index.remote.RemoteStoreEnums.PathType.FIXED;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_INFIX;
import static org.opensearch.index.remote.RemoteStoreEnums.PathType.HASHED_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteIndexPathUploaderTests extends OpenSearchTestCase {

    private static final String CLUSTER_STATE_REPO_KEY = Node.NODE_ATTRIBUTES.getKey()
        + RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;

    private static final String TRANSLOG_REPO_NAME = "translog-repo";
    private static final String SEGMENT_REPO_NAME = "segment-repo";

    private final ThreadPool threadPool = new TestThreadPool(getTestName());
    private Settings settings;
    private ClusterSettings clusterSettings;
    private RepositoriesService repositoriesService;
    private BlobStoreRepository repository;
    private BlobStore blobStore;
    private BlobContainer blobContainer;
    private BlobPath basePath;
    private List<IndexMetadata> indexMetadataList;
    private final AtomicLong successCount = new AtomicLong();
    private final AtomicLong failureCount = new AtomicLong();

    @Before
    public void setup() {
        settings = Settings.builder()
            .put(RemoteIndexPathUploader.TRANSLOG_REPO_NAME_KEY, TRANSLOG_REPO_NAME)
            .put(RemoteIndexPathUploader.SEGMENT_REPO_NAME_KEY, TRANSLOG_REPO_NAME)
            .put(CLUSTER_STATE_REPO_KEY, TRANSLOG_REPO_NAME)
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        basePath = BlobPath.cleanPath().add("test");
        repositoriesService = mock(RepositoriesService.class);
        repository = mock(BlobStoreRepository.class);
        when(repositoriesService.repository(anyString())).thenReturn(repository);
        blobStore = mock(BlobStore.class);
        when(repository.blobStore()).thenReturn(blobStore);
        when(repositoriesService.repository(TRANSLOG_REPO_NAME)).thenReturn(repository);
        when(repository.basePath()).thenReturn(basePath);
        when(repository.getCompressor()).thenReturn(new DeflateCompressor());
        blobContainer = mock(BlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);

        Map<String, String> remoteCustomData = Map.of(
            PathType.NAME,
            HASHED_PREFIX.name(),
            RemoteStoreEnums.PathHashAlgorithm.NAME,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64.name()
        );
        Settings idxSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();
        IndexMetadata indexMetadata = new IndexMetadata.Builder("test").settings(idxSettings)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(IndexMetadata.REMOTE_STORE_CUSTOM_KEY, remoteCustomData)
            .build();
        indexMetadataList = List.of(indexMetadata);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testInterceptWithNoRemoteDataAttributes() {
        Settings settings = Settings.Builder.EMPTY_SETTINGS;
        clusterSettings.applySettings(settings);
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        List<IndexMetadata> indexMetadataList = Mockito.<List>mock(List.class);
        ActionListener<Void> actionListener = ActionListener.wrap(
            res -> successCount.incrementAndGet(),
            ex -> failureCount.incrementAndGet()
        );
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(1, successCount.get());
        assertEquals(0, failureCount.get());
        verify(indexMetadataList, times(0)).stream();
    }

    public void testInterceptWithEmptyIndexMetadataList() {
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();
        ActionListener<Void> actionListener = ActionListener.wrap(
            res -> successCount.incrementAndGet(),
            ex -> failureCount.incrementAndGet()
        );
        remoteIndexPathUploader.doOnNewIndexUpload(Collections.emptyList(), actionListener);
        assertEquals(1, successCount.get());
        assertEquals(0, failureCount.get());
    }

    public void testInterceptWithEmptyEligibleIndexMetadataList() {
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();
        ActionListener<Void> actionListener = ActionListener.wrap(
            res -> successCount.incrementAndGet(),
            ex -> failureCount.incrementAndGet()
        );

        // Case 1 - Null remoteCustomData
        List<IndexMetadata> indexMetadataList = new ArrayList<>();
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        indexMetadataList.add(indexMetadata);
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(1, successCount.get());
        assertEquals(0, failureCount.get());

        // Case 2 - Empty remoteCustomData
        when(indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY)).thenReturn(new HashMap<>());
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(2, successCount.get());
        assertEquals(0, failureCount.get());

        // Case 3 - RemoteStoreEnums.PathType.NAME not in remoteCustomData map
        Map<String, String> remoteCustomData = Map.of("test", "test");
        when(indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY)).thenReturn(remoteCustomData);
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(3, successCount.get());
        assertEquals(0, failureCount.get());

        // Case 4 - RemoteStoreEnums.PathType.NAME is not HASHED_PREFIX
        remoteCustomData = Map.of(PathType.NAME, randomFrom(FIXED, HASHED_INFIX).name());
        when(indexMetadata.getCustomData(IndexMetadata.REMOTE_STORE_CUSTOM_KEY)).thenReturn(remoteCustomData);
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(4, successCount.get());
        assertEquals(0, failureCount.get());
    }

    public void testInterceptWithSameRepo() throws IOException {
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();
        ActionListener<Void> actionListener = ActionListener.wrap(
            res -> successCount.incrementAndGet(),
            ex -> failureCount.incrementAndGet()
        );
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(1, successCount.get());
        assertEquals(0, failureCount.get());
        verify(blobContainer, times(1)).writeBlob(anyString(), any(InputStream.class), anyLong(), anyBoolean());
    }

    public void testInterceptWithDifferentRepo() throws IOException {
        Settings settings = Settings.builder()
            .put(this.settings)
            .put(RemoteIndexPathUploader.SEGMENT_REPO_NAME_KEY, SEGMENT_REPO_NAME)
            .build();
        when(repositoriesService.repository(SEGMENT_REPO_NAME)).thenReturn(repository);
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();
        ActionListener<Void> actionListener = ActionListener.wrap(
            res -> successCount.incrementAndGet(),
            ex -> failureCount.incrementAndGet()
        );
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(1, successCount.get());
        assertEquals(0, failureCount.get());
        verify(blobContainer, times(2)).writeBlob(anyString(), any(InputStream.class), anyLong(), anyBoolean());
    }

    public void testInterceptWithLatchAwaitTimeout() throws IOException {
        blobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(blobContainer);
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();

        Settings settings = Settings.builder()
            .put(this.settings)
            .put(RemoteClusterStateService.INDEX_METADATA_UPLOAD_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            .build();
        clusterSettings.applySettings(settings);
        SetOnce<Exception> exceptionSetOnce = new SetOnce<>();
        ActionListener<Void> actionListener = ActionListener.wrap(res -> successCount.incrementAndGet(), ex -> {
            failureCount.incrementAndGet();
            exceptionSetOnce.set(ex);
        });
        remoteIndexPathUploader.doOnNewIndexUpload(indexMetadataList, actionListener);
        assertEquals(0, successCount.get());
        assertEquals(1, failureCount.get());
        assertTrue(exceptionSetOnce.get() instanceof RemoteStateTransferException);
        assertTrue(
            exceptionSetOnce.get().getMessage().contains("Timed out waiting while uploading remote index path file for indexes=[test/")
        );
        verify(blobContainer, times(0)).writeBlob(anyString(), any(InputStream.class), anyLong(), anyBoolean());
    }

    public void testInterceptWithInterruptedExceptionDuringLatchAwait() throws Exception {
        AsyncMultiStreamBlobContainer asyncMultiStreamBlobContainer = mock(AsyncMultiStreamBlobContainer.class);
        when(blobStore.blobContainer(any(BlobPath.class))).thenReturn(asyncMultiStreamBlobContainer);
        RemoteIndexPathUploader remoteIndexPathUploader = new RemoteIndexPathUploader(
            threadPool,
            settings,
            () -> repositoriesService,
            clusterSettings
        );
        remoteIndexPathUploader.start();
        Settings settings = Settings.builder()
            .put(this.settings)
            .put(RemoteClusterStateService.INDEX_METADATA_UPLOAD_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
        clusterSettings.applySettings(settings);
        SetOnce<Exception> exceptionSetOnce = new SetOnce<>();
        ActionListener<Void> actionListener = ActionListener.wrap(res -> successCount.incrementAndGet(), ex -> {
            failureCount.incrementAndGet();
            exceptionSetOnce.set(ex);
        });
        Thread thread = new Thread(() -> {
            try {
                remoteIndexPathUploader.onNewIndexUpload(indexMetadataList, actionListener);
            } catch (Exception e) {
                assertTrue(e instanceof InterruptedException);
                assertEquals("sleep interrupted", e.getMessage());
            }
        });
        thread.start();
        Thread.sleep(10);
        thread.interrupt();

        assertBusy(() -> {
            assertEquals(0, successCount.get());
            assertEquals(1, failureCount.get());
        });
    }

}
