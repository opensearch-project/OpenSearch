/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.repositories;

import org.apache.lucene.index.IndexCommit;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.crypto.CryptoSettings;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateUpdateTask;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.metadata.RepositoriesMetadata;
import org.opensearch.cluster.metadata.RepositoryMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterApplierService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.component.Lifecycle;
import org.opensearch.common.component.LifecycleListener;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.crypto.CryptoClient;
import org.opensearch.crypto.CryptoClientMissingException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.index.store.Store;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.Transport;
import org.opensearch.transport.TransportService;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;

public class RepositoriesServiceTests extends OpenSearchTestCase {

    private RepositoriesService repositoriesService;
    private Map<String, CryptoClient.Factory> cryptoRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ThreadPool threadPool = mock(ThreadPool.class);
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        final ClusterService clusterService = mock(ClusterService.class);
        repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);
    }

    private RepositoriesService createRepositoriesServiceWithMockedClusterService(ClusterService clusterService) {
        ThreadPool threadPool = mock(ThreadPool.class);
        final TransportService transportService = new TransportService(
            Settings.EMPTY,
            mock(Transport.class),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> DiscoveryNode.createLocal(Settings.EMPTY, boundAddress.publishAddress(), UUIDs.randomBase64UUID()),
            null,
            Collections.emptySet()
        );
        final ClusterApplierService clusterApplierService = mock(ClusterApplierService.class);
        when(clusterApplierService.threadPool()).thenReturn(threadPool);
        when(clusterService.getClusterApplierService()).thenReturn(clusterApplierService);
        Map<String, Repository.Factory> typesRegistry = Map.of(
            TestRepository.TYPE,
            TestRepository::new,
            MeteredRepositoryTypeA.TYPE,
            metadata -> new MeteredRepositoryTypeA(metadata, clusterService),
            MeteredRepositoryTypeB.TYPE,
            metadata -> new MeteredRepositoryTypeB(metadata, clusterService)
        );

        Map<String, CryptoClient.Factory> cryptoRegistry = Map.of(
            TestCryptoClientTypeA.TYPE,
            new CryptoCreator(TestCryptoClientTypeA.TYPE),
            TestCryptoClientTypeB.TYPE,
            new CryptoCreator(TestCryptoClientTypeB.TYPE)
        );
        this.cryptoRegistry = cryptoRegistry;
        DiscoveryNodes nodes = mock(DiscoveryNodes.class);
        when(nodes.getMinNodeVersion()).thenReturn(Version.V_2_9_0);
        ClusterState currentClusterState = mock(ClusterState.class);
        when(currentClusterState.getNodes()).thenReturn(nodes);
        when(clusterService.state()).thenReturn(currentClusterState);

        RepositoriesService repositoriesService = new RepositoriesService(
            Settings.EMPTY,
            clusterService,
            transportService,
            typesRegistry,
            typesRegistry,
            cryptoRegistry,
            threadPool
        );

        repositoriesService.start();
        return repositoriesService;
    }

    class CryptoCreator implements CryptoClient.Factory {
        private final Map<String, CryptoClient> cryptoClients = new HashMap<>();
        private final String type;

        public CryptoCreator(String type) {
            this.type = type;
        }

        @Override
        public CryptoClient create(Settings cryptoSettings, String keyProviderName) {
            if (cryptoClients.containsKey(keyProviderName)) {
                cryptoClients.get(keyProviderName).incRef();
                return cryptoClients.get(keyProviderName);
            }

            CryptoClient cryptoClient;
            switch (type) {
                case TestCryptoClientTypeA.TYPE:
                    cryptoClient = new TestCryptoClientTypeA(cryptoSettings, keyProviderName);
                    cryptoClients.put(keyProviderName, cryptoClient);
                    return cryptoClient;
                case TestCryptoClientTypeB.TYPE:
                    cryptoClient = new TestCryptoClientTypeB(cryptoSettings, keyProviderName);
                    cryptoClients.put(keyProviderName, cryptoClient);
                    return cryptoClient;
            }
            return null;
        }
    }

    public void testRegisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertEquals(repoName, repository.getMetadata().name());
        assertEquals(TestRepository.TYPE, repository.getMetadata().type());
        assertEquals(Settings.EMPTY, repository.getMetadata().settings());
        assertTrue(((TestRepository) repository).isStarted);
    }

    public void testUnregisterInternalRepository() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.unregisterInternalRepository(repoName);
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        assertTrue(((TestRepository) repository).isClosed);
    }

    public void testRegisterWillNotUpdateIfInternalRepositoryWithNameExists() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        Repository repository = repositoriesService.repository(repoName);
        assertFalse(((TestRepository) repository).isClosed);
        repositoriesService.registerInternalRepository(repoName, TestRepository.TYPE);
        assertFalse(((TestRepository) repository).isClosed);
        Repository repository2 = repositoriesService.repository(repoName);
        assertSame(repository, repository2);
    }

    public void testRegisterRejectsInvalidRepositoryNames() {
        assertThrowsOnRegister("");
        assertThrowsOnRegister("contains#InvalidCharacter");
        for (char c : Strings.INVALID_FILENAME_CHARS) {
            assertThrowsOnRegister("contains" + c + "InvalidCharacters");
        }
    }

    public void testRepositoriesStatsCanHaveTheSameNameAndDifferentTypeOverTime() {
        String repoName = "name";
        expectThrows(RepositoryMissingException.class, () -> repositoriesService.repository(repoName));

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithRepo(repoName, MeteredRepositoryTypeA.TYPE);

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", emptyState(), clusterStateWithRepoTypeA));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithRepo(repoName, MeteredRepositoryTypeB.TYPE);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));

        List<RepositoryStatsSnapshot> repositoriesStats = repositoriesService.repositoriesStats();
        assertThat(repositoriesStats.size(), equalTo(2));
        RepositoryStatsSnapshot repositoryStatsTypeA = repositoriesStats.get(0);
        assertThat(repositoryStatsTypeA.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeA.TYPE));
        assertThat(repositoryStatsTypeA.getRepositoryStats(), equalTo(MeteredRepositoryTypeA.STATS));

        RepositoryStatsSnapshot repositoryStatsTypeB = repositoriesStats.get(1);
        assertThat(repositoryStatsTypeB.getRepositoryInfo().type, equalTo(MeteredRepositoryTypeB.TYPE));
        assertThat(repositoryStatsTypeB.getRepositoryStats(), equalTo(MeteredRepositoryTypeB.STATS));
    }

    public void testWithSameKeyProviderNames() {
        CryptoMetadata cryptoMetadata = new CryptoMetadata("kp-name", "kp-type", Settings.EMPTY);
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata("repoName", "repoType", Settings.EMPTY, true, cryptoMetadata);
        expectThrows(CryptoClientMissingException.class, () -> repositoriesService.createCryptoClient(repositoryMetadata, cryptoRegistry));

        String keyProviderName = "kp-name";
        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            TestCryptoClientTypeA.TYPE
        );

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        assertEquals(1, repositoriesService.totalCryptoClients());

        ClusterState clusterStateWithRepoTypeB = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeB.TYPE,
            keyProviderName,
            TestCryptoClientTypeB.TYPE
        );
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeB, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(2));
        assertEquals(1, repositoriesService.totalCryptoClients());
    }

    public void testCryptoClientsUnchangedWithSameCryptoMetadata() {
        String keyProviderName = "kp-name";
        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            TestCryptoClientTypeA.TYPE
        );
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        assertEquals(1, repositoriesService.totalCryptoClients());

        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        assertEquals(1, repositoriesService.totalCryptoClients());
    }

    public void testRepositoryUpdateWithDifferentCryptoMetadata() {
        String keyProviderName = "kp-name";

        ClusterState clusterStateWithRepoTypeA = createClusterStateWithKeyProvider(
            "repoName",
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            TestCryptoClientTypeA.TYPE
        );
        ClusterService clusterService = mock(ClusterService.class);

        PutRepositoryRequest request = new PutRepositoryRequest("repoName");
        request.type(MeteredRepositoryTypeA.TYPE);
        request.settings(Settings.EMPTY);

        doAnswer((invocation) -> {
            AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = (AckedClusterStateUpdateTask<
                ClusterStateUpdateResponse>) invocation.getArguments()[1];
            task.execute(clusterStateWithRepoTypeA);
            return null;
        }).when(clusterService).submitStateUpdateTask(any(), any());

        RepositoriesService repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);
        repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
        assertThat(repositoriesService.repositoriesStats().size(), equalTo(1));
        assertEquals(1, repositoriesService.totalCryptoClients());

        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerRepository(request, null));

        request.encrypted(true);
        CryptoSettings cryptoSettings = new CryptoSettings(keyProviderName);
        cryptoSettings.keyProviderType(TestCryptoClientTypeA.TYPE);
        cryptoSettings.settings(Settings.builder().put("key-1", "val-1"));
        request.cryptoSettings(cryptoSettings);
        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerRepository(request, null));

        cryptoSettings.settings(Settings.builder());
        cryptoSettings.keyProviderName("random");
        expectThrows(IllegalArgumentException.class, () -> repositoriesService.registerRepository(request, null));

        cryptoSettings.keyProviderName(keyProviderName);
        cryptoSettings.keyProviderType("random");
        AtomicBoolean expectedExceptionFound = new AtomicBoolean();
        repositoriesService.registerRepository(request, new ActionListener<>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {}

            @Override
            public void onFailure(Exception e) {
                if (e instanceof CryptoClientMissingException) {
                    expectedExceptionFound.set(true);
                }
            }
        });
        assertTrue(expectedExceptionFound.get());

        cryptoSettings.keyProviderType(TestCryptoClientTypeA.TYPE);
        repositoriesService.registerRepository(request, null);
    }

    public void testCryptoClientClusterStateChanges() {

        ClusterService clusterService = mock(ClusterService.class);
        AtomicBoolean verified = new AtomicBoolean();
        List<RepositoryMetadata> repositoryMetadata = new ArrayList<>();

        String keyProviderName = "kp-name-1";
        String repoName = "repoName";
        String keyProviderType = TestCryptoClientTypeA.TYPE;
        Settings.Builder settings = Settings.builder();
        PutRepositoryRequest request = createPutRepositoryEncryptedRequest(
            repoName,
            MeteredRepositoryTypeA.TYPE,
            keyProviderName,
            settings,
            keyProviderType
        );
        verified.set(false);
        RepositoriesService repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerRepository(request, null);
        TestCryptoClientTypeA cryptoClientTypeA = (TestCryptoClientTypeA) repositoriesService.getCryptoClient(
            keyProviderName + "#" + TestCryptoClientTypeA.TYPE
        );
        assertNotNull(cryptoClientTypeA);
        assertEquals(1, cryptoClientTypeA.getReferenceCount());
        assertTrue(verified.get());

        // No change
        keyProviderType = TestCryptoClientTypeA.TYPE;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerRepository(request, null);
        cryptoClientTypeA = (TestCryptoClientTypeA) repositoriesService.getCryptoClient(keyProviderName + "#" + keyProviderType);
        assertNotNull(cryptoClientTypeA);
        assertEquals(1, cryptoClientTypeA.getReferenceCount());
        assertTrue(verified.get());

        // Same crypto client in new repo
        repoName = "repoName-2";
        keyProviderType = TestCryptoClientTypeA.TYPE;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerRepository(request, null);
        cryptoClientTypeA = (TestCryptoClientTypeA) repositoriesService.getCryptoClient(keyProviderName + "#" + keyProviderType);
        assertNotNull(cryptoClientTypeA);
        assertEquals(2, cryptoClientTypeA.getReferenceCount());
        assertTrue(verified.get());

        // Different crypto client in new repo
        repoName = "repoName-3";
        keyProviderType = TestCryptoClientTypeB.TYPE;
        settings = Settings.builder();
        request = createPutRepositoryEncryptedRequest(repoName, MeteredRepositoryTypeA.TYPE, keyProviderName, settings, keyProviderType);
        verified.set(false);
        repositoriesService = createRepositoriesServiceAndMockCryptoClusterState(
            clusterService,
            repoName,
            keyProviderName,
            keyProviderType,
            settings.build(),
            verified,
            repositoryMetadata
        );
        repositoriesService.registerRepository(request, null);
        TestCryptoClientTypeB cryptoClientTypeB = (TestCryptoClientTypeB) repositoriesService.getCryptoClient(
            keyProviderName + "#" + keyProviderType
        );
        assertNotNull(cryptoClientTypeB);
        assertEquals(1, cryptoClientTypeB.getReferenceCount());
        assertEquals(2, cryptoClientTypeA.getReferenceCount());
        assertTrue(verified.get());

    }

    private RepositoriesService createRepositoriesServiceAndMockCryptoClusterState(
        ClusterService clusterService,
        String repoName,
        String keyProviderName,
        String keyProviderType,
        Settings settings,
        AtomicBoolean verified,
        List<RepositoryMetadata> repositoryMetadataList
    ) {

        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        CryptoMetadata newCryptoMetadata = new CryptoMetadata(keyProviderName, keyProviderType, Settings.EMPTY);
        Metadata.Builder mdBuilder = Metadata.builder();

        RepositoryMetadata newRepositoryMetadata = new RepositoryMetadata(
            repoName,
            MeteredRepositoryTypeA.TYPE,
            Settings.EMPTY,
            true,
            newCryptoMetadata
        );
        if (!repositoryMetadataList.contains(newRepositoryMetadata)) {
            repositoryMetadataList.add(newRepositoryMetadata);
        }
        RepositoriesMetadata newRepositoriesMetadata = new RepositoriesMetadata(repositoryMetadataList);
        mdBuilder.putCustom(RepositoriesMetadata.TYPE, newRepositoriesMetadata);
        state.metadata(mdBuilder);
        ClusterState clusterStateWithRepoTypeA = state.build();

        RepositoriesService repositoriesService = createRepositoriesServiceWithMockedClusterService(clusterService);

        doAnswer((invocation) -> {
            AckedClusterStateUpdateTask<ClusterStateUpdateResponse> task = (AckedClusterStateUpdateTask<
                ClusterStateUpdateResponse>) invocation.getArguments()[1];
            ClusterState clusterState = task.execute(clusterStateWithRepoTypeA);
            RepositoriesMetadata repositories = clusterState.metadata().custom(RepositoriesMetadata.TYPE);
            RepositoryMetadata repositoryMetadata = repositories.repositories().get(repositoryMetadataList.size() - 1);
            assertTrue(repositoryMetadata.encrypted());
            CryptoMetadata cryptoMetadata = repositoryMetadata.cryptoMetadata();
            assertNotNull(cryptoMetadata);
            assertEquals(keyProviderName, cryptoMetadata.keyProviderName());
            assertEquals(keyProviderType, cryptoMetadata.keyProviderType());
            assertEquals(cryptoMetadata.settings(), settings);
            verified.set(true);
            repositoriesService.applyClusterState(new ClusterChangedEvent("new repo", clusterStateWithRepoTypeA, emptyState()));
            return null;
        }).when(clusterService).submitStateUpdateTask(any(), any());

        return repositoriesService;
    }

    private ClusterState createClusterStateWithRepo(String repoName, String repoType) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        mdBuilder.putCustom(
            RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY)))
        );
        state.metadata(mdBuilder);

        return state.build();
    }

    private ClusterState createClusterStateWithKeyProvider(
        String repoName,
        String repoType,
        String keyProviderName,
        String keyProviderType
    ) {
        ClusterState.Builder state = ClusterState.builder(new ClusterName("test"));
        Metadata.Builder mdBuilder = Metadata.builder();
        CryptoMetadata cryptoMetadata = new CryptoMetadata(keyProviderName, keyProviderType, Settings.EMPTY);
        mdBuilder.putCustom(
            RepositoriesMetadata.TYPE,
            new RepositoriesMetadata(
                Collections.singletonList(new RepositoryMetadata(repoName, repoType, Settings.EMPTY, true, cryptoMetadata))
            )
        );
        state.metadata(mdBuilder);

        return state.build();
    }

    private PutRepositoryRequest createPutRepositoryEncryptedRequest(
        String repoName,
        String repoType,
        String keyProviderName,
        Settings.Builder settings,
        String keyProviderType
    ) {
        PutRepositoryRequest repositoryRequest = new PutRepositoryRequest(repoName);
        repositoryRequest.type(repoType);
        repositoryRequest.settings(Settings.EMPTY);
        repositoryRequest.encrypted(true);
        CryptoSettings cryptoSettings = new CryptoSettings(keyProviderName);
        cryptoSettings.keyProviderName(keyProviderName);
        cryptoSettings.keyProviderType(keyProviderType);
        cryptoSettings.settings(settings);
        repositoryRequest.cryptoSettings(cryptoSettings);

        return repositoryRequest;
    }

    private ClusterState emptyState() {
        return ClusterState.builder(new ClusterName("test")).build();
    }

    private void assertThrowsOnRegister(String repoName) {
        PutRepositoryRequest request = new PutRepositoryRequest(repoName);
        expectThrows(RepositoryException.class, () -> repositoriesService.registerRepository(request, null));
    }

    private static abstract class TestCryptoClient implements CryptoClient {
        private final String name;
        private final AtomicInteger ref;

        public TestCryptoClient(Settings settings, String keyProviderName) {
            this.name = keyProviderName;
            this.ref = new AtomicInteger(1);
        }

        @Override
        public void incRef() {
            ref.incrementAndGet();
        }

        @Override
        public boolean tryIncRef() {
            ref.incrementAndGet();
            return true;
        }

        @Override
        public boolean decRef() {
            ref.decrementAndGet();
            return true;
        }

        public int getReferenceCount() {
            return ref.get();
        }

        @Override
        public String name() {
            return name;
        }

        @Override
        public Object initCryptoContext() {
            return new Object();
        }

        @Override
        public long adjustEncryptedStreamSize(Object cryptoContextObj, long streamSize) {
            return 0;
        }

        @Override
        public long estimateEncryptedLength(Object cryptoContextObj, long contentLength) {
            return 0;
        }

        @Override
        public InputStreamContainer createEncryptingStream(Object cryptoContext, InputStreamContainer streamContainer) {
            return null;
        }

        @Override
        public InputStreamContainer createEncryptingStreamOfPart(
            Object cryptoContextObj,
            InputStreamContainer stream,
            int totalStreams,
            int streamIdx
        ) {
            return null;
        }

        @Override
        public InputStream createDecryptingStream(InputStream encryptingStream) {
            return null;
        }
    }

    private static class TestCryptoClientTypeA extends TestCryptoClient {
        public static final String TYPE = "type-A";

        public TestCryptoClientTypeA(Settings settings, String keyProviderName) {
            super(settings, keyProviderName);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    private static class TestCryptoClientTypeB extends TestCryptoClient {
        public static final String TYPE = "type-B";

        public TestCryptoClientTypeB(Settings settings, String keyProviderName) {
            super(settings, keyProviderName);
        }

        @Override
        public String type() {
            return TYPE;
        }
    }

    private static class TestRepository implements Repository {

        private static final String TYPE = "internal";
        private boolean isClosed;
        private boolean isStarted;

        private final RepositoryMetadata metadata;

        private TestRepository(RepositoryMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public RepositoryMetadata getMetadata() {
            return metadata;
        }

        @Override
        public SnapshotInfo getSnapshotInfo(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public Metadata getSnapshotGlobalMetadata(SnapshotId snapshotId) {
            return null;
        }

        @Override
        public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) {
            return null;
        }

        @Override
        public void getRepositoryData(ActionListener<RepositoryData> listener) {
            listener.onResponse(null);
        }

        @Override
        public void finalizeSnapshot(
            ShardGenerations shardGenerations,
            long repositoryStateId,
            Metadata clusterMetadata,
            SnapshotInfo snapshotInfo,
            Version repositoryMetaVersion,
            Function<ClusterState, ClusterState> stateTransformer,
            ActionListener<RepositoryData> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public void deleteSnapshots(
            Collection<SnapshotId> snapshotIds,
            long repositoryStateId,
            Version repositoryMetaVersion,
            ActionListener<RepositoryData> listener
        ) {
            listener.onResponse(null);
        }

        @Override
        public long getSnapshotThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public long getRestoreThrottleTimeInNanos() {
            return 0;
        }

        @Override
        public String startVerification() {
            return null;
        }

        @Override
        public void endVerification(String verificationToken) {

        }

        @Override
        public void verify(String verificationToken, DiscoveryNode localNode) {

        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public void snapshotShard(
            Store store,
            MapperService mapperService,
            SnapshotId snapshotId,
            IndexId indexId,
            IndexCommit snapshotIndexCommit,
            String shardStateIdentifier,
            IndexShardSnapshotStatus snapshotStatus,
            Version repositoryMetaVersion,
            Map<String, Object> userMetadata,
            ActionListener<String> listener
        ) {

        }

        @Override
        public void restoreShard(
            Store store,
            SnapshotId snapshotId,
            IndexId indexId,
            ShardId snapshotShardId,
            RecoveryState recoveryState,
            ActionListener<Void> listener
        ) {

        }

        @Override
        public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
            return null;
        }

        @Override
        public void updateState(final ClusterState state) {}

        @Override
        public void executeConsistentStateUpdate(
            Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
            String source,
            Consumer<Exception> onFailure
        ) {}

        @Override
        public void cloneShardSnapshot(
            SnapshotId source,
            SnapshotId target,
            RepositoryShardId shardId,
            String shardGeneration,
            ActionListener<String> listener
        ) {

        }

        @Override
        public Lifecycle.State lifecycleState() {
            return null;
        }

        @Override
        public void addLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void removeLifecycleListener(LifecycleListener listener) {

        }

        @Override
        public void start() {
            isStarted = true;
        }

        @Override
        public void stop() {

        }

        @Override
        public void close() {
            isClosed = true;
        }
    }

    private static class MeteredRepositoryTypeA extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-a";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("GET", 10L));

        private MeteredRepositoryTypeA(RepositoryMetadata metadata, ClusterService clusterService) {
            super(
                metadata,
                false,
                mock(NamedXContentRegistry.class),
                clusterService,
                mock(RecoverySettings.class),
                Map.of("bucket", "bucket-a")
            );
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }

    private static class MeteredRepositoryTypeB extends MeteredBlobStoreRepository {
        private static final String TYPE = "type-b";
        private static final RepositoryStats STATS = new RepositoryStats(Map.of("LIST", 20L));

        private MeteredRepositoryTypeB(RepositoryMetadata metadata, ClusterService clusterService) {
            super(
                metadata,
                false,
                mock(NamedXContentRegistry.class),
                clusterService,
                mock(RecoverySettings.class),
                Map.of("bucket", "bucket-b")
            );
        }

        @Override
        protected BlobStore createBlobStore() {
            return mock(BlobStore.class);
        }

        @Override
        public RepositoryStats stats() {
            return STATS;
        }

        @Override
        public BlobPath basePath() {
            return BlobPath.cleanPath();
        }
    }
}
