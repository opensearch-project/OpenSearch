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

package org.opensearch.gateway;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.CoordinationMetadata;
import org.opensearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.opensearch.cluster.coordination.CoordinationState;
import org.opensearch.cluster.coordination.CoordinationState.PersistedState;
import org.opensearch.cluster.coordination.PersistedStateRegistry;
import org.opensearch.cluster.coordination.PersistedStateRegistry.PersistedStateType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Manifest;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.MockBigArrays;
import org.opensearch.common.util.MockPageCacheRecycler;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.gateway.GatewayMetaState.RemotePersistedState;
import org.opensearch.gateway.PersistedClusterStateService.Writer;
import org.opensearch.gateway.remote.ClusterMetadataManifest;
import org.opensearch.gateway.remote.RemoteClusterStateService;
import org.opensearch.gateway.remote.RemotePersistenceStats;
import org.opensearch.index.recovery.RemoteStoreRestoreService;
import org.opensearch.index.recovery.RemoteStoreRestoreService.RemoteRestoreResult;
import org.opensearch.index.remote.RemoteIndexPathUploader;
import org.opensearch.node.Node;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOError;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.isRemoteStoreClusterStateEnabled;
import static org.opensearch.test.NodeRoles.nonClusterManagerNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class GatewayMetaStatePersistedStateTests extends OpenSearchTestCase {

    private NodeEnvironment nodeEnvironment;
    private ClusterName clusterName;
    private Settings settings;
    private DiscoveryNode localNode;
    private BigArrays bigArrays;

    private MockGatewayMetaState gateway;

    @Override
    public void setUp() throws Exception {
        bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        nodeEnvironment = newNodeEnvironment();
        localNode = new DiscoveryNode(
            "node1",
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Sets.newHashSet(DiscoveryNodeRole.CLUSTER_MANAGER_ROLE),
            Version.CURRENT
        );
        clusterName = new ClusterName(randomAlphaOfLength(10));
        settings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value()).build();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        nodeEnvironment.close();
        IOUtils.close(gateway);
        super.tearDown();
    }

    private CoordinationState.PersistedState newGatewayPersistedState() throws IOException {
        IOUtils.close(gateway);
        gateway = new MockGatewayMetaState(localNode, bigArrays);
        final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
        gateway.start(settings, nodeEnvironment, xContentRegistry(), persistedStateRegistry);
        final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
        assertThat(persistedState, instanceOf(GatewayMetaState.LucenePersistedState.class));
        assertThat(
            persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL),
            instanceOf(GatewayMetaState.LucenePersistedState.class)
        );
        assertThat(persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE), nullValue());
        return persistedState;
    }

    private CoordinationState.PersistedState maybeNew(CoordinationState.PersistedState persistedState) throws IOException {
        if (randomBoolean()) {
            persistedState.close();
            return newGatewayPersistedState();
        }
        return persistedState;
    }

    public void testInitialState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            ClusterState state = gateway.getLastAcceptedState();
            assertThat(state.getClusterName(), equalTo(clusterName));
            assertTrue(Metadata.isGlobalStateEquals(state.metadata(), Metadata.EMPTY_METADATA));
            assertThat(state.getVersion(), equalTo(Manifest.empty().getClusterStateVersion()));
            assertThat(state.getNodes().getLocalNode(), equalTo(localNode));

            long currentTerm = gateway.getCurrentTerm();
            assertThat(currentTerm, equalTo(Manifest.empty().getCurrentTerm()));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testSetCurrentTerm() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long currentTerm = randomNonNegativeLong();
                gateway.setCurrentTerm(currentTerm);
                gateway = maybeNew(gateway);
                assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
            }
        } finally {
            IOUtils.close(gateway);
        }
    }

    private ClusterState createClusterState(long version, Metadata metadata) {
        return ClusterState.builder(clusterName)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).build())
            .version(version)
            .metadata(metadata)
            .build();
    }

    private CoordinationMetadata createCoordinationMetadata(long term) {
        CoordinationMetadata.Builder builder = CoordinationMetadata.builder();
        builder.term(term);
        builder.lastAcceptedConfiguration(
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
        );
        builder.lastCommittedConfiguration(
            new CoordinationMetadata.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(10, 10, false)))
        );
        for (int i = 0; i < randomIntBetween(0, 5); i++) {
            builder.addVotingConfigExclusion(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10)));
        }

        return builder.build();
    }

    private IndexMetadata createIndexMetadata(String indexName, int numberOfShards, long version) {
        return IndexMetadata.builder(indexName)
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_INDEX_UUID, indexName)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .build()
            )
            .version(version)
            .build();
    }

    private void assertClusterStateEqual(ClusterState expected, ClusterState actual) {
        assertThat(actual.version(), equalTo(expected.version()));
        assertTrue(Metadata.isGlobalStateEquals(actual.metadata(), expected.metadata()));
        for (IndexMetadata indexMetadata : expected.metadata()) {
            assertThat(actual.metadata().index(indexMetadata.getIndex()), equalTo(indexMetadata));
        }
    }

    public void testSetLastAcceptedState() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();
            final long term = randomNonNegativeLong();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                final long version = randomNonNegativeLong();
                final String indexName = randomAlphaOfLength(10);
                final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                final Metadata metadata = Metadata.builder()
                    .persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build())
                    .coordinationMetadata(createCoordinationMetadata(term))
                    .put(indexMetadata, false)
                    .build();
                ClusterState state = createClusterState(version, metadata);

                gateway.setLastAcceptedState(state);
                gateway = maybeNew(gateway);

                ClusterState lastAcceptedState = gateway.getLastAcceptedState();
                assertClusterStateEqual(state, lastAcceptedState);
            }
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testSetLastAcceptedStateTermChanged() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            final String indexName = randomAlphaOfLength(10);
            final int numberOfShards = randomIntBetween(1, 5);
            final long version = randomNonNegativeLong();
            final long term = randomValueOtherThan(Long.MAX_VALUE, OpenSearchTestCase::randomNonNegativeLong);
            final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, version);
            final ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build()
            );
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            final long newTerm = randomLongBetween(term + 1, Long.MAX_VALUE);
            final int newNumberOfShards = randomValueOtherThan(numberOfShards, () -> randomIntBetween(1, 5));
            final IndexMetadata newIndexMetadata = createIndexMetadata(indexName, newNumberOfShards, version);
            final ClusterState newClusterState = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(createCoordinationMetadata(newTerm)).put(newIndexMetadata, false).build()
            );
            gateway.setLastAcceptedState(newClusterState);

            gateway = maybeNew(gateway);
            assertThat(gateway.getLastAcceptedState().metadata().index(indexName), equalTo(newIndexMetadata));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testCurrentTermAndTermAreDifferent() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            long currentTerm = randomNonNegativeLong();
            long term = randomValueOtherThan(currentTerm, OpenSearchTestCase::randomNonNegativeLong);

            gateway.setCurrentTerm(currentTerm);
            gateway.setLastAcceptedState(
                createClusterState(
                    randomNonNegativeLong(),
                    Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).build()
                )
            );

            gateway = maybeNew(gateway);
            assertThat(gateway.getCurrentTerm(), equalTo(currentTerm));
            assertThat(gateway.getLastAcceptedState().coordinationMetadata().term(), equalTo(term));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testMarkAcceptedConfigAsCommitted() throws IOException {
        CoordinationState.PersistedState gateway = null;
        try {
            gateway = newGatewayPersistedState();

            // generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
            CoordinationMetadata coordinationMetadata;
            do {
                coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
            } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

            ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(randomAlphaOfLength(10)).build()
            );
            gateway.setLastAcceptedState(state);

            gateway = maybeNew(gateway);
            assertThat(
                gateway.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(gateway.getLastAcceptedState().getLastCommittedConfiguration()))
            );
            gateway.markLastAcceptedStateAsCommitted();

            CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration())
                .build();
            ClusterState expectedClusterState = ClusterState.builder(state)
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(expectedCoordinationMetadata)
                        .clusterUUID(state.metadata().clusterUUID())
                        .clusterUUIDCommitted(true)
                        .build()
                )
                .build();

            gateway = maybeNew(gateway);
            assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
            gateway.markLastAcceptedStateAsCommitted();

            gateway = maybeNew(gateway);
            assertClusterStateEqual(expectedClusterState, gateway.getLastAcceptedState());
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testStatePersistedOnLoad() throws IOException {
        // open LucenePersistedState to make sure that cluster state is written out to each data path
        final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            getBigArrays(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
        final ClusterState state = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build()
        );
        try (
            GatewayMetaState.LucenePersistedState ignored = new GatewayMetaState.LucenePersistedState(
                persistedClusterStateService,
                42L,
                state
            )
        ) {

        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.getParent().getParent().toString())
                .build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService = new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    getBigArrays(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                );
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(42L));
                assertClusterStateEqual(
                    state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metadata(onDiskState.metadata)
                        .build()
                );
            }
        }
    }

    public void testDataOnlyNodePersistence() throws Exception {
        final List<Closeable> cleanup = new ArrayList<>(2);

        try {
            DiscoveryNode localNode = new DiscoveryNode(
                "node1",
                buildNewFakeTransportAddress(),
                Collections.emptyMap(),
                Sets.newHashSet(DiscoveryNodeRole.DATA_ROLE),
                Version.CURRENT
            );
            Settings settings = Settings.builder()
                .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), clusterName.value())
                .put(nonClusterManagerNode())
                .put(Node.NODE_NAME_SETTING.getKey(), "test")
                .build();
            final MockGatewayMetaState gateway = new MockGatewayMetaState(localNode, bigArrays);
            cleanup.add(gateway);
            final TransportService transportService = mock(TransportService.class);
            TestThreadPool threadPool = new TestThreadPool("testMarkAcceptedConfigAsCommittedOnDataOnlyNode");
            cleanup.add(() -> {
                ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
                threadPool.shutdown();
            });
            when(transportService.getThreadPool()).thenReturn(threadPool);
            ClusterService clusterService = mock(ClusterService.class);
            when(clusterService.getClusterSettings()).thenReturn(
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
            );
            final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
                nodeEnvironment,
                xContentRegistry(),
                getBigArrays(),
                new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                () -> 0L
            );
            Supplier<RemoteClusterStateService> remoteClusterStateServiceSupplier = () -> {
                if (isRemoteStoreClusterStateEnabled(settings)) {
                    Supplier<RepositoriesService> repositoriesServiceSupplier = () -> new RepositoriesService(
                        settings,
                        clusterService,
                        transportService,
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        transportService.getThreadPool()
                    );
                    ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                    return new RemoteClusterStateService(
                        nodeEnvironment.nodeId(),
                        repositoriesServiceSupplier,
                        settings,
                        clusterSettings,
                        () -> 0L,
                        threadPool,
                        List.of(new RemoteIndexPathUploader(threadPool, settings, repositoriesServiceSupplier, clusterSettings))
                    );
                } else {
                    return null;
                }
            };
            gateway.start(
                settings,
                transportService,
                clusterService,
                new MetaStateService(nodeEnvironment, xContentRegistry()),
                null,
                null,
                persistedClusterStateService,
                remoteClusterStateServiceSupplier.get(),
                new PersistedStateRegistry(),
                null
            );
            final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
            assertThat(persistedState, instanceOf(GatewayMetaState.AsyncLucenePersistedState.class));

            // generate random coordinationMetadata with different lastAcceptedConfiguration and lastCommittedConfiguration
            CoordinationMetadata coordinationMetadata;
            do {
                coordinationMetadata = createCoordinationMetadata(randomNonNegativeLong());
            } while (coordinationMetadata.getLastAcceptedConfiguration().equals(coordinationMetadata.getLastCommittedConfiguration()));

            ClusterState state = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder().coordinationMetadata(coordinationMetadata).clusterUUID(randomAlphaOfLength(10)).build()
            );
            persistedState.setCurrentTerm(state.term());
            persistedState.setLastAcceptedState(state);
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

            assertThat(
                persistedState.getLastAcceptedState().getLastAcceptedConfiguration(),
                not(equalTo(persistedState.getLastAcceptedState().getLastCommittedConfiguration()))
            );
            CoordinationMetadata persistedCoordinationMetadata = persistedClusterStateService.loadBestOnDiskState().metadata
                .coordinationMetadata();
            assertThat(
                persistedCoordinationMetadata.getLastAcceptedConfiguration(),
                equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration)
            );
            assertThat(
                persistedCoordinationMetadata.getLastCommittedConfiguration(),
                equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration)
            );

            persistedState.markLastAcceptedStateAsCommitted();
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

            CoordinationMetadata expectedCoordinationMetadata = CoordinationMetadata.builder(coordinationMetadata)
                .lastCommittedConfiguration(coordinationMetadata.getLastAcceptedConfiguration())
                .build();
            ClusterState expectedClusterState = ClusterState.builder(state)
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(expectedCoordinationMetadata)
                        .clusterUUID(state.metadata().clusterUUID())
                        .clusterUUIDCommitted(true)
                        .build()
                )
                .build();

            assertClusterStateEqual(expectedClusterState, persistedState.getLastAcceptedState());
            persistedCoordinationMetadata = persistedClusterStateService.loadBestOnDiskState().metadata.coordinationMetadata();
            assertThat(
                persistedCoordinationMetadata.getLastAcceptedConfiguration(),
                equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration)
            );
            assertThat(
                persistedCoordinationMetadata.getLastCommittedConfiguration(),
                equalTo(GatewayMetaState.AsyncLucenePersistedState.staleStateConfiguration)
            );
            assertTrue(persistedClusterStateService.loadBestOnDiskState().metadata.clusterUUIDCommitted());

            // generate a series of updates and check if batching works
            final String indexName = randomAlphaOfLength(10);
            long currentTerm = state.term();
            final int iterations = randomIntBetween(1, 1000);
            for (int i = 0; i < iterations; i++) {
                if (rarely()) {
                    // bump term
                    currentTerm = currentTerm + (rarely() ? randomIntBetween(1, 5) : 0L);
                    persistedState.setCurrentTerm(currentTerm);
                } else {
                    // update cluster state
                    final int numberOfShards = randomIntBetween(1, 5);
                    final long term = Math.min(state.term() + (rarely() ? randomIntBetween(1, 5) : 0L), currentTerm);
                    final IndexMetadata indexMetadata = createIndexMetadata(indexName, numberOfShards, i);
                    state = createClusterState(
                        state.version() + 1,
                        Metadata.builder().coordinationMetadata(createCoordinationMetadata(term)).put(indexMetadata, false).build()
                    );
                    persistedState.setLastAcceptedState(state);
                }
            }
            assertEquals(currentTerm, persistedState.getCurrentTerm());
            assertClusterStateEqual(state, persistedState.getLastAcceptedState());
            assertBusy(() -> assertTrue(gateway.allPendingAsyncStatesWritten()));

            gateway.close();
            assertTrue(cleanup.remove(gateway));

            try (CoordinationState.PersistedState reloadedPersistedState = newGatewayPersistedState()) {
                assertEquals(currentTerm, reloadedPersistedState.getCurrentTerm());
                assertClusterStateEqual(
                    GatewayMetaState.AsyncLucenePersistedState.resetVotingConfiguration(state),
                    reloadedPersistedState.getLastAcceptedState()
                );
                assertNotNull(reloadedPersistedState.getLastAcceptedState().metadata().index(indexName));
            }
        } finally {
            IOUtils.close(cleanup);
        }
    }

    public void testStatePersistenceWithIOIssues() throws IOException {
        final AtomicReference<Double> ioExceptionRate = new AtomicReference<>(0.01d);
        final List<MockDirectoryWrapper> list = new ArrayList<>();
        final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            getBigArrays(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        ) {
            @Override
            Directory createDirectory(Path path) {
                final MockDirectoryWrapper wrapper = newMockFSDirectory(path);
                wrapper.setAllowRandomFileNotFoundException(randomBoolean());
                wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
                list.add(wrapper);
                return wrapper;
            }
        };
        ClusterState state = createClusterState(randomNonNegativeLong(), Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build());
        long currentTerm = 42L;
        try (
            GatewayMetaState.LucenePersistedState persistedState = new GatewayMetaState.LucenePersistedState(
                persistedClusterStateService,
                currentTerm,
                state
            )
        ) {

            try {
                if (randomBoolean()) {
                    final ClusterState newState = createClusterState(
                        randomNonNegativeLong(),
                        Metadata.builder().clusterUUID(randomAlphaOfLength(10)).build()
                    );
                    persistedState.setLastAcceptedState(newState);
                    state = newState;
                } else {
                    final long newTerm = currentTerm + 1;
                    persistedState.setCurrentTerm(newTerm);
                    currentTerm = newTerm;
                }
            } catch (IOError | Exception e) {
                assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
            }

            ioExceptionRate.set(0.0d);
            for (MockDirectoryWrapper wrapper : list) {
                wrapper.setRandomIOExceptionRate(ioExceptionRate.get());
                wrapper.setRandomIOExceptionRateOnOpen(ioExceptionRate.get());
            }

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                if (randomBoolean()) {
                    final long version = randomNonNegativeLong();
                    final String indexName = randomAlphaOfLength(10);
                    final IndexMetadata indexMetadata = createIndexMetadata(indexName, randomIntBetween(1, 5), randomNonNegativeLong());
                    final Metadata metadata = Metadata.builder()
                        .persistentSettings(Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build())
                        .coordinationMetadata(createCoordinationMetadata(1L))
                        .put(indexMetadata, false)
                        .build();
                    state = createClusterState(version, metadata);
                    persistedState.setLastAcceptedState(state);
                } else {
                    currentTerm += 1;
                    persistedState.setCurrentTerm(currentTerm);
                }
            }

            assertEquals(state, persistedState.getLastAcceptedState());
            assertEquals(currentTerm, persistedState.getCurrentTerm());

        } catch (IOError | Exception e) {
            if (ioExceptionRate.get() == 0.0d) {
                throw e;
            }
            assertNotNull(ExceptionsHelper.unwrap(e, IOException.class));
            return;
        }

        nodeEnvironment.close();

        // verify that the freshest state was rewritten to each data path
        for (Path path : nodeEnvironment.nodeDataPaths()) {
            Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toAbsolutePath())
                .put(Environment.PATH_DATA_SETTING.getKey(), path.getParent().getParent().toString())
                .build();
            try (NodeEnvironment nodeEnvironment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings))) {
                final PersistedClusterStateService newPersistedClusterStateService = new PersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    getBigArrays(),
                    new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    () -> 0L
                );
                final PersistedClusterStateService.OnDiskState onDiskState = newPersistedClusterStateService.loadBestOnDiskState();
                assertFalse(onDiskState.empty());
                assertThat(onDiskState.currentTerm, equalTo(currentTerm));
                assertClusterStateEqual(
                    state,
                    ClusterState.builder(ClusterName.DEFAULT)
                        .version(onDiskState.lastAcceptedVersion)
                        .metadata(onDiskState.metadata)
                        .build()
                );
            }
        }
    }

    public void testRemotePersistedState() throws IOException {
        final RemoteClusterStateService remoteClusterStateService = Mockito.mock(RemoteClusterStateService.class);
        final ClusterMetadataManifest manifest = ClusterMetadataManifest.builder().clusterTerm(1L).stateVersion(5L).build();
        final String previousClusterUUID = "prev-cluster-uuid";
        Mockito.when(remoteClusterStateService.writeFullMetadata(Mockito.any(), Mockito.any())).thenReturn(manifest);

        Mockito.when(remoteClusterStateService.writeIncrementalMetadata(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(manifest);
        CoordinationState.PersistedState remotePersistedState = new RemotePersistedState(remoteClusterStateService, previousClusterUUID);

        assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
        assertThat(remotePersistedState.getCurrentTerm(), equalTo(0L));

        final long clusterTerm = randomNonNegativeLong();
        final ClusterState clusterState = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(clusterTerm).build()).build()
        );

        remotePersistedState.setLastAcceptedState(clusterState);
        Mockito.verify(remoteClusterStateService).writeFullMetadata(clusterState, previousClusterUUID);

        assertThat(remotePersistedState.getLastAcceptedState(), equalTo(clusterState));
        assertThat(remotePersistedState.getCurrentTerm(), equalTo(clusterTerm));

        final ClusterState secondClusterState = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(clusterTerm).build()).build()
        );

        remotePersistedState.setLastAcceptedState(secondClusterState);
        Mockito.verify(remoteClusterStateService, times(1)).writeFullMetadata(secondClusterState, previousClusterUUID);

        assertThat(remotePersistedState.getLastAcceptedState(), equalTo(secondClusterState));
        assertThat(remotePersistedState.getCurrentTerm(), equalTo(clusterTerm));

        remotePersistedState.markLastAcceptedStateAsCommitted();
        Mockito.verify(remoteClusterStateService, times(1)).markLastStateAsCommitted(Mockito.any(), Mockito.any());

        assertThat(remotePersistedState.getLastAcceptedState(), equalTo(secondClusterState));
        assertThat(remotePersistedState.getCurrentTerm(), equalTo(clusterTerm));
        assertThat(remotePersistedState.getLastAcceptedState().metadata().clusterUUIDCommitted(), equalTo(false));

        final ClusterState thirdClusterState = ClusterState.builder(secondClusterState)
            .metadata(Metadata.builder(secondClusterState.getMetadata()).clusterUUID(randomAlphaOfLength(10)).build())
            .build();
        remotePersistedState.setLastAcceptedState(thirdClusterState);
        remotePersistedState.markLastAcceptedStateAsCommitted();
        assertThat(remotePersistedState.getLastAcceptedState().metadata().clusterUUIDCommitted(), equalTo(true));
    }

    public void testRemotePersistedStateNotCommitted() throws IOException {
        final RemoteClusterStateService remoteClusterStateService = Mockito.mock(RemoteClusterStateService.class);
        final String previousClusterUUID = "prev-cluster-uuid";
        final ClusterMetadataManifest manifest = ClusterMetadataManifest.builder()
            .previousClusterUUID(previousClusterUUID)
            .clusterTerm(1L)
            .stateVersion(5L)
            .build();
        Mockito.when(remoteClusterStateService.getLatestClusterMetadataManifest(Mockito.any(), Mockito.any()))
            .thenReturn(Optional.of(manifest));
        Mockito.when(remoteClusterStateService.writeFullMetadata(Mockito.any(), Mockito.any())).thenReturn(manifest);

        Mockito.when(remoteClusterStateService.writeIncrementalMetadata(Mockito.any(), Mockito.any(), Mockito.any())).thenReturn(manifest);
        CoordinationState.PersistedState remotePersistedState = new RemotePersistedState(
            remoteClusterStateService,
            ClusterState.UNKNOWN_UUID
        );

        assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
        assertThat(remotePersistedState.getCurrentTerm(), equalTo(0L));

        final long clusterTerm = randomNonNegativeLong();
        ClusterState clusterState = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(clusterTerm).build()).build()
        );
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.getMetadata()).clusterUUID(randomAlphaOfLength(10)).clusterUUIDCommitted(false).build())
            .build();

        remotePersistedState.setLastAcceptedState(clusterState);
        ArgumentCaptor<String> previousClusterUUIDCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ClusterState> clusterStateCaptor = ArgumentCaptor.forClass(ClusterState.class);
        Mockito.verify(remoteClusterStateService).writeFullMetadata(clusterStateCaptor.capture(), previousClusterUUIDCaptor.capture());
        assertEquals(previousClusterUUID, previousClusterUUIDCaptor.getValue());
    }

    public void testRemotePersistedStateExceptionOnFullStateUpload() throws IOException {
        final RemoteClusterStateService remoteClusterStateService = Mockito.mock(RemoteClusterStateService.class);
        final String previousClusterUUID = "prev-cluster-uuid";
        Mockito.doThrow(IOException.class).when(remoteClusterStateService).writeFullMetadata(Mockito.any(), Mockito.any());

        CoordinationState.PersistedState remotePersistedState = new RemotePersistedState(remoteClusterStateService, previousClusterUUID);

        final long clusterTerm = randomNonNegativeLong();
        final ClusterState clusterState = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(clusterTerm).build()).build()
        );

        assertThrows(OpenSearchException.class, () -> remotePersistedState.setLastAcceptedState(clusterState));
    }

    public void testRemotePersistedStateFailureStats() throws IOException {
        RemotePersistenceStats remoteStateStats = new RemotePersistenceStats();
        final RemoteClusterStateService remoteClusterStateService = Mockito.mock(RemoteClusterStateService.class);
        final String previousClusterUUID = "prev-cluster-uuid";
        Mockito.doThrow(IOException.class).when(remoteClusterStateService).writeFullMetadata(Mockito.any(), Mockito.any());
        when(remoteClusterStateService.getStats()).thenReturn(remoteStateStats);
        doCallRealMethod().when(remoteClusterStateService).writeMetadataFailed();
        CoordinationState.PersistedState remotePersistedState = new RemotePersistedState(remoteClusterStateService, previousClusterUUID);

        final long clusterTerm = randomNonNegativeLong();
        final ClusterState clusterState = createClusterState(
            randomNonNegativeLong(),
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(clusterTerm).build()).build()
        );

        assertThrows(OpenSearchException.class, () -> remotePersistedState.setLastAcceptedState(clusterState));
        assertEquals(1, remoteClusterStateService.getStats().getFailedCount());
        assertEquals(0, remoteClusterStateService.getStats().getSuccessCount());
    }

    public void testGatewayForRemoteState() throws IOException {
        MockGatewayMetaState gateway = null;
        try {
            RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);
            when(remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster")).thenReturn("test-cluster-uuid");
            RemoteStoreRestoreService remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
            when(remoteStoreRestoreService.restore(any(), any(), anyBoolean(), any())).thenReturn(
                RemoteRestoreResult.build("test-cluster-uuid", null, ClusterState.EMPTY_STATE)
            );
            gateway = new MockGatewayMetaState(localNode, bigArrays, remoteClusterStateService, remoteStoreRestoreService);
            final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();

            String stateRepoTypeAttributeKey = String.format(
                Locale.getDefault(),
                "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
                "randomRepoName"
            );
            String stateRepoSettingsAttributeKeyPrefix = String.format(
                Locale.getDefault(),
                "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
                "randomRepoName"
            );

            Settings settings = Settings.builder()
                .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, "randomRepoName")
                .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
                .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
                .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
                .build();
            gateway.start(settings, nodeEnvironment, xContentRegistry(), persistedStateRegistry);

            final CoordinationState.PersistedState persistedState = gateway.getPersistedState();
            assertThat(persistedState, instanceOf(GatewayMetaState.LucenePersistedState.class));
            assertThat(
                persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL),
                instanceOf(GatewayMetaState.LucenePersistedState.class)
            );
            assertThat(
                persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE),
                instanceOf(GatewayMetaState.RemotePersistedState.class)
            );
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testGatewayForRemoteStateForInitialBootstrap() throws IOException {
        MockGatewayMetaState gateway = null;
        try {
            final RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);
            when(remoteClusterStateService.getLastKnownUUIDFromRemote(clusterName.value())).thenReturn(ClusterState.UNKNOWN_UUID);

            final RemoteStoreRestoreService remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
            when(remoteStoreRestoreService.restore(any(), any(), anyBoolean(), any())).thenReturn(
                RemoteRestoreResult.build("test-cluster-uuid", null, ClusterState.EMPTY_STATE)
            );
            final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
            gateway = newGatewayForRemoteState(
                remoteClusterStateService,
                remoteStoreRestoreService,
                persistedStateRegistry,
                ClusterState.EMPTY_STATE,
                false
            );
            final CoordinationState.PersistedState lucenePersistedState = gateway.getPersistedState();
            PersistedState remotePersistedState = persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE);
            verify(remoteClusterStateService).getLastKnownUUIDFromRemote(Mockito.any()); // change this
            verifyNoInteractions(remoteStoreRestoreService);
            assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
            assertThat(lucenePersistedState.getLastAcceptedState().metadata(), equalTo(ClusterState.EMPTY_STATE.metadata()));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testGatewayForRemoteStateForNodeReplacement() throws IOException {
        MockGatewayMetaState gateway = null;
        try {
            final RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);
            when(remoteClusterStateService.getLastKnownUUIDFromRemote("test-cluster")).thenReturn("test-cluster-uuid");
            final ClusterState previousState = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder().term(randomLong()).build())
                    .put(
                        IndexMetadata.builder("test-index1")
                            .settings(settings(Version.CURRENT).put(SETTING_INDEX_UUID, randomAlphaOfLength(10)))
                            .numberOfShards(5)
                            .numberOfReplicas(1)
                            .build(),
                        false
                    )
                    .clusterUUID(randomAlphaOfLength(10))
                    .build()
            );
            when(remoteClusterStateService.getLastKnownUUIDFromRemote(clusterName.value())).thenReturn(
                previousState.metadata().clusterUUID()
            );

            final RemoteStoreRestoreService remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
            when(remoteStoreRestoreService.restore(any(), any(), anyBoolean(), any())).thenReturn(
                RemoteRestoreResult.build("test-cluster-uuid", null, previousState)
            );
            final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
            gateway = newGatewayForRemoteState(
                remoteClusterStateService,
                remoteStoreRestoreService,
                persistedStateRegistry,
                ClusterState.EMPTY_STATE,
                false
            );
            final CoordinationState.PersistedState lucenePersistedState = gateway.getPersistedState();
            PersistedState remotePersistedState = persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE);
            verify(remoteClusterStateService).getLastKnownUUIDFromRemote(Mockito.any());
            verify(remoteStoreRestoreService).restore(any(), any(), anyBoolean(), any());
            assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
            assertThat(lucenePersistedState.getLastAcceptedState().metadata(), equalTo(previousState.metadata()));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testGatewayForRemoteStateForNodeReboot() throws IOException {
        MockGatewayMetaState gateway = null;
        try {
            final RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);
            final RemoteStoreRestoreService remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
            final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
            final IndexMetadata indexMetadata = IndexMetadata.builder("test-index1")
                .settings(settings(Version.CURRENT).put(SETTING_INDEX_UUID, randomAlphaOfLength(10)))
                .numberOfShards(5)
                .numberOfReplicas(1)
                .build();
            final ClusterState clusterState = createClusterState(
                randomNonNegativeLong(),
                Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder().term(randomLong()).build())
                    .put(indexMetadata, false)
                    .clusterUUID(randomAlphaOfLength(10))
                    .build()
            );
            gateway = newGatewayForRemoteState(
                remoteClusterStateService,
                remoteStoreRestoreService,
                persistedStateRegistry,
                clusterState,
                false
            );
            final CoordinationState.PersistedState lucenePersistedState = gateway.getPersistedState();
            PersistedState remotePersistedState = persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE);
            verifyNoInteractions(remoteClusterStateService);
            verifyNoInteractions(remoteStoreRestoreService);
            assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
            logger.info("lucene state metadata: {}", lucenePersistedState.getLastAcceptedState().toString());
            logger.info("initial metadata: {}", clusterState.toString());
            assertThat(lucenePersistedState.getLastAcceptedState().metadata().indices().size(), equalTo(1));
            assertThat(lucenePersistedState.getLastAcceptedState().metadata().indices().get("test-index1"), equalTo(indexMetadata));
        } finally {
            IOUtils.close(gateway);
        }
    }

    public void testGatewayForRemoteStateForInitialBootstrapBlocksApplied() throws IOException {
        MockGatewayMetaState gateway = null;
        try {
            final RemoteClusterStateService remoteClusterStateService = mock(RemoteClusterStateService.class);
            when(remoteClusterStateService.getLastKnownUUIDFromRemote(clusterName.value())).thenReturn("test-cluster-uuid");

            final IndexMetadata indexMetadata = IndexMetadata.builder("test-index1")
                .settings(
                    settings(Version.CURRENT).put(SETTING_INDEX_UUID, randomAlphaOfLength(10))
                        .put(IndexMetadata.INDEX_READ_ONLY_SETTING.getKey(), true)
                )
                .numberOfShards(5)
                .numberOfReplicas(1)
                .build();

            final ClusterState clusterState = ClusterState.builder(
                createClusterState(
                    randomNonNegativeLong(),
                    Metadata.builder()
                        .coordinationMetadata(CoordinationMetadata.builder().term(randomLong()).build())
                        .put(indexMetadata, false)
                        .clusterUUID(ClusterState.UNKNOWN_UUID)
                        .persistentSettings(Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), true).build())
                        .build()
                )
            ).nodes(DiscoveryNodes.EMPTY_NODES).build();

            final RemoteStoreRestoreService remoteStoreRestoreService = mock(RemoteStoreRestoreService.class);
            when(remoteStoreRestoreService.restore(any(), any(), anyBoolean(), any())).thenReturn(
                RemoteRestoreResult.build("test-cluster-uuid", null, clusterState)
            );
            final PersistedStateRegistry persistedStateRegistry = persistedStateRegistry();
            gateway = newGatewayForRemoteState(
                remoteClusterStateService,
                remoteStoreRestoreService,
                persistedStateRegistry,
                ClusterState.EMPTY_STATE,
                true
            );
            PersistedState remotePersistedState = persistedStateRegistry.getPersistedState(PersistedStateType.REMOTE);
            PersistedState lucenePersistedState = persistedStateRegistry.getPersistedState(PersistedStateType.LOCAL);
            verify(remoteClusterStateService).getLastKnownUUIDFromRemote(clusterName.value()); // change this
            verify(remoteStoreRestoreService).restore(any(ClusterState.class), any(String.class), anyBoolean(), any(String[].class));
            assertThat(remotePersistedState.getLastAcceptedState(), nullValue());
            assertThat(
                Metadata.isGlobalStateEquals(lucenePersistedState.getLastAcceptedState().metadata(), clusterState.metadata()),
                equalTo(true)
            );
            assertThat(
                lucenePersistedState.getLastAcceptedState().blocks().hasGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK),
                equalTo(true)
            );
            assertThat(
                IndexMetadata.INDEX_READ_ONLY_SETTING.get(
                    lucenePersistedState.getLastAcceptedState().metadata().index("test-index1").getSettings()
                ),
                equalTo(true)
            );
        } finally {
            IOUtils.close(gateway);
        }
    }

    private MockGatewayMetaState newGatewayForRemoteState(
        RemoteClusterStateService remoteClusterStateService,
        RemoteStoreRestoreService remoteStoreRestoreService,
        PersistedStateRegistry persistedStateRegistry,
        ClusterState currentState,
        boolean prepareFullState
    ) throws IOException {
        MockGatewayMetaState gateway = new MockGatewayMetaState(localNode, bigArrays, prepareFullState);
        String randomRepoName = "randomRepoName";
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            randomRepoName
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            randomRepoName
        );
        Settings settingWithRemoteStateEnabled = Settings.builder()
            .put(settings)
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, randomRepoName)
            .put(stateRepoTypeAttributeKey, FsRepository.TYPE)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", "randomRepoPath")
            .put(RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .build();
        final TransportService transportService = mock(TransportService.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(transportService.getLocalNode()).thenReturn(mock(DiscoveryNode.class));
        final PersistedClusterStateService persistedClusterStateService = new PersistedClusterStateService(
            nodeEnvironment,
            xContentRegistry(),
            getBigArrays(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            () -> 0L
        );
        if (!ClusterState.EMPTY_STATE.equals(currentState)) {
            Writer writer = persistedClusterStateService.createWriter();
            writer.writeFullStateAndCommit(currentState.term(), currentState);
            writer.close();
        }
        final MetaStateService metaStateService = mock(MetaStateService.class);
        when(metaStateService.loadFullState()).thenReturn(new Tuple<>(Manifest.empty(), ClusterState.EMPTY_STATE.metadata()));
        gateway.start(
            settingWithRemoteStateEnabled,
            transportService,
            clusterService,
            metaStateService,
            null,
            null,
            persistedClusterStateService,
            remoteClusterStateService,
            persistedStateRegistry,
            remoteStoreRestoreService
        );
        return gateway;
    }

    private static BigArrays getBigArrays() {
        return usually()
            ? BigArrays.NON_RECYCLING_INSTANCE
            : new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
    }

}
