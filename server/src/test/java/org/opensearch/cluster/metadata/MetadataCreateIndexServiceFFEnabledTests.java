/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexClusterStateUpdateRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.UUIDs;
import org.opensearch.common.ValidationException;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.DefaultRemoteStoreSettings;
import org.opensearch.indices.IndexCreationException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.InvalidIndexContextException;
import org.opensearch.indices.SystemIndices;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.opensearch.cluster.metadata.MetadataCreateIndexService.aggregateIndexSettings;
import static org.opensearch.cluster.metadata.MetadataCreateIndexServiceTests.getRemoteNode;
import static org.opensearch.cluster.metadata.MetadataCreateIndexServiceTests.randomShardLimitService;
import static org.opensearch.cluster.metadata.MetadataCreateIndexServiceTests.verifyRemoteStoreIndexSettings;
import static org.opensearch.index.IndexSettings.INDEX_MERGE_POLICY;
import static org.opensearch.index.IndexSettings.INDEX_REFRESH_INTERVAL_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.opensearch.indices.ShardLimitValidatorTests.createTestShardLimitService;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MetadataCreateIndexServiceFFEnabledTests extends OpenSearchTestCase {

    private CreateIndexClusterStateUpdateRequest request;
    private IndicesService indicesServices;
    private Supplier<RepositoriesService> repositoriesServiceSupplier;

    @BeforeClass
    public static void beforeClass() {
        FeatureFlags.initializeFeatureFlags(
            Settings.builder()
                .put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES, true)
                .put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES, true)
                .build()
        );
    }

    @AfterClass
    public static void afterClass() {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    @Before
    public void setup() throws Exception {
        super.setUp();
        indicesServices = mock(IndicesService.class);
        repositoriesServiceSupplier = mock(Supplier.class);
    }

    @Before
    public void setupCreateIndexRequestAndAliasValidator() {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test");
        Settings indexSettings = Settings.builder()
            .put(SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
    }

    public void testNewIndexIsRemoteStoreBackedForRemoteStoreDirectionAndMixedMode() {
        // non-remote cluster manager node
        DiscoveryNode nonRemoteClusterManagerNode = new DiscoveryNode(UUIDs.base64UUID(), buildNewFakeTransportAddress(), Version.CURRENT);

        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteClusterManagerNode)
            .localNodeId(nonRemoteClusterManagerNode.getId())
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        request = new CreateIndexClusterStateUpdateRequest("create index", "test-index", "test-index");

        Settings indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );
        verifyRemoteStoreIndexSettings(indexSettings, null, null, null, ReplicationType.DOCUMENT.toString(), null);

        // remote data node
        DiscoveryNode remoteDataNode = getRemoteNode();

        discoveryNodes = DiscoveryNodes.builder(discoveryNodes).add(remoteDataNode).localNodeId(remoteDataNode.getId()).build();

        clusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(discoveryNodes).build();

        Settings remoteStoreMigrationSettings = Settings.builder()
            .put(REMOTE_STORE_COMPATIBILITY_MODE_SETTING.getKey(), RemoteStoreNodeService.CompatibilityMode.MIXED)
            .put(MIGRATION_DIRECTION_SETTING.getKey(), RemoteStoreNodeService.Direction.REMOTE_STORE)
            .build();

        clusterSettings = new ClusterSettings(remoteStoreMigrationSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        indexSettings = aggregateIndexSettings(
            clusterState,
            request,
            Settings.EMPTY,
            null,
            Settings.EMPTY,
            IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
            randomShardLimitService(),
            Collections.emptySet(),
            clusterSettings
        );

        verifyRemoteStoreIndexSettings(
            indexSettings,
            "true",
            "my-segment-repo-1",
            "my-translog-repo-1",
            ReplicationType.SEGMENT.toString(),
            null
        );

        Map<String, String> missingTranslogAttribute = Map.of(
            REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "cluster-state-repo-1",
            REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY,
            "my-segment-repo-1"
        );

        DiscoveryNodes finalDiscoveryNodes = DiscoveryNodes.builder()
            .add(nonRemoteClusterManagerNode)
            .add(
                new DiscoveryNode(
                    UUIDs.base64UUID(),
                    buildNewFakeTransportAddress(),
                    missingTranslogAttribute,
                    Set.of(DiscoveryNodeRole.INGEST_ROLE, DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE, DiscoveryNodeRole.DATA_ROLE),
                    Version.CURRENT
                )
            )
            .build();

        ClusterState finalClusterState = ClusterState.builder(ClusterName.DEFAULT).nodes(finalDiscoveryNodes).build();
        ClusterSettings finalClusterSettings = clusterSettings;

        final IndexCreationException error = expectThrows(IndexCreationException.class, () -> {
            aggregateIndexSettings(
                finalClusterState,
                request,
                Settings.EMPTY,
                null,
                Settings.EMPTY,
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                randomShardLimitService(),
                Collections.emptySet(),
                finalClusterSettings
            );
        });
        assertEquals(error.getMessage(), "failed to create index [test-index]");
        assertThat(
            error.getCause().getMessage(),
            containsString("Cluster is migrating to remote store but no remote node found, failing index creation")
        );
    }

    public void testCreateIndexWithContextAbsent() throws Exception {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );
            CountDownLatch counter = new CountDownLatch(1);
            InvalidIndexContextException exception = expectThrows(
                InvalidIndexContextException.class,
                () -> checkerService.validateContext(request)
            );
            assertTrue(
                "Invalid exception message." + exception.getMessage(),
                exception.getMessage().contains("index specifies a context which is not loaded on the cluster.")
            );
        });
    }

    public void testApplyContext() throws IOException {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));

        final Map<String, Object> mappings = new HashMap<>();
        mappings.put("_doc", "\"properties\": { \"field1\": {\"type\": \"text\"}}");
        List<Map<String, Object>> allMappings = new ArrayList<>();
        allMappings.add(mappings);

        Settings.Builder settingsBuilder = Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), "false");

        String templateContent = "{\n"
            + "  \"template\": {\n"
            + "    \"settings\": {\n"
            + "      \"index.codec\": \"best_compression\",\n"
            + "      \"index.merge.policy\": \"log_byte_size\",\n"
            + "      \"index.refresh_interval\": \"60s\"\n"
            + "    },\n"
            + "    \"mappings\": {\n"
            + "      \"properties\": {\n"
            + "        \"field1\": {\n"
            + "          \"type\": \"integer\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"_meta\": {\n"
            + "    \"_type\": \"@abc_template\",\n"
            + "    \"_version\": 1\n"
            + "  },\n"
            + "  \"version\": 1\n"
            + "}\n";

        AtomicReference<ComponentTemplate> componentTemplate = new AtomicReference<>();
        try (
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                templateContent
            )
        ) {
            componentTemplate.set(ComponentTemplate.parse(contentParser));
        }

        String contextName = randomAlphaOfLength(5);
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(contextName));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            ClusterState mockState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);

            when(mockState.metadata()).thenReturn(metadata);
            when(metadata.systemTemplatesLookup()).thenReturn(Map.of(contextName, new TreeMap<>() {
                {
                    put(1L, contextName);
                }
            }));
            when(metadata.componentTemplates()).thenReturn(Map.of(contextName, componentTemplate.get()));

            try {
                Template template = checkerService.applyContext(request, mockState, allMappings, settingsBuilder);
                assertEquals(componentTemplate.get().template(), template);

                assertEquals(2, allMappings.size());
                assertEquals(mappings, allMappings.get(0));
                assertEquals(
                    MapperService.parseMapping(NamedXContentRegistry.EMPTY, componentTemplate.get().template().mappings().toString()),
                    allMappings.get(1)
                );

                assertEquals("60s", settingsBuilder.get(INDEX_REFRESH_INTERVAL_SETTING.getKey()));
                assertEquals("log_byte_size", settingsBuilder.get(INDEX_MERGE_POLICY.getKey()));
                assertEquals("best_compression", settingsBuilder.get(EngineConfig.INDEX_CODEC_SETTING.getKey()));
                assertEquals("false", settingsBuilder.get(INDEX_SOFT_DELETES_SETTING.getKey()));
            } catch (IOException ex) {
                throw new AssertionError(ex);
            }
        });
    }

    public void testApplyContextWithSettingsOverlap() throws IOException {
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(randomAlphaOfLength(5)));
        Settings.Builder settingsBuilder = Settings.builder().put(INDEX_REFRESH_INTERVAL_SETTING.getKey(), "30s");
        String templateContent = "{\n"
            + "  \"template\": {\n"
            + "    \"settings\": {\n"
            + "      \"index.refresh_interval\": \"60s\"\n"
            + "    }\n"
            + "   },\n"
            + "  \"_meta\": {\n"
            + "    \"_type\": \"@abc_template\",\n"
            + "    \"_version\": 1\n"
            + "  },\n"
            + "  \"version\": 1\n"
            + "}\n";

        AtomicReference<ComponentTemplate> componentTemplate = new AtomicReference<>();
        try (
            XContentParser contentParser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                templateContent
            )
        ) {
            componentTemplate.set(ComponentTemplate.parse(contentParser));
        }

        String contextName = randomAlphaOfLength(5);
        request = new CreateIndexClusterStateUpdateRequest("create index", "test", "test").context(new Context(contextName));
        withTemporaryClusterService((clusterService, threadPool) -> {
            MetadataCreateIndexService checkerService = new MetadataCreateIndexService(
                Settings.EMPTY,
                clusterService,
                indicesServices,
                null,
                null,
                createTestShardLimitService(randomIntBetween(1, 1000), false, clusterService),
                mock(Environment.class),
                IndexScopedSettings.DEFAULT_SCOPED_SETTINGS,
                threadPool,
                null,
                new SystemIndices(Collections.emptyMap()),
                false,
                new AwarenessReplicaBalance(Settings.EMPTY, clusterService.getClusterSettings()),
                DefaultRemoteStoreSettings.INSTANCE,
                repositoriesServiceSupplier
            );

            ClusterState mockState = mock(ClusterState.class);
            Metadata metadata = mock(Metadata.class);

            when(mockState.metadata()).thenReturn(metadata);
            when(metadata.systemTemplatesLookup()).thenReturn(Map.of(contextName, new TreeMap<>() {
                {
                    put(1L, contextName);
                }
            }));
            when(metadata.componentTemplates()).thenReturn(Map.of(contextName, componentTemplate.get()));

            ValidationException validationException = expectThrows(
                ValidationException.class,
                () -> checkerService.applyContext(request, mockState, List.of(), settingsBuilder)
            );
            assertEquals(1, validationException.validationErrors().size());
            assertTrue(
                "Invalid exception message: " + validationException.getMessage(),
                validationException.getMessage()
                    .contains("Cannot apply context template as user provide settings have overlap with the included context template")
            );
        });
    }

    private void withTemporaryClusterService(BiConsumer<ClusterService, ThreadPool> consumer) {
        ThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            final ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool);
            consumer.accept(clusterService, threadPool);
        } finally {
            threadPool.shutdown();
        }
    }

}
