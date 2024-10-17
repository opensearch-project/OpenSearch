/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.support.clustermanager.term.GetTermVersionAction;
import org.opensearch.action.support.clustermanager.term.GetTermVersionResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.coordination.PublicationTransportHandler;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.gateway.remote.model.RemoteRoutingTableBlobStore;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.plugins.Plugin;
import org.opensearch.remotestore.RemoteStoreBaseIntegTestCase;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;
import org.junit.Before;

import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_CLUSTER_STATE_ENABLED_SETTING;
import static org.opensearch.gateway.remote.RemoteClusterStateService.REMOTE_PUBLICATION_SETTING_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RemoteClusterStateTermVersionIT extends RemoteStoreBaseIntegTestCase {
    private static final String INDEX_NAME = "test-index";
    private static final String INDEX_NAME_1 = "test-index-1";
    List<BlobPath> indexRoutingPaths;
    AtomicInteger indexRoutingFiles = new AtomicInteger();
    private final RemoteStoreEnums.PathType pathType = RemoteStoreEnums.PathType.HASHED_PREFIX;

    @Before
    public void setup() {
        asyncUploadMockFsRepo = false;
    }

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(REMOTE_CLUSTER_STATE_ENABLED_SETTING.getKey(), true)
            .put(
                RemoteRoutingTableBlobStore.REMOTE_ROUTING_TABLE_PATH_TYPE_SETTING.getKey(),
                RemoteStoreEnums.PathType.HASHED_PREFIX.toString()
            )
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, REMOTE_ROUTING_TABLE_REPO)
            .put(REMOTE_PUBLICATION_SETTING_KEY, true)
            .build();
    }

    public void testRemoteClusterStateFallback() throws Exception {
        BlobStoreRepository repository = prepareClusterAndVerifyRepository();

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );

        String[] dataNodes = internalCluster().getDataNodeNames().toArray(String[]::new);
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodes[0]);

        String cm = internalCluster().getClusterManagerName();
        primaryService.addRequestHandlingBehavior(
            PublicationTransportHandler.COMMIT_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                // not committing the state
                logger.info("ignoring the commit from cluster-manager {}", request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );

        String index = "index_1";
        createIndex(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                .build()
        );
        logger.info("created index {}", index);
        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        addCallCountInterceptor(cm, callCounters);

        ClusterStateResponse stateResponseM = client(cm).admin().cluster().state(new ClusterStateRequest()).actionGet();

        ClusterStateResponse stateResponseD = client(dataNodes[0]).admin().cluster().state(new ClusterStateRequest()).actionGet();
        assertEquals(stateResponseM, stateResponseD);
        assertThat(callCounters.get(ClusterStateAction.NAME).get(), is(0));
        assertThat(callCounters.get(GetTermVersionAction.NAME).get(), is(1));

    }

    public void testNoRemoteClusterStateFound() throws Exception {
        BlobStoreRepository repository = prepareClusterAndVerifyRepository();

        RemoteClusterStateService remoteClusterStateService = internalCluster().getClusterManagerNodeInstance(
            RemoteClusterStateService.class
        );

        RemoteManifestManager remoteManifestManager = remoteClusterStateService.getRemoteManifestManager();
        Optional<ClusterMetadataManifest> latestManifest = remoteManifestManager.getLatestClusterMetadataManifest(
            getClusterState().getClusterName().value(),
            getClusterState().getMetadata().clusterUUID()
        );

        String[] dataNodes = internalCluster().getDataNodeNames().toArray(String[]::new);
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, dataNodes[0]);
        primaryService.addRequestHandlingBehavior(
            PublicationTransportHandler.COMMIT_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                // not committing the state
                logger.info("ignoring the commit from cluster-manager {}", request);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        );

        ClusterState state = internalCluster().clusterService().state();
        String cm = internalCluster().getClusterManagerName();
        MockTransportService cmservice = (MockTransportService) internalCluster().getInstance(TransportService.class, cm);
        cmservice.addRequestHandlingBehavior(GetTermVersionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(
                new GetTermVersionResponse(new ClusterStateTermVersion(state.getClusterName(), state.stateUUID(), -1, -1), true)
            );
        });

        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        addCallCountInterceptor(cm, callCounters);

        ClusterStateResponse stateResponseM = client(cm).admin().cluster().state(new ClusterStateRequest()).actionGet();
        ClusterStateResponse stateResponseD = client(dataNodes[0]).admin().cluster().state(new ClusterStateRequest()).actionGet();
        assertEquals(stateResponseM, stateResponseD);
        assertThat(callCounters.get(ClusterStateAction.NAME).get(), is(1));
        assertThat(callCounters.get(GetTermVersionAction.NAME).get(), is(1));

    }

    private void addCallCountInterceptor(String nodeName, Map<String, AtomicInteger> callCounters) {
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, nodeName);
        for (var ctrEnty : callCounters.entrySet()) {
            primaryService.addRequestHandlingBehavior(ctrEnty.getKey(), (handler, request, channel, task) -> {
                ctrEnty.getValue().incrementAndGet();
                logger.info("-->  {} response redirect", ctrEnty.getKey());
                handler.messageReceived(request, channel, task);
            });
        }
    }

    private BlobStoreRepository prepareClusterAndVerifyRepository() throws Exception {
        clusterSettingsSuppliedByTest = true;
        Path segmentRepoPath = randomRepoPath();
        Path translogRepoPath = randomRepoPath();
        Path remoteRoutingTableRepoPath = randomRepoPath();
        Settings settings = buildRemoteStoreNodeAttributes(
            REPOSITORY_NAME,
            segmentRepoPath,
            REPOSITORY_2_NAME,
            translogRepoPath,
            REMOTE_ROUTING_TABLE_REPO,
            remoteRoutingTableRepoPath,
            false
        );
        prepareCluster(1, 3, INDEX_NAME, 1, 5, settings);
        ensureGreen(INDEX_NAME);

        RepositoriesService repositoriesService = internalCluster().getClusterManagerNodeInstance(RepositoriesService.class);
        BlobStoreRepository repository = (BlobStoreRepository) repositoriesService.repository(REMOTE_ROUTING_TABLE_REPO);

        return repository;
    }

}
