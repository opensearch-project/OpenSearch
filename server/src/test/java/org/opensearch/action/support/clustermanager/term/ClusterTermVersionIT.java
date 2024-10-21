/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.action.admin.cluster.state.ClusterStateAction;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterStateApplier;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterTermVersionIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    public void testClusterStateResponseFromDataNode() throws Exception {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();

        ensureClusterSizeConsistency();
        ensureGreen();

        ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
        clusterStateRequest.waitForTimeout(TimeValue.timeValueHours(1));
        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(clusterStateRequest).get();
        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
        assertThat(stateResponse.isWaitForTimedOut(), is(false));

    }

    public void testClusterStateResponseFromClusterManagerNode() throws Exception {
        String master = internalCluster().startClusterManagerOnlyNode();
        String data = internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency();
        ensureGreen();
        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        addCallCountInterceptor(master, callCounters);

        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(new ClusterStateRequest()).get();

        AtomicInteger clusterStateCallsOnMaster = callCounters.get(ClusterStateAction.NAME);
        AtomicInteger termCallsOnMaster = callCounters.get(GetTermVersionAction.NAME);

        assertThat(clusterStateCallsOnMaster.get(), is(0));
        assertThat(termCallsOnMaster.get(), is(1));

        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));

    }

    public void testDatanodeOutOfSync() throws Exception {
        String master = internalCluster().startClusterManagerOnlyNode();
        String data = internalCluster().startDataOnlyNode();
        ensureClusterSizeConsistency();
        ensureGreen();
        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );

        stubClusterTermResponse(master);
        addCallCountInterceptor(master, callCounters);

        ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(new ClusterStateRequest()).get();

        AtomicInteger clusterStateCallsOnMaster = callCounters.get(ClusterStateAction.NAME);
        AtomicInteger termCallsOnMaster = callCounters.get(GetTermVersionAction.NAME);

        assertThat(clusterStateCallsOnMaster.get(), is(1));
        assertThat(termCallsOnMaster.get(), is(1));

        assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
        assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
    }

    public void testDatanodeWithSlowClusterApplierFallbackToPublish() throws Exception {
        List<String> masters = internalCluster().startClusterManagerOnlyNodes(
            3,
            Settings.builder().put(FeatureFlags.TERM_VERSION_PRECOMMIT_ENABLE, "true").build()
        );
        List<String> datas = internalCluster().startDataOnlyNodes(3);

        Map<String, AtomicInteger> callCounters = Map.ofEntries(
            Map.entry(ClusterStateAction.NAME, new AtomicInteger()),
            Map.entry(GetTermVersionAction.NAME, new AtomicInteger())
        );
        ensureGreen();

        String master = internalCluster().getClusterManagerName();

        AtomicBoolean processState = new AtomicBoolean();
        ClusterService cmClsService = internalCluster().getInstance(ClusterService.class, datas.get(0));
        cmClsService.addStateApplier(new ClusterStateApplier() {
            @Override
            public void applyClusterState(ClusterChangedEvent event) {
                logger.info("Slow applier started");
                while (processState.get()) {
                    try {
                        logger.info("Sleeping for 1s");
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
                logger.info("Slow applier ended");
            }
        });

        ensureGreen();

        GetTermVersionResponse respBeforeUpdate = internalCluster().getInstance(Client.class, master)
            .execute(GetTermVersionAction.INSTANCE, new GetTermVersionRequest())
            .get();

        processState.set(true);
        String index = "index_1";
        ActionFuture<CreateIndexResponse> startCreateIndex1 = prepareCreate(index).setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                .build()
        ).execute();

        ActionFuture<CreateIndexResponse> startCreateIndex2 = prepareCreate("index_2").setSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                .build()
        ).execute();

        // wait for cluster-manager to publish new state
        waitUntil(() -> {
            try {
                // node is yet to ack commit to cluster-manager , only the state-update corresponding to index_1 should have been published
                GetTermVersionResponse respAfterUpdate = internalCluster().getInstance(Client.class, master)
                    .execute(GetTermVersionAction.INSTANCE, new GetTermVersionRequest())
                    .get();
                logger.info(
                    "data has latest , {} , {}",
                    respAfterUpdate.getClusterStateTermVersion().getTerm(),
                    respAfterUpdate.getClusterStateTermVersion().getVersion()
                );
                return respBeforeUpdate.getClusterStateTermVersion().getVersion() + 1 == respAfterUpdate.getClusterStateTermVersion()
                    .getVersion();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        addCallCountInterceptor(master, callCounters);
        ClusterStateResponse stateResponseD = internalCluster().getInstance(Client.class, datas.get(0))
            .admin()
            .cluster()
            .state(new ClusterStateRequest())
            .actionGet();
        logger.info("data has the version , {} , {}", stateResponseD.getState().term(), stateResponseD.getState().version());
        assertTrue(respBeforeUpdate.getClusterStateTermVersion().getVersion() + 1 == stateResponseD.getState().version());

        processState.set(false);

        AtomicInteger clusterStateCallsOnMaster = callCounters.get(ClusterStateAction.NAME);
        AtomicInteger termCallsOnMaster = callCounters.get(GetTermVersionAction.NAME);
        startCreateIndex1.get();
        startCreateIndex2.get();
        assertThat(clusterStateCallsOnMaster.get(), is(0));
        assertThat(termCallsOnMaster.get(), is(1));
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

    private void stubClusterTermResponse(String master) {
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, master);
        primaryService.addRequestHandlingBehavior(GetTermVersionAction.NAME, (handler, request, channel, task) -> {
            channel.sendResponse(new GetTermVersionResponse(new ClusterStateTermVersion(new ClusterName("test"), "1", -1, -1)));
        });
    }

}
