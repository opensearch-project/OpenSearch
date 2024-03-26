/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.clustermanager.term;

import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.coordination.ClusterStateTermVersion;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.is;

@SuppressWarnings("unchecked")
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class FetchByTermVersionIT extends OpenSearchIntegTestCase {

    AtomicBoolean isTermVersionCheckEnabled = new AtomicBoolean();

    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockTransportService.TestPlugin.class);
    }

    AtomicBoolean forceFetchFromCM = new AtomicBoolean();

    public void testClusterStateResponseFromDataNode() throws Exception {
        String cm = internalCluster().startClusterManagerOnlyNode();
        List<String> dns = internalCluster().startDataOnlyNodes(5);
        int numberOfShards = dns.size();
        stubClusterTermResponse(cm);

        ensureClusterSizeConsistency();
        ensureGreen();

        List<String> indices = new ArrayList<>();

        // Create a large sized cluster-state by creating field mappings
        IntStream.range(0, 20).forEachOrdered(n -> {
            String index = "index_" + n;
            createIndex(
                index,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MapperService.INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING.getKey(), Long.MAX_VALUE)
                    .build()
            );
            indices.add(index);
        });
        IntStream.range(0, 5).forEachOrdered(n -> {
            List<String> mappings = new ArrayList<>();
            for (int i = 0; i < 2000; i++) {
                mappings.add("t-123456789-123456789-" + n + "-" + i);
                mappings.add("type=keyword");
            }
            PutMappingRequest request = new PutMappingRequest().source(mappings.toArray(new String[0]))
                .indices(indices.toArray(new String[0]));
            internalCluster().dataNodeClient().admin().indices().putMapping(request).actionGet();
        });
        ensureGreen();

        ClusterStateResponse stateResponseM = internalCluster().clusterManagerClient()
            .admin()
            .cluster()
            .state(new ClusterStateRequest())
            .actionGet();

        waitUntil(() -> {
            ClusterStateResponse stateResponseD = internalCluster().dataNodeClient()
                .admin()
                .cluster()
                .state(new ClusterStateRequest())
                .actionGet();
            return stateResponseD.getState().stateUUID().equals(stateResponseM.getState().stateUUID());
        });
        // cluster state response time with term check enabled on datanode
        isTermVersionCheckEnabled.set(true);
        {
            List<Long> latencies = new ArrayList<>();
            IntStream.range(0, 50).forEachOrdered(n1 -> {
                ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                long start = System.currentTimeMillis();
                ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(clusterStateRequest).actionGet();
                latencies.add(System.currentTimeMillis() - start);
                assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
                assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
                assertThat(stateResponse.getState().metadata().indices().size(), is(indices.size()));
                Map<String, Object> fieldMappings = (Map<String, Object>) stateResponse.getState()
                    .metadata()
                    .index(indices.get(0))
                    .mapping()
                    .sourceAsMap()
                    .get("properties");

                assertThat(fieldMappings.size(), is(10000));
            });
            Collections.sort(latencies);

            logger.info("cluster().state() fetch with Term Version enabled took {} milliseconds", (latencies.get(latencies.size() / 2)));
        }
        // cluster state response time with term check disabled on datanode
        isTermVersionCheckEnabled.set(false);
        {
            List<Long> latencies = new ArrayList<>();
            IntStream.range(0, 50).forEachOrdered(n1 -> {
                ClusterStateRequest clusterStateRequest = new ClusterStateRequest();
                long start = System.currentTimeMillis();
                ClusterStateResponse stateResponse = dataNodeClient().admin().cluster().state(clusterStateRequest).actionGet();
                latencies.add(System.currentTimeMillis() - start);
                assertThat(stateResponse.getClusterName().value(), is(internalCluster().getClusterName()));
                assertThat(stateResponse.getState().nodes().getSize(), is(internalCluster().getNodeNames().length));
                assertThat(stateResponse.getState().metadata().indices().size(), is(indices.size()));
                Map<String, Object> typeProperties = (Map<String, Object>) stateResponse.getState()
                    .metadata()
                    .index(indices.get(0))
                    .mapping()
                    .sourceAsMap()
                    .get("properties");
                assertThat(typeProperties.size(), is(10000));

            });
            Collections.sort(latencies);
            logger.info("cluster().state() fetch with Term Version disabled took {} milliseconds", (latencies.get(latencies.size() / 2)));
        }

    }

    private void stubClusterTermResponse(String master) {
        MockTransportService primaryService = (MockTransportService) internalCluster().getInstance(TransportService.class, master);
        primaryService.addRequestHandlingBehavior(GetTermVersionAction.NAME, (handler, request, channel, task) -> {
            if (isTermVersionCheckEnabled.get()) {
                handler.messageReceived(request, channel, task);
            } else {
                // always return response that does not match
                channel.sendResponse(new GetTermVersionResponse(new ClusterStateTermVersion(new ClusterName("test"), "1", -1, -1)));
            }
        });
    }
}
