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

package org.opensearch.nodesinfo;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.util.List;

import static org.opensearch.action.admin.cluster.node.info.NodesInfoRequest.Metric.INDICES;
import static org.opensearch.transport.client.Requests.nodesInfoRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleNodesInfoIT extends OpenSearchIntegTestCase {

    public void testNodesInfos() throws Exception {
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_1 = nodesIds.get(0);
        final String node_2 = nodesIds.get(1);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = internalCluster().getInstance(ClusterService.class, node_1).state().nodes().getLocalNodeId();
        String server2NodeId = internalCluster().getInstance(ClusterService.class, node_2).state().nodes().getLocalNodeId();
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertThat(response.getNodes().size(), is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest()).actionGet();
        assertThat(response.getNodes().size(), is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().size(), is(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server1NodeId)).actionGet();
        assertThat(response.getNodes().size(), is(1));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().size(), is(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        response = client().admin().cluster().nodesInfo(nodesInfoRequest(server2NodeId)).actionGet();
        assertThat(response.getNodes().size(), is(1));
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
    }

    public void testNodesInfosTotalIndexingBuffer() throws Exception {
        List<String> nodesIds = internalCluster().startNodes(2);
        final String node_1 = nodesIds.get(0);
        final String node_2 = nodesIds.get(1);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = internalCluster().getInstance(ClusterService.class, node_1).state().nodes().getLocalNodeId();
        String server2NodeId = internalCluster().getInstance(ClusterService.class, node_2).state().nodes().getLocalNodeId();
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        assertThat(response.getNodes().size(), is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        // again, using only the indices flag
        response = client().admin().cluster().prepareNodesInfo().clear().addMetric(INDICES.metricName()).execute().actionGet();
        assertThat(response.getNodes().size(), is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server1NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));

        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());
        assertNotNull(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer());
        assertThat(response.getNodesMap().get(server2NodeId).getTotalIndexingBuffer().getBytes(), greaterThan(0L));
    }

    public void testAllocatedProcessors() throws Exception {
        List<String> nodesIds = internalCluster().startNodes(
            Settings.builder().put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 3).build(),
            Settings.builder().put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 6).build()
        );

        final String node_1 = nodesIds.get(0);
        final String node_2 = nodesIds.get(1);

        ClusterHealthResponse clusterHealth = client().admin().cluster().prepareHealth().setWaitForGreenStatus().setWaitForNodes("2").get();
        logger.info("--> done cluster_health, status {}", clusterHealth.getStatus());

        String server1NodeId = internalCluster().getInstance(ClusterService.class, node_1).state().nodes().getLocalNodeId();
        String server2NodeId = internalCluster().getInstance(ClusterService.class, node_2).state().nodes().getLocalNodeId();
        logger.info("--> started nodes: {} and {}", server1NodeId, server2NodeId);

        NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();

        assertThat(response.getNodes().size(), is(2));
        assertThat(response.getNodesMap().get(server1NodeId), notNullValue());
        assertThat(response.getNodesMap().get(server2NodeId), notNullValue());

        assertThat(
            response.getNodesMap().get(server1NodeId).getInfo(OsInfo.class).getAvailableProcessors(),
            equalTo(Runtime.getRuntime().availableProcessors())
        );
        assertThat(
            response.getNodesMap().get(server2NodeId).getInfo(OsInfo.class).getAvailableProcessors(),
            equalTo(Runtime.getRuntime().availableProcessors())
        );

        assertThat(response.getNodesMap().get(server1NodeId).getInfo(OsInfo.class).getAllocatedProcessors(), equalTo(3));
        assertThat(response.getNodesMap().get(server2NodeId).getInfo(OsInfo.class).getAllocatedProcessors(), equalTo(6));
    }
}
