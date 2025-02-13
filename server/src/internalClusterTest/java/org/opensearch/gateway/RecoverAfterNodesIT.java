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

import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.client.Client;

import java.util.Set;

import static org.opensearch.test.NodeRoles.clusterManagerOnlyNode;
import static org.opensearch.test.NodeRoles.dataOnlyNode;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class RecoverAfterNodesIT extends OpenSearchIntegTestCase {
    private static final TimeValue BLOCK_WAIT_TIMEOUT = TimeValue.timeValueSeconds(10);

    public Set<ClusterBlock> waitForNoBlocksOnNode(TimeValue timeout, Client nodeClient) {
        long start = System.currentTimeMillis();
        Set<ClusterBlock> blocks;
        do {
            blocks = nodeClient.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE);
        } while (!blocks.isEmpty() && (System.currentTimeMillis() - start) < timeout.millis());
        return blocks;
    }

    public Client startNode(Settings.Builder settings) {
        String name = internalCluster().startNode(Settings.builder().put(settings.build()));
        return internalCluster().client(name);
    }

    public void testRecoverAfterNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start node (1)");
        Client clientNode1 = startNode(Settings.builder().put("gateway.recover_after_nodes", 3));
        assertThat(
            clientNode1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start node (2)");
        Client clientNode2 = startNode(Settings.builder().put("gateway.recover_after_nodes", 3));
        Thread.sleep(BLOCK_WAIT_TIMEOUT.millis());
        assertThat(
            clientNode1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            clientNode2.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start node (3)");
        Client clientNode3 = startNode(Settings.builder().put("gateway.recover_after_nodes", 3));

        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clientNode3).isEmpty(), equalTo(true));
    }

    public void testRecoverAfterClusterManagerNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start cluster_manager_node (1)");
        Client clusterManager1 = startNode(Settings.builder().put("gateway.recover_after_master_nodes", 2).put(clusterManagerOnlyNode()));
        assertThat(
            clusterManager1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start data_node (1)");
        Client data1 = startNode(Settings.builder().put("gateway.recover_after_master_nodes", 2).put(dataOnlyNode()));
        assertThat(
            clusterManager1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            data1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start data_node (2)");
        Client data2 = startNode(Settings.builder().put("gateway.recover_after_master_nodes", 2).put(dataOnlyNode()));
        assertThat(
            clusterManager1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            data1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            data2.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start cluster_manager_node (2)");
        Client clusterManager2 = startNode(Settings.builder().put("gateway.recover_after_master_nodes", 2).put(clusterManagerOnlyNode()));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clusterManager1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clusterManager2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data2).isEmpty(), equalTo(true));
    }

    public void testRecoverAfterDataNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start cluster_manager_node (1)");
        Client clusterManager1 = startNode(Settings.builder().put("gateway.recover_after_data_nodes", 2).put(clusterManagerOnlyNode()));
        assertThat(
            clusterManager1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start data_node (1)");
        Client data1 = startNode(Settings.builder().put("gateway.recover_after_data_nodes", 2).put(dataOnlyNode()));
        assertThat(
            clusterManager1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            data1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start cluster_manager_node (2)");
        Client clusterManager2 = startNode(Settings.builder().put("gateway.recover_after_data_nodes", 2).put(clusterManagerOnlyNode()));
        assertThat(
            clusterManager2.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            data1.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );
        assertThat(
            clusterManager2.admin()
                .cluster()
                .prepareState()
                .setLocal(true)
                .execute()
                .actionGet()
                .getState()
                .blocks()
                .global(ClusterBlockLevel.METADATA_WRITE),
            hasItem(GatewayService.STATE_NOT_RECOVERED_BLOCK)
        );

        logger.info("--> start data_node (2)");
        Client data2 = startNode(Settings.builder().put("gateway.recover_after_data_nodes", 2).put(dataOnlyNode()));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clusterManager1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, clusterManager2).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data1).isEmpty(), equalTo(true));
        assertThat(waitForNoBlocksOnNode(BLOCK_WAIT_TIMEOUT, data2).isEmpty(), equalTo(true));
    }
}
