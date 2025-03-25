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

package org.opensearch.cluster;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsAction;
import org.opensearch.action.admin.cluster.configuration.AddVotingConfigExclusionsRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.discovery.ClusterManagerNotDiscoveredException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;
import java.util.function.Supplier;

import static org.opensearch.test.NodeRoles.clusterManagerNode;
import static org.opensearch.test.NodeRoles.dataOnlyNode;
import static org.opensearch.test.NodeRoles.nonDataNode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0, autoManageMasterNodes = false)
public class SpecificClusterManagerNodesIT extends OpenSearchIntegTestCase {

    public void testSimpleOnlyClusterManagerNodeElection() throws IOException {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start data node / non cluster-manager node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .setClusterManagerNodeTimeout("100ms")
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getClusterManagerNodeId(),
                nullValue()
            );
            fail("should not be able to find cluster-manager");
        } catch (ClusterManagerNotDiscoveredException e) {
            // all is well, no cluster-manager elected
        }
        logger.info("--> start cluster-manager node");
        final String clusterManagerNodeName = internalCluster().startClusterManagerOnlyNode();
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );
        assertThat(
            internalCluster().clusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );

        logger.info("--> stop cluster-manager node");
        Settings clusterManagerDataPathSettings = internalCluster().dataPathSettings(internalCluster().getClusterManagerName());
        internalCluster().stopCurrentClusterManagerNode();

        try {
            assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .setClusterManagerNodeTimeout("100ms")
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getClusterManagerNodeId(),
                nullValue()
            );
            fail("should not be able to find cluster-manager");
        } catch (ClusterManagerNotDiscoveredException e) {
            // all is well, no cluster-manager elected
        }

        logger.info("--> start previous cluster-manager node again");
        final String nextClusterManagerEligibleNodeName = internalCluster().startNode(
            Settings.builder().put(nonDataNode(clusterManagerNode())).put(clusterManagerDataPathSettings)
        );
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(nextClusterManagerEligibleNodeName)
        );
        assertThat(
            internalCluster().clusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(nextClusterManagerEligibleNodeName)
        );
    }

    public void testElectOnlyBetweenClusterManagerNodes() throws Exception {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start data node / non cluster-manager node");
        internalCluster().startNode(Settings.builder().put(dataOnlyNode()).put("discovery.initial_state_timeout", "1s"));
        try {
            assertThat(
                client().admin()
                    .cluster()
                    .prepareState()
                    .setClusterManagerNodeTimeout("100ms")
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getClusterManagerNodeId(),
                nullValue()
            );
            fail("should not be able to find cluster-manager");
        } catch (ClusterManagerNotDiscoveredException e) {
            // all is well, no cluster-manager elected
        }
        logger.info("--> start cluster-manager node (1)");
        final String clusterManagerNodeName = internalCluster().startClusterManagerOnlyNode();
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );
        assertThat(
            internalCluster().clusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );

        logger.info("--> start cluster-manager node (2)");
        final String nextClusterManagerEligableNodeName = internalCluster().startClusterManagerOnlyNode();
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );
        assertThat(
            internalCluster().clusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(clusterManagerNodeName)
        );

        logger.info("--> closing cluster-manager node (1)");
        client().execute(AddVotingConfigExclusionsAction.INSTANCE, new AddVotingConfigExclusionsRequest(clusterManagerNodeName)).get();
        // removing the cluster-manager from the voting configuration immediately triggers the cluster-manager to step down
        Supplier<String> getClusterManagerIfElected = () -> {
            try {
                return internalCluster().nonClusterManagerClient()
                    .admin()
                    .cluster()
                    .prepareState()
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getClusterManagerNode()
                    .getName();
            } catch (Exception e) {
                logger.debug("failed to get cluster-manager name", e);
                return null;
            }
        };
        assertBusy(() -> {
            assertThat(getClusterManagerIfElected.get(), equalTo(nextClusterManagerEligableNodeName));
            assertThat(
                internalCluster().clusterManagerClient()
                    .admin()
                    .cluster()
                    .prepareState()
                    .execute()
                    .actionGet()
                    .getState()
                    .nodes()
                    .getClusterManagerNode()
                    .getName(),
                equalTo(nextClusterManagerEligableNodeName)
            );
        });
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(clusterManagerNodeName));
        assertThat(
            internalCluster().nonClusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(nextClusterManagerEligableNodeName)
        );
        assertThat(
            internalCluster().clusterManagerClient()
                .admin()
                .cluster()
                .prepareState()
                .execute()
                .actionGet()
                .getState()
                .nodes()
                .getClusterManagerNode()
                .getName(),
            equalTo(nextClusterManagerEligableNodeName)
        );
    }

    public void testAliasFilterValidation() {
        internalCluster().setBootstrapClusterManagerNodeIndex(0);
        logger.info("--> start cluster-manager node / non data");
        internalCluster().startClusterManagerOnlyNode();

        logger.info("--> start data node / non cluster-manager node");
        internalCluster().startDataOnlyNode();

        assertAcked(
            prepareCreate("test").setMapping(
                "{\"properties\" : {\"table_a\" : { \"type\" : \"nested\", "
                    + "\"properties\" : {\"field_a\" : { \"type\" : \"keyword\" },\"field_b\" :{ \"type\" : \"keyword\" }}}}}"
            )
        );
        client().admin()
            .indices()
            .prepareAliases()
            .addAlias(
                "test",
                "a_test",
                QueryBuilders.nestedQuery("table_a", QueryBuilders.termQuery("table_a.field_b", "y"), ScoreMode.Avg)
            )
            .get();
    }

}
