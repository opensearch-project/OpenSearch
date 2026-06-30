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

import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;
import org.opensearch.transport.client.Requests;

import static org.opensearch.common.unit.TimeValue.timeValueSeconds;
import static org.opensearch.test.NodeRoles.dataNode;
import static org.opensearch.test.NodeRoles.nonDataNode;
import static org.opensearch.transport.client.Requests.createIndexRequest;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = Scope.TEST, numDataNodes = 0)
public class SimpleDataNodesIT extends OpenSearchIntegTestCase {

    private static final String SOURCE = "{\"type1\":{\"id\":\"1\",\"name\":\"test\"}}";

    public void testIndexingBeforeAndAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin().indices().create(createIndexRequest("test").waitForActiveShards(ActiveShardCount.NONE)).actionGet();
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON).timeout(timeValueSeconds(1)))
                .actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        internalCluster().startNode(nonDataNode());
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        // still no shard should be allocated
        try {
            client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON).timeout(timeValueSeconds(1)))
                .actionGet();
            fail("no allocation should happen");
        } catch (UnavailableShardsException e) {
            // all is well
        }

        // now, start a node data, and see that it gets with shards
        internalCluster().startNode(dataNode());
        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("3")
                .setLocal(true)
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );

        IndexResponse indexResponse = client().index(Requests.indexRequest("test").id("1").source(SOURCE, MediaTypeRegistry.JSON))
            .actionGet();
        assertThat(indexResponse.getId(), equalTo("1"));
    }

    public void testShardsAllocatedAfterDataNodesStart() {
        internalCluster().startNode(nonDataNode());
        client().admin()
            .indices()
            .create(
                createIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
                    .waitForActiveShards(ActiveShardCount.NONE)
            )
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode(dataNode());

        assertThat(
            client().admin()
                .cluster()
                .prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNodes("2")
                .setWaitForGreenStatus()
                .execute()
                .actionGet()
                .isTimedOut(),
            equalTo(false)
        );
    }

    public void testAutoExpandReplicasAdjustedWhenDataNodeJoins() {
        internalCluster().startNode(nonDataNode());
        client().admin()
            .indices()
            .create(
                createIndexRequest("test").settings(Settings.builder().put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all"))
                    .waitForActiveShards(ActiveShardCount.NONE)
            )
            .actionGet();
        final ClusterHealthResponse healthResponse1 = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .execute()
            .actionGet();
        assertThat(healthResponse1.isTimedOut(), equalTo(false));
        assertThat(healthResponse1.getStatus(), equalTo(ClusterHealthStatus.RED));
        assertThat(healthResponse1.getActiveShards(), equalTo(0));

        internalCluster().startNode();
        internalCluster().startNode();
        client().admin().cluster().prepareReroute().setRetryFailed(true).get();
    }

}
