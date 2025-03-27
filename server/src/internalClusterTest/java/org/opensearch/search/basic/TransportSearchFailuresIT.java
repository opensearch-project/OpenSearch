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

package org.opensearch.search.basic;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;
import org.opensearch.transport.client.Requests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.transport.client.Requests.clusterHealthRequest;
import static org.opensearch.transport.client.Requests.refreshRequest;
import static org.opensearch.transport.client.Requests.searchRequest;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportSearchFailuresIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public TransportSearchFailuresIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected int maximumNumberOfReplicas() {
        return 1;
    }

    public void testFailedSearchWithWrongQuery() throws Exception {
        logger.info("Start Testing failed search with wrong query");
        assertAcked(prepareCreate("test", 1).setMapping("foo", "type=geo_point"));

        NumShards test = getNumShards("test");

        for (int i = 0; i < 100; i++) {
            index(client(), Integer.toString(i), "test", i);
        }
        RefreshResponse refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.numPrimaries));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));
        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                    searchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
                ).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (OpenSearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        allowNodes("test", 2);
        assertThat(
            client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForNodes(">=2").get().isTimedOut(),
            equalTo(false)
        );

        logger.info("Running Cluster Health");
        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .health(
                clusterHealthRequest("test").waitForYellowStatus()
                    .waitForNoRelocatingShards(true)
                    .waitForEvents(Priority.LANGUID)
                    .waitForActiveShards(test.totalNumShards)
            )
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), anyOf(equalTo(ClusterHealthStatus.YELLOW), equalTo(ClusterHealthStatus.GREEN)));
        assertThat(clusterHealth.getActiveShards(), equalTo(test.totalNumShards));

        refreshResponse = client().admin().indices().refresh(refreshRequest("test")).actionGet();
        assertThat(refreshResponse.getTotalShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(test.totalNumShards));
        assertThat(refreshResponse.getFailedShards(), equalTo(0));

        for (int i = 0; i < 5; i++) {
            try {
                SearchResponse searchResponse = client().search(
                    searchRequest("test").source(new SearchSourceBuilder().query(new MatchQueryBuilder("foo", "biz")))
                ).actionGet();
                assertThat(searchResponse.getTotalShards(), equalTo(test.numPrimaries));
                assertThat(searchResponse.getSuccessfulShards(), equalTo(0));
                assertThat(searchResponse.getFailedShards(), equalTo(test.numPrimaries));
                fail("search should fail");
            } catch (OpenSearchException e) {
                assertThat(e.unwrapCause(), instanceOf(SearchPhaseExecutionException.class));
                // all is well
            }
        }

        logger.info("Done Testing failed search");
    }

    private void index(Client client, String id, String nameValue, int age) throws IOException {
        client.index(Requests.indexRequest("test").id(id).source(source(id, nameValue, age))).actionGet();
    }

    private XContentBuilder source(String id, String nameValue, int age) throws IOException {
        StringBuilder multi = new StringBuilder().append(nameValue);
        for (int i = 0; i < age; i++) {
            multi.append(" ").append(nameValue);
        }
        return jsonBuilder().startObject()
            .field("id", id)
            .field("name", nameValue + id)
            .field("age", age)
            .field("multi", multi.toString())
            .endObject();
    }
}
