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

package org.opensearch.cluster.remote.test;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.cluster.RemoteConnectionInfo;
import org.opensearch.client.cluster.RemoteInfoRequest;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class RemoteClustersIT extends AbstractMultiClusterRemoteTestCase {

    @Before
    public void setupIndices() throws IOException {
        assertTrue(cluster1Client().indices().create(new CreateIndexRequest("test1").settings(Settings.builder()
            .put("index.number_of_replicas", 0).build()), RequestOptions.DEFAULT).isAcknowledged());
        cluster1Client().index(new IndexRequest("test1").id("id1").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()), RequestOptions.DEFAULT);
        assertTrue(cluster2Client().indices().create(new CreateIndexRequest("test2").settings(Settings.builder()
            .put("index.number_of_replicas", 0).build()), RequestOptions.DEFAULT).isAcknowledged());
        cluster2Client().index(new IndexRequest("test2").id("id1")
            .source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()), RequestOptions.DEFAULT);
        cluster2Client().index(new IndexRequest("test2").id("id2").setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .source(XContentFactory.jsonBuilder().startObject().field("foo", "bar").endObject()), RequestOptions.DEFAULT);
        assertEquals(1L, cluster1Client().search(new SearchRequest("test1"), RequestOptions.DEFAULT).getHits().getTotalHits().value());
        assertEquals(2L, cluster2Client().search(new SearchRequest("test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value());
    }

    @After
    public void clearIndices() throws IOException {
        assertTrue(cluster1Client().indices().delete(new DeleteIndexRequest("*"), RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(cluster2Client().indices().delete(new DeleteIndexRequest("*"), RequestOptions.DEFAULT).isAcknowledged());
    }

    @After
    public void clearRemoteClusterSettings() throws IOException {
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(
            Settings.builder().putNull("cluster.remote.*").build());
        assertTrue(cluster1Client().cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged());
        assertTrue(cluster2Client().cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged());
    }

    public void testProxyModeConnectionWorks() throws IOException {
        String cluster2RemoteClusterSeed = "opensearch-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder()
            .put("cluster.remote.cluster2.mode", "proxy")
            .put("cluster.remote.cluster2.proxy_address", cluster2RemoteClusterSeed)
            .build());
        assertTrue(cluster1Client().cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged());

        RemoteConnectionInfo rci = cluster1Client().cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT).getInfos().get(0);
        logger.info("Connection info: {}", rci);
        assertTrue(rci.isConnected());

        assertEquals(2L, cluster1Client().search(
            new SearchRequest("cluster2:test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value());
    }

    public void testSniffModeConnectionFails() throws IOException {
        String cluster2RemoteClusterSeed = "opensearch-2:9300";
        logger.info("Configuring remote cluster [{}]", cluster2RemoteClusterSeed);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder()
            .put("cluster.remote.cluster2alt.mode", "sniff")
            .put("cluster.remote.cluster2alt.seeds", cluster2RemoteClusterSeed)
            .build());
        assertTrue(cluster1Client().cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged());

        RemoteConnectionInfo rci = cluster1Client().cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT).getInfos().get(0);
        logger.info("Connection info: {}", rci);
        assertFalse(rci.isConnected());
    }

    public void testHAProxyModeConnectionWorks() throws Exception {
        String proxyAddress = "haproxy:9600";
        logger.info("Configuring remote cluster [{}]", proxyAddress);
        ClusterUpdateSettingsRequest request = new ClusterUpdateSettingsRequest().persistentSettings(Settings.builder()
            .put("cluster.remote.haproxynosn.mode", "proxy")
            .put("cluster.remote.haproxynosn.proxy_address", proxyAddress)
            .build());
        assertTrue(cluster1Client().cluster().putSettings(request, RequestOptions.DEFAULT).isAcknowledged());

        assertBusy(() -> {
            RemoteConnectionInfo rci = cluster1Client().cluster().remoteInfo(new RemoteInfoRequest(), RequestOptions.DEFAULT).getInfos().get(0);
            logger.info("Connection info: {}", rci);
            if (!rci.isConnected()) {
                logger.info("Cluster health: {}", cluster1Client().cluster().health(new ClusterHealthRequest(), RequestOptions.DEFAULT));
            }
            assertTrue(rci.isConnected());
        }, 10, TimeUnit.SECONDS);

        assertEquals(2L, cluster1Client().search(
            new SearchRequest("haproxynosn:test2"), RequestOptions.DEFAULT).getHits().getTotalHits().value());
    }
}
