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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class RestClusterHealthActionTests extends OpenSearchTestCase {

    public void testFromRequest() {
        Map<String, String> params = new HashMap<>();
        String index = "index";
        boolean local = randomBoolean();
        boolean ensureLocalNodeCommissioned = false;
        if (local) {
            ensureLocalNodeCommissioned = randomBoolean();
        }
        String clusterManagerTimeout = randomTimeValue();
        String timeout = randomTimeValue();
        ClusterHealthStatus waitForStatus = randomFrom(ClusterHealthStatus.values());
        boolean waitForNoRelocatingShards = randomBoolean();
        boolean waitForNoInitializingShards = randomBoolean();
        int waitForActiveShards = randomIntBetween(1, 3);
        String waitForNodes = "node";
        Priority waitForEvents = randomFrom(Priority.values());

        params.put("index", index);
        params.put("local", String.valueOf(local));
        params.put("ensure_node_commissioned", String.valueOf(ensureLocalNodeCommissioned));
        params.put("cluster_manager_timeout", clusterManagerTimeout);
        params.put("timeout", timeout);
        params.put("wait_for_status", waitForStatus.name());
        if (waitForNoRelocatingShards || randomBoolean()) {
            params.put("wait_for_no_relocating_shards", String.valueOf(waitForNoRelocatingShards));
        }
        if (waitForNoInitializingShards || randomBoolean()) {
            params.put("wait_for_no_initializing_shards", String.valueOf(waitForNoInitializingShards));
        }
        params.put("wait_for_active_shards", String.valueOf(waitForActiveShards));
        params.put("wait_for_nodes", waitForNodes);
        params.put("wait_for_events", waitForEvents.name());

        FakeRestRequest restRequest = buildRestRequest(params);
        ClusterHealthRequest clusterHealthRequest = RestClusterHealthAction.fromRequest(restRequest);
        assertThat(clusterHealthRequest.indices().length, equalTo(1));
        assertThat(clusterHealthRequest.indices()[0], equalTo(index));
        assertThat(clusterHealthRequest.local(), equalTo(local));
        assertThat(clusterHealthRequest.ensureNodeCommissioned(), equalTo(ensureLocalNodeCommissioned));
        assertThat(clusterHealthRequest.clusterManagerNodeTimeout(), equalTo(TimeValue.parseTimeValue(clusterManagerTimeout, "test")));
        assertThat(clusterHealthRequest.timeout(), equalTo(TimeValue.parseTimeValue(timeout, "test")));
        assertThat(clusterHealthRequest.waitForStatus(), equalTo(waitForStatus));
        assertThat(clusterHealthRequest.waitForNoRelocatingShards(), equalTo(waitForNoRelocatingShards));
        assertThat(clusterHealthRequest.waitForNoInitializingShards(), equalTo(waitForNoInitializingShards));
        assertThat(clusterHealthRequest.waitForActiveShards(), equalTo(ActiveShardCount.parseString(String.valueOf(waitForActiveShards))));
        assertThat(clusterHealthRequest.waitForNodes(), equalTo(waitForNodes));
        assertThat(clusterHealthRequest.waitForEvents(), equalTo(waitForEvents));

    }

    private FakeRestRequest buildRestRequest(Map<String, String> params) {
        return new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_cluster/health")
            .withParams(params)
            .build();
    }
}
