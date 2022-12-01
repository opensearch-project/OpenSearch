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

package org.opensearch.upgrades;

import org.hamcrest.MatcherAssert;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.XContentTestUtils.JsonMapView;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SystemIndicesUpgradeIT extends AbstractRollingTestCase {

    @SuppressWarnings("unchecked")
    public void testSystemIndicesUpgrades() throws Exception {
        final String systemIndexWarning = "this request accesses system indices: [.tasks], but in a future major version, direct " +
            "access to system indices will be prevented by default";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // create index
            Request createTestIndex = new Request("PUT", "/test_index_old");
            createTestIndex.setJsonEntity("{\"settings\": {\"index.number_of_shards\": 1, \"index.number_of_replicas\": 0}}");
            client().performRequest(createTestIndex);

            Request bulk = new Request("POST", "/_bulk");
            bulk.addParameter("refresh", "true");
            bulk.setJsonEntity("{\"index\": {\"_index\": \"test_index_old\"}\n" +
                "{\"f1\": \"v1\", \"f2\": \"v2\"}\n");
            client().performRequest(bulk);

            createAndVerifyStoredTask();

            // make sure .tasks index exists
            Request getTasksIndex = new Request("GET", "/.tasks");
            getTasksIndex.addParameter("allow_no_indices", "false");
            getTasksIndex.setOptions(expectVersionSpecificWarnings(v -> {
                v.current(systemIndexWarning);
                v.compatible(systemIndexWarning);
            }));
            assertBusy(() -> {
                try {
                    assertThat(client().performRequest(getTasksIndex).getStatusLine().getStatusCode(), is(200));
                } catch (ResponseException e) {
                    throw new AssertionError(".tasks index does not exist yet");
                }
            });
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            createAndVerifyStoredTask();

            assertBusy(() -> {
                Request clusterStateRequest = new Request("GET", "/_cluster/state/metadata");
                Map<String, Object> indices = new JsonMapView(entityAsMap(client().performRequest(clusterStateRequest)))
                    .get("metadata.indices");

                // Make sure our non-system index is still non-system
                assertThat(new JsonMapView(indices).get("test_index_old.system"), is(false));

                // Can't get the .tasks index via JsonMapView because it splits on `.`
                assertThat(indices, hasKey(".tasks"));
                JsonMapView tasksIndex = new JsonMapView((Map<String, Object>) indices.get(".tasks"));
                assertThat(tasksIndex.get("system"), is(true));

                final String tasksCreatedVersionString = tasksIndex.get("settings.index.version.created");
                assertThat(tasksCreatedVersionString, notNullValue());
            });
        }
    }

    /**
     * Completed tasks get persisted into the .tasks index, so this method waits
     * until the task is completed in order to verify that it has been successfully
     * written to the index and can be retrieved.
     */
    private static void createAndVerifyStoredTask() throws Exception {
        // Use update by query to create an async task
        final Request updateByQueryRequest = new Request("POST", "/test_index_old/_update_by_query");
        updateByQueryRequest.addParameter("wait_for_completion", "false");
        final Response updateByQueryResponse = client().performRequest(updateByQueryRequest);
        MatcherAssert.assertThat(updateByQueryResponse.getStatusLine().getStatusCode(), equalTo(200));
        final String taskId = (String) entityAsMap(updateByQueryResponse).get("task");

        // wait for task to complete
        waitUntil(() -> {
            try {
                final Response getTaskResponse = client().performRequest(new Request("GET", "/_tasks/" + taskId));
                MatcherAssert.assertThat(getTaskResponse.getStatusLine().getStatusCode(), equalTo(200));
                return (Boolean) entityAsMap(getTaskResponse).get("completed");
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
    }
}
