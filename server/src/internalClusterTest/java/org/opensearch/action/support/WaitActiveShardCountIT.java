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

package org.opensearch.action.support;

import org.opensearch.action.UnavailableShardsException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.Priority;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.common.unit.TimeValue.timeValueSeconds;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests setting the active shard count for replication operations (e.g. index) operates correctly.
 */
public class WaitActiveShardCountIT extends OpenSearchIntegTestCase {
    public void testReplicationWaitsForActiveShardCount() throws Exception {
        CreateIndexResponse createIndexResponse = prepareCreate(
            "test",
            1,
            Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 2)
        ).get();

        assertAcked(createIndexResponse);

        // indexing, by default, will work (waiting for one shard copy only)
        client().prepareIndex("test").setId("1").setSource(source("1", "test"), MediaTypeRegistry.JSON).execute().actionGet();
        try {
            client().prepareIndex("test")
                .setId("1")
                .setSource(source("1", "test"), MediaTypeRegistry.JSON)
                .setWaitForActiveShards(2) // wait for 2 active shard copies
                .setTimeout(timeValueMillis(100))
                .execute()
                .actionGet();
            fail("can't index, does not enough active shard copies");
        } catch (UnavailableShardsException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                e.getMessage(),
                startsWith("[test][0] Not enough active copies to meet shard count of [2] (have 1, needed 2). Timeout: [100ms]")
            );
            // but really, all is well
        }

        allowNodes("test", 2);

        ClusterHealthResponse clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForActiveShards(2)
            .setWaitForYellowStatus()
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.YELLOW));

        // this should work, since we now have two
        client().prepareIndex("test")
            .setId("1")
            .setSource(source("1", "test"), MediaTypeRegistry.JSON)
            .setWaitForActiveShards(2)
            .setTimeout(timeValueSeconds(1))
            .execute()
            .actionGet();

        try {
            client().prepareIndex("test")
                .setId("1")
                .setSource(source("1", "test"), MediaTypeRegistry.JSON)
                .setWaitForActiveShards(ActiveShardCount.ALL)
                .setTimeout(timeValueMillis(100))
                .execute()
                .actionGet();
            fail("can't index, not enough active shard copies");
        } catch (UnavailableShardsException e) {
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
            assertThat(
                e.getMessage(),
                startsWith(
                    "[test][0] Not enough active copies to meet shard count of ["
                        + ActiveShardCount.ALL
                        + "] (have 2, needed 3). Timeout: [100ms]"
                )
            );
            // but really, all is well
        }

        allowNodes("test", 3);
        clusterHealth = client().admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForActiveShards(3)
            .setWaitForGreenStatus()
            .execute()
            .actionGet();
        logger.info("Done Cluster Health, status {}", clusterHealth.getStatus());
        assertThat(clusterHealth.isTimedOut(), equalTo(false));
        assertThat(clusterHealth.getStatus(), equalTo(ClusterHealthStatus.GREEN));

        // this should work, since we now have all shards started
        client().prepareIndex("test")
            .setId("1")
            .setSource(source("1", "test"), MediaTypeRegistry.JSON)
            .setWaitForActiveShards(ActiveShardCount.ALL)
            .setTimeout(timeValueSeconds(1))
            .execute()
            .actionGet();
    }

    private String source(String id, String nameValue) {
        return "{ \"type1\" : { \"id\" : \"" + id + "\", \"name\" : \"" + nameValue + "\" } }";
    }
}
