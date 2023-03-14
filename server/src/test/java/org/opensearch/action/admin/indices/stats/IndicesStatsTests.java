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

package org.opensearch.action.admin.indices.stats;

import org.opensearch.action.ActionFuture;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexModule;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

public class IndicesStatsTests extends OpenSearchSingleNodeTestCase {

    public void testSegmentStatsEmptyIndex() {
        createIndex("test");
        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats = rsp.getTotal().getSegments();
        assertEquals(0, stats.getCount());
    }

    public void testSegmentStats() throws Exception {
        IndexModule.Type storeType = IndexModule.defaultStoreType(true);
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("foo")
            .field("type", "keyword")
            .field("doc_values", true)
            .field("store", true)
            .endObject()
            .startObject("bar")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .startObject("baz")
            .field("type", "long")
            .endObject()
            .endObject()
            .endObject();
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping(mapping)
                .setSettings(Settings.builder().put("index.store.type", storeType.getSettingsKey()))
        );
        ensureGreen("test");
        client().prepareIndex("test").setId("1").setSource("foo", "bar", "bar", "baz", "baz", 42).get();
        client().admin().indices().prepareRefresh("test").get();

        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats = rsp.getIndex("test").getTotal().getSegments();
        // should be more than one segment since data was indexed
        assertThat(stats.getCount(), greaterThan(0L));

        // now check multiple segments stats are merged together
        client().prepareIndex("test").setId("2").setSource("foo", "bar", "bar", "baz", "baz", 43).get();
        client().admin().indices().prepareRefresh("test").get();

        rsp = client().admin().indices().prepareStats("test").get();
        SegmentsStats stats2 = rsp.getIndex("test").getTotal().getSegments();
        // stats2 should exceed stats since multiple segments stats were merged
        assertThat(stats2.getCount(), greaterThan(stats.getCount()));
    }

    public void testCommitStats() throws Exception {
        createIndex("test");
        ensureGreen("test");

        IndicesStatsResponse rsp = client().admin().indices().prepareStats("test").get();
        for (ShardStats shardStats : rsp.getIndex("test").getShards()) {
            final CommitStats commitStats = shardStats.getCommitStats();
            assertNotNull(commitStats);
            assertThat(commitStats.getGeneration(), greaterThan(0L));
            assertThat(commitStats.getId(), notNullValue());
            assertThat(commitStats.getUserData(), hasKey(Translog.TRANSLOG_UUID_KEY));
        }
    }

    public void testRefreshListeners() throws Exception {
        // Create an index without automatic refreshes
        createIndex("test", Settings.builder().put("refresh_interval", -1).build());

        // Index a document asynchronously so the request will only return when document is refreshed
        ActionFuture<IndexResponse> index = client().prepareIndex("test")
            .setId("test")
            .setSource("test", "test")
            .setRefreshPolicy(RefreshPolicy.WAIT_UNTIL)
            .execute();

        // Wait for the refresh listener to appear in the stats. Wait a long time because NFS tests can be quite slow!
        logger.info("starting to wait");
        long end = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
        while (true) {
            IndicesStatsResponse stats = client().admin().indices().prepareStats("test").clear().setRefresh(true).setDocs(true).get();
            CommonStats common = stats.getIndices().get("test").getTotal();
            // There shouldn't be a doc. If there is then we did *something* weird.
            assertEquals(0, common.docs.getCount());
            if (1 == common.refresh.getListeners()) {
                break;
            }
            if (end - System.nanoTime() < 0) {
                logger.info("timed out");
                fail("didn't get a refresh listener in time: " + Strings.toString(XContentType.JSON, common));
            }
        }

        // Refresh the index and wait for the request to come back
        client().admin().indices().prepareRefresh("test").get();
        index.get();

        // The document should appear in the statistics and the refresh listener should be gone
        IndicesStatsResponse stats = client().admin().indices().prepareStats("test").clear().setRefresh(true).setDocs(true).get();
        CommonStats common = stats.getIndices().get("test").getTotal();
        assertEquals(1, common.docs.getCount());
        assertEquals(0, common.refresh.getListeners());
    }

    public void testUuidOnRootStatsIndices() {
        String uuid = createIndex("test").indexUUID();
        IndicesStatsResponse rsp = client().admin().indices().prepareStats().get();
        assertEquals(uuid, rsp.getIndex("test").getUuid());
    }

    /**
     * Gives access to package private IndicesStatsResponse constructor for test purpose.
     **/
    public static IndicesStatsResponse newIndicesStatsResponse(
        ShardStats[] shards,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<DefaultShardOperationFailedException> shardFailures
    ) {
        return new IndicesStatsResponse(shards, totalShards, successfulShards, failedShards, shardFailures);
    }
}
