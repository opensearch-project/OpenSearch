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

package org.opensearch.indices.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.open.OpenIndexResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_METADATA;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_READ;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_BLOCKS_WRITE;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.opensearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBlocked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class OpenCloseIndexIT extends OpenSearchIntegTestCase {
    public void testSimpleCloseOpen() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testSimpleOpenMissingIndex() {
        Client client = client();
        Exception e = expectThrows(IndexNotFoundException.class, () -> client.admin().indices().prepareOpen("test1").execute().actionGet());
        assertThat(e.getMessage(), is("no such index [test1]"));
    }

    public void testOpenOneMissingIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        Exception e = expectThrows(
            IndexNotFoundException.class,
            () -> client.admin().indices().prepareOpen("test1", "test2").execute().actionGet()
        );
        assertThat(e.getMessage(), is("no such index [test2]"));
    }

    public void testOpenOneMissingIndexIgnoreMissing() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));
        OpenIndexResponse openIndexResponse = client.admin()
            .indices()
            .prepareOpen("test1", "test2")
            .setIndicesOptions(IndicesOptions.lenientExpandOpen())
            .execute()
            .actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testCloseOpenMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse1 = client.admin().indices().prepareClose("test1").execute().actionGet();
        assertThat(closeIndexResponse1.isAcknowledged(), equalTo(true));
        AcknowledgedResponse closeIndexResponse2 = client.admin().indices().prepareClose("test2").execute().actionGet();
        assertThat(closeIndexResponse2.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("test3");

        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        OpenIndexResponse openIndexResponse2 = client.admin().indices().prepareOpen("test2").execute().actionGet();
        assertThat(openIndexResponse2.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse2.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testCloseOpenWildcard() {
        Client client = client();
        createIndex("test1", "test2", "a");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");
        assertIndexIsOpened("a");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test*").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "a");
    }

    public void testCloseOpenAll() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("_all").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("_all").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testCloseOpenAllWildcard() {
        Client client = client();
        createIndex("test1", "test2", "test3");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("*").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2", "test3");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("*").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2", "test3");
    }

    public void testOpenNoIndex() {
        Client client = client();
        Exception e = expectThrows(
            ActionRequestValidationException.class,
            () -> client.admin().indices().prepareOpen().execute().actionGet()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenNullIndex() {
        Client client = client();
        Exception e = expectThrows(
            ActionRequestValidationException.class,
            () -> client.admin().indices().prepareOpen((String[]) null).execute().actionGet()
        );
        assertThat(e.getMessage(), containsString("index is missing"));
    }

    public void testOpenAlreadyOpenedIndex() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        // no problem if we try to open an index that's already in open state
        OpenIndexResponse openIndexResponse1 = client.admin().indices().prepareOpen("test1").execute().actionGet();
        assertThat(openIndexResponse1.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse1.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testSimpleCloseOpenAlias() {
        Client client = client();
        createIndex("test1");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse aliasesResponse = client.admin()
            .indices()
            .prepareAliases()
            .addAlias("test1", "test1-alias")
            .execute()
            .actionGet();
        assertThat(aliasesResponse.isAcknowledged(), equalTo(true));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test1-alias").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test1-alias").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1");
    }

    public void testCloseOpenAliasMultipleIndices() {
        Client client = client();
        createIndex("test1", "test2");
        ClusterHealthResponse healthResponse = client.admin().cluster().prepareHealth().setWaitForGreenStatus().execute().actionGet();
        assertThat(healthResponse.isTimedOut(), equalTo(false));

        AcknowledgedResponse aliasesResponse1 = client.admin()
            .indices()
            .prepareAliases()
            .addAlias("test1", "test-alias")
            .execute()
            .actionGet();
        assertThat(aliasesResponse1.isAcknowledged(), equalTo(true));
        AcknowledgedResponse aliasesResponse2 = client.admin()
            .indices()
            .prepareAliases()
            .addAlias("test2", "test-alias")
            .execute()
            .actionGet();
        assertThat(aliasesResponse2.isAcknowledged(), equalTo(true));

        AcknowledgedResponse closeIndexResponse = client.admin().indices().prepareClose("test-alias").execute().actionGet();
        assertThat(closeIndexResponse.isAcknowledged(), equalTo(true));
        assertIndexIsClosed("test1", "test2");

        OpenIndexResponse openIndexResponse = client.admin().indices().prepareOpen("test-alias").execute().actionGet();
        assertThat(openIndexResponse.isAcknowledged(), equalTo(true));
        assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
        assertIndexIsOpened("test1", "test2");
    }

    public void testOpenWaitingForActiveShardsFailed() throws Exception {
        Client client = client();
        Settings settings = Settings.builder()
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0)
            .build();
        assertAcked(client.admin().indices().prepareCreate("test").setSettings(settings).get());
        assertAcked(client.admin().indices().prepareClose("test").get());

        OpenIndexResponse response = client.admin().indices().prepareOpen("test").setTimeout("100ms").setWaitForActiveShards(2).get();
        assertThat(response.isShardsAcknowledged(), equalTo(false));
        assertBusy(
            () -> assertThat(
                client.admin().cluster().prepareState().get().getState().metadata().index("test").getState(),
                equalTo(IndexMetadata.State.OPEN)
            )
        );
        ensureGreen("test");
    }

    public void testOpenCloseWithDocs() throws IOException, ExecutionException, InterruptedException {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("test")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));
        ensureGreen();
        int docs = between(10, 100);
        IndexRequestBuilder[] builder = new IndexRequestBuilder[docs];
        for (int i = 0; i < docs; i++) {
            builder[i] = client().prepareIndex("test").setId("" + i).setSource("test", "init");
        }
        indexRandom(true, builder);
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").setForce(true).execute().get();
        }
        client().admin().indices().prepareClose("test").execute().get();

        // check the index still contains the records that we indexed
        client().admin().indices().prepareOpen("test").execute().get();
        ensureGreen();
        SearchResponse searchResponse = client().prepareSearch().setQuery(QueryBuilders.matchQuery("test", "init")).get();
        assertNoFailures(searchResponse);
        assertHitCount(searchResponse, docs);
    }

    public void testOpenCloseIndexWithBlocks() {
        createIndex("test");
        ensureGreen("test");

        int docs = between(10, 100);
        for (int i = 0; i < docs; i++) {
            client().prepareIndex("test").setId("" + i).setSource("test", "init").execute().actionGet();
        }

        for (String blockSetting : Arrays.asList(SETTING_BLOCKS_READ, SETTING_BLOCKS_WRITE)) {
            try {
                enableIndexBlock("test", blockSetting);

                // Closing an index is not blocked
                AcknowledgedResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
                assertAcked(closeIndexResponse);
                assertIndexIsClosed("test");

                // Opening an index is not blocked
                OpenIndexResponse openIndexResponse = client().admin().indices().prepareOpen("test").execute().actionGet();
                assertAcked(openIndexResponse);
                assertThat(openIndexResponse.isShardsAcknowledged(), equalTo(true));
                assertIndexIsOpened("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        // Closing an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareClose("test"));
                assertIndexIsOpened("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }

        AcknowledgedResponse closeIndexResponse = client().admin().indices().prepareClose("test").execute().actionGet();
        assertAcked(closeIndexResponse);
        assertIndexIsClosed("test");

        // Opening an index is blocked
        for (String blockSetting : Arrays.asList(SETTING_READ_ONLY, SETTING_READ_ONLY_ALLOW_DELETE, SETTING_BLOCKS_METADATA)) {
            try {
                enableIndexBlock("test", blockSetting);
                assertBlocked(client().admin().indices().prepareOpen("test"));
                assertIndexIsClosed("test");
            } finally {
                disableIndexBlock("test", blockSetting);
            }
        }
    }

    public void testTranslogStats() throws Exception {
        final String indexName = "test";
        createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

        final int nbDocs = randomIntBetween(0, 50);
        int uncommittedOps = 0;
        for (long i = 0; i < nbDocs; i++) {
            final IndexResponse indexResponse = client().prepareIndex(indexName).setId(Long.toString(i)).setSource("field", i).get();
            assertThat(indexResponse.status(), is(RestStatus.CREATED));

            if (rarely()) {
                client().admin().indices().prepareFlush(indexName).get();
                uncommittedOps = 0;
            } else {
                uncommittedOps += 1;
            }
        }

        final int uncommittedTranslogOps = uncommittedOps;
        assertBusy(() -> {
            IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setTranslog(true).get();
            assertThat(stats.getIndex(indexName), notNullValue());
            assertThat(
                stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(),
                equalTo(uncommittedTranslogOps)
            );
            assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(uncommittedTranslogOps));
        });

        assertAcked(client().admin().indices().prepareClose("test").setWaitForActiveShards(ActiveShardCount.ONE));

        IndicesOptions indicesOptions = IndicesOptions.STRICT_EXPAND_OPEN_CLOSED;
        IndicesStatsResponse stats = client().admin()
            .indices()
            .prepareStats(indexName)
            .setIndicesOptions(indicesOptions)
            .clear()
            .setTranslog(true)
            .get();
        assertThat(stats.getIndex(indexName), notNullValue());
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().estimatedNumberOfOperations(), equalTo(0));
        assertThat(stats.getIndex(indexName).getPrimaries().getTranslog().getUncommittedOperations(), equalTo(0));
    }
}
