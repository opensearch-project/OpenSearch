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

package org.opensearch.document;

import org.opensearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest.RefreshPolicy;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import java.io.IOException;

import static org.opensearch.action.DocWriteRequest.OpType;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.transport.client.Requests.clearIndicesCacheRequest;
import static org.opensearch.transport.client.Requests.getRequest;
import static org.opensearch.transport.client.Requests.indexRequest;
import static org.opensearch.transport.client.Requests.refreshRequest;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Integration test for document action like index, bulk, and get. It has a very long history: it was in the second commit of Elasticsearch.
 */
public class DocumentActionsIT extends OpenSearchIntegTestCase {
    protected void createIndex() {
        OpenSearchAssertions.assertAcked(prepareCreate(getConcreteIndexName()).setMapping("name", "type=keyword,store=true"));
    }

    protected String getConcreteIndexName() {
        return "test";
    }

    public void testIndexActions() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("Running Cluster Health");
        ensureGreen();
        logger.info("Indexing [type1/1]");
        IndexResponse indexResponse = client().prepareIndex()
            .setIndex("test")
            .setId("1")
            .setSource(source("1", "test"))
            .setRefreshPolicy(RefreshPolicy.IMMEDIATE)
            .get();
        assertThat(indexResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(indexResponse.getId(), equalTo("1"));
        logger.info("Refreshing");
        RefreshResponse refreshResponse = refresh();
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        logger.info("--> index exists?");
        assertThat(indexExists(getConcreteIndexName()), equalTo(true));
        logger.info("--> index exists?, fake index");
        assertThat(indexExists("test1234565"), equalTo(false));

        logger.info("Clearing cache");
        ClearIndicesCacheResponse clearIndicesCacheResponse = client().admin()
            .indices()
            .clearCache(clearIndicesCacheRequest("test").fieldDataCache(true).queryCache(true))
            .actionGet();
        assertNoFailures(clearIndicesCacheResponse);
        assertThat(clearIndicesCacheResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        logger.info("Force Merging");
        waitForRelocation(ClusterHealthStatus.GREEN);
        ForceMergeResponse mergeResponse = forceMerge();
        assertThat(mergeResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        GetResponse getResult;

        logger.info("Get [type1/1]");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "1").execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").toString()));
            assertThat("cycle(map) #" + i, (String) getResult.getSourceAsMap().get("name"), equalTo("test"));
            getResult = client().get(getRequest("test").id("1")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").toString()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Get [type1/1] with script");
        for (int i = 0; i < 5; i++) {
            getResult = client().prepareGet("test", "1").setStoredFields("name").execute().actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat(getResult.isExists(), equalTo(true));
            assertThat(getResult.getSourceAsBytes(), nullValue());
            assertThat(getResult.getField("name").getValues().get(0).toString(), equalTo("test"));
        }

        logger.info("Get [type1/2] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").id("2")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Delete [type1/1]");
        DeleteResponse deleteResponse = client().prepareDelete("test", "1").execute().actionGet();
        assertThat(deleteResponse.getIndex(), equalTo(getConcreteIndexName()));
        assertThat(deleteResponse.getId(), equalTo("1"));
        logger.info("Refreshing");
        client().admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] (should be empty)");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").id("1")).actionGet();
            assertThat(getResult.isExists(), equalTo(false));
        }

        logger.info("Index [type1/1]");
        client().index(indexRequest("test").id("1").source(source("1", "test"))).actionGet();
        logger.info("Index [type1/2]");
        client().index(indexRequest("test").id("2").source(source("2", "test2"))).actionGet();

        logger.info("Flushing");
        FlushResponse flushResult = client().admin().indices().prepareFlush("test").execute().actionGet();
        assertThat(flushResult.getSuccessfulShards(), equalTo(numShards.totalNumShards));
        assertThat(flushResult.getFailedShards(), equalTo(0));
        logger.info("Refreshing");
        client().admin().indices().refresh(refreshRequest("test")).actionGet();

        logger.info("Get [type1/1] and [type1/2]");
        for (int i = 0; i < 5; i++) {
            getResult = client().get(getRequest("test").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("1", "test").toString()));
            getResult = client().get(getRequest("test").id("2")).actionGet();
            String ste1 = getResult.getSourceAsString();
            String ste2 = source("2", "test2").toString();
            assertThat("cycle #" + i, ste1, equalTo(ste2));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }

        logger.info("Count");
        // check count
        for (int i = 0; i < 5; i++) {
            // test successful
            SearchResponse countResponse = client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).execute().actionGet();
            assertNoFailures(countResponse);
            assertThat(countResponse.getHits().getTotalHits().value(), equalTo(2L));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));

            // count with no query is a match all one
            countResponse = client().prepareSearch("test").setSize(0).execute().actionGet();
            assertThat(
                "Failures " + countResponse.getShardFailures(),
                countResponse.getShardFailures() == null ? 0 : countResponse.getShardFailures().length,
                equalTo(0)
            );
            assertThat(countResponse.getHits().getTotalHits().value(), equalTo(2L));
            assertThat(countResponse.getSuccessfulShards(), equalTo(numShards.numPrimaries));
            assertThat(countResponse.getFailedShards(), equalTo(0));
        }
    }

    public void testBulk() throws Exception {
        createIndex();
        NumShards numShards = getNumShards(getConcreteIndexName());
        logger.info("-> running Cluster Health");
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
            .add(client().prepareIndex().setIndex("test").setId("1").setSource(source("1", "test")))
            .add(client().prepareIndex().setIndex("test").setId("2").setSource(source("2", "test")).setCreate(true))
            .add(client().prepareIndex().setIndex("test").setSource(source("3", "test")))
            .add(client().prepareIndex().setIndex("test").setCreate(true).setSource(source("4", "test")))
            .add(client().prepareDelete().setIndex("test").setId("1"))
            .add(client().prepareIndex().setIndex("test").setSource("{ xxx }", MediaTypeRegistry.JSON)) // failure
            .execute()
            .actionGet();

        assertThat(bulkResponse.hasFailures(), equalTo(true));
        assertThat(bulkResponse.getItems().length, equalTo(6));

        assertThat(bulkResponse.getItems()[0].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[0].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[0].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[0].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[1].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[1].getOpType(), equalTo(OpType.CREATE));
        assertThat(bulkResponse.getItems()[1].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[1].getId(), equalTo("2"));

        assertThat(bulkResponse.getItems()[2].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[2].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[2].getIndex(), equalTo(getConcreteIndexName()));
        String generatedId3 = bulkResponse.getItems()[2].getId();

        assertThat(bulkResponse.getItems()[3].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[3].getOpType(), equalTo(OpType.CREATE));
        assertThat(bulkResponse.getItems()[3].getIndex(), equalTo(getConcreteIndexName()));
        String generatedId4 = bulkResponse.getItems()[3].getId();

        assertThat(bulkResponse.getItems()[4].isFailed(), equalTo(false));
        assertThat(bulkResponse.getItems()[4].getOpType(), equalTo(OpType.DELETE));
        assertThat(bulkResponse.getItems()[4].getIndex(), equalTo(getConcreteIndexName()));
        assertThat(bulkResponse.getItems()[4].getId(), equalTo("1"));

        assertThat(bulkResponse.getItems()[5].isFailed(), equalTo(true));
        assertThat(bulkResponse.getItems()[5].getOpType(), equalTo(OpType.INDEX));
        assertThat(bulkResponse.getItems()[5].getIndex(), equalTo(getConcreteIndexName()));

        waitForRelocation(ClusterHealthStatus.GREEN);
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh("test").execute().actionGet();
        assertNoFailures(refreshResponse);
        assertThat(refreshResponse.getSuccessfulShards(), equalTo(numShards.totalNumShards));

        for (int i = 0; i < 5; i++) {
            GetResponse getResult = client().get(getRequest("test").id("1")).actionGet();
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
            assertThat("cycle #" + i, getResult.isExists(), equalTo(false));

            getResult = client().get(getRequest("test").id("2")).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("2", "test").toString()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));

            getResult = client().get(getRequest("test").id(generatedId3)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("3", "test").toString()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));

            getResult = client().get(getRequest("test").id(generatedId4)).actionGet();
            assertThat("cycle #" + i, getResult.getSourceAsString(), equalTo(source("4", "test").toString()));
            assertThat(getResult.getIndex(), equalTo(getConcreteIndexName()));
        }
    }

    private XContentBuilder source(String id, String nameValue) throws IOException {
        return XContentFactory.jsonBuilder().startObject().field("id", id).field("name", nameValue).endObject();
    }
}
