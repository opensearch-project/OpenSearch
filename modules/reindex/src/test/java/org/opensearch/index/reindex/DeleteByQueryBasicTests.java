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

package org.opensearch.index.reindex;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.cluster.ClusterInfoService;
import org.opensearch.cluster.InternalClusterInfoService;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.hasSize;

public class DeleteByQueryBasicTests extends ReindexTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    public void testBasics() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("foo", "a"),
            client().prepareIndex("test").setId("2").setSource("foo", "a"),
            client().prepareIndex("test").setId("3").setSource("foo", "b"),
            client().prepareIndex("test").setId("4").setSource("foo", "c"),
            client().prepareIndex("test").setId("5").setSource("foo", "d"),
            client().prepareIndex("test").setId("6").setSource("foo", "e"),
            client().prepareIndex("test").setId("7").setSource("foo", "f")
        );

        assertHitCount(client().prepareSearch("test").setSize(0).get(), 7);

        // Deletes two docs that matches "foo:a"
        assertThat(deleteByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).get(), matcher().deleted(2));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 5);

        // Deletes the two first docs with limit by size
        DeleteByQueryRequestBuilder request = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).size(2).refresh(true);
        request.source().addSort("foo.keyword", SortOrder.ASC);
        assertThat(request.get(), matcher().deleted(2));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 3);

        // Deletes but match no docs
        assertThat(deleteByQuery().source("test").filter(termQuery("foo", "no_match")).refresh(true).get(), matcher().deleted(0));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 3);

        // Deletes all remaining docs
        assertThat(deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get(), matcher().deleted(3));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 0);
    }

    public void testDeleteByQueryWithOneIndex() throws Exception {
        final long docs = randomIntBetween(1, 50);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test").setId(String.valueOf(i)).setSource("fields1", 1));
        }
        indexRandom(true, true, true, builders);

        assertThat(deleteByQuery().source("t*").filter(QueryBuilders.matchAllQuery()).refresh(true).get(), matcher().deleted(docs));
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 0);
    }

    public void testDeleteByQueryWithMultipleIndices() throws Exception {
        final int indices = randomIntBetween(2, 5);
        final int docs = randomIntBetween(2, 10) * 2;
        long[] candidates = new long[indices];

        // total number of expected deletions
        long deletions = 0;

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < indices; i++) {
            // number of documents to be deleted with the upcoming delete-by-query
            // (this number differs for each index)
            candidates[i] = randomIntBetween(1, docs);
            deletions = deletions + candidates[i];

            for (int j = 0; j < docs; j++) {
                boolean candidate = (j < candidates[i]);
                builders.add(client().prepareIndex("test-" + i).setId(String.valueOf(j)).setSource("candidate", candidate));
            }
        }
        indexRandom(true, true, true, builders);

        // Deletes all the documents with candidate=true
        assertThat(deleteByQuery().source("test-*").filter(termQuery("candidate", true)).refresh(true).get(), matcher().deleted(deletions));

        for (int i = 0; i < indices; i++) {
            long remaining = docs - candidates[i];
            assertHitCount(client().prepareSearch("test-" + i).setSize(0).get(), remaining);
        }

        assertHitCount(client().prepareSearch().setSize(0).get(), (indices * docs) - deletions);
    }

    public void testDeleteByQueryWithMissingIndex() throws Exception {
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("foo", "a"));
        assertHitCount(client().prepareSearch().setSize(0).get(), 1);

        try {
            deleteByQuery().source("missing").filter(QueryBuilders.matchAllQuery()).get();
            fail("should have thrown an exception because of a missing index");
        } catch (IndexNotFoundException e) {
            // Ok
        }
    }

    public void testDeleteByQueryWithRouting() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put("number_of_shards", 2)));
        ensureGreen("test");

        final int docs = randomIntBetween(2, 10);
        logger.info("--> indexing [{}] documents with routing", docs);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test").setId(String.valueOf(i)).setRouting(String.valueOf(i)).setSource("field1", 1));
        }
        indexRandom(true, true, true, builders);

        logger.info("--> counting documents with no routing, should be equal to [{}]", docs);
        assertHitCount(client().prepareSearch().setSize(0).get(), docs);

        String routing = String.valueOf(randomIntBetween(2, docs));

        logger.info("--> counting documents with routing [{}]", routing);
        long expected = client().prepareSearch().setSize(0).setRouting(routing).get().getHits().getTotalHits().value;

        logger.info("--> delete all documents with routing [{}] with a delete-by-query", routing);
        DeleteByQueryRequestBuilder delete = deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery());
        delete.source().setRouting(routing);
        assertThat(delete.refresh(true).get(), matcher().deleted(expected));

        assertHitCount(client().prepareSearch().setSize(0).get(), docs - expected);
    }

    public void testDeleteByMatchQuery() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));

        final int docs = scaledRandomIntBetween(10, 100);

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setRouting(randomAlphaOfLengthBetween(1, 5))
                    .setSource("foo", "bar")
            );
        }
        indexRandom(true, true, true, builders);

        int n = between(0, docs - 1);
        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(matchQuery("_id", Integer.toString(n))).get(), 1);
        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get(), docs);

        DeleteByQueryRequestBuilder delete = deleteByQuery().source("alias").filter(matchQuery("_id", Integer.toString(n)));
        assertThat(delete.refresh(true).get(), matcher().deleted(1L));

        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(QueryBuilders.matchAllQuery()).get(), docs - 1);
    }

    public void testDeleteByQueryWithDateMath() throws Exception {
        indexRandom(true, client().prepareIndex("test").setId("1").setSource("d", "2013-01-01"));

        DeleteByQueryRequestBuilder delete = deleteByQuery().source("test").filter(rangeQuery("d").to("now-1h"));
        assertThat(delete.refresh(true).get(), matcher().deleted(1L));

        assertHitCount(client().prepareSearch("test").setSize(0).get(), 0);
    }

    public void testDeleteByQueryOnReadOnlyIndex() throws Exception {
        createIndex("test");

        final int docs = randomIntBetween(1, 50);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", 1));
        }
        indexRandom(true, true, true, builders);

        try {
            enableIndexBlock("test", SETTING_READ_ONLY);
            assertThat(
                deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get(),
                matcher().deleted(0).failures(docs)
            );
        } finally {
            disableIndexBlock("test", SETTING_READ_ONLY);
        }

        assertHitCount(client().prepareSearch("test").setSize(0).get(), docs);
    }

    public void testDeleteByQueryOnReadOnlyAllowDeleteIndex() throws Exception {
        createIndex("test");

        final int docs = randomIntBetween(1, 50);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docs; i++) {
            builders.add(client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", 1));
        }
        indexRandom(true, true, true, builders);

        // Because the index level read_only_allow_delete block can be automatically released by disk allocation decider,
        // so we should test both case of disk allocation decider is enabled and disabled
        boolean diskAllocationDeciderEnabled = randomBoolean();
        try {
            if (diskAllocationDeciderEnabled == false) {
                // Disable the disk allocation decider to ensure the read_only_allow_delete block cannot be released
                setDiskAllocationDeciderEnabled(false);
            }
            // When a read_only_allow_delete block is set on the index,
            // it will trigger a retry policy in the delete by query request because the rest status of the block is 429
            enableIndexBlock("test", SETTING_READ_ONLY_ALLOW_DELETE);
            if (diskAllocationDeciderEnabled) {
                InternalTestCluster internalTestCluster = internalCluster();
                InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(
                    ClusterInfoService.class,
                    internalTestCluster.getClusterManagerName()
                );
                ThreadPool threadPool = internalTestCluster.getInstance(ThreadPool.class, internalTestCluster.getClusterManagerName());
                // Refresh the cluster info after a random delay to check the disk threshold and release the block on the index
                threadPool.schedule(infoService::refresh, TimeValue.timeValueMillis(randomIntBetween(1, 100)), ThreadPool.Names.MANAGEMENT);
                // The delete by query request will be executed successfully because the block will be released
                assertThat(
                    deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).get(),
                    matcher().deleted(docs)
                );
            } else {
                // The delete by query request will not be executed successfully because the block cannot be released
                assertThat(
                    deleteByQuery().source("test")
                        .filter(QueryBuilders.matchAllQuery())
                        .refresh(true)
                        .setMaxRetries(2)
                        .setRetryBackoffInitialTime(TimeValue.timeValueMillis(50))
                        .get(),
                    matcher().deleted(0).failures(docs)
                );
            }
        } finally {
            disableIndexBlock("test", SETTING_READ_ONLY_ALLOW_DELETE);
            if (diskAllocationDeciderEnabled == false) {
                setDiskAllocationDeciderEnabled(true);
            }
        }
        if (diskAllocationDeciderEnabled) {
            assertHitCount(client().prepareSearch("test").setSize(0).get(), 0);
        } else {
            assertHitCount(client().prepareSearch("test").setSize(0).get(), docs);
        }
    }

    public void testSlices() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("foo", "a"),
            client().prepareIndex("test").setId("2").setSource("foo", "a"),
            client().prepareIndex("test").setId("3").setSource("foo", "b"),
            client().prepareIndex("test").setId("4").setSource("foo", "c"),
            client().prepareIndex("test").setId("5").setSource("foo", "d"),
            client().prepareIndex("test").setId("6").setSource("foo", "e"),
            client().prepareIndex("test").setId("7").setSource("foo", "f")
        );
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 7);

        int slices = randomSlices();
        int expectedSlices = expectedSliceStatuses(slices, "test");

        // Deletes the two docs that matches "foo:a"
        assertThat(
            deleteByQuery().source("test").filter(termQuery("foo", "a")).refresh(true).setSlices(slices).get(),
            matcher().deleted(2).slices(hasSize(expectedSlices))
        );
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 5);

        // Delete remaining docs
        assertThat(
            deleteByQuery().source("test").filter(QueryBuilders.matchAllQuery()).refresh(true).setSlices(slices).get(),
            matcher().deleted(5).slices(hasSize(expectedSlices))
        );
        assertHitCount(client().prepareSearch("test").setSize(0).get(), 0);
    }

    public void testMultipleSources() throws Exception {
        int sourceIndices = between(2, 5);

        Map<String, List<IndexRequestBuilder>> docs = new HashMap<>();
        for (int sourceIndex = 0; sourceIndex < sourceIndices; sourceIndex++) {
            String indexName = "test" + sourceIndex;
            docs.put(indexName, new ArrayList<>());
            int numDocs = between(5, 15);
            for (int i = 0; i < numDocs; i++) {
                docs.get(indexName).add(client().prepareIndex(indexName).setId(Integer.toString(i)).setSource("foo", "a"));
            }
        }

        List<IndexRequestBuilder> allDocs = docs.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
        indexRandom(true, allDocs);
        for (Map.Entry<String, List<IndexRequestBuilder>> entry : docs.entrySet()) {
            assertHitCount(client().prepareSearch(entry.getKey()).setSize(0).get(), entry.getValue().size());
        }

        int slices = randomSlices(1, 10);
        int expectedSlices = expectedSliceStatuses(slices, docs.keySet());

        String[] sourceIndexNames = docs.keySet().toArray(new String[0]);

        assertThat(
            deleteByQuery().source(sourceIndexNames).filter(QueryBuilders.matchAllQuery()).refresh(true).setSlices(slices).get(),
            matcher().deleted(allDocs.size()).slices(hasSize(expectedSlices))
        );

        for (String index : docs.keySet()) {
            assertHitCount(client().prepareSearch(index).setSize(0).get(), 0);
        }

    }

    public void testMissingSources() {
        BulkByScrollResponse response = updateByQuery().source("missing-index-*")
            .refresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .get();
        assertThat(response, matcher().deleted(0).slices(hasSize(0)));
    }

    /** Enables or disables the cluster disk allocation decider **/
    private void setDiskAllocationDeciderEnabled(boolean value) {
        Settings settings = value
            ? Settings.builder().putNull(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey()).build()
            : Settings.builder()
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), value)
                .build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }
}
