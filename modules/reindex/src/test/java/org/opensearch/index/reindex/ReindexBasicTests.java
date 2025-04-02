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

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ReindexBasicTests extends ReindexTestCase {
    public void testFiltering() throws Exception {
        indexRandom(
            true,
            client().prepareIndex("source").setId("1").setSource("foo", "a"),
            client().prepareIndex("source").setId("2").setSource("foo", "a"),
            client().prepareIndex("source").setId("3").setSource("foo", "b"),
            client().prepareIndex("source").setId("4").setSource("foo", "c")
        );
        assertHitCount(client().prepareSearch("source").setSize(0).get(), 4);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        assertThat(copy.get(), matcher().created(4));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), 4);

        // Now none of them
        createIndex("none");
        copy = reindex().source("source").destination("none").filter(termQuery("foo", "no_match")).refresh(true);
        assertThat(copy.get(), matcher().created(0));
        assertHitCount(client().prepareSearch("none").setSize(0).get(), 0);

        // Now half of them
        copy = reindex().source("source").destination("dest_half").filter(termQuery("foo", "a")).refresh(true);
        assertThat(copy.get(), matcher().created(2));
        assertHitCount(client().prepareSearch("dest_half").setSize(0).get(), 2);

        // Limit with maxDocs
        copy = reindex().source("source").destination("dest_size_one").maxDocs(1).refresh(true);
        assertThat(copy.get(), matcher().created(1));
        assertHitCount(client().prepareSearch("dest_size_one").setSize(0).get(), 1);
    }

    public void testCopyMany() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), max);

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        assertThat(copy.get(), matcher().created(max).batches(max, 5));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), max);

        // Copy some of the docs
        int half = max / 2;
        copy = reindex().source("source").destination("dest_half").refresh(true);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        copy.maxDocs(half);
        assertThat(copy.get(), matcher().created(half).batches(half, 5));
        assertHitCount(client().prepareSearch("dest_half").setSize(0).get(), half);
    }

    public void testCopyManyWithSlices() throws Exception {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        int max = between(150, 500);
        for (int i = 0; i < max; i++) {
            docs.add(client().prepareIndex("source").setId(Integer.toString(i)).setSource("foo", "a"));
        }

        indexRandom(true, docs);
        assertHitCount(client().prepareSearch("source").setSize(0).get(), max);

        int slices = randomSlices();
        int expectedSlices = expectedSliceStatuses(slices, "source");

        // Copy all the docs
        ReindexRequestBuilder copy = reindex().source("source").destination("dest").refresh(true).setSlices(slices);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        assertThat(copy.get(), matcher().created(max).batches(greaterThanOrEqualTo(max / 5)).slices(hasSize(expectedSlices)));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), max);

        // Copy some of the docs
        int half = max / 2;
        copy = reindex().source("source").destination("dest_half").refresh(true).setSlices(slices);
        // Use a small batch size so we have to use more than one batch
        copy.source().setSize(5);
        copy.maxDocs(half);
        BulkByScrollResponse response = copy.get();
        assertThat(response, matcher().created(lessThanOrEqualTo((long) half)).slices(hasSize(expectedSlices)));
        assertHitCount(client().prepareSearch("dest_half").setSize(0).get(), response.getCreated());
    }

    public void testMultipleSources() throws Exception {
        int sourceIndices = between(2, 5);

        Map<String, List<IndexRequestBuilder>> docs = new HashMap<>();
        for (int sourceIndex = 0; sourceIndex < sourceIndices; sourceIndex++) {
            String indexName = "source" + sourceIndex;
            String typeName = "test" + sourceIndex;
            docs.put(indexName, new ArrayList<>());
            int numDocs = between(50, 200);
            for (int i = 0; i < numDocs; i++) {
                docs.get(indexName).add(client().prepareIndex(indexName).setId("id_" + sourceIndex + "_" + i).setSource("foo", "a"));
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
        ReindexRequestBuilder request = reindex().source(sourceIndexNames).destination("dest").refresh(true).setSlices(slices);

        BulkByScrollResponse response = request.get();
        assertThat(response, matcher().created(allDocs.size()).slices(hasSize(expectedSlices)));
        assertHitCount(client().prepareSearch("dest").setSize(0).get(), allDocs.size());
    }

    public void testMissingSources() {
        BulkByScrollResponse response = updateByQuery().source("missing-index-*")
            .refresh(true)
            .setSlices(AbstractBulkByScrollRequest.AUTO_SLICES)
            .get();
        assertThat(response, matcher().created(0).slices(hasSize(0)));
    }

    public void testReindexWithDerivedSource() throws Exception {
        // Create source index with _source option set as derived
        String sourceIndexMapping = """
            {
                "settings": {
                    "index": {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                },
                "mappings": {
                    "_doc": {
                        "_source": {
                            "enabled": "derived"
                        },
                        "properties": {
                            "foo": {
                                "type": "keyword",
                                "store": true
                            },
                            "bar": {
                                "type": "integer",
                                "store": true
                            }
                        }
                    }
                }
            }""";

        // Create indices
        assertAcked(prepareCreate("source_index").setSource(sourceIndexMapping, XContentType.JSON));
        assertAcked(prepareCreate("dest_index").setSource(sourceIndexMapping, XContentType.JSON));
        ensureGreen();

        // Index some documents
        int numDocs = randomIntBetween(5, 20);
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(client().prepareIndex("source_index").setId(Integer.toString(i)).setSource("foo", "value_" + i, "bar", i));
        }
        indexRandom(true, docs);

        // Test 1: Basic reindex
        ReindexRequestBuilder copy = reindex().source("source_index").destination("dest_index").refresh(true);

        BulkByScrollResponse response = copy.get();
        assertThat(response, matcher().created(numDocs));
        long expectedCount = client().prepareSearch("dest_index").setQuery(matchAllQuery()).get().getHits().getTotalHits().value();
        assertEquals(numDocs, expectedCount);

        // Test 2: Reindex with query filter
        String destIndexFiltered = "dest_index_filtered";
        assertAcked(prepareCreate(destIndexFiltered).setSource(sourceIndexMapping, XContentType.JSON));

        copy = reindex().source("source_index").destination(destIndexFiltered).filter(termQuery("bar", 1)).refresh(true);

        response = copy.get();
        expectedCount = client().prepareSearch("source_index").setQuery(termQuery("bar", 1)).get().getHits().getTotalHits().value();
        assertThat(response, matcher().created(expectedCount));

        // Test 3: Reindex with slices
        String destIndexSliced = "dest_index_sliced";
        assertAcked(prepareCreate(destIndexSliced).setSource(sourceIndexMapping, XContentType.JSON));

        int slices = randomSlices();
        int expectedSlices = expectedSliceStatuses(slices, "source_index");

        copy = reindex().source("source_index").destination(destIndexSliced).setSlices(slices).refresh(true);

        response = copy.get();
        assertThat(response, matcher().created(numDocs).slices(hasSize(expectedSlices)));

        // Test 4: Reindex with maxDocs
        String destIndexMaxDocs = "dest_index_maxdocs";
        assertAcked(prepareCreate(destIndexMaxDocs).setSource(sourceIndexMapping, XContentType.JSON));

        int maxDocs = numDocs / 2;
        copy = reindex().source("source_index").destination(destIndexMaxDocs).maxDocs(maxDocs).refresh(true);

        response = copy.get();
        assertThat(response, matcher().created(maxDocs));
        expectedCount = client().prepareSearch(destIndexMaxDocs).setQuery(matchAllQuery()).get().getHits().getTotalHits().value();
        assertEquals(maxDocs, expectedCount);

        // Test 5: Multiple source indices
        String sourceIndex2 = "source_index_2";
        assertAcked(prepareCreate(sourceIndex2).setSource(sourceIndexMapping, XContentType.JSON));

        int numDocs2 = randomIntBetween(5, 20);
        List<IndexRequestBuilder> docs2 = new ArrayList<>();
        for (int i = 0; i < numDocs2; i++) {
            docs2.add(
                client().prepareIndex(sourceIndex2).setId(Integer.toString(i + numDocs)).setSource("foo", "value2_" + i, "bar", i + numDocs)
            );
        }
        indexRandom(true, docs2);

        String destIndexMulti = "dest_index_multi";
        assertAcked(prepareCreate(destIndexMulti).setSource(sourceIndexMapping, XContentType.JSON));

        copy = reindex().source("source_index", "source_index_2").destination(destIndexMulti).refresh(true);

        response = copy.get();
        assertThat(response, matcher().created(numDocs + numDocs2));
        expectedCount = client().prepareSearch(destIndexMulti).setQuery(matchAllQuery()).get().getHits().getTotalHits().value();
        assertEquals(numDocs + numDocs2, expectedCount);
    }
}
