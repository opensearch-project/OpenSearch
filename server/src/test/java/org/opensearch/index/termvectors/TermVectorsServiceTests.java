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

package org.opensearch.index.termvectors;

import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.termvectors.TermVectorsRequest;
import org.opensearch.action.termvectors.TermVectorsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Math.abs;
import static java.util.stream.Collectors.toList;
import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class TermVectorsServiceTests extends OpenSearchSingleNodeTestCase {

    public void testTook() throws Exception {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject();
        createIndex("test", Settings.EMPTY, mapping);
        ensureGreen();

        client().prepareIndex("test").setId("0").setSource("field", "foo bar").setRefreshPolicy(IMMEDIATE).get();

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());

        List<Long> longs = Stream.of(abs(randomLong()), abs(randomLong())).sorted().collect(toList());

        TermVectorsRequest request = new TermVectorsRequest("test", "0");
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request, longs.iterator()::next);

        assertThat(response, notNullValue());
        assertThat(response.getTook().getMillis(), equalTo(TimeUnit.NANOSECONDS.toMillis(longs.get(1) - longs.get(0))));
    }

    public void testDocFreqs() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder().put("number_of_shards", 1).build();
        createIndex("test", settings, mapping);
        ensureGreen();

        int max = between(3, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < max; i++) {
            bulk.add(
                client().prepareIndex("test").setId(Integer.toString(i)).setSource("text", "the quick brown fox jumped over the lazy dog")
            );
        }
        bulk.get();

        TermVectorsRequest request = new TermVectorsRequest("test", "0").termStatistics(true);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request);
        assertEquals(1, response.getFields().size());

        Terms terms = response.getFields().terms("text");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            assertEquals(max, iterator.docFreq());
        }
    }

    public void testWithIndexedPhrases() throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("text")
            .field("type", "text")
            .field("index_phrases", true)
            .field("term_vector", "with_positions_offsets_payloads")
            .endObject()
            .endObject()
            .endObject();
        Settings settings = Settings.builder().put("number_of_shards", 1).build();
        createIndex("test", settings, mapping);
        ensureGreen();

        int max = between(3, 10);
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < max; i++) {
            bulk.add(
                client().prepareIndex("test").setId(Integer.toString(i)).setSource("text", "the quick brown fox jumped over the lazy dog")
            );
        }
        bulk.get();

        TermVectorsRequest request = new TermVectorsRequest("test", "0").termStatistics(true);

        IndicesService indicesService = getInstanceFromNode(IndicesService.class);
        IndexService test = indicesService.indexService(resolveIndex("test"));
        IndexShard shard = test.getShardOrNull(0);
        assertThat(shard, notNullValue());
        TermVectorsResponse response = TermVectorsService.getTermVectors(shard, request);
        assertEquals(2, response.getFields().size());

        Terms terms = response.getFields().terms("text");
        TermsEnum iterator = terms.iterator();
        while (iterator.next() != null) {
            assertEquals(max, iterator.docFreq());
        }

        Terms phrases = response.getFields().terms("text._index_phrase");
        TermsEnum phraseIterator = phrases.iterator();
        while (phraseIterator.next() != null) {
            assertEquals(max, phraseIterator.docFreq());
        }
    }
}
