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

package org.opensearch.similarity;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimilarityIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public SimilarityIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testCustomBM25Similarity() throws Exception {
        try {
            client().admin().indices().prepareDelete("test").execute().actionGet();
        } catch (Exception e) {
            // ignore
        }

        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("similarity", "custom")
                    .field("type", "text")
                    .endObject()
                    .startObject("field2")
                    .field("similarity", "boolean")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("similarity.custom.type", "BM25")
                    .put("similarity.custom.k1", 2.0f)
                    .put("similarity.custom.b", 0.5f)
            )
            .execute()
            .actionGet();

        client().prepareIndex("test")
            .setId("1")
            .setSource("field1", "the quick brown fox jumped over the lazy dog", "field2", "the quick brown fox jumped over the lazy dog")
            .setRefreshPolicy(IMMEDIATE)
            .execute()
            .actionGet();

        SearchResponse bm25SearchResponse = client().prepareSearch()
            .setQuery(matchQuery("field1", "quick brown fox"))
            .execute()
            .actionGet();
        assertThat(bm25SearchResponse.getHits().getTotalHits().value(), equalTo(1L));
        float bm25Score = bm25SearchResponse.getHits().getHits()[0].getScore();

        SearchResponse booleanSearchResponse = client().prepareSearch()
            .setQuery(matchQuery("field2", "quick brown fox"))
            .execute()
            .actionGet();
        assertThat(booleanSearchResponse.getHits().getTotalHits().value(), equalTo(1L));
        float defaultScore = booleanSearchResponse.getHits().getHits()[0].getScore();

        assertThat(bm25Score, not(equalTo(defaultScore)));
    }
}
