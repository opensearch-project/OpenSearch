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

package org.opensearch.index.search;

import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.search.MatchQuery.ZeroTermsQuery;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.opensearch.index.query.QueryBuilders.matchPhraseQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class MatchPhraseQueryIT extends OpenSearchIntegTestCase {
    private static final String INDEX = "test";

    @Before
    public void setUp() throws Exception {
        super.setUp();
        CreateIndexRequestBuilder createIndexRequest = prepareCreate(INDEX).setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.analysis.analyzer.standard_stopwords.type", "standard")
                .putList("index.analysis.analyzer.standard_stopwords.stopwords", "of", "the", "who"));
        assertAcked(createIndexRequest);
        ensureGreen();
    }

    public void testZeroTermsQuery() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> indexRequests = getIndexRequests();
        indexRandom(true, false, indexRequests);

        MatchPhraseQueryBuilder baseQuery = matchPhraseQuery("name", "the who")
            .analyzer("standard_stopwords");

        MatchPhraseQueryBuilder matchNoneQuery = baseQuery.zeroTermsQuery(ZeroTermsQuery.NONE);
        SearchResponse matchNoneResponse = client().prepareSearch(INDEX).setQuery(matchNoneQuery).get();
        assertHitCount(matchNoneResponse, 0L);

        MatchPhraseQueryBuilder matchAllQuery = baseQuery.zeroTermsQuery(ZeroTermsQuery.ALL);
        SearchResponse matchAllResponse = client().prepareSearch(INDEX).setQuery(matchAllQuery).get();
        assertHitCount(matchAllResponse, 2L);
    }

    private List<IndexRequestBuilder> getIndexRequests() {
        List<IndexRequestBuilder> requests = new ArrayList<>();
        requests.add(client().prepareIndex(INDEX, "band").setSource("name", "the beatles"));
        requests.add(client().prepareIndex(INDEX, "band").setSource("name", "led zeppelin"));
        return requests;
    }
}
