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

package org.opensearch.action.search;

import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.OpenSearchClient;

import org.mockito.Mockito;

import static org.hamcrest.CoreMatchers.equalTo;

public class SearchRequestBuilderTests extends OpenSearchTestCase {

    private SearchRequestBuilder createBuilder() {
        OpenSearchClient client = Mockito.mock(OpenSearchClient.class);
        return new SearchRequestBuilder(client, SearchAction.INSTANCE);
    }

    public void testEmptySourceToString() {
        SearchRequestBuilder searchRequestBuilder = createBuilder();
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().toString()));
    }

    public void testQueryBuilderQueryToString() {
        SearchRequestBuilder searchRequestBuilder = createBuilder();
        searchRequestBuilder.setQuery(QueryBuilders.matchAllQuery());
        assertThat(searchRequestBuilder.toString(), equalTo(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).toString()));
    }

    public void testSearchSourceBuilderToString() {
        SearchRequestBuilder searchRequestBuilder = createBuilder();
        searchRequestBuilder.setSource(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")));
        assertThat(
            searchRequestBuilder.toString(),
            equalTo(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).toString())
        );
    }

    public void testThatToStringDoesntWipeRequestSource() {
        SearchRequestBuilder searchRequestBuilder = createBuilder().setSource(
            new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value"))
        );
        String preToString = searchRequestBuilder.request().toString();
        assertThat(
            searchRequestBuilder.toString(),
            equalTo(new SearchSourceBuilder().query(QueryBuilders.termQuery("field", "value")).toString())
        );
        String postToString = searchRequestBuilder.request().toString();
        assertThat(preToString, equalTo(postToString));
    }
}
