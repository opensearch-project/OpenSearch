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

package org.opensearch.action;

import org.opensearch.ExceptionsHelper;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;

import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2)
public class RejectionActionIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("thread_pool.search.size", 1)
            .put("thread_pool.search.queue_size", 1)
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .put("thread_pool.get.size", 1)
            .put("thread_pool.get.queue_size", 1)
            .build();
    }

    public void testSimulatedSearchRejectionLoad() throws Throwable {
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "1").get();
        }

        int numberOfAsyncOps = randomIntBetween(200, 700);
        final CountDownLatch latch = new CountDownLatch(numberOfAsyncOps);
        final CopyOnWriteArrayList<Object> responses = new CopyOnWriteArrayList<>();
        for (int i = 0; i < numberOfAsyncOps; i++) {
            client().prepareSearch("test")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(QueryBuilders.matchQuery("field", "1"))
                .execute(new LatchedActionListener<>(new ActionListener<SearchResponse>() {
                    @Override
                    public void onResponse(SearchResponse searchResponse) {
                        responses.add(searchResponse);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        responses.add(e);
                    }
                }, latch));
        }
        latch.await();

        // validate all responses
        for (Object response : responses) {
            if (response instanceof SearchResponse) {
                SearchResponse searchResponse = (SearchResponse) response;
                for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                    assertThat(
                        failure.reason().toLowerCase(Locale.ENGLISH),
                        anyOf(containsString("cancelled"), containsString("rejected"))
                    );
                }
            } else {
                Exception t = (Exception) response;
                Throwable unwrap = ExceptionsHelper.unwrapCause(t);
                if (unwrap instanceof SearchPhaseExecutionException) {
                    SearchPhaseExecutionException e = (SearchPhaseExecutionException) unwrap;
                    for (ShardSearchFailure failure : e.shardFailures()) {
                        assertThat(
                            failure.reason().toLowerCase(Locale.ENGLISH),
                            anyOf(containsString("cancelled"), containsString("rejected"))
                        );
                    }
                } else if ((unwrap instanceof OpenSearchRejectedExecutionException) == false) {
                    throw new AssertionError("unexpected failure", (Throwable) response);
                }
            }
        }
        assertThat(responses.size(), equalTo(numberOfAsyncOps));
    }
}
