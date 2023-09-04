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

package org.opensearch.action.admin;

import org.apache.lucene.util.Constants;
import org.opensearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequestBuilder;
import org.opensearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsEmptyCollection.empty;

public class HotThreadsIT extends OpenSearchIntegTestCase {

    public void testHotThreadsDontFail() throws ExecutionException, InterruptedException {
        /**
         * This test just checks if nothing crashes or gets stuck etc.
         */
        createIndex("test");
        final int iters = scaledRandomIntBetween(2, 20);
        final AtomicBoolean hasErrors = new AtomicBoolean(false);
        for (int i = 0; i < iters; i++) {
            final String type;
            NodesHotThreadsRequestBuilder nodesHotThreadsRequestBuilder = client().admin().cluster().prepareNodesHotThreads();
            if (randomBoolean()) {
                TimeValue timeValue = new TimeValue(rarely() ? randomIntBetween(500, 5000) : randomIntBetween(20, 500));
                nodesHotThreadsRequestBuilder.setInterval(timeValue);
            }
            if (randomBoolean()) {
                nodesHotThreadsRequestBuilder.setThreads(rarely() ? randomIntBetween(500, 5000) : randomIntBetween(1, 500));
            }
            nodesHotThreadsRequestBuilder.setIgnoreIdleThreads(randomBoolean());
            if (randomBoolean()) {
                switch (randomIntBetween(0, 2)) {
                    case 2:
                        type = "cpu";
                        break;
                    case 1:
                        type = "wait";
                        break;
                    default:
                        type = "block";
                        break;
                }
                assertThat(type, notNullValue());
                nodesHotThreadsRequestBuilder.setType(type);
            } else {
                type = null;
            }
            final CountDownLatch latch = new CountDownLatch(1);
            nodesHotThreadsRequestBuilder.execute(new ActionListener<NodesHotThreadsResponse>() {
                @Override
                public void onResponse(NodesHotThreadsResponse nodeHotThreads) {
                    boolean success = false;
                    try {
                        assertThat(nodeHotThreads, notNullValue());
                        Map<String, NodeHotThreads> nodesMap = nodeHotThreads.getNodesMap();
                        assertThat(nodeHotThreads.failures(), empty());
                        assertThat(nodesMap.size(), equalTo(cluster().size()));
                        for (NodeHotThreads ht : nodeHotThreads.getNodes()) {
                            assertNotNull(ht.getHotThreads());
                            // logger.info(ht.getHotThreads());
                        }
                        success = true;
                    } finally {
                        if (!success) {
                            hasErrors.set(true);
                        }
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("FAILED", e);
                    hasErrors.set(true);
                    latch.countDown();
                    fail();
                }
            });

            indexRandom(
                true,
                client().prepareIndex("test").setId("1").setSource("field1", "value1"),
                client().prepareIndex("test").setId("2").setSource("field1", "value2"),
                client().prepareIndex("test").setId("3").setSource("field1", "value3")
            );
            ensureSearchable();
            while (latch.getCount() > 0) {
                assertHitCount(
                    client().prepareSearch()
                        .setQuery(matchAllQuery())
                        .setPostFilter(
                            boolQuery().must(matchAllQuery())
                                .mustNot(boolQuery().must(termQuery("field1", "value1")).must(termQuery("field1", "value2")))
                        )
                        .get(),
                    3L
                );
            }
            latch.await();
            assertThat(hasErrors.get(), is(false));
        }
    }

    public void testIgnoreIdleThreads() throws ExecutionException, InterruptedException {
        assumeTrue("no support for hot_threads on FreeBSD", Constants.FREE_BSD == false);

        // First time, don't ignore idle threads:
        NodesHotThreadsRequestBuilder builder = client().admin().cluster().prepareNodesHotThreads();
        builder.setIgnoreIdleThreads(false);
        builder.setThreads(Integer.MAX_VALUE);
        NodesHotThreadsResponse response = builder.execute().get();

        final Matcher<String> containsCachedTimeThreadRunMethod = containsString(
            "org.opensearch.threadpool.ThreadPool$CachedTimeThread.run"
        );

        int totSizeAll = 0;
        for (NodeHotThreads node : response.getNodesMap().values()) {
            totSizeAll += node.getHotThreads().length();
            assertThat(node.getHotThreads(), containsCachedTimeThreadRunMethod);
        }

        // Second time, do ignore idle threads:
        builder = client().admin().cluster().prepareNodesHotThreads();
        builder.setThreads(Integer.MAX_VALUE);

        // Make sure default is true:
        assertEquals(true, builder.request().ignoreIdleThreads());
        response = builder.execute().get();

        int totSizeIgnoreIdle = 0;
        for (NodeHotThreads node : response.getNodesMap().values()) {
            totSizeIgnoreIdle += node.getHotThreads().length();
            assertThat(node.getHotThreads(), not(containsCachedTimeThreadRunMethod));
        }

        // The filtered stacks should be smaller than unfiltered ones:
        assertThat(totSizeIgnoreIdle, lessThan(totSizeAll));
    }

    public void testTimestampAndParams() throws ExecutionException, InterruptedException {

        NodesHotThreadsResponse response = client().admin().cluster().prepareNodesHotThreads().execute().get();

        if (Constants.FREE_BSD) {
            for (NodeHotThreads node : response.getNodesMap().values()) {
                String result = node.getHotThreads();
                assertTrue(result.indexOf("hot_threads is not supported") != -1);
            }
        } else {
            for (NodeHotThreads node : response.getNodesMap().values()) {
                String result = node.getHotThreads();
                assertTrue(result.indexOf("Hot threads at") != -1);
                assertTrue(result.indexOf("interval=500ms") != -1);
                assertTrue(result.indexOf("busiestThreads=3") != -1);
                assertTrue(result.indexOf("ignoreIdleThreads=true") != -1);
            }
        }
    }
}
