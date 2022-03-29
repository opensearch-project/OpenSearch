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

package org.opensearch.index.engine;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.translog.Translog;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.hamcrest.OpenSearchAssertions;

import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class MaxDocsLimitIT extends OpenSearchIntegTestCase {

    private static final AtomicInteger maxDocs = new AtomicInteger();

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> {
                assert maxDocs.get() > 0 : "maxDocs is unset";
                return EngineTestCase.createEngine(config, maxDocs.get());
            });
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestEnginePlugin.class);
        return plugins;
    }

    @Before
    public void setMaxDocs() {
        maxDocs.set(randomIntBetween(10, 100)); // Do not set this too low as we can fail to write the cluster state
        setIndexWriterMaxDocs(maxDocs.get());
    }

    @After
    public void restoreMaxDocs() {
        restoreIndexWriterMaxDocs();
    }

    public void testMaxDocsLimit() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                )
        );
        IndexingResult indexingResult = indexDocs(maxDocs.get(), 1);
        assertThat(indexingResult.numSuccess, equalTo(maxDocs.get()));
        assertThat(indexingResult.numFailures, equalTo(0));
        int rejectedRequests = between(1, 10);
        indexingResult = indexDocs(rejectedRequests, between(1, 8));
        assertThat(indexingResult.numFailures, equalTo(rejectedRequests));
        assertThat(indexingResult.numSuccess, equalTo(0));
        final IllegalArgumentException deleteError = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareDelete("test", "any-id").get()
        );
        assertThat(deleteError.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        OpenSearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
        if (randomBoolean()) {
            client().admin().indices().prepareFlush("test").get();
        }
        internalCluster().fullRestart();
        internalCluster().ensureAtLeastNumDataNodes(2);
        ensureGreen("test");
        searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        OpenSearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) maxDocs.get()));
    }

    public void testMaxDocsLimitConcurrently() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(1);
        assertAcked(
            client().admin().indices().prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
        );
        IndexingResult indexingResult = indexDocs(between(maxDocs.get() + 1, maxDocs.get() * 2), between(2, 8));
        assertThat(indexingResult.numFailures, greaterThan(0));
        assertThat(indexingResult.numSuccess, both(greaterThan(0)).and(lessThanOrEqualTo(maxDocs.get())));
        client().admin().indices().prepareRefresh("test").get();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        OpenSearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) indexingResult.numSuccess));
        int totalSuccess = indexingResult.numSuccess;
        while (totalSuccess < maxDocs.get()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, greaterThan(0));
            totalSuccess += indexingResult.numSuccess;
        }
        if (randomBoolean()) {
            indexingResult = indexDocs(between(1, 10), between(1, 8));
            assertThat(indexingResult.numSuccess, equalTo(0));
        }
        client().admin().indices().prepareRefresh("test").get();
        searchResponse = client().prepareSearch("test")
            .setQuery(new MatchAllQueryBuilder())
            .setTrackTotalHitsUpTo(Integer.MAX_VALUE)
            .setSize(0)
            .get();
        OpenSearchAssertions.assertNoFailures(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) totalSuccess));
    }

    static final class IndexingResult {
        final int numSuccess;
        final int numFailures;

        IndexingResult(int numSuccess, int numFailures) {
            this.numSuccess = numSuccess;
            this.numFailures = numFailures;
        }
    }

    static IndexingResult indexDocs(int numRequests, int numThreads) throws Exception {
        final AtomicInteger completedRequests = new AtomicInteger();
        final AtomicInteger numSuccess = new AtomicInteger();
        final AtomicInteger numFailure = new AtomicInteger();
        Thread[] indexers = new Thread[numThreads];
        Phaser phaser = new Phaser(indexers.length);
        for (int i = 0; i < indexers.length; i++) {
            indexers[i] = new Thread(() -> {
                phaser.arriveAndAwaitAdvance();
                while (completedRequests.incrementAndGet() <= numRequests) {
                    try {
                        final IndexResponse resp = client().prepareIndex("test").setSource("{}", XContentType.JSON).get();
                        numSuccess.incrementAndGet();
                        assertThat(resp.status(), equalTo(RestStatus.CREATED));
                    } catch (IllegalArgumentException e) {
                        numFailure.incrementAndGet();
                        assertThat(e.getMessage(), containsString("Number of documents in the index can't exceed [" + maxDocs.get() + "]"));
                    }
                }
            });
            indexers[i].start();
        }
        for (Thread indexer : indexers) {
            indexer.join();
        }
        internalCluster().assertNoInFlightDocsInEngine();
        return new IndexingResult(numSuccess.get(), numFailure.get());
    }
}
