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

package org.opensearch.search.ccs;

import org.opensearch.action.search.SearchPhaseContext;
import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestContext;
import org.opensearch.action.search.SearchRequestOperationsListener;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.test.AbstractMultiClustersTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.Client;
import org.opensearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CrossClusterSearchIT extends AbstractMultiClustersTestCase {

    @Override
    protected Collection<String> remoteClusterAlias() {
        return Collections.singleton("cluster_a");
    }

    @Override
    protected boolean reuseClusters() {
        return false;
    }

    private int indexDocs(Client client, String index) {
        int numDocs = between(1, 10);
        for (int i = 0; i < numDocs; i++) {
            client.prepareIndex(index).setSource("f", "v").get();
        }
        client.admin().indices().prepareRefresh(index).get();
        return numDocs;
    }

    public void testProxyConnectionDisconnect() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        final String remoteNode = cluster("cluster_a").startDataOnlyNode();
        assertAcked(
            client("cluster_a").admin()
                .indices()
                .prepareCreate("prod")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", remoteNode)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .build()
                )
        );
        indexDocs(client("cluster_a"), "prod");
        SearchListenerPlugin.blockQueryPhase();
        try {
            PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
            SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
            searchRequest.allowPartialSearchResults(false);
            searchRequest.setCcsMinimizeRoundtrips(false);
            searchRequest.source(new SearchSourceBuilder().query(new MatchAllQueryBuilder()).size(1000));
            client(LOCAL_CLUSTER).search(searchRequest, future);
            SearchListenerPlugin.waitSearchStarted();
            disconnectFromRemoteClusters();
            assertBusy(() -> assertTrue(future.isDone()));
            configureAndConnectsToRemoteClusters();
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }
    }

    public void testTookCurrentReturnsForRemoteOnlySearch() {
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        indexDocs(client("cluster_a"), "prod");
        String coordinator = randomFrom(cluster(LOCAL_CLUSTER).getNodeNames());
        SearchRequestStats searchRequestStats = cluster(LOCAL_CLUSTER).getInstance(SearchRequestStats.class, coordinator);
        Client client = cluster(LOCAL_CLUSTER).client(coordinator);
        long tookCurrent = searchRequestStats.getTookCurrent();
        long tookTotal = searchRequestStats.getTookTotal();

        // Minimizing roundtrips runs no search phase on the coordinator when every index is remote.
        SearchRequest searchRequest = new SearchRequest("cluster_a:prod");
        searchRequest.setCcsMinimizeRoundtrips(true);
        client.search(searchRequest).actionGet();
        assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
        assertEquals(tookTotal + 1, searchRequestStats.getTookTotal());

        SearchRequest failingRequest = new SearchRequest("cluster_a:does_not_exist");
        failingRequest.setCcsMinimizeRoundtrips(true);
        expectThrows(Exception.class, () -> client.search(failingRequest).actionGet());
        assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
        assertEquals(tookTotal + 1, searchRequestStats.getTookTotal());

        // A skipped cluster turns the same failure into an empty response.
        String skipUnavailable = "cluster.remote.cluster_a.skip_unavailable";
        assertAcked(
            client(LOCAL_CLUSTER).admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().put(skipUnavailable, true))
        );
        try {
            SearchRequest skippedRequest = new SearchRequest("cluster_a:does_not_exist");
            skippedRequest.setCcsMinimizeRoundtrips(true);
            client.search(skippedRequest).actionGet();
            assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
            assertEquals(tookTotal + 2, searchRequestStats.getTookTotal());
        } finally {
            assertAcked(
                client(LOCAL_CLUSTER).admin()
                    .cluster()
                    .prepareUpdateSettings()
                    .setPersistentSettings(Settings.builder().putNull(skipUnavailable))
            );
        }
    }

    public void testTookCurrentCoversTheWholeMixedSearch() {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        indexDocs(client("cluster_a"), "prod");
        String coordinator = randomFrom(cluster(LOCAL_CLUSTER).getNodeNames());
        SearchRequestStats searchRequestStats = cluster(LOCAL_CLUSTER).getInstance(SearchRequestStats.class, coordinator);
        Client client = cluster(LOCAL_CLUSTER).client(coordinator);
        long tookCurrent = searchRequestStats.getTookCurrent();
        long tookTotal = searchRequestStats.getTookTotal();

        SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
        searchRequest.setCcsMinimizeRoundtrips(true);
        client.search(searchRequest).actionGet();
        assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
        assertEquals(tookTotal + 1, searchRequestStats.getTookTotal());

        // The local search succeeds, but the request fails with the remote cluster, so it must not count as a success.
        SearchRequest failingRequest = new SearchRequest("demo", "cluster_a:does_not_exist");
        failingRequest.setCcsMinimizeRoundtrips(true);
        expectThrows(Exception.class, () -> client.search(failingRequest).actionGet());
        assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
        assertEquals(tookTotal + 1, searchRequestStats.getTookTotal());
    }

    public void testTookCurrentCoversTheRemoteClustersOfAMixedSearch() throws Exception {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        indexDocs(client("cluster_a"), "prod");
        String coordinator = randomFrom(cluster(LOCAL_CLUSTER).getNodeNames());
        SearchRequestStats searchRequestStats = cluster(LOCAL_CLUSTER).getInstance(SearchRequestStats.class, coordinator);
        long tookCurrent = searchRequestStats.getTookCurrent();
        long tookTotal = searchRequestStats.getTookTotal();
        long expandTotal = searchRequestStats.getPhaseTotal(SearchPhaseName.EXPAND);

        PlainActionFuture<SearchResponse> future = new PlainActionFuture<>();
        SearchListenerPlugin.blockQueryPhase();
        try {
            SearchRequest searchRequest = new SearchRequest("demo", "cluster_a:prod");
            searchRequest.setCcsMinimizeRoundtrips(true);
            cluster(LOCAL_CLUSTER).client(coordinator).search(searchRequest, future);
            SearchListenerPlugin.waitSearchStarted();
            // The local search ends with its expand phase, while the remote cluster is still blocked.
            assertBusy(() -> assertEquals(expandTotal + 1, searchRequestStats.getPhaseTotal(SearchPhaseName.EXPAND)));
            assertEquals(tookCurrent + 1, searchRequestStats.getTookCurrent());
            assertEquals(tookTotal, searchRequestStats.getTookTotal());
        } finally {
            SearchListenerPlugin.allowQueryPhase();
        }
        future.actionGet();
        assertEquals(tookCurrent, searchRequestStats.getTookCurrent());
        assertEquals(tookTotal + 1, searchRequestStats.getTookTotal());
    }

    public void testRequestEndReachesTwoArgumentListenersWithTheLocalSearch() {
        assertAcked(client(LOCAL_CLUSTER).admin().indices().prepareCreate("demo"));
        indexDocs(client(LOCAL_CLUSTER), "demo");
        assertAcked(client("cluster_a").admin().indices().prepareCreate("prod"));
        indexDocs(client("cluster_a"), "prod");
        Client client = cluster(LOCAL_CLUSTER).client(randomFrom(cluster(LOCAL_CLUSTER).getNodeNames()));

        // No search runs on this node, so listeners of the deprecated two-argument methods hear nothing.
        SearchRequest remoteOnly = new SearchRequest("cluster_a:prod");
        remoteOnly.setCcsMinimizeRoundtrips(true);
        client.search(remoteOnly).actionGet();
        assertEquals(List.of(), TwoArgumentListenerPlugin.EVENTS);

        SearchRequest mixed = new SearchRequest("demo", "cluster_a:prod");
        mixed.setCcsMinimizeRoundtrips(true);
        client.search(mixed).actionGet();
        assertEquals(List.of("end:demo"), TwoArgumentListenerPlugin.EVENTS);

        SearchRequest failingMixed = new SearchRequest("demo", "cluster_a:does_not_exist");
        failingMixed.setCcsMinimizeRoundtrips(true);
        expectThrows(Exception.class, () -> client.search(failingMixed).actionGet());
        assertEquals(List.of("end:demo", "failure:demo"), TwoArgumentListenerPlugin.EVENTS);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        if (clusterAlias.equals(LOCAL_CLUSTER)) {
            final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
            plugins.add(TwoArgumentListenerPlugin.class);
            return plugins;
        } else {
            final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins(clusterAlias));
            plugins.add(SearchListenerPlugin.class);
            return plugins;
        }
    }

    @Before
    public void resetSearchListenerPlugin() throws Exception {
        SearchListenerPlugin.reset();
        TwoArgumentListenerPlugin.EVENTS.clear();
    }

    /**
     * Records the request ends that a listener of the deprecated two-argument methods hears, as query insights does.
     */
    public static class TwoArgumentListenerPlugin extends Plugin {
        static final List<String> EVENTS = new CopyOnWriteArrayList<>();

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier
        ) {
            return List.of(new SearchRequestOperationsListener() {
                @Override
                protected void onRequestEnd(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                    EVENTS.add("end:" + String.join(",", context.getRequest().indices()));
                }

                @Override
                protected void onRequestFailure(SearchPhaseContext context, SearchRequestContext searchRequestContext) {
                    EVENTS.add("failure:" + String.join(",", context.getRequest().indices()));
                }
            });
        }
    }

    public static class SearchListenerPlugin extends Plugin {
        private static final AtomicReference<CountDownLatch> startedLatch = new AtomicReference<>();
        private static final AtomicReference<CountDownLatch> queryLatch = new AtomicReference<>();

        static void reset() {
            startedLatch.set(new CountDownLatch(1));
        }

        static void blockQueryPhase() {
            queryLatch.set(new CountDownLatch(1));
        }

        static void allowQueryPhase() {
            final CountDownLatch latch = queryLatch.get();
            if (latch != null) {
                latch.countDown();
            }
        }

        static void waitSearchStarted() throws InterruptedException {
            assertTrue(startedLatch.get().await(60, TimeUnit.SECONDS));
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext searchContext) {
                    startedLatch.get().countDown();
                    final CountDownLatch latch = queryLatch.get();
                    if (latch != null) {
                        try {
                            assertTrue(latch.await(60, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                    }
                }
            });
            super.onIndexModule(indexModule);
        }
    }
}
