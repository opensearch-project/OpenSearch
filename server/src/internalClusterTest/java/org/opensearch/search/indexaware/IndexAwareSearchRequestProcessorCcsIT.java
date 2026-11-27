/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.AbstractMultiClustersTestCase;

import java.util.Collection;
import java.util.Collections;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Checks, with two real clusters, that {@link IndexAwareSearchRequestProcessor}s do not run for a
 * cross-cluster search, on either side of it.
 * <p>
 * The coordinating node of a cross-cluster search sends the source it holds to the remote cluster, whether
 * or not it minimizes round trips. If its processors had changed that source for the local indices, the
 * remote cluster would receive the changed source: remote shards would execute a query that was decided
 * from local mappings, or the remote coordinating node would process a source that had already been
 * processed. A search with a remote index is therefore not processed at all, and a remote coordinating
 * node does not process the request it is sent.
 * <p>
 * Both clusters run the processor of {@link IndexAwareSearchRequestProcessorIT}, which turns a
 * {@code match} on {@code title} into {@code match_all} for an index that opted in. Each index holds three
 * documents of which one matches "shoes", so an index contributes 1 hit when its part of the search was
 * left alone and 3 when it was rewritten.
 * <p>
 * The clusters are set up by each test rather than in {@code setUp()}: the base class starts them in a
 * {@code @Before} method of its own, which is not guaranteed to run before a subclass's.
 */
public class IndexAwareSearchRequestProcessorCcsIT extends AbstractMultiClustersTestCase {

    private static final String REMOTE_CLUSTER = "cluster_a";
    private static final String LOCAL_INDEX = "local_index";
    private static final String REMOTE_INDEX = "prod";

    @Override
    protected Collection<String> remoteClusterAlias() {
        return Collections.singleton(REMOTE_CLUSTER);
    }

    @Override
    protected boolean reuseClusters() {
        // Fresh clusters per test, so no test has to undo the settings and indices of another.
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins(String clusterAlias) {
        return Collections.singletonList(IndexAwareSearchRequestProcessorIT.TestIndexAwareSearchRequestProcessorPlugin.class);
    }

    // The local index opted in, so a local-only search for "shoes" would return 3 hits. With a remote
    // index in the request nothing is processed: 1 local hit and 1 remote hit. If the local processors
    // had run, the rewritten source would have reached the remote cluster too, for 6 hits.
    public void testSearchOverLocalAndRemoteIndicesIsNotProcessedWhenMinimizingRoundtrips() throws Exception {
        setUpClustersAndIndices(false);

        SearchResponse response = client(LOCAL_CLUSTER).prepareSearch(LOCAL_INDEX, REMOTE_CLUSTER + ":" + REMOTE_INDEX)
            .setQuery(QueryBuilders.matchQuery("title", "shoes"))
            .get();

        assertHitCount(response, 2);
    }

    // The same search with ccs_minimize_roundtrips off, where the local coordinating node sends its source
    // directly to the remote shards.
    public void testSearchOverLocalAndRemoteIndicesIsNotProcessedWithoutMinimizingRoundtrips() throws Exception {
        setUpClustersAndIndices(false);
        SearchRequest request = new SearchRequest(LOCAL_INDEX, REMOTE_CLUSTER + ":" + REMOTE_INDEX).source(
            new SearchSourceBuilder().query(QueryBuilders.matchQuery("title", "shoes"))
        );
        request.setCcsMinimizeRoundtrips(false);

        SearchResponse response = client(LOCAL_CLUSTER).search(request).actionGet();

        assertHitCount(response, 2);
    }

    // A search of the remote index only. The baseline for the next test: the remote index did not opt in,
    // so 1 hit says nothing yet about whether the remote cluster's processors ran.
    public void testRemoteOnlySearchOfAnIndexThatDidNotOptInReturnsTheLiteralMatches() throws Exception {
        setUpClustersAndIndices(false);

        SearchResponse response = client(LOCAL_CLUSTER).prepareSearch(REMOTE_CLUSTER + ":" + REMOTE_INDEX)
            .setQuery(QueryBuilders.matchQuery("title", "shoes"))
            .get();

        assertHitCount(response, 1);
    }

    // The remote index opted in this time. The remote coordinating node receives a request that, seen from
    // its own cluster, has no remote index, and its processors are enabled. It must still leave the request
    // alone, because the request was forwarded as part of a cross-cluster search: 1 hit, not 3.
    public void testRemoteCoordinatingNodeDoesNotProcessAForwardedRequest() throws Exception {
        setUpClustersAndIndices(true);

        SearchResponse response = client(LOCAL_CLUSTER).prepareSearch(REMOTE_CLUSTER + ":" + REMOTE_INDEX)
            .setQuery(QueryBuilders.matchQuery("title", "shoes"))
            .get();

        assertHitCount(response, 1);
    }

    // The control for the tests above: on the same clusters, a search of the local index alone is
    // processed. Without it, "not rewritten" could also mean that the processors never run here at all.
    public void testLocalOnlySearchIsStillProcessed() throws Exception {
        setUpClustersAndIndices(false);

        SearchResponse response = client(LOCAL_CLUSTER).prepareSearch(LOCAL_INDEX)
            .setQuery(QueryBuilders.matchQuery("title", "shoes"))
            .get();

        assertHitCount(response, 3);
    }

    /** Creates an opted-in local index, a remote index that opts in or not, and enables processors on both clusters. */
    private void setUpClustersAndIndices(boolean remoteOptIn) throws Exception {
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(LOCAL_CLUSTER), LOCAL_INDEX, true);
        IndexAwareSearchRequestProcessorIT.createIndexWithTitleField(client(REMOTE_CLUSTER), REMOTE_INDEX, remoteOptIn);
        IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(LOCAL_CLUSTER), true);
        IndexAwareSearchRequestProcessorIT.setProcessorsEnabled(client(REMOTE_CLUSTER), true);
    }
}
