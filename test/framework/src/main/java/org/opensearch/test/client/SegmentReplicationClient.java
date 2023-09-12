/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.client;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.client.Client;
import org.opensearch.client.FilterClient;
import org.opensearch.cluster.routing.Preference;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.unit.TimeValue;
import org.junit.Assert;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.OpenSearchTestCase.assertBusy;
import static org.junit.Assert.assertTrue;

/** A {@link Client} that randomizes request parameters. */
public class SegmentReplicationClient extends FilterClient {

    private final SearchType defaultSearchType;
    private final String defaultPreference;
    private final int batchedReduceSize;
    private final int maxConcurrentShardRequests;
    private final int preFilterShardSize;
    private final boolean doTimeout;

    public SegmentReplicationClient(Client client, Random random) {
        super(client);
        defaultSearchType = RandomPicks.randomFrom(random, Arrays.asList(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH));
        if (random.nextInt(10) == 0) {
            defaultPreference = RandomPicks.randomFrom(random, EnumSet.of(Preference.PRIMARY_FIRST, Preference.LOCAL)).type();
        } else if (random.nextInt(10) == 0) {
            String s = TestUtil.randomRealisticUnicodeString(random, 1, 10);
            defaultPreference = s.startsWith("_") ? null : s; // '_' is a reserved character
        } else {
            defaultPreference = null;
        }
        this.batchedReduceSize = 2 + random.nextInt(10);
        if (random.nextBoolean()) {
            this.maxConcurrentShardRequests = 1 + random.nextInt(1 << random.nextInt(8));
        } else {
            this.maxConcurrentShardRequests = -1; // randomly use the default
        }
        if (random.nextBoolean()) {
            preFilterShardSize = 1 + random.nextInt(1 << random.nextInt(7));
        } else {
            preFilterShardSize = -1;
        }
        doTimeout = random.nextBoolean();
    }

    @Override
    public SearchRequestBuilder prepareSearch(String... indices) {
        try {
            String[] indexes = this.admin().indices().prepareGetIndex().get().indices();
            // wait until replica shard is caught up before performing search request.
            assertBusy(() -> {
                for (String index : indexes) {
                    final IndicesStatsResponse indicesStatsResponse = this.admin().indices().prepareStats(index).execute().actionGet();

                    assertTrue(indicesStatsResponse.getIndex(index).getTotal().getSegments().getReplicationStats().maxBytesBehind == 0);
                }
            }, 30, TimeUnit.SECONDS);
        } catch (Exception e) {
            Assert.fail("failed to wait until replica shard caught up" + e);
        }
        SearchRequestBuilder searchRequestBuilder = in.prepareSearch(indices)
            .setSearchType(defaultSearchType)
            .setPreference(defaultPreference)
            .setBatchedReduceSize(batchedReduceSize);
        if (maxConcurrentShardRequests != -1) {
            searchRequestBuilder.setMaxConcurrentShardRequests(maxConcurrentShardRequests);
        }
        if (preFilterShardSize != -1) {
            searchRequestBuilder.setPreFilterShardSize(preFilterShardSize);
        }
        if (doTimeout) {
            searchRequestBuilder.setTimeout(new TimeValue(1, TimeUnit.DAYS));
        }
        return searchRequestBuilder;
    }

    @Override
    public ActionFuture<SearchResponse> search(SearchRequest request) {
        String[] indexes;
        if (request.indices().length == 0) {
            indexes = this.admin().indices().prepareGetIndex().get().indices();
            ;
        } else {
            indexes = request.indices();
        }
        try {
            // wait until replica shard is caught up before performing search request.
            assertBusy(() -> {
                for (String index : indexes) {
                    final IndicesStatsResponse indicesStatsResponse = this.admin().indices().prepareStats(index).execute().actionGet();

                    assertTrue(indicesStatsResponse.getIndex(index).getTotal().getSegments().getReplicationStats().maxBytesBehind == 0);
                }
            });
        } catch (Exception e) {
            Assert.fail("failed to wait until replica shard caught up");
        }
        request.preference(defaultPreference);
        return execute(SearchAction.INSTANCE, request);
    }

    @Override
    public String toString() {
        return "randomized(" + super.toString() + ")";
    }

    public Client in() {
        return super.in();
    }

}
