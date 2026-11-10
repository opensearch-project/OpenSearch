/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.aliases;

import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * A {@code dfs_query_then_fetch} search across a {@code filtered_stats} view and a {@code no_stats} view.
 */
public class FilteredStatisticsDfsMixedShardsIT extends OpenSearchIntegTestCase {

    public void testNoStatsShardDoesNotChangeFilteredStatsScores() throws Exception {
        buildIndex("scoped", 10, "visible", "beta");
        buildIndex("unscored", 300, "other", "beta");
        addAlias("scoped", "scoped_view", "visible", "filtered_stats");
        addAlias("unscored", "unscored_view", "other", "no_stats");

        Map<String, Float> alone = scores(search("scoped_view"), "scoped");
        Map<String, Float> together = scores(search("scoped_view", "unscored_view"), "scoped");

        assertFalse(alone.isEmpty());
        assertEquals(alone.keySet(), together.keySet());
        for (Map.Entry<String, Float> e : alone.entrySet()) {
            assertEquals(
                "score of " + e.getKey() + " must not depend on the no_stats shard",
                e.getValue(),
                together.get(e.getKey()),
                1e-5f
            );
        }
    }

    /** One shard, {@code docs} documents of tenant {@code tenant} that contain {@code term}, plus 50 of another tenant. */
    private void buildIndex(String index, int docs, String tenant, String term) throws Exception {
        assertAcked(
            prepareCreate(index).setMapping("tenant", "type=keyword", "content", "type=text")
                .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
        );
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < docs; i++) {
            bulk.add(client().prepareIndex(index).setId(tenant + i).setSource("tenant", tenant, "content", term + " note " + i));
        }
        for (int i = 0; i < 50; i++) {
            bulk.add(client().prepareIndex(index).setId("rest" + i).setSource("tenant", "rest", "content", term + " " + term + " x" + i));
        }
        assertFalse(bulk.get().hasFailures());
        refresh(index);
    }

    private void addAlias(String index, String alias, String tenant, String scoringMode) {
        AliasActions add = AliasActions.add()
            .index(index)
            .alias(alias)
            .filter(QueryBuilders.termQuery("tenant", tenant))
            .scoringMode(scoringMode);
        assertAcked(client().admin().indices().prepareAliases().addAliasAction(add));
    }

    /** dfs search with a scoring rescore query, so every shard collects statistics for {@code beta}. */
    private SearchResponse search(String... views) {
        SearchResponse resp = client().prepareSearch(views)
            .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
            .setQuery(QueryBuilders.matchQuery("content", "beta note"))
            .addRescorer(new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "beta")).windowSize(500))
            .setSize(500)
            .get();
        assertEquals(0, resp.getFailedShards());
        return resp;
    }

    private static Map<String, Float> scores(SearchResponse resp, String index) {
        Map<String, Float> scores = new HashMap<>();
        for (SearchHit hit : resp.getHits().getHits()) {
            if (hit.getIndex().equals(index)) {
                scores.put(hit.getId(), hit.getScore());
            }
        }
        return scores;
    }
}
