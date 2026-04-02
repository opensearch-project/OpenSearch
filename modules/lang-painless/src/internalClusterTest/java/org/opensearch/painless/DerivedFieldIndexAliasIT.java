/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * Integration tests to verify that derived fields are correctly resolved
 * during the can_match phase when querying across multiple indices via an alias.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class DerivedFieldIndexAliasIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    private void setupIndex(String indexName) {
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );

        client().prepareIndex(indexName)
            .setId("1")
            .setSource("{\"field\":\"value1\", \"num\": 20}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex(indexName)
            .setId("2")
            .setSource("{\"field\":\"value2\", \"num\": 30}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureSearchable(indexName);
    }

    @Override
    public void setupSuiteScopeCluster() {
        setupIndex("test-index-1");
        setupIndex("test-index-2");
        assertAcked(
            client().admin().indices().prepareAliases().addAlias(new String[] { "test-index-1", "test-index-2" }, "alias-test").get()
        );
    }

    public void testIndexAliasSearch() {
        SearchRequest searchRequest = new SearchRequest("alias-test").source(
            SearchSourceBuilder.searchSource()
                .derivedField("derived_num", "long", new Script("emit(doc['num'].value)"))
                .query(QueryBuilders.rangeQuery("derived_num").gte(15).lte(25))
        );
        // Setting pre_filter_shard_size to 1 forces the can_match phase.
        searchRequest.setPreFilterShardSize(1);

        SearchResponse response = client().search(searchRequest).actionGet();
        assertSearchResponse(response);

        // We expect 2 hits (doc 1 from each index has num=20).
        assertEquals(2, response.getHits().getTotalHits().value());
    }

    public void testIndexAliasAggregation() {
        SearchRequest searchRequest = new SearchRequest("alias-test").source(
            SearchSourceBuilder.searchSource()
                .derivedField("derived_num", "long", new Script("emit(doc['num'].value)"))
                .query(QueryBuilders.matchAllQuery())
                .aggregation(AggregationBuilders.terms("derived_terms").field("derived_num"))
        );
        // Setting pre_filter_shard_size to 1 forces the can_match phase.
        searchRequest.setPreFilterShardSize(1);

        SearchResponse response = client().search(searchRequest).actionGet();
        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("derived_terms");
        assertEquals(2, terms.getBuckets().size());

        Terms.Bucket bucket1 = terms.getBuckets().get(0);
        assertEquals(20L, bucket1.getKeyAsNumber().longValue());
        assertEquals(2, bucket1.getDocCount());

        Terms.Bucket bucket2 = terms.getBuckets().get(1);
        assertEquals(30L, bucket2.getKeyAsNumber().longValue());
        assertEquals(2, bucket2.getDocCount());
    }
}
