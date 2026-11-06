/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Aggregations through a filtered alias on an index with a star-tree.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE)
public class StarTreeAliasFilterIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "sales";

    public void testAggregationsThroughFilteredAliasApplyTheFilter() throws Exception {
        createIndex(
            INDEX,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.composite_index", true)
                .put("index.append_only.enabled", true)
                .put("index.search.star_tree_index.enabled", true)
                .put("index.codec", "default")
                .build(),
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("composite")
                .startObject("by_product")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "product")
                .endObject()
                .startObject()
                .field("name", "region")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "sales")
                .field("stats", new String[] { "sum" })
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("product")
                .field("type", "keyword")
                .endObject()
                .startObject("region")
                .field("type", "keyword")
                .endObject()
                .startObject("sales")
                .field("type", "double")
                .endObject()
                .endObject()
                .endObject()
                .toString()
        );
        ensureGreen(INDEX);
        client().prepareIndex(INDEX).setSource("product", "laptop", "region", "eu", "sales", 1200.0).get();
        client().prepareIndex(INDEX).setSource("product", "keyboard", "region", "eu", "sales", 150.0).get();
        client().prepareIndex(INDEX).setSource("product", "mouse", "region", "us", "sales", 50.0).get();
        client().admin().indices().forceMerge(new ForceMergeRequest(INDEX).maxNumSegments(1)).get();
        refresh(INDEX);
        assertAcked(client().admin().indices().prepareAliases().addAlias(INDEX, "laptops", termQuery("product", "laptop")));

        // Control: on the index itself the star-tree answers with all three documents.
        SearchResponse all = client().prepareSearch(INDEX)
            .setQuery(matchAllQuery())
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("sales"))
            .get();
        assertEquals(1400.0, ((Sum) all.getAggregations().get("total")).getValue(), 0.0);

        SearchResponse viaAlias = client().prepareSearch("laptops")
            .setQuery(matchAllQuery())
            .setSize(0)
            .addAggregation(AggregationBuilders.sum("total").field("sales"))
            .addAggregation(AggregationBuilders.terms("products").field("product"))
            .get();
        assertEquals(1, viaAlias.getHits().getTotalHits().value());
        assertEquals(1200.0, ((Sum) viaAlias.getAggregations().get("total")).getValue(), 0.0);
        Terms products = viaAlias.getAggregations().get("products");
        assertEquals(1, products.getBuckets().size());
        assertEquals("laptop", products.getBuckets().get(0).getKeyAsString());
    }
}
