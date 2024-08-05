/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.composite.InternalComposite;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class SimplePainlessIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public SimplePainlessIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_SETTING.getKey(), "4")
            .build();
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .field("dynamic", "false")
            .startObject("_meta")
            .field("schema_version", 5)
            .endObject()
            .startObject("properties")
            .startObject("entity")
            .field("type", "nested")
            .endObject()
            .endObject()
            .endObject();

        assertAcked(
            prepareCreate("test").setMapping(xContentBuilder)
                .setSettings(
                    Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
        );

        assertAcked(
            prepareCreate("test-df").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            )
        );

        client().prepareIndex("test")
            .setId("a")
            .setSource(
                "{\"entity\":[{\"name\":\"ip-field\",\"value\":\"1.2.3.4\"},{\"name\":\"keyword-field\",\"value\":\"field-1\"}]}",
                MediaTypeRegistry.JSON
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test")
            .setId("b")
            .setSource(
                "{\"entity\":[{\"name\":\"ip-field\",\"value\":\"5.6.7.8\"},{\"name\":\"keyword-field\",\"value\":\"field-2\"}]}",
                MediaTypeRegistry.JSON
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test")
            .setId("c")
            .setSource(
                "{\"entity\":[{\"name\":\"ip-field\",\"value\":\"1.6.3.8\"},{\"name\":\"keyword-field\",\"value\":\"field-2\"}]}",
                MediaTypeRegistry.JSON
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test")
            .setId("d")
            .setSource(
                "{\"entity\":[{\"name\":\"ip-field\",\"value\":\"2.6.4.8\"},{\"name\":\"keyword-field\",\"value\":\"field-2\"}]}",
                MediaTypeRegistry.JSON
            )
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureSearchable("test");

        client().prepareIndex("test-df")
            .setId("a")
            .setSource("{\"field\":\"value1\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test-df")
            .setId("b")
            .setSource("{\"field\":\"value2\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test-df")
            .setId("c")
            .setSource("{\"field\":\"value3\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        client().prepareIndex("test-df")
            .setId("d")
            .setSource("{\"field\":\"value1\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureSearchable("test");
    }

    public void testTermsValuesSource() throws Exception {
        AggregationBuilder agg = AggregationBuilders.composite(
            "multi_buckets",
            Collections.singletonList(
                new TermsValuesSourceBuilder("keyword-field").script(
                    new Script(
                        ScriptType.INLINE,
                        "painless",
                        "String value = null; if (params == null || params._source == null || params._source.entity == null) { return \"\"; } for (item in params._source.entity) { if (item[\"name\"] == \"keyword-field\") { value = item['value']; break; } } return value;",
                        Collections.emptyMap()
                    )
                )
            )
        );
        SearchResponse response = client().prepareSearch("test").setQuery(matchAllQuery()).addAggregation(agg).get();

        assertSearchResponse(response);
        assertEquals(2, ((InternalComposite) response.getAggregations().get("multi_buckets")).getBuckets().size());
        assertEquals(
            "field-1",
            ((InternalComposite) response.getAggregations().get("multi_buckets")).getBuckets().get(0).getKey().get("keyword-field")
        );
        assertEquals(1, ((InternalComposite) response.getAggregations().get("multi_buckets")).getBuckets().get(0).getDocCount());
        assertEquals(
            "field-2",
            ((InternalComposite) response.getAggregations().get("multi_buckets")).getBuckets().get(1).getKey().get("keyword-field")
        );
        assertEquals(3, ((InternalComposite) response.getAggregations().get("multi_buckets")).getBuckets().get(1).getDocCount());
    }

    public void testSimpleDerivedFieldsQuery() {
        assumeFalse(
            "Derived fields do not support concurrent search https://github.com/opensearch-project/OpenSearch/issues/15007",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
        SearchRequest searchRequest = new SearchRequest("test-df").source(
            SearchSourceBuilder.searchSource()
                .derivedField("result", "keyword", new Script("emit(params._source[\"field\"])"))
                .fetchField("result")
                .query(new TermsQueryBuilder("result", "value1"))
        );
        SearchResponse response = client().search(searchRequest).actionGet();
        assertSearchResponse(response);
        assertEquals(2, Objects.requireNonNull(response.getHits().getTotalHits()).value);
    }

    public void testSimpleDerivedFieldsAgg() {
        assumeFalse(
            "Derived fields do not support concurrent search https://github.com/opensearch-project/OpenSearch/issues/15007",
            internalCluster().clusterService().getClusterSettings().get(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING)
        );
        SearchRequest searchRequest = new SearchRequest("test-df").source(
            SearchSourceBuilder.searchSource()
                .derivedField("result", "keyword", new Script("emit(params._source[\"field\"])"))
                .fetchField("result")
                .aggregation(new TermsAggregationBuilder("derived-agg").field("result"))
        );
        SearchResponse response = client().search(searchRequest).actionGet();
        assertSearchResponse(response);
        Terms aggResponse = response.getAggregations().get("derived-agg");
        assertEquals(3, aggResponse.getBuckets().size());
        Terms.Bucket bucket = aggResponse.getBuckets().get(0);
        assertEquals("value1", bucket.getKey());
        assertEquals(2, bucket.getDocCount());
        bucket = aggResponse.getBuckets().get(1);
        assertEquals("value2", bucket.getKey());
        assertEquals(1, bucket.getDocCount());
        bucket = aggResponse.getBuckets().get(2);
        assertEquals("value3", bucket.getKey());
        assertEquals(1, bucket.getDocCount());
    }
}
