/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * End-to-end search-flow tests for the {@code low_cardinality} mapping parameter.
 *
 * <p>{@code low_cardinality: true} suppresses Lucene indexing for a keyword field (sets {@code index: false}) and
 * relies on the Parquet column (plus its bloom filter) to serve lookups. These tests verify that, despite Lucene
 * indexing being disabled, the field can still be <em>filtered on</em> and <em>projected</em> through the analytics
 * (DataFusion) query path — i.e. search works end to end against the Parquet-backed column.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class LowCardinalitySearchIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "lc_search_idx";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /**
     * Full search flow: the {@code low_cardinality} keyword field has Lucene indexing disabled, yet a
     * {@code WHERE city = ...} filter and a projection of {@code city} both resolve correctly through the
     * Parquet-backed query path.
     */
    public void testSearchOnLowCardinalityField() {
        createAndSeedLowCardinalityIndex();

        Map<String, Object> city = fieldMapping("city");
        assertEquals(Boolean.TRUE, city.get("low_cardinality"));
        assertEquals("Lucene indexing must be disabled for a low_cardinality field", Boolean.FALSE, city.get("index"));

        SqlPlanRunner runner = sqlPlanRunner();

        List<Integer> parisVals = intColumn(runner.executeSql("SELECT val FROM " + INDEX + " WHERE city = 'paris'"));
        java.util.Collections.sort(parisVals);
        assertEquals("filter on low_cardinality field must return the matching rows", List.of(1, 2, 5), parisVals);

        assertTrue(
            "filter with no matches must return zero rows",
            runner.executeSql("SELECT val FROM " + INDEX + " WHERE city = 'berlin'").isEmpty()
        );

        List<Object[]> londonRows = runner.executeSql("SELECT city FROM " + INDEX + " WHERE val = 3");
        assertEquals(1, londonRows.size());
        assertEquals("london", asString(londonRows.getFirst()[0]));

        List<Object[]> countRows = runner.executeSql("SELECT COUNT(*) FROM " + INDEX);
        assertEquals(1, countRows.size());
        assertEquals(5L, ((Number) countRows.getFirst()[0]).longValue());
    }

    // --- Infrastructure ---

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createAndSeedLowCardinalityIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("city", "type=keyword,low_cardinality=true", "val", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        // paris -> {1,2,5}, london -> {3}, tokyo -> {4}
        Object[][] docs = { { "paris", 1 }, { "paris", 2 }, { "london", 3 }, { "tokyo", 4 }, { "paris", 5 } };
        for (Object[] doc : docs) {
            client().prepareIndex(INDEX).setSource("city", doc[0], "val", doc[1]).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }

    private List<Integer> intColumn(List<Object[]> rows) {
        List<Integer> out = new ArrayList<>();
        for (Object[] row : rows) {
            out.add(((Number) row[0]).intValue());
        }
        return out;
    }

    private static String asString(Object value) {
        return value == null ? null : value.toString();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> fieldMapping(String field) {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(INDEX).get();
        MappingMetadata mappingMetadata = response.getMappings().get(INDEX);
        assertNotNull("mapping metadata must exist for " + INDEX, mappingMetadata);
        Map<String, Object> properties = (Map<String, Object>) mappingMetadata.getSourceAsMap().get("properties");
        assertNotNull("mapping must contain properties", properties);
        Map<String, Object> mapping = (Map<String, Object>) properties.get(field);
        assertNotNull("field [" + field + "] must be present in mapping", mapping);
        return mapping;
    }
}
