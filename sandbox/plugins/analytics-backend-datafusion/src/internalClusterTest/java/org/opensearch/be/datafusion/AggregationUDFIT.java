/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end test for PPL -> Datafusion UDAFs.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class AggregationUDFIT extends OpenSearchIntegTestCase {

    private static final String BANK_INDEX = "bank";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // Plugins with no extendedPlugins requirement go here. Plugins that need
        // explicit extendedPlugins (so SPI ExtensionLoader walks the right parent
        // classloader) are declared in additionalNodePlugins() below.
        return List.of(TestPPLPlugin.class, FlightStreamPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        // OpenSearchIntegTestCase's nodePlugins() builds PluginInfo with empty
        // extendedPlugins, which breaks ExtensiblePlugin.loadExtensions(...) for
        // plugins like DataFusionPlugin that ride on AnalyticsPlugin's SPI. Use
        // additionalNodePlugins() to declare the parent relationships explicitly.
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
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
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createBankIndex();
        indexBankDocs();
        ensureGreen(BANK_INDEX);
        refresh(BANK_INDEX);
    }

    public void testTake() throws Exception {
        PPLRequest request = new PPLRequest("source=" + BANK_INDEX + " | stats take(firstname, 2) as take");
        PPLResponse response = client().execute(UnifiedPPLExecuteAction.INSTANCE, request).actionGet();

        assertNotNull("PPLResponse must not be null", response);
        assertEquals("schema columns", List.of("take"), response.getColumns());
        assertEquals("scalar agg → exactly 1 result row", 1, response.getRows().size());

        Object cell = response.getRows().get(0)[0];
        assertNotNull("take cell must not be null", cell);
        assertTrue("take cell must materialize as a List, got " + cell.getClass(), cell instanceof List);
        @SuppressWarnings("unchecked")
        List<Object> taken = (List<Object>) cell;
        assertEquals("take(firstname, 2) over the bank fixture", List.of("Amber JOHnny", "Hattie"), taken);
    }

    private void createBankIndex() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("account_number")
            .field("type", "long")
            .endObject()
            .startObject("firstname")
            .field("type", "keyword")
            .endObject()
            .startObject("lastname")
            .field("type", "keyword")
            .endObject()
            .startObject("balance")
            .field("type", "long")
            .endObject()
            .startObject("age")
            .field("type", "integer")
            .endObject()
            .startObject("gender")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject();

        // Parquet-backed composite index — DataFusion only reads parquet, so the take
        // UDAF will only fire if the data lands in a parquet shard. Mirrors the
        // settings used by CoordinatorReduceIT on df_reduce.
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
            .prepareCreate(BANK_INDEX)
            .setSettings(indexSettings)
            .setMapping(mapping)
            .get();
        assertTrue("bank index creation must be acknowledged", response.isAcknowledged());
    }

    private void indexBankDocs() {
        // First 5 docs of sql/integ-test/src/test/resources/bank.json — preserves the
        // ordering the SQL plugin's testTake assumes (Amber JOHnny, Hattie are #0 and #1).
        client().prepareIndex(BANK_INDEX)
            .setId("1")
            .setSource(
                "account_number",
                1,
                "firstname",
                "Amber JOHnny",
                "lastname",
                "Duke Willmington",
                "balance",
                39225L,
                "age",
                32,
                "gender",
                "M"
            )
            .get();
        client().prepareIndex(BANK_INDEX)
            .setId("6")
            .setSource("account_number", 6, "firstname", "Hattie", "lastname", "Bond", "balance", 5686L, "age", 36, "gender", "M")
            .get();
        client().prepareIndex(BANK_INDEX)
            .setId("13")
            .setSource("account_number", 13, "firstname", "Nanette", "lastname", "Bates", "balance", 32838L, "age", 28, "gender", "F")
            .get();
        client().prepareIndex(BANK_INDEX)
            .setId("18")
            .setSource("account_number", 18, "firstname", "Dale", "lastname", "Adams", "balance", 4180L, "age", 33, "gender", "M")
            .get();
        client().prepareIndex(BANK_INDEX)
            .setId("20")
            .setSource("account_number", 20, "firstname", "Elinor", "lastname", "Ratliff", "balance", 16418L, "age", 36, "gender", "M")
            .get();
    }
}
