/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.FeatureFlagSetter;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CompositeIndexConfigIntegTests extends OpenSearchIntegTestCase {

    private static final XContentBuilder TEST_MAPPING = createTestMapping();

    private static XContentBuilder createTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.COMPOSITE_INDEX, "true").build();
    }

    @Before
    public final void setupNodeSettings() {
        Settings request = Settings.builder().put(IndicesService.COMPOSITE_INDEX_ENABLED_SETTING.getKey(), true).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
    }

    @After
    public final void cleanupNodeSettings() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }

    public void testInvalidCompositeIndex() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "1")
                    .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric"))
                    .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric_dv"))
            ).setMapping(TEST_MAPPING).get()
        );
        assertEquals("specify field type [date] for dimension field [timestamp] as part of of composite field [my_field]", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "1")
                    .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric"))
                    .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric_dv"))
                    .put("index.composite_index.config.my_field.dimensions_config.timestamp.type", "date")
            ).setMapping(TEST_MAPPING).get()
        );
        assertEquals(
            "Aggregations not supported for the dimension field [numeric] with field type [integer] as part of composite field [my_field]",
            ex.getMessage()
        );

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "1")
                    .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric_dv"))
                    .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric"))
                    .put("index.composite_index.config.my_field.dimensions_config.timestamp.type", "date")
            ).setMapping(TEST_MAPPING).get()
        );
        assertEquals(
            "Aggregations not supported for the composite index metric field [numeric] with field type [integer] as part of composite field [my_field]",
            ex.getMessage()
        );

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "1")
                    .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("invalid", "numeric_dv"))
                    .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric_dv"))
            ).setMapping(TEST_MAPPING).get()
        );
        assertEquals("unknown dimension field [invalid] as part of composite field [my_field]", ex.getMessage());

        ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "1")
                    .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric_dv"))
                    .putList("index.composite_index.config.my_field.metrics", Arrays.asList("invalid"))
                    .put("index.composite_index.config.my_field.dimensions_config.timestamp.type", "date")
            ).setMapping(TEST_MAPPING).get()
        );
        assertEquals("unknown metric field [invalid] as part of composite field [my_field]", ex.getMessage());

        FeatureFlagSetter.set(FeatureFlags.COMPOSITE_INDEX);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "2")
                .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric_dv"))
                .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric_dv"))
                .put("index.composite_index.config.my_field.dimensions_config.timestamp.type", "date")
        ).setMapping(TEST_MAPPING).get();
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("test");
        GetSettingsResponse indexSettings = client().admin().indices().getSettings(getSettingsRequest).actionGet();
        indexSettings.getIndexToSettings().get("test");
        final Index index = resolveIndex("test");
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                CompositeIndexConfig compositeIndexConfig = indexService.getIndexSettings().getCompositeIndexConfig();
                assertTrue(compositeIndexConfig.hasCompositeFields());
                assertEquals(1, compositeIndexConfig.getCompositeFields().size());
                assertEquals("timestamp", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0).getField());
                assertEquals("numeric_dv", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(1).getField());
                assertTrue(compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0) instanceof DateDimension);
            }
        }
    }

    public void testValidCompositeIndex() {
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "2")
                .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("timestamp", "numeric_dv"))
                .putList("index.composite_index.config.my_field.metrics", Arrays.asList("numeric_dv"))
                .put("index.composite_index.config.my_field.dimensions_config.timestamp.type", "date")
        ).setMapping(TEST_MAPPING).get();
        GetSettingsRequest getSettingsRequest = new GetSettingsRequest().indices("test");
        GetSettingsResponse indexSettings = client().admin().indices().getSettings(getSettingsRequest).actionGet();
        indexSettings.getIndexToSettings().get("test");
        final Index index = resolveIndex("test");
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                CompositeIndexConfig compositeIndexConfig = indexService.getIndexSettings().getCompositeIndexConfig();
                assertTrue(compositeIndexConfig.hasCompositeFields());
                assertEquals(1, compositeIndexConfig.getCompositeFields().size());
                assertEquals("timestamp", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0).getField());
                assertEquals("numeric_dv", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(1).getField());
                assertTrue(compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0) instanceof DateDimension);
                List<MetricType> expectedMetrics = Arrays.asList(
                    MetricType.AVG,
                    MetricType.COUNT,
                    MetricType.SUM,
                    MetricType.MAX,
                    MetricType.MIN
                );
                assertEquals(expectedMetrics, compositeIndexConfig.getCompositeFields().get(0).getMetrics().get(0).getMetrics());
                List<Rounding.DateTimeUnit> expectedIntervals = Arrays.asList(
                    Rounding.DateTimeUnit.MINUTES_OF_HOUR,
                    Rounding.DateTimeUnit.HOUR_OF_DAY
                );
                assertEquals(
                    expectedIntervals,
                    ((DateDimension) compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0)).getIntervals()
                );
            }
        }

    }

}
