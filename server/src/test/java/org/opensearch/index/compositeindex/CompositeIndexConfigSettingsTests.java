/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;

/**
 * Composite index config settings unit tests
 */
public class CompositeIndexConfigSettingsTests extends OpenSearchTestCase {
    private static IndexSettings indexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public void testDefaultSettings() {
        Settings settings = Settings.EMPTY;
        IndexSettings indexSettings = indexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertFalse(compositeIndexConfig.hasCompositeFields());
    }

    public void testMinimumMetrics() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertEquals("metrics is required for composite index field [my_field]", exception.getMessage());
    }

    public void testMinimumDimensions() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "invalid")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertEquals("Atleast two dimensions are required to build composite index field [my_field]", exception.getMessage());
    }

    public void testInvalidDimensionType() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "invalid")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertEquals("Invalid dimension type in composite index config: [invalid] ", exception.getMessage());
    }

    public void testInvalidIndexMode() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .put("index.composite_index.config.my_field.index_mode", "invalid")
            .build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> indexSettings(settings));
        assertEquals("Invalid index mode in composite index config: [invalid] ", exception.getMessage());
    }

    public void testValidCompositeIndexConfig() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
    }

    public void testCompositeIndexMultipleFields() {
        Settings settings = Settings.builder()
            .put("indices.composite_index.max_fields", 2)
            .putList("index.composite_index.config.field1.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.field1.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.field1.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.field1.metrics", Arrays.asList("metric1"))
            .putList("index.composite_index.config.field2.dimensions_order", Arrays.asList("dim3", "dim4"))
            .put("index.composite_index.config.field2.dimensions_config.dim3.field", "dim3_field")
            .put("index.composite_index.config.field2.dimensions_config.dim4.field", "dim4_field")
            .putList("index.composite_index.config.field2.metrics", Arrays.asList("metric2"))
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(2, compositeIndexConfig.getCompositeFields().size());
    }

    public void testCompositeIndexDateIntervalsSetting() {
        Settings settings = Settings.builder()
            .putList("indices.composite_index.field.default.date_intervals", Arrays.asList("day", "week"))
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "date")
            .putList("index.composite_index.config.my_field.dimensions_config.dim1.calendar_interval", Arrays.asList("day", "week"))
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        CompositeField compositeField = compositeIndexConfig.getCompositeFields().get(0);
        List<Rounding.DateTimeUnit> expectedIntervals = Arrays.asList(
            Rounding.DateTimeUnit.DAY_OF_MONTH,
            Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR
        );
        assertEquals(expectedIntervals, ((DateDimension) compositeField.getDimensionsOrder().get(0)).getIntervals());
    }

    public void testCompositeIndexMetricsSetting() {
        Settings settings = Settings.builder()
            .putList("indices.composite_index.field.default.metrics", Arrays.asList("count", "max"))
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .putList("index.composite_index.config.my_field.metrics_config.metric1.metrics", Arrays.asList("count", "max"))
            .build();
        IndexSettings indexSettings = indexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        CompositeField compositeField = compositeIndexConfig.getCompositeFields().get(0);
        List<MetricType> expectedMetrics = Arrays.asList(MetricType.COUNT, MetricType.MAX);
        assertEquals(expectedMetrics, compositeField.getMetrics().get(0).getMetrics());
    }

    public void testValidateWithoutCompositeSettingEnabled() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "default")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.LONG));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("metric2", new NumberFieldMapper.NumberFieldType("metric2", NumberFieldMapper.NumberType.LONG));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(createIndexSettings(settings));
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        CompositeIndexSettings compositeIndexSettings = new CompositeIndexSettings(clusterSettings);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, compositeIndexSettings)
        );
        assertEquals(
            "composite index cannot be created, enable it using [indices.composite_index.enabled] setting",
            exception.getMessage()
        );

        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
    }

    public void testEnabledWithFFOff() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(testWithEnabledSettings(settings));
        Settings settings1 = Settings.builder().put(settings).put("indices.composite_index.enabled", true).build();
        ClusterSettings clusterSettings = new ClusterSettings(settings1, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexSettings(clusterSettings)
        );
        assertEquals(
            "star tree index is under an experimental feature and can be activated only by enabling opensearch.experimental.feature.composite_index.enabled feature flag in the JVM options",
            exception.getMessage()
        );
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        CompositeField compositeField = compositeIndexConfig.getCompositeFields().get(0);
        assertTrue(compositeField.getDimensionsOrder().get(0) instanceof Dimension);
        assertTrue(compositeField.getDimensionsOrder().get(1) instanceof DateDimension);
    }

    private IndexSettings createIndexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    public IndexSettings testWithEnabledSettings(Settings settings) {
        Settings settings1 = Settings.builder().put(settings).put("indices.composite_index.enabled", true).build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", settings1), Settings.EMPTY);
        return indexSettings;
    }

}
