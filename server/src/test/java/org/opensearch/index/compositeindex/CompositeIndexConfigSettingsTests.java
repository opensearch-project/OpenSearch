/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static org.opensearch.common.util.FeatureFlags.COMPOSITE_INDEX;
import static org.opensearch.index.IndexSettingsTests.newIndexMeta;

/**
 * Composite index config settings unit tests
 */
public class CompositeIndexConfigSettingsTests extends OpenSearchTestCase {

    public void testDefaultSettings() {
        Settings settings = Settings.EMPTY;
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertFalse(compositeIndexConfig.hasCompositeFields());
    }

    public void testMinimumMetrics() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .build();
        ;
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getEnabledIndexSettings(settings));
        assertEquals("metrics is required for composite index field [my_field]", exception.getMessage());

        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testMinimumDimensions() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "invalid")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getEnabledIndexSettings(settings));
        assertEquals("Atleast two dimensions are required to build composite index field [my_field]", exception.getMessage());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testInvalidDimensionType() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "invalid")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getEnabledIndexSettings(settings));
        assertEquals("Invalid dimension type in composite index config: [invalid] ", exception.getMessage());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testInvalidIndexMode() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .put("index.composite_index.config.my_field.index_mode", "invalid")
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getEnabledIndexSettings(settings));
        assertEquals("Invalid index mode in composite index config: [invalid] ", exception.getMessage());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testDefaultNumberofCompositeFieldsValidation() {
        Settings settings = Settings.builder()
            .put("index.composite_index.max_fields", 1)
            .putList("index.composite_index.config.field1.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.field1.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.field1.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.field1.metrics", Arrays.asList("metric1"))
            .putList("index.composite_index.config.field2.dimensions_order", Arrays.asList("dim3", "dim4"))
            .put("index.composite_index.config.field2.dimensions_config.dim3.field", "dim3_field")
            .put("index.composite_index.config.field2.dimensions_config.dim4.field", "dim4_field")
            .putList("index.composite_index.config.field2.metrics", Arrays.asList("metric2"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> getEnabledIndexSettings(settings));
        assertEquals("composite index can have atmost [1] fields", exception.getMessage());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testValidCompositeIndexConfig() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .put("index.composite_index.config.my_field.index_mode", "startree")
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IndexSettings indexSettings = getEnabledIndexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        assertEquals("dim1_field", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0).getField());
        assertEquals("dim2_field", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(1).getField());
        assertTrue(compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(1) instanceof DateDimension);
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testCompositeIndexMultipleFields() {
        Settings settings = Settings.builder()
            .put("index.composite_index.max_fields", 2)
            .putList("index.composite_index.config.field1.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.field1.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.field1.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.field1.metrics", Arrays.asList("metric1"))
            .putList("index.composite_index.config.field2.dimensions_order", Arrays.asList("dim3", "dim4"))
            .put("index.composite_index.config.field2.dimensions_config.dim3.field", "dim3_field")
            .put("index.composite_index.config.field2.dimensions_config.dim4.field", "dim4_field")
            .putList("index.composite_index.config.field2.metrics", Arrays.asList("metric2"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        Exception ex = expectThrows(IllegalArgumentException.class, () -> getEnabledAndMultiFieldIndexSettings(settings));
        assertEquals("Failed to parse value [2] for setting [index.composite_index.max_fields] must be <= 1", ex.getMessage());
        /**
         * // uncomment once we add support for multiple fields
        IndexSettings indexSettings = getEnabledAndMultiFieldIndexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(2, compositeIndexConfig.getCompositeFields().size());
        assertEquals(2, compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().size());
        assertEquals(2, compositeIndexConfig.getCompositeFields().get(1).getDimensionsOrder().size());
        assertEquals(1, compositeIndexConfig.getCompositeFields().get(0).getMetrics().size());
        assertEquals(1, compositeIndexConfig.getCompositeFields().get(1).getMetrics().size());
        assertEquals("dim1_field", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(0).getField());
        assertEquals("dim2_field", compositeIndexConfig.getCompositeFields().get(0).getDimensionsOrder().get(1).getField());
        assertEquals("dim3_field", compositeIndexConfig.getCompositeFields().get(1).getDimensionsOrder().get(0).getField());
        assertEquals("dim4_field", compositeIndexConfig.getCompositeFields().get(1).getDimensionsOrder().get(1).getField());
        assertEquals("metric1", compositeIndexConfig.getCompositeFields().get(0).getMetrics().get(0).getField());
        assertEquals("metric2", compositeIndexConfig.getCompositeFields().get(1).getMetrics().get(0).getField());
         **/
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testCompositeIndexDateIntervalsSetting() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.field.default.date_intervals", Arrays.asList("day", "week"))
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "date")
            .putList("index.composite_index.config.my_field.dimensions_config.dim1.calendar_interval", Arrays.asList("day", "week"))
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IndexSettings indexSettings = getEnabledIndexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        CompositeField compositeField = compositeIndexConfig.getCompositeFields().get(0);
        List<Rounding.DateTimeUnit> expectedIntervals = Arrays.asList(
            Rounding.DateTimeUnit.DAY_OF_MONTH,
            Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR
        );
        assertEquals(expectedIntervals, ((DateDimension) compositeField.getDimensionsOrder().get(0)).getIntervals());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testCompositeIndexMetricsSetting() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.field.default.metrics", Arrays.asList("count", "max"))
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1"))
            .putList("index.composite_index.config.my_field.metrics_config.metric1.metrics", Arrays.asList("count", "max"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        IndexSettings indexSettings = getEnabledIndexSettings(settings);
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(indexSettings);
        assertTrue(compositeIndexConfig.hasCompositeFields());
        assertEquals(1, compositeIndexConfig.getCompositeFields().size());
        CompositeField compositeField = compositeIndexConfig.getCompositeFields().get(0);
        List<MetricType> expectedMetrics = Arrays.asList(MetricType.COUNT, MetricType.MAX);
        assertEquals(expectedMetrics, compositeField.getMetrics().get(0).getMetrics());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testCompositeIndexEnabledSetting() {
        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim1.type", "default")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));
        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.LONG));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("metric2", new NumberFieldMapper.NumberFieldType("metric2", NumberFieldMapper.NumberType.LONG));
        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(createIndexSettings(settings));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getDisabledSupplier())
        );
        assertEquals(
            "composite index cannot be created, enable it using [indices.composite_index.enabled] setting",
            exception.getMessage()
        );
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
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

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexConfig(getEnabledIndexSettings(settings))
        );
        assertEquals(
            "star tree index is under an experimental feature and can be activated only by enabling "
                + "opensearch.experimental.feature.composite_index.enabled feature flag in the JVM options",
            exception.getMessage()
        );
    }

    public void testUnknownDimField() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

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

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals("unknown dimension field [dim1_field] as part of composite field [my_field]", exception.getMessage());
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testUnknownMetricField() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals("unknown metric field [metric2] as part of composite field [my_field]", exception.getMessage());
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testInvalidDimensionMappedType() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new IpFieldMapper.IpFieldType("dim1_field"));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "composite index is not supported for the dimension field [dim1_field] with field type [ip] as part of composite field [my_field]",
            exception.getMessage()
        );
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testDimsWithNoDocValues() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put(
            "dim1_field",
            new NumberFieldMapper.NumberFieldType(
                "dim1_field",
                NumberFieldMapper.NumberType.LONG,
                false,
                false,
                false,
                true,
                null,
                Collections.emptyMap()
            )
        );
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "Aggregations not supported for the dimension field [dim1_field] with field type [long] as part of composite field [my_field]",
            exception.getMessage()
        );
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testMetricsWithNoDocValues() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put(
            "metric1",
            new NumberFieldMapper.NumberFieldType(
                "metric1",
                NumberFieldMapper.NumberType.LONG,
                false,
                false,
                false,
                true,
                null,
                Collections.emptyMap()
            )
        );
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "Aggregations not supported for the composite index metric field [metric1] with field type [long] as part of composite field [my_field]",
            exception.getMessage()
        );
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testInvalidDimensionMappedKeywordType() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new KeywordFieldMapper.KeywordFieldType("dim1_field"));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "composite index is not supported for the dimension field [dim1_field] with "
                + "field type [keyword] as part of composite field [my_field]",
            exception.getMessage()
        );
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testInvalidMetricMappedKeywordType() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new DateFieldMapper.DateFieldType("metric1"));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "composite index is not supported for the metric field [metric1] with field type [date] as part of composite field [my_field]",
            exception.getMessage()
        );
        // reset FeatureFlags to defaults
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testDefaults() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.type", "date")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("metric2", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        CompositeIndexConfig config = compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier());

        assertTrue(config.hasCompositeFields());
        assertEquals(1, config.getCompositeFields().size());
        CompositeField compositeField = config.getCompositeFields().get(0);
        List<MetricType> expectedMetrics = Arrays.asList(MetricType.AVG, MetricType.COUNT, MetricType.SUM, MetricType.MAX, MetricType.MIN);
        assertEquals(expectedMetrics, compositeField.getMetrics().get(0).getMetrics());
        StarTreeFieldSpec spec = (StarTreeFieldSpec) compositeField.getSpec();
        assertEquals(10000, spec.maxLeafDocs());
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    public void testDimTypeValidation() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(COMPOSITE_INDEX, true).build());
        assertTrue(FeatureFlags.isEnabled(COMPOSITE_INDEX));

        Settings settings = Settings.builder()
            .putList("index.composite_index.config.my_field.dimensions_order", Arrays.asList("dim1", "dim2"))
            .put("index.composite_index.config.my_field.dimensions_config.dim1.field", "dim1_field")
            .put("index.composite_index.config.my_field.dimensions_config.dim2.field", "dim2_field")
            .putList("index.composite_index.config.my_field.metrics", Arrays.asList("metric1", "metric2"))
            .build();

        Map<String, MappedFieldType> fieldTypes = new HashMap<>();
        fieldTypes.put("dim1_field", new NumberFieldMapper.NumberFieldType("dim1_field", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("dim2_field", new DateFieldMapper.DateFieldType("dim2_field"));
        fieldTypes.put("metric1", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));
        fieldTypes.put("metric2", new NumberFieldMapper.NumberFieldType("metric1", NumberFieldMapper.NumberType.DOUBLE));

        Function<String, MappedFieldType> fieldTypeLookup = fieldTypes::get;

        CompositeIndexConfig compositeIndexConfig = new CompositeIndexConfig(getEnabledIndexSettings(settings));
        Exception ex = expectThrows(
            IllegalArgumentException.class,
            () -> compositeIndexConfig.validateAndGetCompositeIndexConfig(fieldTypeLookup, getEnabledSupplier())
        );
        assertEquals(
            "specify field type [date] for dimension field [dim2_field] as part of of composite field [my_field]",
            ex.getMessage()
        );
    }

    private IndexSettings createIndexSettings(Settings settings) {
        return new IndexSettings(newIndexMeta("test", settings), Settings.EMPTY);
    }

    private Settings getEnabledSettings() {
        return Settings.builder().put("index.composite_index.enabled", true).build();
    }

    public IndexSettings getEnabledIndexSettings(Settings settings) {
        Settings enabledSettings = Settings.builder().put(settings).put("index.composite_index.enabled", true).build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", enabledSettings), getEnabledSettings());
        return indexSettings;
    }

    public IndexSettings getEnabledAndMultiFieldIndexSettings(Settings settings) {
        Settings multiFieldEnabledSettings = Settings.builder()
            .put(settings)
            .put("index.composite_index.enabled", true)
            .put(CompositeIndexConfig.COMPOSITE_INDEX_MAX_FIELDS_SETTING.getKey(), 2)
            .build();
        IndexSettings indexSettings = new IndexSettings(newIndexMeta("test", multiFieldEnabledSettings), multiFieldEnabledSettings);
        return indexSettings;
    }

    private BooleanSupplier getEnabledSupplier() {
        return new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return true;
            }
        };
    }

    private BooleanSupplier getDisabledSupplier() {
        return new BooleanSupplier() {
            @Override
            public boolean getAsBoolean() {
                return false;
            }
        };
    }
}
