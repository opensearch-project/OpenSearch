/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexValidator;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.ReadDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

/**
 * Tests for {@link StarTreeMapper}.
 */
public class StarTreeMapperTests extends MapperTestCase {

    @Before
    public void setup() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
    }

    @After
    public void teardown() {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    @Override
    protected Settings getIndexSettings() {
        return Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .put(SETTINGS)
            .build();
    }

    public void testValidStarTree() throws IOException {

        MapperService mapperService = createMapperService(getExpandedMappingWithJustAvg("status", "size"));
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        for (CompositeMappedFieldType type : compositeFieldTypes) {
            StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) type;
            assertEquals(2, starTreeFieldType.getDimensions().size());
            assertEquals("@timestamp", starTreeFieldType.getDimensions().get(0).getField());
            assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
            DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
            List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                Rounding.DateTimeUnit.DAY_OF_MONTH,
                Rounding.DateTimeUnit.MONTH_OF_YEAR
            );
            assertEquals(expectedTimeUnits, dateDim.getIntervals());
            assertEquals("status", starTreeFieldType.getDimensions().get(1).getField());
            assertEquals(2, starTreeFieldType.getMetrics().size());
            assertEquals("size", starTreeFieldType.getMetrics().get(0).getField());

            // Assert COUNT and SUM gets added when AVG is defined
            List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.AVG, MetricStat.VALUE_COUNT, MetricStat.SUM);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
            assertEquals(100, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
            assertEquals(StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP, starTreeFieldType.getStarTreeConfig().getBuildMode());
            assertEquals(
                new HashSet<>(Arrays.asList("@timestamp", "status")),
                starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims()
            );
        }
    }

    public void testMetricsWithJustSum() throws IOException {
        MapperService mapperService = createMapperService(getExpandedMappingWithJustSum("status", "size"));
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        for (CompositeMappedFieldType type : compositeFieldTypes) {
            StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) type;
            assertEquals("@timestamp", starTreeFieldType.getDimensions().get(0).getField());
            assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
            DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
            List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                Rounding.DateTimeUnit.DAY_OF_MONTH,
                Rounding.DateTimeUnit.MONTH_OF_YEAR
            );
            assertEquals(expectedTimeUnits, dateDim.getIntervals());
            assertEquals("status", starTreeFieldType.getDimensions().get(1).getField());
            assertEquals("size", starTreeFieldType.getMetrics().get(0).getField());

            // Assert AVG gets added when both of its base metrics is already present
            List<MetricStat> expectedMetrics = List.of(MetricStat.SUM);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
            assertEquals(100, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
            assertEquals(StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP, starTreeFieldType.getStarTreeConfig().getBuildMode());
            assertEquals(
                new HashSet<>(Arrays.asList("@timestamp", "status")),
                starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims()
            );
        }
    }

    public void testMetricsWithCountAndSum() throws IOException {
        MapperService mapperService = createMapperService(getExpandedMappingWithSumAndCount("status", "size"));
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        for (CompositeMappedFieldType type : compositeFieldTypes) {
            StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) type;
            assertEquals("@timestamp", starTreeFieldType.getDimensions().get(0).getField());
            assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
            DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
            List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                Rounding.DateTimeUnit.DAY_OF_MONTH,
                Rounding.DateTimeUnit.MONTH_OF_YEAR
            );
            assertEquals(expectedTimeUnits, dateDim.getIntervals());
            assertEquals("status", starTreeFieldType.getDimensions().get(1).getField());
            assertEquals("size", starTreeFieldType.getMetrics().get(0).getField());

            // Assert AVG gets added when both of its base metrics is already present
            List<MetricStat> expectedMetrics = List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.AVG);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());

            Metric metric = starTreeFieldType.getMetrics().get(1);
            assertEquals("_doc_count", metric.getField());
            assertEquals(List.of(MetricStat.DOC_COUNT), metric.getMetrics());

            assertEquals(100, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
            assertEquals(StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP, starTreeFieldType.getStarTreeConfig().getBuildMode());
            assertEquals(
                new HashSet<>(Arrays.asList("@timestamp", "status")),
                starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims()
            );
        }
    }

    public void testValidStarTreeDefaults() throws IOException {
        MapperService mapperService = createMapperService(getMinMapping());
        Set<CompositeMappedFieldType> compositeFieldTypes = mapperService.getCompositeFieldTypes();
        for (CompositeMappedFieldType type : compositeFieldTypes) {
            StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) type;
            assertEquals("@timestamp", starTreeFieldType.getDimensions().get(0).getField());
            assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
            DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
            List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                Rounding.DateTimeUnit.MINUTES_OF_HOUR,
                Rounding.DateTimeUnit.HOUR_OF_DAY
            );
            assertEquals(expectedTimeUnits, dateDim.getIntervals());
            assertEquals("status", starTreeFieldType.getDimensions().get(1).getField());
            assertEquals(3, starTreeFieldType.getMetrics().size());
            assertEquals("status", starTreeFieldType.getMetrics().get(0).getField());
            List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());

            assertEquals("metric_field", starTreeFieldType.getMetrics().get(1).getField());
            expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(1).getMetrics());
            Metric metric = starTreeFieldType.getMetrics().get(2);
            assertEquals("_doc_count", metric.getField());
            assertEquals(List.of(MetricStat.DOC_COUNT), metric.getMetrics());
            assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
            assertEquals(StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP, starTreeFieldType.getStarTreeConfig().getBuildMode());
            assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
        }
    }

    public void testInvalidDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getExpandedMappingWithJustAvg("invalid", "size"))
        );
        assertEquals("Failed to parse mapping [_doc]: unknown dimension field [invalid]", ex.getMessage());
    }

    public void testInvalidMetric() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getExpandedMappingWithJustAvg("status", "invalid"))
        );
        assertEquals("Failed to parse mapping [_doc]: unknown metric field [invalid]", ex.getMessage());
    }

    public void testNoMetrics() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getMinMapping(false, true, false, false))
        );
        assertThat(
            ex.getMessage(),
            containsString("Failed to parse mapping [_doc]: metrics section is required for star tree field [startree]")
        );
    }

    public void testInvalidParam() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(false, false, false, false, true, false))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Star tree mapping definition has unsupported parameters:  [invalid : {invalid=invalid}]",
            ex.getMessage()
        );
    }

    public void testNoDims() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getMinMapping(true, false, false, false))
        );
        assertThat(
            ex.getMessage(),
            containsString("Failed to parse mapping [_doc]: ordered_dimensions is required for star tree field [startree]")
        );
    }

    public void testMissingDims() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getMinMapping(false, false, true, false))
        );
        assertThat(ex.getMessage(), containsString("Failed to parse mapping [_doc]: unknown dimension field [@timestamp]"));
    }

    public void testMissingMetrics() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getMinMapping(false, false, false, true))
        );
        assertThat(ex.getMessage(), containsString("Failed to parse mapping [_doc]: unknown metric field [metric_field]"));
    }

    public void testInvalidMetricType() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(false, false, false, true))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: non-numeric field type is associated with star tree metric [startree]",
            ex.getMessage()
        );
    }

    public void testInvalidMetricTypeWithDocCount() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(false, false, false, false, false, true))
        );
        assertEquals("Failed to parse mapping [_doc]: Invalid metric stat: _doc_count", ex.getMessage());
    }

    public void testInvalidDimType() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(false, false, true, false))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: unsupported field type associated with dimension [@timestamp] as part of star tree field [startree]",
            ex.getMessage()
        );
    }

    public void testInvalidSkipDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(false, true, false, false))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: [invalid] in skip_star_node_creation_for_dimensions should be part of ordered_dimensions",
            ex.getMessage()
        );
    }

    public void testInvalidSingleDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getInvalidMapping(true, false, false, false))
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Atleast two dimensions are required to build star tree index field [startree]",
            ex.getMessage()
        );
    }

    public void testMetric() {
        List<MetricStat> m1 = new ArrayList<>();
        m1.add(MetricStat.MAX);
        Metric metric1 = new Metric("name", m1);
        Metric metric2 = new Metric("name", m1);
        assertEquals(metric1, metric2);
        List<MetricStat> m2 = new ArrayList<>();
        m2.add(MetricStat.MAX);
        m2.add(MetricStat.VALUE_COUNT);
        metric2 = new Metric("name", m2);
        assertNotEquals(metric1, metric2);

        assertEquals(MetricStat.VALUE_COUNT, MetricStat.fromTypeName("value_count"));
        assertEquals(MetricStat.MAX, MetricStat.fromTypeName("max"));
        assertEquals(MetricStat.MIN, MetricStat.fromTypeName("min"));
        assertEquals(MetricStat.SUM, MetricStat.fromTypeName("sum"));
        assertEquals(MetricStat.AVG, MetricStat.fromTypeName("avg"));

        assertEquals(List.of(MetricStat.VALUE_COUNT, MetricStat.SUM), MetricStat.AVG.getBaseMetrics());

        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MetricStat.fromTypeName("invalid"));
        assertEquals("Invalid metric stat: invalid", ex.getMessage());
    }

    public void testDimensions() {
        List<Rounding.DateTimeUnit> d1CalendarIntervals = new ArrayList<>();
        d1CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        DateDimension d1 = new DateDimension("name", d1CalendarIntervals);
        DateDimension d2 = new DateDimension("name", d1CalendarIntervals);
        assertEquals(d1, d2);
        d2 = new DateDimension("name1", d1CalendarIntervals);
        assertNotEquals(d1, d2);
        List<Rounding.DateTimeUnit> d2CalendarIntervals = new ArrayList<>();
        d2CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        d2CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        d2 = new DateDimension("name", d2CalendarIntervals);
        assertNotEquals(d1, d2);
        NumericDimension n1 = new NumericDimension("name");
        NumericDimension n2 = new NumericDimension("name");
        assertEquals(n1, n2);
        n2 = new NumericDimension("name1");
        assertNotEquals(n1, n2);
    }

    public void testReadDimensions() {
        ReadDimension r1 = new ReadDimension("name");
        ReadDimension r2 = new ReadDimension("name");
        assertEquals(r1, r2);
        r2 = new ReadDimension("name1");
        assertNotEquals(r1, r2);
    }

    public void testStarTreeField() {
        List<MetricStat> m1 = new ArrayList<>();
        m1.add(MetricStat.MAX);
        Metric metric1 = new Metric("name", m1);
        List<Rounding.DateTimeUnit> d1CalendarIntervals = new ArrayList<>();
        d1CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        DateDimension d1 = new DateDimension("name", d1CalendarIntervals);
        NumericDimension n1 = new NumericDimension("numeric");
        NumericDimension n2 = new NumericDimension("name1");

        List<Metric> metrics = List.of(metric1);
        List<Dimension> dims = List.of(d1, n2);
        StarTreeFieldConfiguration config = new StarTreeFieldConfiguration(
            100,
            Set.of("name"),
            StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP
        );

        StarTreeField field1 = new StarTreeField("starTree", dims, metrics, config);
        StarTreeField field2 = new StarTreeField("starTree", dims, metrics, config);
        assertEquals(field1, field2);

        dims = List.of(d1, n2, n1);
        field2 = new StarTreeField("starTree", dims, metrics, config);
        assertNotEquals(field1, field2);

        dims = List.of(d1, n2);
        metrics = List.of(metric1, metric1);
        field2 = new StarTreeField("starTree", dims, metrics, config);
        assertNotEquals(field1, field2);

        dims = List.of(d1, n2);
        metrics = List.of(metric1);
        StarTreeFieldConfiguration config1 = new StarTreeFieldConfiguration(
            1000,
            Set.of("name"),
            StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP
        );
        field2 = new StarTreeField("starTree", dims, metrics, config1);
        assertNotEquals(field1, field2);

        config1 = new StarTreeFieldConfiguration(100, Set.of("name", "field2"), StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP);
        field2 = new StarTreeField("starTree", dims, metrics, config1);
        assertNotEquals(field1, field2);

        config1 = new StarTreeFieldConfiguration(100, Set.of("name"), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP);
        field2 = new StarTreeField("starTree", dims, metrics, config1);
        assertNotEquals(field1, field2);

        field2 = new StarTreeField("starTree", dims, metrics, config);
        assertEquals(field1, field2);
    }

    public void testValidations() throws IOException {
        MapperService mapperService = createMapperService(getExpandedMappingWithJustAvg("status", "size"));
        Settings settings = Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), true).build();
        CompositeIndexSettings enabledCompositeIndexSettings = new CompositeIndexSettings(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        CompositeIndexValidator.validate(mapperService, enabledCompositeIndexSettings, mapperService.getIndexSettings());
        settings = Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), false).build();
        CompositeIndexSettings compositeIndexSettings = new CompositeIndexSettings(
            settings,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        MapperService finalMapperService = mapperService;
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexValidator.validate(finalMapperService, compositeIndexSettings, finalMapperService.getIndexSettings())
        );
        assertEquals(
            "star tree index cannot be created, enable it using [indices.composite_index.star_tree.enabled] setting",
            ex.getMessage()
        );

        MapperService mapperServiceInvalid = createMapperService(getInvalidMappingWithDv(false, false, false, true));
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexValidator.validate(
                mapperServiceInvalid,
                enabledCompositeIndexSettings,
                mapperServiceInvalid.getIndexSettings()
            )
        );
        assertEquals(
            "Aggregations not supported for the metrics field [metric_field] with field type [integer] as part of star tree field",
            ex.getMessage()
        );

        MapperService mapperServiceInvalidDim = createMapperService(getInvalidMappingWithDv(false, false, true, false));
        ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexValidator.validate(
                mapperServiceInvalidDim,
                enabledCompositeIndexSettings,
                mapperServiceInvalidDim.getIndexSettings()
            )
        );
        assertEquals(
            "Aggregations not supported for the dimension field [@timestamp] with field type [date] as part of star tree field",
            ex.getMessage()
        );

        MapperParsingException mapperParsingExceptionex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getMinMappingWith2StarTrees())
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Composite fields cannot have more than [1] fields",
            mapperParsingExceptionex.getMessage()
        );
    }

    private XContentBuilder getExpandedMappingWithJustAvg(String dim, String metric) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 100);
            b.startArray("skip_star_node_creation_for_dimensions");
            {
                b.value("@timestamp");
                b.value("status");
            }
            b.endArray();
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "@timestamp");
            b.startArray("calendar_intervals");
            b.value("day");
            b.value("month");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", dim);
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", metric);
            b.startArray("stats");
            b.value("avg");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder getExpandedMappingWithJustSum(String dim, String metric) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 100);
            b.startArray("skip_star_node_creation_for_dimensions");
            {
                b.value("@timestamp");
                b.value("status");
            }
            b.endArray();
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "@timestamp");
            b.startArray("calendar_intervals");
            b.value("day");
            b.value("month");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", dim);
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", metric);
            b.startArray("stats");
            b.value("sum");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder getExpandedMappingWithSumAndCount(String dim, String metric) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 100);
            b.startArray("skip_star_node_creation_for_dimensions");
            {
                b.value("@timestamp");
                b.value("status");
            }
            b.endArray();
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "@timestamp");
            b.startArray("calendar_intervals");
            b.value("day");
            b.value("month");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", dim);
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", metric);
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder getMinMapping() throws IOException {
        return getMinMapping(false, false, false, false);
    }

    private XContentBuilder getMinMapping(boolean isEmptyDims, boolean isEmptyMetrics, boolean missingDim, boolean missingMetric)
        throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            if (!isEmptyDims) {
                b.startArray("ordered_dimensions");
                b.startObject();
                b.field("name", "@timestamp");
                b.endObject();
                b.startObject();
                b.field("name", "status");
                b.endObject();
                b.endArray();
            }
            if (!isEmptyMetrics) {
                b.startArray("metrics");
                b.startObject();
                b.field("name", "status");
                b.endObject();
                b.startObject();
                b.field("name", "metric_field");
                b.endObject();
                b.endArray();
            }
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            if (!missingDim) {
                b.startObject("@timestamp");
                b.field("type", "date");
                b.endObject();
            }
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            if (!missingMetric) {
                b.startObject("metric_field");
                b.field("type", "integer");
                b.endObject();
            }
            b.endObject();
        });
    }

    private XContentBuilder getMinMappingWith2StarTrees() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");

            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "@timestamp");
            b.endObject();
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.endArray();

            b.startArray("metrics");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "metric_field");
            b.endObject();
            b.endArray();

            b.endObject();
            b.endObject();

            b.startObject("startree1");
            b.field("type", "star_tree");
            b.startObject("config");

            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "@timestamp");
            b.endObject();
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.endArray();

            b.startArray("metrics");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "metric_field");
            b.endObject();
            b.endArray();

            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            b.field("type", "date");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("metric_field");
            b.field("type", "integer");
            b.endObject();

            b.endObject();
        });
    }

    private XContentBuilder getInvalidMapping(
        boolean singleDim,
        boolean invalidSkipDims,
        boolean invalidDimType,
        boolean invalidMetricType,
        boolean invalidParam,
        boolean invalidDocCountMetricType
    ) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");

            b.startArray("skip_star_node_creation_for_dimensions");
            {
                if (invalidSkipDims) {
                    b.value("invalid");
                }
                b.value("status");
            }
            b.endArray();
            if (invalidParam) {
                b.startObject("invalid");
                b.field("invalid", "invalid");
                b.endObject();
            }
            b.startArray("ordered_dimensions");
            if (!singleDim) {
                b.startObject();
                b.field("name", "@timestamp");
                b.endObject();
            }
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "metric_field");
            if (invalidDocCountMetricType) {
                b.startArray("stats");
                b.value("_doc_count");
                b.value("avg");
                b.endArray();
            }
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            if (!invalidDimType) {
                b.field("type", "date");
            } else {
                b.field("type", "keyword");
            }
            b.endObject();

            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("metric_field");
            if (invalidMetricType) {
                b.field("type", "date");
            } else {
                b.field("type", "integer");
            }
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder getInvalidMappingWithDv(
        boolean singleDim,
        boolean invalidSkipDims,
        boolean invalidDimType,
        boolean invalidMetricType
    ) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");

            b.startArray("skip_star_node_creation_for_dimensions");
            {
                if (invalidSkipDims) {
                    b.value("invalid");
                }
                b.value("status");
            }
            b.endArray();
            b.startArray("ordered_dimensions");
            if (!singleDim) {
                b.startObject();
                b.field("name", "@timestamp");
                b.endObject();
            }
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "status");
            b.endObject();
            b.startObject();
            b.field("name", "metric_field");
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("@timestamp");
            if (!invalidDimType) {
                b.field("type", "date");
                b.field("doc_values", "true");
            } else {
                b.field("type", "date");
                b.field("doc_values", "false");
            }
            b.endObject();

            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.startObject("metric_field");
            if (invalidMetricType) {
                b.field("type", "integer");
                b.field("doc_values", "false");
            } else {
                b.field("type", "integer");
                b.field("doc_values", "true");
            }
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder getInvalidMapping(boolean singleDim, boolean invalidSkipDims, boolean invalidDimType, boolean invalidMetricType)
        throws IOException {
        return getInvalidMapping(singleDim, invalidSkipDims, invalidDimType, invalidMetricType, false, false);
    }

    protected boolean supportsOrIgnoresBoost() {
        return false;
    }

    protected boolean supportsMeta() {
        return false;
    }

    @Override
    protected void assertExistsQuery(MapperService mapperService) {}

    // Overriding fieldMapping to make it create composite mappings by default.
    // This way, the parent tests are checking the right behavior for this Mapper.
    @Override
    protected final XContentBuilder fieldMapping(CheckedConsumer<XContentBuilder, IOException> buildField) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            buildField.accept(b);
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }

    @Override
    public void testEmptyName() {
        MapperParsingException e = expectThrows(MapperParsingException.class, () -> createMapperService(topMapping(b -> {
            b.startObject("composite");
            b.startObject("");
            minimalMapping(b);
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("size");
            b.field("type", "integer");
            b.endObject();
            b.startObject("status");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        })));
        assertThat(e.getMessage(), containsString("name cannot be empty string"));
        assertParseMinimalWarnings();
    }

    @Override
    protected void minimalMapping(XContentBuilder b) throws IOException {
        b.field("type", "star_tree");
        b.startObject("config");
        b.startArray("ordered_dimensions");
        b.startObject();
        b.field("name", "size");
        b.endObject();
        b.startObject();
        b.field("name", "status");
        b.endObject();
        b.endArray();
        b.startArray("metrics");
        b.startObject();
        b.field("name", "status");
        b.endObject();
        b.endArray();
        b.endObject();
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {}

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

    }
}
