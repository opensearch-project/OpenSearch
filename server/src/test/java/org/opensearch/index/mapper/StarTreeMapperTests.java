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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.DateDimension;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.startree.StarTreeFieldSpec;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
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

    public void testValidStarTree() throws IOException {
        MapperService mapperService = createMapperService(getExpandedMapping("status", "size"));
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
            List<MetricType> expectedMetrics = Arrays.asList(MetricType.SUM, MetricType.AVG);
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
            assertEquals(100, starTreeFieldType.getStarTreeFieldSpec().maxLeafDocs());
            assertEquals(StarTreeFieldSpec.StarTreeBuildMode.ON_HEAP, starTreeFieldType.getStarTreeFieldSpec().getBuildMode());
            assertEquals(Arrays.asList("@timestamp", "status"), starTreeFieldType.getStarTreeFieldSpec().getSkipStarNodeCreationInDims());
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
            assertEquals("status", starTreeFieldType.getMetrics().get(0).getField());
            List<MetricType> expectedMetrics = Arrays.asList(
                MetricType.AVG,
                MetricType.COUNT,
                MetricType.SUM,
                MetricType.MAX,
                MetricType.MIN
            );
            assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
            assertEquals(10000, starTreeFieldType.getStarTreeFieldSpec().maxLeafDocs());
            assertEquals(StarTreeFieldSpec.StarTreeBuildMode.OFF_HEAP, starTreeFieldType.getStarTreeFieldSpec().getBuildMode());
            assertEquals(Collections.emptyList(), starTreeFieldType.getStarTreeFieldSpec().getSkipStarNodeCreationInDims());
        }
    }

    public void testInvalidDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getExpandedMapping("invalid", "size"))
        );
        assertEquals("Failed to parse mapping [_doc]: unknown dimension field [invalid]", ex.getMessage());
    }

    public void testInvalidMetric() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> createMapperService(getExpandedMapping("status", "invalid"))
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

    private XContentBuilder getExpandedMapping(String dim, String metric) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("build_mode", "onheap");
            b.field("max_leaf_docs", 100);
            b.startArray("skip_star_node_creation_for_dimensions");
            {
                b.value("@timestamp");
                b.value("status");
            }
            b.endArray();
            b.startObject("ordered_dimensions");
            b.startObject("@timestamp");
            b.startArray("calendar_interval");
            b.value("day");
            b.value("month");
            b.endArray();
            b.endObject();
            b.startObject(dim);
            b.endObject();
            b.endObject();
            b.startObject("metrics");
            b.startObject(metric);
            b.startArray("metrics");
            b.value("sum");
            b.value("avg");
            b.endArray();
            b.endObject();
            b.endObject();
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
                b.startObject("ordered_dimensions");
                b.startObject("@timestamp");
                b.endObject();
                b.startObject("status");
                b.endObject();
                b.endObject();
            }
            if (!isEmptyMetrics) {
                b.startObject("metrics");
                b.startObject("status");
                b.endObject();
                b.startObject("metric_field");
                b.endObject();
                b.endObject();
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

    private XContentBuilder getInvalidMapping(boolean singleDim, boolean invalidSkipDims, boolean invalidDimType, boolean invalidMetricType)
        throws IOException {
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
            b.startObject("ordered_dimensions");
            if (!singleDim) {
                b.startObject("@timestamp");
                b.endObject();
            }
            b.startObject("status");
            b.endObject();
            b.endObject();
            b.startObject("metrics");
            b.startObject("status");
            b.endObject();
            b.startObject("metric_field");
            b.endObject();
            b.endObject();
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
        b.startObject("ordered_dimensions");
        b.startObject("size");
        b.endObject();
        b.startObject("status");
        b.endObject();
        b.endObject();
        b.startObject("metrics");
        b.startObject("status");
        b.endObject();
        b.endObject();
        b.endObject();
    }

    @Override
    protected void writeFieldValue(XContentBuilder builder) throws IOException {}

    @Override
    protected void registerParameters(ParameterChecker checker) throws IOException {

    }
}
