/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.ExtendedDateTimeUnit;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for star tree mapper
 */
public class StarTreeMapperIT extends OpenSearchIntegTestCase {
    private static final String TEST_INDEX = "test";

    private static XContentBuilder createMinimalTestMapping(boolean invalidDim, boolean invalidMetric, boolean keywordDim) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", getDim(invalidDim, keywordDim))
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", getDim(invalidMetric, false))
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
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

    private static XContentBuilder createDateTestMapping(boolean duplicate) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .startArray("calendar_intervals")
                .value("day")
                .value("quarter-hour")
                .value(duplicate ? "quarter-hour" : "half-hour")
                .endArray()
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
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

    private static XContentBuilder createMaxDimTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .startArray("calendar_intervals")
                .value("day")
                .value("month")
                .value("half-hour")
                .endArray()
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "dim2")
                .endObject()
                .startObject()
                .field("name", "dim3")
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "dim2")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("dim2")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("dim3")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createTestMappingWithoutStarTree() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
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

    private static XContentBuilder createUpdateTestMapping(boolean changeDim, boolean sameStarTree) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject(sameStarTree ? "startree-1" : "startree-2")
                .field("type", "star_tree")
                .startObject("config")
                .startObject("date_dimension")
                .field("name", "timestamp")
                .endObject()
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", changeDim ? "numeric_new" : getDim(false, false))
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", getDim(false, false))
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endObject()
                .startObject("properties")
                .startObject("timestamp")
                .field("type", "date")
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("numeric_new")
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

    private static String getDim(boolean hasDocValues, boolean isKeyword) {
        if (hasDocValues) {
            return "numeric";
        } else if (isKeyword) {
            return "keyword";
        }
        return "numeric_dv";
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.STAR_TREE_INDEX, "true").build();
    }

    @Before
    public final void setupNodeSettings() {
        Settings request = Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), true).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setPersistentSettings(request).get());
    }

    public void testValidCompositeIndex() {
        prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, false, false)).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertTrue(ft instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
                        ExtendedDateTimeUnit.HALF_HOUR_OF_DAY
                    );
                    for (int i = 0; i < dateDim.getSortedCalendarIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(
                        MetricStat.AVG,
                        MetricStat.COUNT,
                        MetricStat.SUM,
                        MetricStat.MAX,
                        MetricStat.MIN
                    );
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testValidCompositeIndexWithDates() {
        prepareCreate(TEST_INDEX).setMapping(createDateTestMapping(false)).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertTrue(ft instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        ExtendedDateTimeUnit.QUARTER_HOUR_OF_DAY,
                        ExtendedDateTimeUnit.HALF_HOUR_OF_DAY,
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                    );
                    for (int i = 0; i < dateDim.getIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(
                        MetricStat.AVG,
                        MetricStat.COUNT,
                        MetricStat.SUM,
                        MetricStat.MAX,
                        MetricStat.MIN
                    );
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testValidCompositeIndexWithDuplicateDates() {
        prepareCreate(TEST_INDEX).setMapping(createDateTestMapping(true)).get();
        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertTrue(ft instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        ExtendedDateTimeUnit.QUARTER_HOUR_OF_DAY,
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
                    );
                    for (int i = 0; i < dateDim.getIntervals().size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getSortedCalendarIntervals().get(i).shortName());
                    }
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(
                        MetricStat.AVG,
                        MetricStat.VALUE_COUNT,
                        MetricStat.SUM,
                        MetricStat.MAX,
                        MetricStat.MIN
                    );
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testUpdateIndexWithAdditionOfStarTree() {
        prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, false, false)).get();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(false, false)).get()
        );
        assertEquals("Index cannot have more than [1] star tree fields", ex.getMessage());
    }

    public void testUpdateIndexWithNewerStarTree() {
        prepareCreate(TEST_INDEX).setMapping(createTestMappingWithoutStarTree()).get();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(false, false)).get()
        );
        assertEquals(
            "Composite fields must be specified during index creation, addition of new composite fields during update is not supported",
            ex.getMessage()
        );
    }

    public void testUpdateIndexWhenMappingIsDifferent() {
        prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, false, false)).get();

        // update some field in the mapping
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(true, true)).get()
        );
        assertTrue(ex.getMessage().contains("Cannot update parameter [config] from"));
    }

    public void testUpdateIndexWhenMappingIsSame() {
        prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, false, false)).get();

        // update some field in the mapping
        AcknowledgedResponse putMappingResponse = client().admin()
            .indices()
            .preparePutMapping(TEST_INDEX)
            .setSource(createMinimalTestMapping(false, false, false))
            .get();
        assertAcked(putMappingResponse);

        Iterable<IndicesService> dataNodeInstances = internalCluster().getDataNodeInstances(IndicesService.class);
        for (IndicesService service : dataNodeInstances) {
            final Index index = resolveIndex("test");
            if (service.hasIndex(index)) {
                IndexService indexService = service.indexService(index);
                Set<CompositeMappedFieldType> fts = indexService.mapperService().getCompositeFieldTypes();

                for (CompositeMappedFieldType ft : fts) {
                    assertTrue(ft instanceof StarTreeMapper.StarTreeFieldType);
                    StarTreeMapper.StarTreeFieldType starTreeFieldType = (StarTreeMapper.StarTreeFieldType) ft;
                    assertEquals("timestamp", starTreeFieldType.getDimensions().get(0).getField());
                    assertTrue(starTreeFieldType.getDimensions().get(0) instanceof DateDimension);
                    DateDimension dateDim = (DateDimension) starTreeFieldType.getDimensions().get(0);
                    List<DateTimeUnitRounding> expectedTimeUnits = Arrays.asList(
                        new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
                        ExtendedDateTimeUnit.HALF_HOUR_OF_DAY
                    );
                    for (int i = 0; i < expectedTimeUnits.size(); i++) {
                        assertEquals(expectedTimeUnits.get(i).shortName(), dateDim.getIntervals().get(i).shortName());
                    }

                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());
                    List<MetricStat> expectedMetrics = Arrays.asList(
                        MetricStat.AVG,
                        MetricStat.VALUE_COUNT,
                        MetricStat.SUM,
                        MetricStat.MAX,
                        MetricStat.MIN
                    );
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());
                    assertEquals(10000, starTreeFieldType.getStarTreeConfig().maxLeafDocs());
                    assertEquals(
                        StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP,
                        starTreeFieldType.getStarTreeConfig().getBuildMode()
                    );
                    assertEquals(Collections.emptySet(), starTreeFieldType.getStarTreeConfig().getSkipStarNodeCreationInDims());
                }
            }
        }
    }

    public void testInvalidDimCompositeIndex() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(true, false, false)).get()
        );
        assertEquals(
            "Aggregations not supported for the dimension field [numeric] with field type [integer] as part of star tree field",
            ex.getMessage()
        );
    }

    public void testMaxDimsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMaxDimTestMapping())
                // Date dimension is considered as one dimension regardless of number of actual calendar intervals
                .setSettings(Settings.builder().put(StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(), 2))
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: ordered_dimensions cannot have more than 2 dimensions for star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testMaxCalendarIntervalsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMaxDimTestMapping())
                .setSettings(Settings.builder().put(StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING.getKey(), 1))
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: At most [1] calendar intervals are allowed in dimension [timestamp]",
            ex.getMessage()
        );
    }

    public void testUnsupportedDim() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, false, true)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: unsupported field type associated with dimension [keyword] as part of star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testInvalidMetric() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMinimalTestMapping(false, true, false)).get()
        );
        assertEquals(
            "Aggregations not supported for the metrics field [numeric] with field type [integer] as part of star tree field",
            ex.getMessage()
        );
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
}
