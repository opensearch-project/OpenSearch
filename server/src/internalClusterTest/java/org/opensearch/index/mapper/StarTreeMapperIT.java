/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.SearchHit;
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
    Settings settings = Settings.builder()
        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
        .build();

    private static XContentBuilder createMinimalTestMapping(boolean invalidDim, boolean invalidMetric, boolean keywordDim) {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "timestamp")
                .endObject()
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

    private static XContentBuilder createMaxDimTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "timestamp")
                .startArray("calendar_intervals")
                .value("day")
                .value("month")
                .endArray()
                .endObject()
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
                .startObject()
                .field("name", "dim3")
                .endObject()
                .startObject()
                .field("name", "dim4")
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
                .startObject("dim4")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createTestMappingWithoutStarTree(boolean invalidDim, boolean invalidMetric, boolean keywordDim) {
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
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "timestamp")
                .endObject()
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

    private XContentBuilder getMappingWithDuplicateFields(boolean isDuplicateDim, boolean isDuplicateMetric) {
        XContentBuilder mapping = null;
        try {
            mapping = jsonBuilder().startObject()
                .startObject("composite")
                .startObject("startree-1")
                .field("type", "star_tree")
                .startObject("config")
                .startArray("ordered_dimensions")
                .startObject()
                .field("name", "timestamp")
                .endObject()
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .startObject()
                .field("name", isDuplicateDim ? "numeric_dv" : "numeric_dv1")  // Duplicate dimension
                .endObject()
                .endArray()
                .startArray("metrics")
                .startObject()
                .field("name", "numeric_dv")
                .endObject()
                .startObject()
                .field("name", isDuplicateMetric ? "numeric_dv" : "numeric_dv1")  // Duplicate metric
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
                .startObject("numeric_dv1")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            fail("Failed to create mapping: " + e.getMessage());
        }
        return mapping;
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
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
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
                    List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                        Rounding.DateTimeUnit.MINUTES_OF_HOUR,
                        Rounding.DateTimeUnit.HOUR_OF_DAY
                    );
                    assertEquals(expectedTimeUnits, dateDim.getIntervals());
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals(2, starTreeFieldType.getMetrics().size());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());

                    // Assert default metrics
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
                    assertEquals(expectedMetrics, starTreeFieldType.getMetrics().get(0).getMetrics());

                    assertEquals("_doc_count", starTreeFieldType.getMetrics().get(1).getField());
                    assertEquals(List.of(MetricStat.DOC_COUNT), starTreeFieldType.getMetrics().get(1).getMetrics());

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

    public void testCompositeIndexWithIndexNotSpecified() {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .build();
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Set 'index.composite_index' as true as part of index settings to use star tree index",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithHigherTranslogFlushSize() {
        Settings settings = Settings.builder()
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(513, ByteSizeUnit.MB))
            .build();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );
        assertEquals("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index", ex.getMessage());
    }

    public void testCompositeIndexWithArraysInCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
        // Attempt to index a document with an array field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("numeric_dv")
            .value(10)
            .value(20)
            .value(30)
            .endArray()
            .endObject();

        // Index the document and refresh
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().prepareIndex(TEST_INDEX).setSource(doc).get()
        );
        assertEquals(
            "object mapping for [_doc] with array for [numeric_dv] cannot be accepted as field is also part of composite index mapping which does not accept arrays",
            ex.getMessage()
        );
    }

    public void testCompositeIndexWithArraysInNonCompositeField() throws IOException {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();
        // Attempt to index a document with an array field
        XContentBuilder doc = jsonBuilder().startObject()
            .field("timestamp", "2023-06-01T12:00:00Z")
            .startArray("numeric")
            .value(10)
            .value(20)
            .value(30)
            .endArray()
            .endObject();

        // Index the document and refresh
        IndexResponse indexResponse = client().prepareIndex(TEST_INDEX).setSource(doc).get();

        assertEquals(RestStatus.CREATED, indexResponse.status());

        client().admin().indices().prepareRefresh(TEST_INDEX).get();
        // Verify the document was indexed
        SearchResponse searchResponse = client().prepareSearch(TEST_INDEX).setQuery(QueryBuilders.matchAllQuery()).get();

        assertEquals(1, searchResponse.getHits().getTotalHits().value);

        // Verify the values in the indexed document
        SearchHit hit = searchResponse.getHits().getAt(0);
        assertEquals("2023-06-01T12:00:00Z", hit.getSourceAsMap().get("timestamp"));

        List<Integer> values = (List<Integer>) hit.getSourceAsMap().get("numeric");
        assertEquals(3, values.size());
        assertTrue(values.contains(10));
        assertTrue(values.contains(20));
        assertTrue(values.contains(30));
    }

    public void testUpdateIndexWithAdditionOfStarTree() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(false, false)).get()
        );
        assertEquals("Index cannot have more than [1] star tree fields", ex.getMessage());
    }

    public void testUpdateIndexWithNewerStarTree() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createTestMappingWithoutStarTree(false, false, false)).get();

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
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        // update some field in the mapping
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(TEST_INDEX).setSource(createUpdateTestMapping(true, true)).get()
        );
        assertTrue(ex.getMessage().contains("Cannot update parameter [config] from"));
    }

    public void testUpdateIndexWhenMappingIsSame() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

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
                    List<Rounding.DateTimeUnit> expectedTimeUnits = Arrays.asList(
                        Rounding.DateTimeUnit.MINUTES_OF_HOUR,
                        Rounding.DateTimeUnit.HOUR_OF_DAY
                    );
                    assertEquals(expectedTimeUnits, dateDim.getIntervals());
                    assertEquals("numeric_dv", starTreeFieldType.getDimensions().get(1).getField());
                    assertEquals("numeric_dv", starTreeFieldType.getMetrics().get(0).getField());

                    // Assert default metrics
                    List<MetricStat> expectedMetrics = Arrays.asList(MetricStat.VALUE_COUNT, MetricStat.SUM, MetricStat.AVG);
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
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(true, false, false)).get()
        );
        assertEquals(
            "Aggregations not supported for the dimension field [numeric] with field type [integer] as part of star tree field",
            ex.getMessage()
        );
    }

    public void testMaxDimsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings)
                .setMapping(createMaxDimTestMapping())
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_DIMENSIONS_SETTING.getKey(), 2)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: ordered_dimensions cannot have more than 2 dimensions for star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testMaxMetricsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings)
                .setMapping(createMaxDimTestMapping())
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_BASE_METRICS_SETTING.getKey(), 4)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
                .get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: There cannot be more than [4] base metrics for star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testMaxCalendarIntervalsCompositeIndex() {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setMapping(createMaxDimTestMapping())
                .setSettings(
                    Settings.builder()
                        .put(StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING.getKey(), 1)
                        .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
                        .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
                )
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
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, true)).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: unsupported field type associated with dimension [keyword] as part of star tree field [startree-1]",
            ex.getMessage()
        );
    }

    public void testInvalidMetric() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, true, false)).get()
        );
        assertEquals(
            "Aggregations not supported for the metrics field [numeric] with field type [integer] as part of star tree field",
            ex.getMessage()
        );
    }

    public void testDuplicateDimensions() {
        XContentBuilder finalMapping = getMappingWithDuplicateFields(true, false);
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(finalMapping).setSettings(settings).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Duplicate dimension [numeric_dv] present as part star tree index field [startree-1]",
            ex.getMessage()
        );
    }

    public void testDuplicateMetrics() {
        XContentBuilder finalMapping = getMappingWithDuplicateFields(false, true);
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(settings).setMapping(finalMapping).setSettings(settings).get()
        );
        assertEquals(
            "Failed to parse mapping [_doc]: Duplicate metrics [numeric_dv] present as part star tree index field [startree-1]",
            ex.getMessage()
        );
    }

    public void testValidTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(256, ByteSizeUnit.MB))
            .build();

        AcknowledgedResponse response = prepareCreate(TEST_INDEX).setSettings(indexSettings)
            .setMapping(createMinimalTestMapping(false, false, false))
            .get();

        assertTrue(response.isAcknowledged());
    }

    public void testInvalidTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1024, ByteSizeUnit.MB))
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(indexSettings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );

        assertTrue(
            ex.getMessage().contains("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index")
        );
    }

    public void testUpdateTranslogFlushThresholdSize() {
        prepareCreate(TEST_INDEX).setSettings(settings).setMapping(createMinimalTestMapping(false, false, false)).get();

        // Update to a valid value
        AcknowledgedResponse validUpdateResponse = client().admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "256mb"))
            .get();
        assertTrue(validUpdateResponse.isAcknowledged());

        // Try to update to an invalid value
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings(TEST_INDEX)
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1024mb"))
                .get()
        );

        assertTrue(
            ex.getMessage().contains("You can configure 'index.translog.flush_threshold_size' with upto '512mb' for composite index")
        );

        // update cluster settings to higher value
        Settings updatedSettings = Settings.builder()
            .put(CompositeIndexSettings.COMPOSITE_INDEX_MAX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1030m")
            .build();

        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest().transientSettings(updatedSettings);

        client().admin().cluster().updateSettings(updateSettingsRequest).actionGet();

        // update index threshold flush to higher value
        validUpdateResponse = client().admin()
            .indices()
            .prepareUpdateSettings(TEST_INDEX)
            .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), "1024mb"))
            .get();
        assertTrue(validUpdateResponse.isAcknowledged());
    }

    public void testMinimumTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(56, ByteSizeUnit.BYTES))
            .build();

        AcknowledgedResponse response = prepareCreate(TEST_INDEX).setSettings(indexSettings)
            .setMapping(createMinimalTestMapping(false, false, false))
            .get();

        assertTrue(response.isAcknowledged());
    }

    public void testBelowMinimumTranslogFlushThresholdSize() {
        Settings indexSettings = Settings.builder()
            .put(settings)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(55, ByteSizeUnit.BYTES))
            .build();

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate(TEST_INDEX).setSettings(indexSettings).setMapping(createMinimalTestMapping(false, false, false)).get()
        );

        assertEquals("failed to parse value [55b] for setting [index.translog.flush_threshold_size], must be >= [56b]", ex.getMessage());
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
