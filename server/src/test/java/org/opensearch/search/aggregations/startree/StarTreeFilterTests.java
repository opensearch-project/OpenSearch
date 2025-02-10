/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene101.Lucene101Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite101.Composite101Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.StarTreeTraversalUtil;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.mockito.Mockito;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;

public class StarTreeFilterTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";
    private static final String SNDV = "sndv";
    private static final String SDV = "sdv";
    private static final String DV = "dv";

    public static final LinkedHashMap<String, String> DIMENSION_TYPE_MAP = new LinkedHashMap<>();
    public static final Map<String, String> METRIC_TYPE_MAP = Map.of(FIELD_NAME, "integer");

    static {
        // Ordered dimensions
        DIMENSION_TYPE_MAP.put(SNDV, "integer");
        DIMENSION_TYPE_MAP.put(SDV, "integer");
        DIMENSION_TYPE_MAP.put(DV, "integer");
    }

    @Before
    public void setup() {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(FeatureFlags.STAR_TREE_INDEX, true).build());
    }

    @After
    public void teardown() throws IOException {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    protected Codec getCodec(int maxLeafDoc, boolean skipStarNodeCreationForSDVDimension) {
        final Logger testLogger = LogManager.getLogger(StarTreeFilterTests.class);
        MapperService mapperService;
        try {
            mapperService = StarTreeDocValuesFormatTests.createMapperService(
                getExpandedMapping(maxLeafDoc, skipStarNodeCreationForSDVDimension, DIMENSION_TYPE_MAP, METRIC_TYPE_MAP)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite101Codec(Lucene101Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeFilterWithNoDocsInSVDField() throws IOException {
        testStarTreeFilter(5, true);
    }

    public void testStarTreeFilterWithDocsInSVDFieldButNoStarNode() throws IOException {
        testStarTreeFilter(10, false);
    }

    public void testStarTreeFilterMerging() {

        StarTreeFilter mergedStarTreeFilter;
        String dimensionToMerge = "dim";

        DimensionFilter exactMatchDimFilter = new ExactMatchDimFilter(dimensionToMerge, Collections.emptyList());
        DimensionFilter rangeMatchDimFilter = new RangeMatchDimFilter(dimensionToMerge, null, null, true, true);

        // When Star Tree doesn't have the same dimension as @dimensionToMerge
        StarTreeFilter starTreeFilter = new StarTreeFilter(Collections.emptyMap());
        mergedStarTreeFilter = StarTreeQueryHelper.mergeDimensionFilterIfNotExists(
            starTreeFilter,
            dimensionToMerge,
            List.of(exactMatchDimFilter)
        );
        assertEquals(1, mergedStarTreeFilter.getDimensions().size());
        DimensionFilter mergedDimensionFilter1 = mergedStarTreeFilter.getFiltersForDimension(dimensionToMerge).get(0);
        assertEquals(ExactMatchDimFilter.class, mergedDimensionFilter1.getClass());

        // When Star Tree has the same dimension as @dimensionToMerge
        starTreeFilter = new StarTreeFilter(Map.of(dimensionToMerge, List.of(rangeMatchDimFilter)));
        mergedStarTreeFilter = StarTreeQueryHelper.mergeDimensionFilterIfNotExists(
            starTreeFilter,
            dimensionToMerge,
            List.of(exactMatchDimFilter)
        );
        assertEquals(1, mergedStarTreeFilter.getDimensions().size());
        DimensionFilter mergedDimensionFilter2 = mergedStarTreeFilter.getFiltersForDimension(dimensionToMerge).get(0);
        assertEquals(RangeMatchDimFilter.class, mergedDimensionFilter2.getClass());

        // When Star Tree has the same dimension as @dimensionToMerge with other dimensions
        starTreeFilter = new StarTreeFilter(Map.of(dimensionToMerge, List.of(rangeMatchDimFilter), "status", List.of(rangeMatchDimFilter)));
        mergedStarTreeFilter = StarTreeQueryHelper.mergeDimensionFilterIfNotExists(
            starTreeFilter,
            dimensionToMerge,
            List.of(exactMatchDimFilter)
        );
        assertEquals(2, mergedStarTreeFilter.getDimensions().size());
        DimensionFilter mergedDimensionFilter3 = mergedStarTreeFilter.getFiltersForDimension(dimensionToMerge).get(0);
        assertEquals(RangeMatchDimFilter.class, mergedDimensionFilter3.getClass());
        DimensionFilter mergedDimensionFilter4 = mergedStarTreeFilter.getFiltersForDimension("status").get(0);
        assertEquals(RangeMatchDimFilter.class, mergedDimensionFilter4.getClass());

        // When Star Tree doesn't have the same dimension as @dimensionToMerge but has other dimensions
        starTreeFilter = new StarTreeFilter(Map.of("status", List.of(rangeMatchDimFilter)));
        mergedStarTreeFilter = StarTreeQueryHelper.mergeDimensionFilterIfNotExists(
            starTreeFilter,
            dimensionToMerge,
            List.of(exactMatchDimFilter)
        );
        assertEquals(2, mergedStarTreeFilter.getDimensions().size());
        DimensionFilter mergedDimensionFilter5 = mergedStarTreeFilter.getFiltersForDimension(dimensionToMerge).get(0);
        assertEquals(ExactMatchDimFilter.class, mergedDimensionFilter5.getClass());
        DimensionFilter mergedDimensionFilter6 = mergedStarTreeFilter.getFiltersForDimension("status").get(0);
        assertEquals(RangeMatchDimFilter.class, mergedDimensionFilter6.getClass());

    }

    private Directory createStarTreeIndex(int maxLeafDoc, boolean skipStarNodeCreationForSDVDimension, List<Document> docs)
        throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec(maxLeafDoc, skipStarNodeCreationForSDVDimension));
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        int totalDocs = 100;

        for (int i = 0; i < totalDocs; i++) {
            Document doc = new Document();
            doc.add(new SortedNumericDocValuesField(SNDV, i));
            doc.add(new SortedNumericDocValuesField(DV, 2 * i));
            doc.add(new SortedNumericDocValuesField(FIELD_NAME, 3 * i));
            if (skipStarNodeCreationForSDVDimension) {
                // adding SDV field only star node creation is skipped for SDV dimension
                doc.add(new SortedNumericDocValuesField(SDV, 4 * i));
            }
            iw.addDocument(doc);
            docs.add(doc);
        }
        iw.forceMerge(1);
        iw.close();
        return directory;
    }

    private void testStarTreeFilter(int maxLeafDoc, boolean skipStarNodeCreationForSDVDimension) throws IOException {
        List<Document> docs = new ArrayList<>();

        Directory directory = createStarTreeIndex(maxLeafDoc, skipStarNodeCreationForSDVDimension, docs);

        int totalDocs = docs.size();

        DirectoryReader ir = DirectoryReader.open(directory);
        initValuesSourceRegistry();
        LeafReaderContext context = ir.leaves().get(0);
        SegmentReader reader = Lucene.segmentReader(context.reader());
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        long starTreeDocCount, docCount;

        MapperService mapperService = Mockito.mock(MapperService.class);
        SearchContext searchContext = Mockito.mock(SearchContext.class);

        Mockito.when(searchContext.mapperService()).thenReturn(mapperService);
        Mockito.when(mapperService.fieldType(SNDV))
            .thenReturn(new NumberFieldMapper.NumberFieldType(SNDV, NumberFieldMapper.NumberType.INTEGER));
        Mockito.when(mapperService.fieldType(DV))
            .thenReturn(new NumberFieldMapper.NumberFieldType(DV, NumberFieldMapper.NumberType.INTEGER));
        Mockito.when(mapperService.fieldType(SDV))
            .thenReturn(new NumberFieldMapper.NumberFieldType(SDV, NumberFieldMapper.NumberType.INTEGER));

        // assert that all documents are included if no filters are given
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(Collections.emptyMap()),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of());
        assertEquals(totalDocs, starTreeDocCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter - matches docs
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(0L))))),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter on 3rd field in ordered dimension - matches docs
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(Map.of(DV, List.of(new ExactMatchDimFilter(DV, List.of(0L))))),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(DV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter - does not match docs
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(101L))))),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, 101L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter on 3rd field in ordered dimension - does not match docs
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(-101L))))),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, -101L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // multiple filters - matches docs
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(
                Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(0L))), DV, List.of(new ExactMatchDimFilter(DV, List.of(0L))))
            ),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // no document should match the filter
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(
                Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(0L))), DV, List.of(new ExactMatchDimFilter(DV, List.of(-11L))))
            ),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, -11L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // Only the first filter should match some documents, second filter matches none
        starTreeDocCount = getDocCountFromStarTree(
            starTreeDocValuesReader,
            new StarTreeFilter(
                Map.of(SNDV, List.of(new ExactMatchDimFilter(SNDV, List.of(0L))), DV, List.of(new ExactMatchDimFilter(DV, List.of(-100L))))
            ),
            context,
            searchContext
        );
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, -100L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // non-dimension fields in filter - should throw IllegalArgumentException
        expectThrows(
            IllegalStateException.class,
            () -> getDocCountFromStarTree(
                starTreeDocValuesReader,
                new StarTreeFilter(Map.of(FIELD_NAME, List.of(new ExactMatchDimFilter(FIELD_NAME, List.of(0L))))),
                context,
                searchContext
            )
        );

        if (skipStarNodeCreationForSDVDimension == true) {
            // Documents are not indexed
            starTreeDocCount = getDocCountFromStarTree(
                starTreeDocValuesReader,
                new StarTreeFilter(Map.of(SDV, List.of(new ExactMatchDimFilter(SDV, List.of(4L))))),
                context,
                searchContext
            );
            docCount = getDocCount(docs, Map.of(SDV, 4L));
            assertEquals(1, docCount);
            assertEquals(docCount, starTreeDocCount);
        } else {
            // Documents are indexed
            starTreeDocCount = getDocCountFromStarTree(
                starTreeDocValuesReader,
                new StarTreeFilter(Map.of(SDV, List.of(new ExactMatchDimFilter(SDV, List.of(4L))))),
                context,
                searchContext
            );
            docCount = getDocCount(docs, Map.of(SDV, 4L));
            assertEquals(0, docCount);
            assertEquals(docCount, starTreeDocCount);
        }

        ir.close();
        directory.close();
    }

    // Counts the documents having field SNDV & applied filters
    private long getDocCount(List<Document> documents, Map<String, Long> filters) {
        long count = 0;
        for (Document doc : documents) {
            // Check if SNDV field is present
            IndexableField sndvField = doc.getField(SNDV);
            if (sndvField == null) continue; // Skip if SNDV is not present

            // Apply filters if provided
            if (!filters.isEmpty()) {
                boolean matches = filters.entrySet().stream().allMatch(entry -> {
                    IndexableField field = doc.getField(entry.getKey());
                    return field != null && field.numericValue().longValue() == entry.getValue();
                });
                if (!matches) continue;
            }

            // Increment count if the document passes all conditions
            count++;
        }
        return count;
    }

    // Returns count of documents in the star tree having field SNDV & applied filters
    private long getDocCountFromStarTree(
        CompositeIndexReader starTreeDocValuesReader,
        StarTreeFilter starTreeFilter,
        LeafReaderContext context,
        SearchContext searchContext
    ) throws IOException {
        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(context, starTree);
        FixedBitSet filteredValues = StarTreeTraversalUtil.getStarTreeResult(starTreeValues, starTreeFilter, searchContext);

        SortedNumericStarTreeValuesIterator valuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues.getMetricValuesIterator(
            StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(),
                SNDV,
                MetricStat.VALUE_COUNT.getTypeName()
            )
        );

        long docCount = 0;
        int numBits = filteredValues.length();
        if (numBits > 0) {
            for (int bit = filteredValues.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = (bit + 1 < numBits)
                ? filteredValues.nextSetBit(bit + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {

                // Assert that we can advance to the document ID in the values iterator
                boolean canAdvance = valuesIterator.advanceExact(bit);
                assert canAdvance : "Cannot advance to document ID " + bit + " in values iterator.";

                // Iterate over values for the current document ID
                for (int i = 0, count = valuesIterator.entryValueCount(); i < count; i++) {
                    long value = valuesIterator.nextValue();
                    // Assert that the value is as expected using the provided consumer
                    docCount += value;
                }
            }
        }
        return docCount;
    }

    public static XContentBuilder getExpandedMapping(
        int maxLeafDocs,
        boolean skipStarNodeCreationForSDVDimension,
        LinkedHashMap<String, String> dimensionNameAndType,
        Map<String, String> metricFieldNameAndType
    ) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", maxLeafDocs);
            if (skipStarNodeCreationForSDVDimension) {
                b.startArray("skip_star_node_creation_for_dimensions");
                b.value("sdv");
                b.endArray();
            }
            // FIXME : Change to take dimension order and other inputs as method params.
            // FIXME : Create default constants for the existing so other can call easily.
            b.startArray("ordered_dimensions");
            for (String dimension : dimensionNameAndType.keySet()) {
                b.startObject();
                b.field("name", dimension);
                b.endObject();
            }
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "field");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("avg");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", "sndv");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("avg");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            for (String dimension : dimensionNameAndType.keySet()) {
                b.startObject(dimension);
                b.field("type", dimensionNameAndType.get(dimension));
                b.endObject();
            }
            for (String metricField : metricFieldNameAndType.keySet()) {
                b.startObject(metricField);
                b.field("type", metricFieldNameAndType.get(metricField));
                b.endObject();
            }
            b.endObject();
        });
    }
}
