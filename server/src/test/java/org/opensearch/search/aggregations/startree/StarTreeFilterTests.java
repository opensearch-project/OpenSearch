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
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
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
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite912.datacube.startree.StarTreeDocValuesFormatTests;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.search.aggregations.AggregatorTestCase;
import org.opensearch.search.startree.StarTreeFilter;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.codec.composite912.datacube.startree.AbstractStarTreeDVFormatTests.topMapping;

public class StarTreeFilterTests extends AggregatorTestCase {

    private static final String FIELD_NAME = "field";
    private static final String SNDV = "sndv";
    private static final String SDV = "sdv";
    private static final String DV = "dv";

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
                getExpandedMapping(maxLeafDoc, skipStarNodeCreationForSDVDimension)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new Composite912Codec(Lucene912Codec.Mode.BEST_SPEED, mapperService, testLogger);
    }

    public void testStarTreeFilterWithNoDocsInSVDField() throws IOException {
        testStarTreeFilter(5, true);
    }

    public void testStarTreeFilterWithDocsInSVDFieldButNoStarNode() throws IOException {
        testStarTreeFilter(10, false);
    }

    private void testStarTreeFilter(int maxLeafDoc, boolean skipStarNodeCreationForSDVDimension) throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setCodec(getCodec(maxLeafDoc, skipStarNodeCreationForSDVDimension));
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        int totalDocs = 100;

        List<Document> docs = new ArrayList<>();
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

        DirectoryReader ir = DirectoryReader.open(directory);
        initValuesSourceRegistry();
        LeafReaderContext context = ir.leaves().get(0);
        SegmentReader reader = Lucene.segmentReader(context.reader());
        CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();

        long starTreeDocCount, docCount;

        // assert that all documents are included if no filters are given
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(), context);
        docCount = getDocCount(docs, Map.of());
        assertEquals(totalDocs, starTreeDocCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter - matches docs
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SNDV, 0L), context);
        docCount = getDocCount(docs, Map.of(SNDV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter on 3rd field in ordered dimension - matches docs
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(DV, 0L), context);
        docCount = getDocCount(docs, Map.of(DV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter - does not match docs
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SNDV, 101L), context);
        docCount = getDocCount(docs, Map.of(SNDV, 101L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // single filter on 3rd field in ordered dimension - does not match docs
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(DV, -101L), context);
        docCount = getDocCount(docs, Map.of(SNDV, -101L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // multiple filters - matches docs
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SNDV, 0L, DV, 0L), context);
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, 0L));
        assertEquals(1, docCount);
        assertEquals(docCount, starTreeDocCount);

        // no document should match the filter
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SNDV, 0L, DV, -11L), context);
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, -11L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // Only the first filter should match some documents, second filter matches none
        starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SNDV, 0L, DV, -100L), context);
        docCount = getDocCount(docs, Map.of(SNDV, 0L, DV, -100L));
        assertEquals(0, docCount);
        assertEquals(docCount, starTreeDocCount);

        // non-dimension fields in filter - should throw IllegalArgumentException
        expectThrows(
            IllegalArgumentException.class,
            () -> getDocCountFromStarTree(starTreeDocValuesReader, Map.of(FIELD_NAME, 0L), context)
        );

        if (skipStarNodeCreationForSDVDimension == true) {
            // Documents are not indexed
            starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SDV, 4L), context);
            docCount = getDocCount(docs, Map.of(SDV, 4L));
            assertEquals(1, docCount);
            assertEquals(docCount, starTreeDocCount);
        } else {
            // Documents are indexed
            starTreeDocCount = getDocCountFromStarTree(starTreeDocValuesReader, Map.of(SDV, 4L), context);
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
    private long getDocCountFromStarTree(CompositeIndexReader starTreeDocValuesReader, Map<String, Long> filters, LeafReaderContext context)
        throws IOException {
        List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();
        CompositeIndexFieldInfo starTree = compositeIndexFields.get(0);
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(context, starTree);
        FixedBitSet filteredValues = StarTreeFilter.getStarTreeResult(starTreeValues, filters);

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

    public static XContentBuilder getExpandedMapping(int maxLeafDocs, boolean skipStarNodeCreationForSDVDimension) throws IOException {
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
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "sndv");
            b.endObject();
            b.startObject();
            b.field("name", "sdv");
            b.endObject();
            b.startObject();
            b.field("name", "dv");
            b.endObject();
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
            b.startObject("sndv");
            b.field("type", "integer");
            b.endObject();
            b.startObject("sdv");
            b.field("type", "integer");
            b.endObject();
            b.startObject("dv");
            b.field("type", "integer");
            b.endObject();
            b.startObject("field");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }
}
