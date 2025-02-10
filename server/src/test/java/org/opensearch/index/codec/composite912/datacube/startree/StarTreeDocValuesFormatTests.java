/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite912.datacube.startree;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.STAR_TREE_DOCS_COUNT;
import static org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils.assertStarTreeDocuments;

/**
 * Star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class StarTreeDocValuesFormatTests extends AbstractStarTreeDVFormatTests {

    public StarTreeDocValuesFormatTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        super(buildMode);
    }

    public void testStarTreeDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("unsignedLongDimension", 10));
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv1", 1));
        doc.add(new SortedNumericDocValuesField("field1", -1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("unsignedLongDimension", 10));
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv1", 1));
        doc.add(new SortedNumericDocValuesField("field1", -1));
        iw.addDocument(doc);
        doc = new Document();
        iw.forceMerge(1);
        doc.add(new SortedNumericDocValuesField("unsignedLongDimension", -20));
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv1", 2));
        doc.add(new SortedNumericDocValuesField("field1", -2));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("unsignedLongDimension", -20));
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv1", 2));
        doc.add(new SortedNumericDocValuesField("field1", -2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * unsignedLongDimension sndv dv field
         * [10, 1,  1, -1]
         * [10, 1,  1, -1]
         * [-20, 2,  2, -2]
         * [-20, 2,  2, -2]
         */
        // Star tree documents
        /**
         * unsignedLongDimension sndv dv | [ sum, value_count, min, max[field]] , [ sum, value_count, min, max[sndv]], doc_count
         * [10, 1, 1] | [-2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0]
         * [-20, 2, 2] | [-4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0]
         * [null, 1, 1] | [-2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0]
         * [null, 2, 2] | [-4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0]
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[4];
        expectedStarTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 10L, 1L, 1L },
            new Double[] { -2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0 }
        );
        expectedStarTreeDocuments[1] = new StarTreeDocument(
            new Long[] { -20L, 2L, 2L },
            new Double[] { -4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0 }
        );
        expectedStarTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, 1L, 1L },
            new Double[] { -2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0 }
        );
        expectedStarTreeDocuments[3] = new StarTreeDocument(
            new Long[] { null, 2L, 2L },
            new Double[] { -4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0 }
        );

        for (LeafReaderContext context : ir.leaves()) {
            SegmentReader reader = Lucene.segmentReader(context.reader());
            CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
            List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();

            for (CompositeIndexFieldInfo compositeIndexFieldInfo : compositeIndexFields) {
                StarTreeValues starTreeValues = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(compositeIndexFieldInfo);
                StarTreeDocument[] starTreeDocuments = StarTreeTestUtils.getSegmentsStarTreeDocuments(
                    List.of(starTreeValues),
                    List.of(
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG
                    ),
                    reader.maxDoc()
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    public void testStarTreeDocValuesWithDeletions() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        conf.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        conf.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        int iterations = 3;
        Map<String, Integer> map = new HashMap<>();
        List<String> allIds = new ArrayList<>();
        for (int iter = 0; iter < iterations; iter++) {
            // Add 10 documents
            for (int i = 0; i < 10; i++) {
                String id = String.valueOf(random().nextInt() + i);
                allIds.add(id);
                Document doc = new Document();
                doc.add(new StringField("_id", id, Field.Store.YES));
                int fieldValue = random().nextInt(5) + 1;
                doc.add(new SortedNumericDocValuesField("field", fieldValue));

                int sndvValue = random().nextInt(3);

                doc.add(new SortedNumericDocValuesField("sndv", sndvValue));
                int dvValue = random().nextInt(3);

                doc.add(new SortedNumericDocValuesField("dv", dvValue));
                map.put(sndvValue + "-" + dvValue, fieldValue + map.getOrDefault(sndvValue + "-" + dvValue, 0));
                doc.add(new NumericDocValuesField("field-ndv", fieldValue));

                iw.addDocument(doc);
            }
            iw.flush();
        }
        iw.commit();
        // Update random number of documents
        int docsToDelete = random().nextInt(9); // Delete up to 9 documents
        for (int i = 0; i < docsToDelete; i++) {
            if (!allIds.isEmpty()) {
                String idToDelete = allIds.remove(random().nextInt(allIds.size() - 1));
                Document doc = new Document();
                doc.add(new NumericDocValuesField("field-ndv", 1L));
                iw.w.softUpdateDocuments(
                    new Term("_id", idToDelete),
                    List.of(doc),
                    new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1)
                );
                allIds.remove(idToDelete);
            }
        }
        iw.flush();
        iw.commit();
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = DirectoryReader.open(directory);
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Assert star tree documents
        for (LeafReaderContext context : ir.leaves()) {
            SegmentReader reader = Lucene.segmentReader(context.reader());
            CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
            List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();

            for (CompositeIndexFieldInfo compositeIndexFieldInfo : compositeIndexFields) {
                StarTreeValues starTreeValues = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(compositeIndexFieldInfo);
                StarTreeDocument[] actualStarTreeDocuments = StarTreeTestUtils.getSegmentsStarTreeDocuments(
                    List.of(starTreeValues),
                    List.of(
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.DOUBLE,
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                for (StarTreeDocument starDoc : actualStarTreeDocuments) {
                    Long sndvVal = null;
                    if (starDoc.dimensions[0] != null) {
                        sndvVal = starDoc.dimensions[0];
                    }
                    Long dvVal = null;
                    if (starDoc.dimensions[1] != null) {
                        dvVal = starDoc.dimensions[1];
                    }
                    if (starDoc.metrics[0] != null) {
                        double metric = (double) starDoc.metrics[0];
                        if (map.containsKey(sndvVal + "-" + dvVal)) {
                            assertEquals((long) map.get(sndvVal + "-" + dvVal), (long) metric);
                        }
                    }
                }
            }
        }
        ir.close();
        directory.close();
    }

    @Override
    protected XContentBuilder getMapping() throws IOException {
        return getExpandedMapping();
    }

    public static XContentBuilder getExpandedMapping() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 1);
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "unsignedLongDimension"); // UnsignedLongDimension
            b.endObject();
            b.startObject();
            b.field("name", "sndv");
            b.endObject();
            b.startObject();
            b.field("name", "dv1");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "field1");
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
            b.startObject("dv1");
            b.field("type", "integer");
            b.endObject();
            b.startObject("field1");
            b.field("type", "integer");
            b.endObject();
            b.startObject("unsignedLongDimension");
            b.field("type", "unsigned_long");
            b.endObject();
            b.endObject();
        });
    }
}
