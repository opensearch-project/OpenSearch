/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite912.datacube.startree;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
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
import java.util.List;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.STAR_TREE_DOCS_COUNT;
import static org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils.assertStarTreeDocuments;

/**
 * Star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class StarTreeKeywordDocValuesFormatTests extends AbstractStarTreeDVFormatTests {

    public StarTreeKeywordDocValuesFormatTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        super(buildMode);
    }

    public void testStarTreeKeywordDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text1")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text2")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text11")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text22")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text1")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text2")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text11")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text22")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree documents
        /**
         SortedSet dv | [ sum, value_count, min, max[sndv]] , doc_count
         [0, 0] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [1, 1] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, 0] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, 1] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, null] | [6.0, 4.0, 1.0, 2.0, 4.0]
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[5];
        expectedStarTreeDocuments[0] = new StarTreeDocument(new Long[] { 0L, 0L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[1] = new StarTreeDocument(new Long[] { 1L, 1L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[2] = new StarTreeDocument(new Long[] { null, 0L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[3] = new StarTreeDocument(new Long[] { null, 1L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[4] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 6.0, 4.0, 1.0, 2.0, 4.0 });

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
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    public void testStarKeywordDocValuesWithMissingDocs() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text2")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text22")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text1")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text2")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text11")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text22")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree documents
        /**
         * sndv dv | [ sum, value_count, min, max[sndv]] , doc_count
         [0, 0] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [1, 1] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [null, 0] | [1.0, 1.0, 1.0, 1.0, 1.0]
         [null, 1] | [1.0, 1.0, 1.0, 1.0, 1.0]
         [null, 0] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, 1] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, null] | [6.0, 4.0, 1.0, 2.0, 4.0]
         [null, null] | [2.0, 2.0, 1.0, 1.0, 2.0]
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[8];
        expectedStarTreeDocuments[0] = new StarTreeDocument(new Long[] { 0L, 0L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[1] = new StarTreeDocument(new Long[] { 1L, 1L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[2] = new StarTreeDocument(new Long[] { null, 0L }, new Double[] { 1.0, 1.0, 1.0, 1.0, 1.0 });
        expectedStarTreeDocuments[3] = new StarTreeDocument(new Long[] { null, 1L }, new Double[] { 1.0, 1.0, 1.0, 1.0, 1.0 });
        expectedStarTreeDocuments[4] = new StarTreeDocument(new Long[] { null, 0L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[5] = new StarTreeDocument(new Long[] { null, 1L }, new Double[] { 3.0, 2.0, 1.0, 2.0, 2.0 });
        expectedStarTreeDocuments[6] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 6.0, 4.0, 1.0, 2.0, 4.0 });
        expectedStarTreeDocuments[7] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 2.0, 2.0, 1.0, 1.0, 2.0 });

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
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    public void testStarKeywordDocValuesWithMissingDocsInSegment() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text1")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text2")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text11")));
        doc.add(new SortedSetDocValuesField("keyword2", new BytesRef("text22")));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree documents
        /**
         * sndv dv | [ sum, value_count, min, max[sndv]] , doc_count
         [0, 0] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [1, 1] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [null, null] | [2.0, 2.0, 1.0, 1.0, 2.0] // This is for missing doc
         [null, 0] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [null, 1] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [null, null] | [2.0, 2.0, 1.0, 1.0, 2.0]
         [null, null] | [6.0, 4.0, 1.0, 2.0, 4.0] // This is star document
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[7];
        expectedStarTreeDocuments[0] = new StarTreeDocument(new Long[] { 0L, 0L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[1] = new StarTreeDocument(new Long[] { 1L, 1L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[2] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 2.0, 2.0, 1.0, 1.0, 2.0 });
        expectedStarTreeDocuments[3] = new StarTreeDocument(new Long[] { null, 0L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[4] = new StarTreeDocument(new Long[] { null, 1L }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[5] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 2.0, 2.0, 1.0, 1.0, 2.0 });
        expectedStarTreeDocuments[6] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 6.0, 4.0, 1.0, 2.0, 4.0 });

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
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    public void testStarKeywordDocValuesWithMissingDocsInAllSegments() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree documents
        /**
         * sndv dv | [ sum, value_count, min, max[sndv]] , doc_count
         [null, null] | [6.0, 4.0, 1.0, 2.0, 4.0]

         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[1];
        expectedStarTreeDocuments[0] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 6.0, 4.0, 1.0, 2.0, 4.0 });

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
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    public void testStarKeywordDocValuesWithMissingDocsInMixedSegments() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedSetDocValuesField("keyword1", new BytesRef("text1")));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree documents
        /**
         * sndv dv | [ sum, value_count, min, max[sndv]] , doc_count
         [0, 0] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [1, 1] | [2.0, 1.0, 2.0, 2.0, 1.0]
         [null, 0] | [1.0, 1.0, 1.0, 1.0, 1.0]
         [null, 1] | [1.0, 1.0, 1.0, 1.0, 1.0]
         [null, 0] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, 1] | [3.0, 2.0, 1.0, 2.0, 2.0]
         [null, null] | [6.0, 4.0, 1.0, 2.0, 4.0]
         [null, null] | [2.0, 2.0, 1.0, 1.0, 2.0]
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[3];
        expectedStarTreeDocuments[0] = new StarTreeDocument(new Long[] { 0L, null }, new Double[] { 2.0, 1.0, 2.0, 2.0, 1.0 });
        expectedStarTreeDocuments[1] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 4.0, 3.0, 1.0, 2.0, 3.0 });
        expectedStarTreeDocuments[2] = new StarTreeDocument(new Long[] { null, null }, new Double[] { 6.0, 4.0, 1.0, 2.0, 4.0 });

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
                        NumberFieldMapper.NumberType.LONG
                    ),
                    Integer.parseInt(starTreeValues.getAttributes().get(STAR_TREE_DOCS_COUNT))
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    @Override
    protected XContentBuilder getExpandedMapping() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 1);
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "keyword1");
            b.endObject();
            b.startObject();
            b.field("name", "keyword2");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
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
            b.startObject("keyword1");
            b.field("type", "keyword");
            b.endObject();
            b.startObject("keyword2");
            b.field("type", "keyword");
            b.endObject();
            b.endObject();
        });
    }
}
