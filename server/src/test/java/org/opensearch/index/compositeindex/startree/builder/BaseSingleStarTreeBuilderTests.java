/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.BaseSingleStarTreeBuilder;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.StarTreeFieldSpec;
import org.opensearch.index.compositeindex.startree.aggregators.MetricTypeFieldPair;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocValues;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocument;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;

public class BaseSingleStarTreeBuilderTests extends OpenSearchTestCase {

    private static BaseSingleStarTreeBuilder builder;
    private static List<Dimension> dimensionsOrder;
    private static List<String> fields = List.of(
        "field1",
        "field2",
        "field3",
        "field4",
        "field5",
        "field6",
        "field7",
        "field8",
        "field9",
        "field10"
    );
    private static List<Metric> metrics;
    private static Directory directory;
    private static FieldInfo[] fieldsInfo;

    @BeforeClass
    public static void setup() throws IOException {

        dimensionsOrder = List.of(new Dimension("field1"), new Dimension("field3"), new Dimension("field5"), new Dimension("field8"));
        metrics = List.of(new Metric("field2", List.of(MetricType.SUM)), new Metric("field4", List.of(MetricType.SUM)));

        CompositeField compositeField = new CompositeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldSpec(1, Set.of("field8"), StarTreeFieldSpec.StarTreeBuildMode.ON_HEAP)
        );
        DocValuesConsumer docValuesConsumer = mock(DocValuesConsumer.class);
        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);
        directory = newFSDirectory(createTempDir());
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            5,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];

        for (int i = 0; i < fieldsInfo.length; i++) {
            fieldsInfo[i] = new FieldInfo(
                fields.get(i),
                i,
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                DocValuesType.SORTED_NUMERIC,
                -1,
                Collections.emptyMap(),
                0,
                0,
                0,
                0,
                VectorEncoding.FLOAT32,
                VectorSimilarityFunction.EUCLIDEAN,
                false,
                false
            );
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        final SegmentWriteState state = new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            fieldInfos,
            null,
            newIOContext(random())
        );

        builder = new BaseSingleStarTreeBuilder(compositeField, docValuesProducer, docValuesConsumer, state) {
            @Override
            public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {}

            @Override
            public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
                return null;
            }

            @Override
            public long getDimensionValue(int docId, int dimensionId) throws IOException {
                return 0;
            }

            @Override
            public Iterator<StarTreeDocument> sortAndAggregateSegmentStarTreeDocument(int numDocs) throws IOException {
                return null;
            }

            @Override
            public Iterator<StarTreeDocument> generateStarTreeForStarNode(int startDocId, int endDocId, int dimensionId)
                throws IOException {
                return null;
            }

            @Override
            public void build(List<StarTreeDocValues> starTreeDocValues) throws IOException {

            }
        };
    }

    public void test_generateAggregationFunctionColumnPairs() throws IOException {
        List<MetricTypeFieldPair> metricTypeFieldPairs = builder.generateAggregationFunctionColumnPairs();
        List<MetricTypeFieldPair> expectedMetricTypeFieldPairs = List.of(
            new MetricTypeFieldPair(MetricType.SUM, "field2"),
            new MetricTypeFieldPair(MetricType.SUM, "field4")
        );
        assertEquals(metricTypeFieldPairs, expectedMetricTypeFieldPairs);
    }

    public void test_mergeStarTreeDocument() {
        StarTreeDocument starTreeDocument1 = new StarTreeDocument(new long[] { 1, 3, 5, 8 }, new Double[] { 4.0, 8.0 });
        StarTreeDocument starTreeDocument2 = new StarTreeDocument(new long[] { 1, 3, 5, 8 }, new Double[] { 10.0, 6.0 });

        StarTreeDocument expectedeMergedStarTreeDocument = new StarTreeDocument(new long[] { 1, 3, 5, 8 }, new Double[] { 14.0, 14.0 });
        StarTreeDocument mergedStarTreeDocument = builder.mergeStarTreeDocument(starTreeDocument1, starTreeDocument2);

        assertEquals(mergedStarTreeDocument.metrics[0], expectedeMergedStarTreeDocument.metrics[0]);
        assertEquals(mergedStarTreeDocument.metrics[1], expectedeMergedStarTreeDocument.metrics[1]);
    }

    public void test_mergeStarTreeDocument_nullAggregatedStarTreeDocument() {
        StarTreeDocument starTreeDocument = new StarTreeDocument(new long[] { 1, 3, 5, 8 }, new Double[] { 10.0, 6.0 });

        StarTreeDocument expectedeMergedStarTreeDocument = new StarTreeDocument(new long[] { 1, 3, 5, 8 }, new Double[] { 10.0, 6.0 });
        StarTreeDocument mergedStarTreeDocument = builder.mergeStarTreeDocument(null, starTreeDocument);

        assertEquals(mergedStarTreeDocument.metrics[0], expectedeMergedStarTreeDocument.metrics[0]);
        assertEquals(mergedStarTreeDocument.metrics[1], expectedeMergedStarTreeDocument.metrics[1]);
    }

    // Test could not be added due to package private classes
    // public void test_getDocValuesWriter() {
    // assertTrue(
    // builder.getDocValuesWriter(DocValuesType.SORTED_SET, fieldsInfo[0], Counter.newCounter()) instanceof SortedSetDocValuesWriter
    // );
    // assertTrue(
    // builder.getDocValuesWriter(
    // DocValuesType.SORTED_NUMERIC,
    // fieldsInfo[0],
    // Counter.newCounter()
    // ) instanceof SortedNumericDocValuesWriter
    // );
    // }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
