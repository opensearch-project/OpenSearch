/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.codec.composite.LuceneDocValuesConsumerFactory;
import org.opensearch.index.codec.composite.composite99.Composite99DocValuesFormat;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getSortedNumericMock;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.validateStarTree;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.mapper.CompositeMappedFieldType.CompositeFieldType.STAR_TREE;
import static org.mockito.Mockito.mock;

public class StarTreeBuilderFlushFlowTests extends StarTreeBuilderTestCase {

    public StarTreeBuilderFlushFlowTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        super(buildMode);
    }

    public void testFlushFlow() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5);

        List<Long> metricsList = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0)
        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5);

        compositeField = getStarTreeFieldWithMultipleMetrics();
        SortedNumericStarTreeValuesIterator d1sndv = new SortedNumericStarTreeValuesIterator(getSortedNumericMock(dimList, docsWithField));
        SortedNumericStarTreeValuesIterator d2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(dimList2, docsWithField2)
        );
        SortedNumericStarTreeValuesIterator m1sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );
        SortedNumericStarTreeValuesIterator m2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );

        writeState = getWriteState(6, writeState.segmentInfo.getId());
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        SequentialDocValuesIterator[] dimDvs = { new SequentialDocValuesIterator(d1sndv), new SequentialDocValuesIterator(d2sndv) };
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(m1sndv), new SequentialDocValuesIterator(m2sndv))
        );
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [0, 0] | [0.0, 1]
         [1, 1] | [10.0, 1]
         [3, 3] | [30.0, 1]
         [4, 4] | [40.0, 1]
         [5, 5] | [50.0, 1]
         [null, 2] | [20.0, 1]
         */
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        int count = 0;
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            count++;
            if (starTreeDocument.dimensions[1] != null) {
                assertEquals(
                    starTreeDocument.dimensions[0] == null
                        ? starTreeDocument.dimensions[1] * 1 * 10.0
                        : starTreeDocument.dimensions[0] * 10,
                    starTreeDocument.metrics[0]
                );
                assertEquals(1L, starTreeDocument.metrics[1]);
            } else {
                assertEquals(150D, starTreeDocument.metrics[0]);
                assertEquals(6L, starTreeDocument.metrics[1]);
            }
        }
        assertEquals(13, count);
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> docValues = new LinkedHashMap<>();
        docValues.put("field1", DocValuesType.SORTED_NUMERIC);
        docValues.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            VERSION_CURRENT,
            builder.numStarTreeNodes,
            docValues,
            List.of(new Metric("field2", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.AVG))),
            6,
            builder.numStarTreeDocs,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            264
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );

    }

    public void testFlushFlowDimsReverse() throws IOException {
        List<Long> dimList = List.of(5L, 4L, 3L, 2L, 1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(5L, 4L, 3L, 2L, 1L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5);

        List<Long> metricsList = List.of(
            getLongFromDouble(50.0),
            getLongFromDouble(40.0),
            getLongFromDouble(30.0),
            getLongFromDouble(20.0),
            getLongFromDouble(10.0),
            getLongFromDouble(0.0)
        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5);

        compositeField = getStarTreeFieldWithMultipleMetrics();
        SortedNumericStarTreeValuesIterator d1sndv = new SortedNumericStarTreeValuesIterator(getSortedNumericMock(dimList, docsWithField));
        SortedNumericStarTreeValuesIterator d2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(dimList2, docsWithField2)
        );
        SortedNumericStarTreeValuesIterator m1sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );
        SortedNumericStarTreeValuesIterator m2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );

        writeState = getWriteState(6, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        SequentialDocValuesIterator[] dimDvs = { new SequentialDocValuesIterator(d1sndv), new SequentialDocValuesIterator(d2sndv) };
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(m1sndv), new SequentialDocValuesIterator(m2sndv))
        );
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [1, 1] | [10.0, 1]
         [2, 2] | [20.0, 1]
         [3, 3] | [30.0, 1]
         [4, 4] | [40.0, 1]
         [5, 5] | [50.0, 1]
         [null, 0] | [0.0, 1]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(6, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            if (count <= 6) {
                count++;
                if (starTreeDocument.dimensions[0] != null) {
                    assertEquals(count, (long) starTreeDocument.dimensions[0]);
                }
                assertEquals(starTreeDocument.dimensions[1] * 10.0, starTreeDocument.metrics[0]);
                assertEquals(1L, starTreeDocument.metrics[1]);
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        LinkedHashMap<String, DocValuesType> docValues = new LinkedHashMap<>();
        docValues.put("field1", DocValuesType.SORTED_NUMERIC);
        docValues.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            VERSION_CURRENT,
            builder.numStarTreeNodes,
            docValues,
            List.of(new Metric("field2", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.AVG))),
            6,
            builder.numStarTreeDocs,
            1000,
            Set.of(),
            getBuildMode(),
            0,
            264
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testFlushFlowBuild() throws IOException {
        List<Long> dimList = new ArrayList<>(100);
        List<Integer> docsWithField = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            dimList.add((long) i);
            docsWithField.add(i);
        }

        List<Long> dimList2 = new ArrayList<>(100);
        List<Integer> docsWithField2 = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 100; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1, new HashSet<>(), getBuildMode());
        compositeField = new StarTreeField("sf", dims, metrics, c);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);

        writeState = getWriteState(100, writeState.segmentInfo.getId());
        SegmentWriteState consumerWriteState = getWriteState(DocIdSetIterator.NO_MORE_DOCS, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            consumerWriteState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);

        DocValuesProducer d1vp = getDocValuesProducer(d1sndv);
        DocValuesProducer d2vp = getDocValuesProducer(d2sndv);
        DocValuesProducer m1vp = getDocValuesProducer(m1sndv);
        Map<String, DocValuesProducer> fieldProducerMap = Map.of("field1", d1vp, "field3", d2vp, "field2", m1vp);
        builder.build(fieldProducerMap, new AtomicInteger(), docValuesConsumer);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [ metric] ]
         [0, 0] | [0.0]
         [1, 1] | [10.0]
         [2, 2] | [20.0]
         [3, 3] | [30.0]
         [4, 4] | [40.0]
         ....
         [null, 0] | [0.0]
         [null, 1] | [10.0]
         ...
         [null, null] | [49500.0]
         */
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            assertEquals(
                starTreeDocument.dimensions[1] != null ? starTreeDocument.dimensions[1] * 10.0 : 49500.0,
                starTreeDocument.metrics[0]
            );
        }
        validateStarTree(builder.getRootNode(), 2, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 100, 1, 6699);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testFlushFlowWithTimestamps() throws IOException {
        List<Long> dimList = List.of(1655288152000L, 1655288092000L, 1655288032000L, 1655287972000L, 1655288092000L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5);

        List<Long> metricsList = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0)
        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5);

        compositeField = getStarTreeFieldWithDateDimension();
        SortedNumericStarTreeValuesIterator d1sndv = new SortedNumericStarTreeValuesIterator(getSortedNumericMock(dimList, docsWithField));
        SortedNumericStarTreeValuesIterator d2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(dimList2, docsWithField2)
        );
        SortedNumericStarTreeValuesIterator m1sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );
        SortedNumericStarTreeValuesIterator m2sndv = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(metricsList, metricsWithField)
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, getWriteState(6, writeState.segmentInfo.getId()), mapperService);
        SequentialDocValuesIterator[] dimDvs = { new SequentialDocValuesIterator(d1sndv), new SequentialDocValuesIterator(d2sndv) };
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(m1sndv), new SequentialDocValuesIterator(m2sndv))
        );
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [1655287920000, 1655287200000, 1655287200000, 4] | [40.0, 1]
         [1655287980000, 1655287200000, 1655287200000, 3] | [30.0, 1]
         [1655288040000, 1655287200000, 1655287200000, 1] | [10.0, 1]
         [1655288040000, 1655287200000, 1655287200000, 5] | [50.0, 1]
         [1655288100000, 1655287200000, 1655287200000, 0] | [0.0, 1]
         [null, null, null, 2] | [20.0, 1]
         */
        int count = 0;
        List<StarTreeDocument> starTreeDocumentsList = new ArrayList<>();
        starTreeDocumentIterator.forEachRemaining(starTreeDocumentsList::add);
        starTreeDocumentIterator = starTreeDocumentsList.iterator();
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            assertEquals(starTreeDocument.dimensions[3] * 1 * 10.0, starTreeDocument.metrics[1]);
            assertEquals(1L, starTreeDocument.metrics[0]);
        }
        assertEquals(6, count);
        builder.build(starTreeDocumentsList.iterator(), new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 3, 10, builder.getStarTreeDocuments());
    }

    private StarTreeField getStarTreeFieldWithMultipleMetrics() {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        Metric m2 = new Metric("field2", List.of(MetricStat.VALUE_COUNT));
        Metric m3 = new Metric("field2", List.of(MetricStat.AVG));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1, m2, m3);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1000, new HashSet<>(), getBuildMode());
        return new StarTreeField("sf", dims, metrics, c);
    }

    private static DocValuesProducer getDocValuesProducer(SortedNumericDocValues sndv) {
        return new EmptyDocValuesProducer() {
            @Override
            public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
                return sndv;
            }
        };
    }
}
