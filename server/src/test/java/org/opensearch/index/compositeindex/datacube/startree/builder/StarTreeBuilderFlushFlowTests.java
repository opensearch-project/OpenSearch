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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Rounding;
import org.opensearch.common.time.DateUtils;
import org.opensearch.index.codec.composite.LuceneDocValuesConsumerFactory;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.DimensionDataType;
import org.opensearch.index.compositeindex.datacube.IpDimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.UnsignedLongDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.DimensionConfig;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.search.aggregations.metrics.CompensatedSum;

import java.io.IOException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getSortedNumericMock;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getSortedSetMock;
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
            4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
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
                    ((CompensatedSum) starTreeDocument.metrics[0]).value(),
                    0
                );
                assertEquals(1L, starTreeDocument.metrics[1]);
            } else {
                assertEquals(150D, ((CompensatedSum) starTreeDocument.metrics[0]).value(), 0);
                assertEquals(6L, starTreeDocument.metrics[1]);
            }
        }
        assertEquals(13, count);
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DimensionConfig> docValues = new LinkedHashMap<>();
        docValues.put("field1", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
        docValues.put("field3", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
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

    public void testFlushFlowAggregatedDocs() throws IOException {
        // Create lists to hold the dimension and metric values from the log data
        List<Long> statusDimList = List.of(
            200L,
            200L,
            200L,
            404L,
            404L,
            404L,
            500L,
            500L,
            500L,
            301L,
            301L,
            301L,
            200L,
            404L,
            500L,
            503L,
            503L,
            503L,
            200L,
            404L
        );

        List<Long> clientIpDimList = List.of(
            36L,
            36L,
            32L,
            39L,
            39L,
            36L,
            38L,
            38L,
            32L,
            34L,
            34L,
            33L,
            36L,
            39L,
            38L,
            32L,
            32L,
            35L,
            36L,
            39L
        );

        List<Long> sizeDimList = List.of(
            getLongFromDouble(81.0),
            getLongFromDouble(81.0),
            getLongFromDouble(81.0),
            getLongFromDouble(15.0),
            getLongFromDouble(15.0),
            getLongFromDouble(15.0),
            getLongFromDouble(50.0),
            getLongFromDouble(50.0),
            getLongFromDouble(50.0),
            getLongFromDouble(23.0),
            getLongFromDouble(23.0),
            getLongFromDouble(23.0),
            getLongFromDouble(81.0),
            getLongFromDouble(15.0),
            getLongFromDouble(50.0),
            getLongFromDouble(60.0),
            getLongFromDouble(60.0),
            getLongFromDouble(60.0),
            getLongFromDouble(81.0),
            getLongFromDouble(15.0)
        );

        List<Long> timestampList = List.of(
            86400000000L,
            86500000000L,
            86600000000L,
            86700000000L,
            86800000000L,
            86900000000L,
            87000000000L,
            87100000000L,
            87200000000L,
            87300000000L,
            87400000000L,
            87500000000L,
            87600000000L,
            87700000000L,
            87800000000L,
            87900000000L,
            88000000000L,
            88100000000L,
            88200000000L,
            88300000000L
        );

        List<Long> statusMetricsList = List.of(
            getLongFromDouble(200L),
            getLongFromDouble(200L),
            getLongFromDouble(200L),
            getLongFromDouble(404L),
            getLongFromDouble(404L),
            getLongFromDouble(404L),
            getLongFromDouble(500L),
            getLongFromDouble(500L),
            getLongFromDouble(500L),
            getLongFromDouble(301L),
            getLongFromDouble(301L),
            getLongFromDouble(301L),
            getLongFromDouble(200L),
            getLongFromDouble(404L),
            getLongFromDouble(500L),
            getLongFromDouble(503L),
            getLongFromDouble(503L),
            getLongFromDouble(503L),
            getLongFromDouble(200L),
            getLongFromDouble(404L)
        );

        List<Integer> docsWithField = IntStream.range(0, 20).boxed().collect(Collectors.toList());

        List<DateTimeUnitRounding> intervals = new ArrayList<>();
        intervals.add(new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH));
        intervals.add(new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR));

        // Setup the star tree field with dimensions and metrics
        compositeField = new StarTreeField(
            "sf",
            List.of(
                new NumericDimension("field1"),
                new NumericDimension("field3"),
                new NumericDimension("field5"),
                new DateDimension("field7", intervals, DateFieldMapper.Resolution.MILLISECONDS)
            ),
            List.of(new Metric("field2", List.of(MetricStat.SUM, MetricStat.VALUE_COUNT, MetricStat.AVG))),
            new StarTreeFieldConfiguration(1, Set.of("field1"), getBuildMode())
        );

        // Create document value iterators
        SortedNumericStarTreeValuesIterator statusIter = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(statusDimList, docsWithField)
        );
        SortedNumericStarTreeValuesIterator clientIpIter = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(clientIpDimList, docsWithField)
        );
        SortedNumericStarTreeValuesIterator sizeIter = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(sizeDimList, docsWithField)
        );
        SortedNumericStarTreeValuesIterator timestampIter = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(timestampList, docsWithField)
        );

        SortedNumericStarTreeValuesIterator statusMetricsList1 = new SortedNumericStarTreeValuesIterator(
            getSortedNumericMock(statusMetricsList, docsWithField)
        );

        // Initialize builder
        writeState = getWriteState(20, writeState.segmentInfo.getId());
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);

        // Process documents
        SequentialDocValuesIterator[] dimDvs = {
            new SequentialDocValuesIterator(statusIter),
            new SequentialDocValuesIterator(clientIpIter),
            new SequentialDocValuesIterator(sizeIter),
            new SequentialDocValuesIterator(timestampIter) };

        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.sortAndAggregateSegmentDocuments(
            dimDvs,
            List.of(new SequentialDocValuesIterator(statusMetricsList1))
        );

        // Build star tree
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            4096,
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
        );

        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        // Validate results
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();

        StarTreeDocument rootDoc = starTreeDocuments.getLast();
        assertNull(rootDoc.dimensions[0]);
        assertNull(rootDoc.dimensions[1]);
        assertNull(rootDoc.dimensions[2]);
        assertNull(rootDoc.dimensions[3]);
        assertEquals(7432.0, ((CompensatedSum) rootDoc.metrics[0]).value(), 0);
        assertEquals(59, starTreeDocuments.size());

        // Assert all documents by formulating the matching docs based on the value of resultant documents
        // For example : [200, null, 4635400285215260672, null, null] | [1000.0 0.0]
        // We will filter documents that match status = 200 and size = 4635400285215260672 and perform sum in java
        // and match against the resultant metric
        for (StarTreeDocument doc : starTreeDocuments) {
            List<Integer> matchingIndices = IntStream.range(0, statusDimList.size()).boxed().collect(Collectors.toList());

            // Filter by status if not null
            if (doc.dimensions[0] != null) {
                matchingIndices.removeIf(i -> !statusDimList.get(i).equals(doc.dimensions[0]));
            }

            // Filter by clientip if not null
            if (doc.dimensions[1] != null) {
                matchingIndices.removeIf(i -> !clientIpDimList.get(i).equals(doc.dimensions[1]));
            }

            // Filter by size if not null
            if (doc.dimensions[2] != null) {
                matchingIndices.removeIf(i -> !sizeDimList.get(i).equals(doc.dimensions[2]));
            }

            // Filter by timestamp day if not null
            if (doc.dimensions[3] != null) {
                matchingIndices.removeIf(i -> {
                    long timestamp = timestampList.get(i);
                    long dayRoundedTimestamp = DateUtils.roundFloor(
                        timestamp,
                        ChronoField.DAY_OF_MONTH.getBaseUnit().getDuration().toMillis()
                    );

                    // Match if either day or month rounding matches
                    return doc.dimensions[3] != dayRoundedTimestamp;
                });
            }

            // Filter by timestamp month if not null
            if (doc.dimensions[4] != null) {
                matchingIndices.removeIf(i -> {
                    long timestamp = timestampList.get(i);
                    long monthRoundedTimestamp = DateUtils.roundMonthOfYear(timestamp);

                    // Match if either day or month rounding matches
                    return monthRoundedTimestamp != doc.dimensions[4];
                });
            }
            double expectedSum = matchingIndices.stream()
                .mapToDouble(i -> NumericUtils.sortableLongToDouble(statusMetricsList.get(i)))
                .sum();
            // Assert
            assertEquals(expectedSum, ((CompensatedSum) (doc.metrics[0])).value(), 0);
            assertEquals(matchingIndices.size(), (long) (doc.metrics[1]));
        }

        validateStarTree(builder.getRootNode(), 5, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
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
            4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
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
                assertEquals(starTreeDocument.dimensions[1] * 10.0, ((CompensatedSum) starTreeDocument.metrics[0]).value(), 0);
                assertEquals(1L, starTreeDocument.metrics[1]);
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        LinkedHashMap<String, DimensionConfig> docValues = new LinkedHashMap<>();
        docValues.put("field1", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
        docValues.put("field3", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
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

    public void testFlushFlowWithUnsignedLongDimensions() throws IOException {
        List<Long> dimList = List.of(0L, -1L, 9223372036854775806L, 4987L, -9223372036854775807L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5);
        List<Long> dimList2 = List.of(0L, -1L, 2L, 9223372036854775806L, 4987L, -9223372036854775807L);
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

        compositeField = getStarTreeFieldWithUnsignedLongField();
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

        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();

        /*
         Asserting following dim / metrics [ dim1, dim2 / Sum [metric], count [metric] ]
         [0, 0] | [0.0, 1]
         [4987, 4987] | [40.0, 1]
         [9223372036854775806, 9223372036854775806] | [30.0, 1]
         [-9223372036854775807, -9223372036854775807] | [50.0, 1]
         [-1, -1] | [10.0, 1]
         [null, 2] | [20.0, 1]
         */
        Object[][] expectedSortedDimensions = {
            { 0L, 0L },
            { 4987L, 4987L },
            { 9223372036854775806L, 9223372036854775806L },
            { -9223372036854775807L, -9223372036854775807L },
            { -1L, -1L },
            { null, 2L } };

        double[] expectedSumMetrics = { 0.0, 40.0, 30.0, 50.0, 10.0, 20.0 };
        long expectedCountMetric = 1L;

        int count = 0;
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            if (count < 6) {
                assertEquals(expectedSumMetrics[count], ((CompensatedSum) starTreeDocument.metrics[0]).value(), 0);
                assertEquals(expectedCountMetric, starTreeDocument.metrics[1]);

                Long dim1 = starTreeDocument.dimensions[0];
                Long dim2 = starTreeDocument.dimensions[1];
                assertEquals(expectedSortedDimensions[count][0], dim1);
                assertEquals(expectedSortedDimensions[count][1], dim2);
            }
            count++;
        }
        assertEquals(13, count);
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DimensionConfig> docValues = new LinkedHashMap<>();
        docValues.put("field1", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.UNSIGNED_LONG));
        docValues.put("field3", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.UNSIGNED_LONG));
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

        // validateStarTreeFileFormats(
        // builder.getRootNode(),
        // builder.getStarTreeDocuments().size(),
        // starTreeMetadata,
        // builder.getStarTreeDocuments()
        // );

        // TODO: Fix this post 2.19 [Handling search for unsigned-long as part of star-tree]
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
            4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
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
                ((CompensatedSum) starTreeDocument.metrics[0]).value(),
                0
            );
        }
        validateStarTree(builder.getRootNode(), 2, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        LinkedHashMap<String, DimensionConfig> map = new LinkedHashMap<>();
        map.put("field1", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
        map.put("field3", new DimensionConfig(DocValuesType.SORTED_NUMERIC, DimensionDataType.LONG));
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
            4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
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
            assertEquals(starTreeDocument.dimensions[3] * 1 * 10.0, ((CompensatedSum) starTreeDocument.metrics[1]).value(), 0);
            assertEquals(1L, starTreeDocument.metrics[0]);
        }
        assertEquals(6, count);
        builder.build(starTreeDocumentsList.iterator(), new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 3, 10, builder.getStarTreeDocuments());
    }

    public void testFlushFlowForKeywords() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 5L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 5);
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

        compositeField = getStarTreeFieldWithKeywordField(random().nextBoolean());
        SortedSetStarTreeValuesIterator d1sndv = new SortedSetStarTreeValuesIterator(getSortedSetMock(dimList, docsWithField));
        SortedSetStarTreeValuesIterator d2sndv = new SortedSetStarTreeValuesIterator(getSortedSetMock(dimList2, docsWithField2));
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
         [2, 2] | [20.0, 1]
         [3, 3] | [30.0, 1]
         [4, 4] | [40.0, 1]
         [5, 5] | [50.0, 1]
         */

        SegmentWriteState w = getWriteState(DocIdSetIterator.NO_MORE_DOCS, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            w,
            4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        Map<String, SortedSetDocValues> dv = new LinkedHashMap<>();
        dv.put("field1", getSortedSetMock(dimList, docsWithField));
        dv.put("field3", getSortedSetMock(dimList2, docsWithField2));
        builder.setFlushSortedSetDocValuesMap(dv);
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
                    ((CompensatedSum) starTreeDocument.metrics[0]).value(),
                    0
                );
                assertEquals(1L, starTreeDocument.metrics[1]);
            } else {
                assertEquals(150D, ((CompensatedSum) starTreeDocument.metrics[0]).value(), 0);
                assertEquals(6L, starTreeDocument.metrics[1]);
            }
        }
        assertEquals(13, count);
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DimensionConfig> docValues = new LinkedHashMap<>();
        docValues.put("field1", new DimensionConfig(DocValuesType.SORTED_SET, DimensionDataType.LONG));
        docValues.put("field3", new DimensionConfig(DocValuesType.SORTED_SET, DimensionDataType.LONG));
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

    private StarTreeField getStarTreeFieldWithUnsignedLongField() {
        Dimension d1 = new UnsignedLongDimension("field1");
        Dimension d2 = new UnsignedLongDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        Metric m2 = new Metric("field2", List.of(MetricStat.VALUE_COUNT));
        Metric m3 = new Metric("field2", List.of(MetricStat.AVG));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1, m2, m3);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1000, new HashSet<>(), getBuildMode());
        return new StarTreeField("sf", dims, metrics, c);
    }

    private StarTreeField getStarTreeFieldWithKeywordField(boolean isIp) {
        Dimension d1 = isIp ? new IpDimension("field1") : new OrdinalDimension("field1");
        Dimension d2 = isIp ? new IpDimension("field3") : new OrdinalDimension("field3");
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
