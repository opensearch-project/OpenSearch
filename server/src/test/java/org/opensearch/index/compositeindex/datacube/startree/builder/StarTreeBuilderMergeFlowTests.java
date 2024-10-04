/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.settings.Settings;
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
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.SEGMENT_DOCS_COUNT;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.getSortedNumericMock;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.traverseStarTree;
import static org.opensearch.index.compositeindex.datacube.startree.builder.BuilderTestsUtils.validateStarTree;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeBuilderMergeFlowTests extends StarTreeBuilderTestCase {

    public StarTreeBuilderMergeFlowTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        super(buildMode);
    }

    public void testMergeFlow() throws IOException {
        List<Long> dimList1 = new ArrayList<>(1000);
        List<Integer> docsWithField1 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList1.add((long) i);
            docsWithField1.add(i);
        }

        List<Long> dimList2 = new ArrayList<>(1000);
        List<Integer> docsWithField2 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
        }

        List<Long> dimList3 = new ArrayList<>(1000);
        List<Integer> docsWithField3 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList3.add((long) i);
            docsWithField3.add(i);
        }

        List<Long> dimList4 = new ArrayList<>(1000);
        List<Integer> docsWithField4 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList4.add((long) i);
            docsWithField4.add(i);
        }

        List<Long> dimList5 = new ArrayList<>(1000);
        List<Integer> docsWithField5 = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            dimList5.add((long) i);
            docsWithField5.add(i);
        }

        List<Long> metricsList = new ArrayList<>(1000);
        List<Integer> metricsWithField = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        List<Long> metricsListValueCount = new ArrayList<>(1000);
        List<Integer> metricsWithFieldValueCount = new ArrayList<>(1000);
        for (int i = 0; i < 1000; i++) {
            metricsListValueCount.add((long) i);
            metricsWithFieldValueCount.add(i);
        }

        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Dimension d3 = new NumericDimension("field5");
        Dimension d4 = new NumericDimension("field8");
        // Dimension d5 = new NumericDimension("field5");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM, MetricStat.AVG, MetricStat.VALUE_COUNT));
        Metric m2 = new Metric("_doc_count", List.of(MetricStat.DOC_COUNT));
        List<Dimension> dims = List.of(d1, d2, d3, d4);
        List<Metric> metrics = List.of(m1, m2);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1, new HashSet<>(), getBuildMode());
        compositeField = new StarTreeField("sf", dims, metrics, c);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues valucountsndv = getSortedNumericMock(metricsListValueCount, metricsWithFieldValueCount);
        SortedNumericDocValues m2sndv = DocValues.emptySortedNumeric();
        Map<String, Supplier<StarTreeValuesIterator>> dimDocIdSetIterators = Map.of(
            "field1",
            () -> new SortedNumericStarTreeValuesIterator(d1sndv),
            "field3",
            () -> new SortedNumericStarTreeValuesIterator(d2sndv),
            "field5",
            () -> new SortedNumericStarTreeValuesIterator(d3sndv),
            "field8",
            () -> new SortedNumericStarTreeValuesIterator(d4sndv)
        );

        Map<String, Supplier<StarTreeValuesIterator>> metricDocIdSetIterators = Map.of(
            "sf_field2_sum_metric",
            () -> new SortedNumericStarTreeValuesIterator(m1sndv),
            "sf_field2_value_count_metric",
            () -> new SortedNumericStarTreeValuesIterator(valucountsndv),
            "sf__doc_count_doc_count_metric",
            () -> new SortedNumericStarTreeValuesIterator(m2sndv)
        );

        StarTreeValues starTreeValues = new StarTreeValues(
            compositeField,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            getAttributes(1000),
            null
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues f2d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues f2d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues f2valucountsndv = getSortedNumericMock(metricsListValueCount, metricsWithFieldValueCount);
        SortedNumericDocValues f2m2sndv = DocValues.emptySortedNumeric();
        Map<String, Supplier<StarTreeValuesIterator>> f2dimDocIdSetIterators = Map.of(
            "field1",
            () -> new SortedNumericStarTreeValuesIterator(f2d1sndv),
            "field3",
            () -> new SortedNumericStarTreeValuesIterator(f2d2sndv),
            "field5",
            () -> new SortedNumericStarTreeValuesIterator(f2d3sndv),
            "field8",
            () -> new SortedNumericStarTreeValuesIterator(f2d4sndv)
        );

        Map<String, Supplier<StarTreeValuesIterator>> f2metricDocIdSetIterators = Map.of(
            "sf_field2_sum_metric",
            () -> new SortedNumericStarTreeValuesIterator(f2m1sndv),
            "sf_field2_value_count_metric",
            () -> new SortedNumericStarTreeValuesIterator(f2valucountsndv),
            "sf__doc_count_doc_count_metric",
            () -> new SortedNumericStarTreeValuesIterator(f2m2sndv)
        );
        StarTreeValues starTreeValues2 = new StarTreeValues(
            compositeField,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            getAttributes(1000),
            null
        );

        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         [0, 0, 0, 0] | [0.0, 2]
         [1, 1, 1, 1] | [20.0, 2]
         [2, 2, 2, 2] | [40.0, 2]
         [3, 3, 3, 3] | [60.0, 2]
         [4, 4, 4, 4] | [80.0, 2]
         [5, 5, 5, 5] | [100.0, 2]
         ...
         [999, 999, 999, 999] | [19980.0]
         */
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            assertEquals(starTreeDocument.dimensions[0] * 20.0, starTreeDocument.metrics[0]);
            assertEquals(2L, starTreeDocument.metrics[1]);
        }
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);

        // Validate the star tree structure
        validateStarTree(builder.getRootNode(), 4, 1, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            1000,
            compositeField.getStarTreeConfig().maxLeafDocs(),
            132165
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlow_randomNumberTypes() throws Exception {

        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder(
            "field1",
            randomFrom(NumberFieldMapper.NumberType.values()),
            false,
            true
        ).build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder(
            "field2",
            randomFrom(NumberFieldMapper.NumberType.values()),
            false,
            true
        ).build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder(
            "field3",
            randomFrom(NumberFieldMapper.NumberType.values()),
            false,
            true
        ).build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        testMergeFlowWithSum();
    }

    public void testMergeFlowWithSum() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0),
            getLongFromDouble(60.0)

        );
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        compositeField = getStarTreeField(MetricStat.SUM);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
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
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Sum [ metric] ]
         * [0, 0] | [0.0]
         * [1, 1] | [20.0]
         * [3, 3] | [60.0]
         * [4, 4] | [80.0]
         * [5, 5] | [100.0]
         * [null, 2] | [40.0]
         * ------------------ We only take non star docs
         * [6,-1] | [120.0]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(6, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                assertEquals(
                    starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 * 10.0 : 40.0,
                    starTreeDocument.metrics[0]
                );
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 6, 1000, 264);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithCount() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
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
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [2]
         [3, 3] | [6]
         [4, 4] | [8]
         [5, 5] | [10]
         [null, 2] | [4]
         ---------------
         [6,-1] | [12]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(6, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                assertEquals(starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 : 4, starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 6, 1000, 264);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );

    }

    public void testMergeFlowNumSegmentsDocs() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, -1L, -1L, -1L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> dimList3 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 2, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        StarTreeField sf = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            sf,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            sf,
            "4"
        );
        builder = getStarTreeBuilder(metaOut, dataOut, sf, getWriteState(4, writeState.segmentInfo.getId()), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [10]
         [6, 6] | [6]
         [7, 7] | [7]
         [8, 8] | [8]
         */
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            if (Objects.equals(starTreeDocument.dimensions[0], 5L)) {
                assertEquals(starTreeDocument.dimensions[0] * 2, starTreeDocument.metrics[0]);
            } else {
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }
        assertEquals(9, count);
    }

    public void testMergeFlowWithMissingDocs() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        writeState = getWriteState(4, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 5] | [5]
         [null, 7] | [7]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(10, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 10, 1000, 363);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithMissingDocsWithZero() throws IOException {
        List<Long> dimList = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField = List.of(0, 1, 2, 6);
        List<Long> dimList2 = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "7"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        writeState = getWriteState(4, writeState.segmentInfo.getId());
        SegmentWriteState consumerWriteState = getWriteState(DocIdSetIterator.NO_MORE_DOCS, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            consumerWriteState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [9]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         [null, null] | [12]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(6, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                if (starTreeDocument.dimensions[0] == null && starTreeDocument.dimensions[1] == null) {
                    assertEquals(12L, (long) starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(7L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == 0) {
                    assertEquals(9L, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 6, 1000, 231);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithMissingDocsWithZeroComplexCase() throws IOException {
        List<Long> dimList = List.of(0L, 0L, 0L, 0L, 0L);
        List<Integer> docsWithField = List.of(0, 1, 2, 6, 8);
        List<Long> dimList2 = List.of(0L, 0L, 0L, 0L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "9"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        writeState = getWriteState(4, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [9]
         [0, null] | [8]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         [null, null] | [19]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(7, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 7) {
                if (starTreeDocument.dimensions[0] == null && starTreeDocument.dimensions[1] == null) {
                    assertEquals(19L, (long) starTreeDocument.metrics[0]);
                    assertEquals(7, count);
                } else if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(7L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[1] == null) {
                    assertEquals(8L, starTreeDocument.metrics[0]);
                } else if (starTreeDocument.dimensions[0] == 0) {
                    assertEquals(9L, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 7, 1000, 231);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithMissingDocsInSecondDim() throws IOException {
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 6L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        writeState = getWriteState(4, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [5, null] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 7] | [7]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(10, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] != null && starTreeDocument.dimensions[0] == 5) {
                    assertEquals(starTreeDocument.dimensions[0], starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 10, 1000, 363);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithDocsMissingAtTheEnd() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [5]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 5] | [5]
         [null, 7] | [7]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(10, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 10) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }

        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 10, 1000, 363);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithEmptyFieldsInOneSegment() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            DocValues.emptySortedNumeric(),
            DocValues.emptySortedNumeric(),
            DocValues.emptySortedNumeric(),
            compositeField,
            "0"
        );
        writeState = getWriteState(0, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [null, 5] | [5]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(6, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 6) {
                if (starTreeDocument.dimensions[0] == null) {
                    assertEquals(5L, (long) starTreeDocument.dimensions[1]);
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 6, 1000, 264);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithDuplicateDimensionValues() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }

        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList2.add((long) i);
                docsWithField2.add(i * 5 + j);
            }
        }

        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }
        List<Long> docCountMetricsList = new ArrayList<>(100);
        List<Integer> docCountMetricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            docCountMetricsList.add(i * 10L);
            docCountMetricsWithField.add(i);
        }

        compositeField = getStarTreeFieldWithDocCount(1, true);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            docCountMetricsList,
            docCountMetricsWithField,
            compositeField
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            docCountMetricsList,
            docCountMetricsWithField,
            compositeField
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(401, starTreeDocuments.size());
        int count = 0;
        double sum = 0;
        /**
         401 docs get generated
         [0, 0, 0, 0] | [200.0, 10]
         [1, 1, 1, 1] | [700.0, 10]
         [2, 2, 2, 2] | [1200.0, 10]
         [3, 3, 3, 3] | [1700.0, 10]
         [4, 4, 4, 4] | [2200.0, 10]
         .....
         [null, null, null, 99] | [49700.0, 10]
         [null, null, null, null] | [2495000.0, 1000]
         */
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            if (starTreeDocument.dimensions[3] == null) {
                assertEquals(sum, starTreeDocument.metrics[0]);
                assertEquals(2495000L, (long) starTreeDocument.metrics[1]);
            } else {
                if (starTreeDocument.dimensions[0] != null) {
                    sum += (double) starTreeDocument.metrics[0];
                }
                assertEquals(starTreeDocument.dimensions[3] * 500 + 200.0, starTreeDocument.metrics[0]);
                assertEquals(starTreeDocument.dimensions[3] * 500 + 200L, (long) starTreeDocument.metrics[1]);

            }
            count++;
        }
        assertEquals(401, count);
        validateStarTree(builder.getRootNode(), 4, compositeField.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());
        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            100,
            1,
            13365
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithMaxLeafDocs() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 50; j++) {
                dimList2.add((long) i);
                docsWithField2.add(i * 50 + j);
            }
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        List<Long> metricsList1 = new ArrayList<>(100);
        List<Integer> metricsWithField1 = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList1.add(1L);
            metricsWithField1.add(i);
        }

        compositeField = getStarTreeFieldWithDocCount(3, true);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            metricsList1,
            metricsWithField1,
            compositeField
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            metricsList1,
            metricsWithField1,
            compositeField
        );

        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        /**
         635 docs get generated
         [0, 0, 0, 0] | [200.0, 10]
         [0, 0, 1, 1] | [700.0, 10]
         [0, 0, 2, 2] | [1200.0, 10]
         [0, 0, 3, 3] | [1700.0, 10]
         [1, 0, 4, 4] | [2200.0, 10]
         [1, 0, 5, 5] | [2700.0, 10]
         [1, 0, 6, 6] | [3200.0, 10]
         [1, 0, 7, 7] | [3700.0, 10]
         [2, 0, 8, 8] | [4200.0, 10]
         [2, 0, 9, 9] | [4700.0, 10]
         [2, 1, 10, 10] | [5200.0, 10]
         [2, 1, 11, 11] | [5700.0, 10]
         .....
         [18, 7, null, null] | [147800.0, 40]
         ...
         [7, 2, null, null] | [28900.0, 20]
         ...
         [null, null, null, 99] | [49700.0, 10]
         .....
         [null, null, null, null] | [2495000.0, 1000]
         */
        assertEquals(635, starTreeDocuments.size());
        for (StarTreeDocument starTreeDocument : starTreeDocuments) {
            if (starTreeDocument.dimensions[0] != null
                && starTreeDocument.dimensions[1] != null
                && starTreeDocument.dimensions[2] != null
                && starTreeDocument.dimensions[3] != null) {
                assertEquals(10L, starTreeDocument.metrics[1]);
            } else if (starTreeDocument.dimensions[1] != null
                && starTreeDocument.dimensions[2] != null
                && starTreeDocument.dimensions[3] != null) {
                    assertEquals(10L, starTreeDocument.metrics[1]);
                } else if (starTreeDocument.dimensions[0] != null
                    && starTreeDocument.dimensions[2] != null
                    && starTreeDocument.dimensions[3] != null) {
                        assertEquals(10L, starTreeDocument.metrics[1]);
                    } else if (starTreeDocument.dimensions[0] != null
                        && starTreeDocument.dimensions[1] != null
                        && starTreeDocument.dimensions[3] != null) {
                            assertEquals(10L, starTreeDocument.metrics[1]);
                        } else if (starTreeDocument.dimensions[0] != null && starTreeDocument.dimensions[3] != null) {
                            assertEquals(10L, starTreeDocument.metrics[1]);
                        } else if (starTreeDocument.dimensions[0] != null && starTreeDocument.dimensions[1] != null) {
                            assertTrue((long) starTreeDocument.metrics[1] == 20L || (long) starTreeDocument.metrics[1] == 40L);
                        } else if (starTreeDocument.dimensions[1] != null && starTreeDocument.dimensions[3] != null) {
                            assertEquals(10L, starTreeDocument.metrics[1]);
                        } else if (starTreeDocument.dimensions[1] != null) {
                            assertEquals(100L, starTreeDocument.metrics[1]);
                        } else if (starTreeDocument.dimensions[0] != null) {
                            assertEquals(40L, starTreeDocument.metrics[1]);
                        }
        }
        validateStarTree(builder.getRootNode(), 4, compositeField.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());
        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            100,
            3,
            23199
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithDifferentDocsFromSegments() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(5L, 6L, 8L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(5L, 6L, 7L, 8L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 2, 3, 4);

        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeField(MetricStat.VALUE_COUNT);
        StarTreeValues starTreeValues = getStarTreeValues(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(metricsList, metricsWithField),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(metricsList2, metricsWithField2),
            compositeField,
            "4"
        );
        writeState = getWriteState(4, writeState.segmentInfo.getId());
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [3, 3] | [3]
         [4, 4] | [4]
         [5, 5] | [10]
         [6, 6] | [6]
         [8, 8] | [8]
         [null, 2] | [2]
         [null, 7] | [7]
         */
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        assertEquals(9, builder.getStarTreeDocuments().size());
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        int count = 0;
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            if (count <= 9) {
                if (Objects.equals(starTreeDocument.dimensions[0], 5L)) {
                    assertEquals(starTreeDocument.dimensions[0] * 2, starTreeDocument.metrics[0]);
                } else {
                    assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
                }
            }
        }
        validateStarTree(builder.getRootNode(), 2, 1000, builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();
        LinkedHashMap<String, DocValuesType> map = new LinkedHashMap<>();
        map.put("field1", DocValuesType.SORTED_NUMERIC);
        map.put("field3", DocValuesType.SORTED_NUMERIC);
        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(map, 9, 1000, 330);

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithDuplicateDimensionValueWithMaxLeafDocs() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);

        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
        }
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList3.add((long) i);
                docsWithField3.add(i * 5 + j);
            }
        }
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList2.add((long) 1);
            docsWithField2.add(i);
        }

        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList4.add((long) i);
                docsWithField4.add(i * 5 + j);
            }
        }

        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 10.0));
            metricsWithField.add(i);
        }

        List<Long> docCountMetricsList = new ArrayList<>(100);
        List<Integer> docCountMetricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(i * 2));
            metricsWithField.add(i);
        }

        compositeField = getStarTreeFieldWithDocCount(3, true);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            docCountMetricsList,
            docCountMetricsWithField,
            compositeField
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            docCountMetricsList,
            docCountMetricsWithField,
            compositeField
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(401, starTreeDocuments.size());
        validateStarTree(builder.getRootNode(), 4, compositeField.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            100,
            compositeField.getStarTreeConfig().maxLeafDocs(),
            15345
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithMaxLeafDocsAndStarTreeNodesAssertion() throws IOException {
        List<Long> dimList1 = new ArrayList<>(500);
        List<Integer> docsWithField1 = new ArrayList<>(500);
        Map<Integer, Map<Long, Double>> expectedDimToValueMap = new HashMap<>();
        Map<Long, Double> dimValueMap = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 20; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 20 + j);
            }
            // metric = no of docs * 10.0
            dimValueMap.put((long) i, 200.0);
        }
        for (int i = 80; i < 100; i++) {
            for (int j = 0; j < 5; j++) {
                dimList1.add((long) i);
                docsWithField1.add(i * 5 + j);
            }
            // metric = no of docs * 10.0
            dimValueMap.put((long) i, 50.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(0, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList3 = new ArrayList<>(500);
        List<Integer> docsWithField3 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList3.add((long) 1);
            docsWithField3.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(2, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList2 = new ArrayList<>(500);
        List<Integer> docsWithField2 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList2.add((long) i);
            docsWithField2.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 200.0);
        expectedDimToValueMap.put(1, dimValueMap);
        dimValueMap = new HashMap<>();
        List<Long> dimList4 = new ArrayList<>(500);
        List<Integer> docsWithField4 = new ArrayList<>(500);
        for (int i = 0; i < 500; i++) {
            dimList4.add((long) 1);
            docsWithField4.add(i);
            dimValueMap.put((long) i, 10.0);
        }
        dimValueMap.put(Long.MAX_VALUE, 5000.0);
        expectedDimToValueMap.put(3, dimValueMap);
        List<Long> metricsList = new ArrayList<>(100);
        List<Integer> metricsWithField = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(getLongFromDouble(10.0));
            metricsWithField.add(i);
        }
        List<Long> metricsList1 = new ArrayList<>(100);
        List<Integer> metricsWithField1 = new ArrayList<>(100);
        for (int i = 0; i < 500; i++) {
            metricsList.add(1L);
            metricsWithField.add(i);
        }
        compositeField = getStarTreeFieldWithDocCount(10, true);
        StarTreeValues starTreeValues = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            metricsList1,
            metricsWithField1,
            compositeField
        );

        StarTreeValues starTreeValues2 = getStarTreeValues(
            dimList1,
            docsWithField1,
            dimList2,
            docsWithField2,
            dimList3,
            docsWithField3,
            dimList4,
            docsWithField4,
            metricsList,
            metricsWithField,
            metricsList1,
            metricsWithField1,
            compositeField
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, writeState, mapperService);
        builder.build(builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState), new AtomicInteger(), docValuesConsumer);
        List<StarTreeDocument> starTreeDocuments = builder.getStarTreeDocuments();
        Map<Integer, Map<Long, Integer>> dimValueToDocIdMap = new HashMap<>();
        traverseStarTree(builder.rootNode, dimValueToDocIdMap, true);
        for (Map.Entry<Integer, Map<Long, Integer>> entry : dimValueToDocIdMap.entrySet()) {
            int dimId = entry.getKey();
            if (dimId == -1) continue;
            Map<Long, Double> map = expectedDimToValueMap.get(dimId);
            for (Map.Entry<Long, Integer> dimValueToDocIdEntry : entry.getValue().entrySet()) {
                long dimValue = dimValueToDocIdEntry.getKey();
                int docId = dimValueToDocIdEntry.getValue();
                assertEquals(map.get(dimValue) * 2, starTreeDocuments.get(docId).metrics[0]);
            }
        }
        assertEquals(1041, starTreeDocuments.size());
        validateStarTree(builder.getRootNode(), 4, compositeField.getStarTreeConfig().maxLeafDocs(), builder.getStarTreeDocuments());

        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            500,
            compositeField.getStarTreeConfig().maxLeafDocs(),
            31779
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    public void testMergeFlowWithTimestamps() throws IOException {
        List<Long> dimList = List.of(1655288152000L, 1655288092000L, 1655288032000L, 1655287972000L, 1655288092000L, 1655288092000L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList2 = List.of(1655288152000L, 1655288092000L, 1655288032000L, 1655287972000L, 1655288092000L, 1655288092000L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 6);
        List<Long> dimList7 = List.of(1655288152000L, 1655288092000L, 1655288032000L, 1655287972000L, 1655288092000L, 1655288092000L, -1L);
        List<Integer> docsWithField7 = List.of(0, 1, 2, 3, 4, 6);

        List<Long> dimList5 = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField5 = List.of(0, 1, 2, 3, 4, 5, 6);
        List<Long> metricsList1 = List.of(
            getLongFromDouble(0.0),
            getLongFromDouble(10.0),
            getLongFromDouble(20.0),
            getLongFromDouble(30.0),
            getLongFromDouble(40.0),
            getLongFromDouble(50.0),
            getLongFromDouble(60.0)
        );
        List<Integer> metricsWithField1 = List.of(0, 1, 2, 3, 4, 5, 6);
        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> dimList3 = List.of(1655288152000L, 1655288092000L, 1655288032000L, -1L);
        List<Integer> docsWithField3 = List.of(0, 1, 3, 4);
        List<Long> dimList4 = List.of(1655288152000L, 1655288092000L, 1655288032000L, -1L);
        List<Integer> docsWithField4 = List.of(0, 1, 3, 4);
        List<Long> dimList8 = List.of(1655288152000L, 1655288092000L, 1655288032000L, -1L);
        List<Integer> docsWithField8 = List.of(0, 1, 3, 4);

        List<Long> dimList6 = List.of(5L, 6L, 7L, 8L);
        List<Integer> docsWithField6 = List.of(0, 1, 2, 3);
        List<Long> metricsList21 = List.of(
            getLongFromDouble(50.0),
            getLongFromDouble(60.0),
            getLongFromDouble(70.0),
            getLongFromDouble(80.0),
            getLongFromDouble(90.0)
        );
        List<Integer> metricsWithField21 = List.of(0, 1, 2, 3, 4);
        List<Long> metricsList2 = List.of(5L, 6L, 7L, 8L, 9L);
        List<Integer> metricsWithField2 = List.of(0, 1, 2, 3, 4);

        compositeField = getStarTreeFieldWithDateDimension();
        StarTreeValues starTreeValues = getStarTreeValuesWithDates(
            getSortedNumericMock(dimList, docsWithField),
            getSortedNumericMock(dimList2, docsWithField2),
            getSortedNumericMock(dimList7, docsWithField7),
            getSortedNumericMock(dimList5, docsWithField5),
            getSortedNumericMock(metricsList, metricsWithField),
            getSortedNumericMock(metricsList1, metricsWithField1),
            compositeField,
            "6"
        );

        StarTreeValues starTreeValues2 = getStarTreeValuesWithDates(
            getSortedNumericMock(dimList3, docsWithField3),
            getSortedNumericMock(dimList4, docsWithField4),
            getSortedNumericMock(dimList8, docsWithField8),
            getSortedNumericMock(dimList6, docsWithField6),
            getSortedNumericMock(metricsList2, metricsWithField2),
            getSortedNumericMock(metricsList21, metricsWithField21),
            compositeField,
            "4"
        );
        this.docValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            writeState,
            Composite99DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite99DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite99DocValuesFormat.META_DOC_VALUES_EXTENSION
        );
        builder = getStarTreeBuilder(metaOut, dataOut, compositeField, getWriteState(4, writeState.segmentInfo.getId()), mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2), mergeState);
        /**
         [1655287972000, 1655287972000, 1655287972000, 3] | [30.0, 3]
         [1655288032000, 1655288032000, 1655288032000, 2] | [20.0, 2]
         [1655288032000, 1655288032000, 1655288032000, 8] | [80.0, 8]
         [1655288092000, 1655288092000, 1655288092000, 1] | [10.0, 1]
         [1655288092000, 1655288092000, 1655288092000, 4] | [40.0, 4]
         [1655288092000, 1655288092000, 1655288092000, 6] | [60.0, 6]
         [1655288152000, 1655288152000, 1655288152000, 0] | [0.0, 0]
         [1655288152000, 1655288152000, 1655288152000, 5] | [50.0, 5]
         [null, null, null, 5] | [50.0, 5]
         [null, null, null, 7] | [70.0, 7]
         */
        int count = 0;
        builder.appendDocumentsToStarTree(starTreeDocumentIterator);
        for (StarTreeDocument starTreeDocument : builder.getStarTreeDocuments()) {
            count++;
            assertEquals(starTreeDocument.dimensions[3] * 10.0, (double) starTreeDocument.metrics[1], 0);
            assertEquals(starTreeDocument.dimensions[3], starTreeDocument.metrics[0]);
        }
        assertEquals(10, count);
        builder.build(starTreeDocumentIterator, new AtomicInteger(), docValuesConsumer);
        validateStarTree(builder.getRootNode(), 4, 10, builder.getStarTreeDocuments());
        metaOut.close();
        dataOut.close();
        docValuesConsumer.close();

        StarTreeMetadata starTreeMetadata = getStarTreeMetadata(
            getStarTreeDimensionNames(compositeField.getDimensionsOrder()),
            10,
            compositeField.getStarTreeConfig().maxLeafDocs(),
            231
        );

        validateStarTreeFileFormats(
            builder.getRootNode(),
            builder.getStarTreeDocuments().size(),
            starTreeMetadata,
            builder.getStarTreeDocuments()
        );
    }

    private StarTreeValues getStarTreeValuesWithDates(
        SortedNumericDocValues dimList,
        SortedNumericDocValues dimList2,
        SortedNumericDocValues dimList4,
        SortedNumericDocValues dimList3,
        SortedNumericDocValues metricsList,
        SortedNumericDocValues metricsList1,
        StarTreeField sf,
        String number
    ) {
        Map<String, Supplier<StarTreeValuesIterator>> dimDocIdSetIterators = Map.of(
            "field1_minute",
            () -> new SortedNumericStarTreeValuesIterator(dimList),
            "field1_half-hour",
            () -> new SortedNumericStarTreeValuesIterator(dimList4),
            "field1_hour",
            () -> new SortedNumericStarTreeValuesIterator(dimList2),
            "field3",
            () -> new SortedNumericStarTreeValuesIterator(dimList3)
        );
        Map<String, Supplier<StarTreeValuesIterator>> metricDocIdSetIterators = new LinkedHashMap<>();

        metricDocIdSetIterators.put(
            fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                sf.getName(),
                "field2",
                sf.getMetrics().get(0).getMetrics().get(0).getTypeName()
            ),
            () -> new SortedNumericStarTreeValuesIterator(metricsList)
        );
        metricDocIdSetIterators.put(
            fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                sf.getName(),
                "field2",
                sf.getMetrics().get(0).getMetrics().get(1).getTypeName()
            ),
            () -> new SortedNumericStarTreeValuesIterator(metricsList1)
        );
        return new StarTreeValues(sf, null, dimDocIdSetIterators, metricDocIdSetIterators, Map.of(SEGMENT_DOCS_COUNT, number), null);
    }

    private StarTreeValues getStarTreeValues(
        SortedNumericDocValues dimList,
        SortedNumericDocValues dimList2,
        SortedNumericDocValues metricsList,
        StarTreeField sf,
        String number
    ) {
        SortedNumericDocValues d1sndv = dimList;
        SortedNumericDocValues d2sndv = dimList2;
        SortedNumericDocValues m1sndv = metricsList;
        Map<String, Supplier<StarTreeValuesIterator>> dimDocIdSetIterators = Map.of(
            "field1",
            () -> new SortedNumericStarTreeValuesIterator(d1sndv),
            "field3",
            () -> new SortedNumericStarTreeValuesIterator(d2sndv)
        );

        Map<String, Supplier<StarTreeValuesIterator>> metricDocIdSetIterators = new LinkedHashMap<>();
        for (Metric metric : sf.getMetrics()) {
            for (MetricStat metricStat : metric.getMetrics()) {
                String metricFullName = fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                    sf.getName(),
                    metric.getField(),
                    metricStat.getTypeName()
                );
                metricDocIdSetIterators.put(metricFullName, () -> new SortedNumericStarTreeValuesIterator(m1sndv));
            }
        }

        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of(SEGMENT_DOCS_COUNT, number),
            null
        );
        return starTreeValues;
    }

    private StarTreeValues getStarTreeValues(
        List<Long> dimList1,
        List<Integer> docsWithField1,
        List<Long> dimList2,
        List<Integer> docsWithField2,
        List<Long> dimList3,
        List<Integer> docsWithField3,
        List<Long> dimList4,
        List<Integer> docsWithField4,
        List<Long> metricsList,
        List<Integer> metricsWithField,
        List<Long> metricsList1,
        List<Integer> metricsWithField1,
        StarTreeField sf
    ) {
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList1, docsWithField1);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues d3sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues d4sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues m2sndv = getSortedNumericMock(metricsList1, metricsWithField1);
        Map<String, Supplier<StarTreeValuesIterator>> dimDocIdSetIterators = Map.of(
            "field1",
            () -> new SortedNumericStarTreeValuesIterator(d1sndv),
            "field3",
            () -> new SortedNumericStarTreeValuesIterator(d2sndv),
            "field5",
            () -> new SortedNumericStarTreeValuesIterator(d3sndv),
            "field8",
            () -> new SortedNumericStarTreeValuesIterator(d4sndv)
        );

        Map<String, Supplier<StarTreeValuesIterator>> metricDocIdSetIterators = new LinkedHashMap<>();

        metricDocIdSetIterators.put(
            fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                sf.getName(),
                "field2",
                sf.getMetrics().get(0).getMetrics().get(0).getTypeName()
            ),
            () -> new SortedNumericStarTreeValuesIterator(m1sndv)
        );
        metricDocIdSetIterators.put(
            fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                sf.getName(),
                "_doc_count",
                sf.getMetrics().get(1).getMetrics().get(0).getTypeName()
            ),
            () -> new SortedNumericStarTreeValuesIterator(m2sndv)
        );
        // metricDocIdSetIterators.put("field2", () -> m1sndv);
        // metricDocIdSetIterators.put("_doc_count", () -> m2sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            getAttributes(500),
            null
        );
        return starTreeValues;
    }
}
