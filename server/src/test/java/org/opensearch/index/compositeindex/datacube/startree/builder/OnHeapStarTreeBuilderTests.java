/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnHeapStarTreeBuilderTests extends OpenSearchTestCase {

    private OnHeapStarTreeBuilder builder;
    private MapperService mapperService;
    private List<Dimension> dimensionsOrder;
    private List<String> fields = List.of();
    private List<Metric> metrics;
    private Directory directory;
    private FieldInfo[] fieldsInfo;
    private StarTreeField compositeField;
    private Map<String, DocValuesProducer> fieldProducerMap;
    private SegmentWriteState writeState;

    @Before
    public void setup() throws IOException {
        fields = List.of("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10");

        dimensionsOrder = List.of(
            new NumericDimension("field1"),
            new NumericDimension("field3"),
            new NumericDimension("field5"),
            new NumericDimension("field8")
        );
        metrics = List.of(
            new Metric("field2", List.of(MetricStat.SUM)),
            new Metric("field4", List.of(MetricStat.SUM)),
            new Metric("field6", List.of(MetricStat.COUNT))
        );

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
        );
        directory = newFSDirectory(createTempDir());
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            6,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];
        fieldProducerMap = new HashMap<>();
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, writeState, mapperService);
    }

    public void test_sortAndAggregateStarTreeDocuments() {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble() });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetric() {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, null, randomDouble() });
        StarTreeDocument expectedStarTreeDocument = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 21.0, 14.0, 2.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            Long metric2 = starTreeDocuments[i].metrics[1] != null
                ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1])
                : null;
            Long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Object[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);

        StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
        assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
        assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
        assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
        assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
        assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
        assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);

        assertThrows(
            "Null metric should have resulted in IllegalStateException",
            IllegalStateException.class,
            segmentStarTreeDocumentIterator::next
        );

    }

    public void test_sortAndAggregateStarTreeDocument_longMaxAndLongMinDimensions() {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Double[] { 10.0, 6.0, randomDouble() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Double[] { 14.0, 12.0, randomDouble() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Double[] { 11.0, 16.0, randomDouble() });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Object[] { 35.0, 34.0, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_sortAndAggregateStarTreeDocument_DoubleMaxAndDoubleMinMetrics() {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { Double.MAX_VALUE, 10.0, randomDouble() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, Double.MIN_VALUE, randomDouble() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble() });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { Double.MAX_VALUE + 9, 14.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, Double.MIN_VALUE + 22, 3L })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_build_halfFloatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] { new HalfFloatPoint("hf1", 12), new HalfFloatPoint("hf6", 10), new HalfFloatPoint("field6", 10) }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] { new HalfFloatPoint("hf2", 10), new HalfFloatPoint("hf7", 6), new HalfFloatPoint("field6", 10) }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] { new HalfFloatPoint("hf3", 14), new HalfFloatPoint("hf8", 12), new HalfFloatPoint("field6", 10) }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] { new HalfFloatPoint("hf4", 9), new HalfFloatPoint("hf9", 4), new HalfFloatPoint("field6", 10) }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] { new HalfFloatPoint("hf5", 11), new HalfFloatPoint("hf10", 16), new HalfFloatPoint("field6", 10) }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[0]).numericValue().floatValue()
            );
            long metric2 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[1]).numericValue().floatValue()
            );
            long metric3 = HalfFloatPoint.halfFloatToSortableShort(
                ((HalfFloatPoint) starTreeDocuments[i].metrics[2]).numericValue().floatValue()
            );
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);
        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    public void test_build_floatMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Float[] { 12.0F, 10.0F, randomFloat() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Float[] { 10.0F, 6.0F, randomFloat() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Float[] { 14.0F, 12.0F, randomFloat() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Float[] { 9.0F, 4.0F, randomFloat() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Float[] { 11.0F, 16.0F, randomFloat() });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    public void test_build_longMetrics() throws IOException {

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper3 = new NumberFieldMapper.Builder("field6", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 12L, 10L, randomLong() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 10L, 6L, randomLong() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 14L, 12L, randomLong() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 9L, 4L, randomLong() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 11L, 16L, randomLong() });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = (Long) starTreeDocuments[i].metrics[0];
            long metric2 = (Long) starTreeDocuments[i].metrics[1];
            long metric3 = (Long) starTreeDocuments[i].metrics[2];
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private static Iterator<StarTreeDocument> getExpectedStarTreeDocumentIterator() {
        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L }),
            new StarTreeDocument(new Long[] { null, 4L, null, 1L }, new Object[] { 35.0, 34.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, null, 4L }, new Object[] { 21.0, 14.0, 2L }),
            new StarTreeDocument(new Long[] { null, 4L, null, null }, new Object[] { 56.0, 48.0, 5L }),
            new StarTreeDocument(new Long[] { null, null, null, null }, new Object[] { 56.0, 48.0, 5L })
        );
        return expectedStarTreeDocuments.iterator();
    }

    public void test_build() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble() });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble() });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble() });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble() });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble() });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            long metric2 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[1]);
            long metric3 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[2]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1, metric2, metric3 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(7, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private void assertStarTreeDocuments(
        List<StarTreeDocument> resultStarTreeDocuments,
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator
    ) {
        Iterator<StarTreeDocument> resultStarTreeDocumentIterator = resultStarTreeDocuments.iterator();
        while (resultStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = resultStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.dimensions[3], resultStarTreeDocument.dimensions[3]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
            assertEquals(expectedStarTreeDocument.metrics[1], resultStarTreeDocument.metrics[1]);
            assertEquals(expectedStarTreeDocument.metrics[2], resultStarTreeDocument.metrics[2]);
        }
    }

    public void test_build_starTreeDataset() throws IOException {

        fields = List.of("fieldC", "fieldB", "fieldL", "fieldI");

        dimensionsOrder = List.of(new NumericDimension("fieldC"), new NumericDimension("fieldB"), new NumericDimension("fieldL"));
        metrics = List.of(new Metric("fieldI", List.of(MetricStat.SUM)));

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of(), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
        );
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            7,
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];
        fieldProducerMap = new HashMap<>();
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
            fieldProducerMap.put(fields.get(i), docValuesProducer);
        }
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("fieldI", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, writeState, mapperService);

        int noOfStarTreeDocuments = 7;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 1L, 11L, 21L }, new Double[] { 400.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 1L, 12L, 22L }, new Double[] { 200.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 2L, 13L, 23L }, new Double[] { 300.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 13L, 21L }, new Double[] { 100.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 11L, 21L }, new Double[] { 600.0 });
        starTreeDocuments[5] = new StarTreeDocument(new Long[] { 3L, 12L, 23L }, new Double[] { 200.0 });
        starTreeDocuments[6] = new StarTreeDocument(new Long[] { 3L, 12L, 21L }, new Double[] { 400.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            long metric1 = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[0]);
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, new Long[] { metric1 });
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = expectedStarTreeDocuments();
        Iterator<StarTreeDocument> resultStarTreeDocumentIterator = resultStarTreeDocuments.iterator();
        while (resultStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = resultStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            assertEquals(expectedStarTreeDocument.dimensions[0], resultStarTreeDocument.dimensions[0]);
            assertEquals(expectedStarTreeDocument.dimensions[1], resultStarTreeDocument.dimensions[1]);
            assertEquals(expectedStarTreeDocument.dimensions[2], resultStarTreeDocument.dimensions[2]);
            assertEquals(expectedStarTreeDocument.metrics[0], resultStarTreeDocument.metrics[0]);
        }

    }

    private Iterator<StarTreeDocument> expectedStarTreeDocuments() {
        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 1L, 11L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, 12L, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, 21L }, new Object[] { 100.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, 23L }, new Object[] { 300.0 }),
            new StarTreeDocument(new Long[] { 3L, 11L, 21L }, new Object[] { 600.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 11L, 21L }, new Object[] { 1000.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 12L, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, 13L, 21L }, new Object[] { 100.0 }),
            new StarTreeDocument(new Long[] { null, 13L, 23L }, new Object[] { 300.0 }),
            new StarTreeDocument(new Long[] { null, null, 21L }, new Object[] { 1500.0 }),
            new StarTreeDocument(new Long[] { null, null, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { null, null, 23L }, new Object[] { 500.0 }),
            new StarTreeDocument(new Long[] { null, null, null }, new Object[] { 2200.0 }),
            new StarTreeDocument(new Long[] { null, 12L, null }, new Object[] { 800.0 }),
            new StarTreeDocument(new Long[] { null, 13L, null }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, null, 21L }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 1L, null, 22L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 1L, null, null }, new Object[] { 600.0 }),
            new StarTreeDocument(new Long[] { 2L, 13L, null }, new Object[] { 400.0 }),
            new StarTreeDocument(new Long[] { 3L, null, 21L }, new Object[] { 1000.0 }),
            new StarTreeDocument(new Long[] { 3L, null, 23L }, new Object[] { 200.0 }),
            new StarTreeDocument(new Long[] { 3L, null, null }, new Object[] { 1200.0 }),
            new StarTreeDocument(new Long[] { 3L, 12L, null }, new Object[] { 600.0 })
        );

        return expectedStarTreeDocuments.iterator();
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

        StarTreeField sf = getStarTreeFieldWithMultipleMetrics();
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        SortedNumericDocValues m2sndv = getSortedNumericMock(metricsList, metricsWithField);

        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            assertEquals(
                starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 1 * 10.0 : 20.0,
                starTreeDocument.metrics[0]
            );
            assertEquals(1L, starTreeDocument.metrics[1]);
        }
        assertEquals(6, count);
    }

    private static StarTreeField getStarTreeFieldWithMultipleMetrics() {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        Metric m2 = new Metric("field2", List.of(MetricStat.COUNT));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1, m2);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(
            1000,
            new HashSet<>(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );
        StarTreeField sf = new StarTreeField("sf", dims, metrics, c);
        return sf;
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

        StarTreeField sf = getStarTreeField(MetricStat.SUM);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            assertEquals(
                starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 * 10.0 : 40.0,
                starTreeDocument.metrics[0]
            );
        }
        assertEquals(6, count);
    }

    public void testMergeFlowWithCount() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 3L, 4L, 5L, 6L);
        List<Integer> docsWithField = List.of(0, 1, 3, 4, 5, 6);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            assertEquals(starTreeDocument.dimensions[0] != null ? starTreeDocument.dimensions[0] * 2 : 4, starTreeDocument.metrics[0]);
        }
        assertEquals(6, count);
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

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList2, metricsWithField2);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "4")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            if (starTreeDocument.dimensions[0] == null) {
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            } else {
                assertEquals(starTreeDocument.dimensions[0] * 2, starTreeDocument.metrics[0]);
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

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList2, metricsWithField2);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "4")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            if (starTreeDocument.dimensions[0] == null) {
                assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
            }
            assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
        }
        assertEquals(6, count);
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

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = getSortedNumericMock(dimList3, docsWithField3);
        SortedNumericDocValues f2d2sndv = getSortedNumericMock(dimList4, docsWithField4);
        SortedNumericDocValues f2m1sndv = getSortedNumericMock(metricsList2, metricsWithField2);
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "4")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
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
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            while (starTreeDocumentIterator.hasNext()) {
                count++;
                StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
                if (starTreeDocument.dimensions[0] == null) {
                    assertTrue(List.of(5L, 7L).contains(starTreeDocument.dimensions[1]));
                }
                assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
            }
        }
        assertEquals(10, count);
    }

    public void testMergeFlowWithEmptyFieldsInOneSegment() throws IOException {
        List<Long> dimList = List.of(0L, 1L, 2L, 3L, 4L);
        List<Integer> docsWithField = List.of(0, 1, 2, 3, 4);
        List<Long> dimList2 = List.of(0L, 1L, 2L, 3L, 4L, 5L, -1L);
        List<Integer> docsWithField2 = List.of(0, 1, 2, 3, 4, 5, 6);

        List<Long> metricsList = List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        List<Integer> metricsWithField = List.of(0, 1, 2, 3, 4, 5, 6);

        StarTreeField sf = getStarTreeField(MetricStat.COUNT);
        SortedNumericDocValues d1sndv = getSortedNumericMock(dimList, docsWithField);
        SortedNumericDocValues d2sndv = getSortedNumericMock(dimList2, docsWithField2);
        SortedNumericDocValues m1sndv = getSortedNumericMock(metricsList, metricsWithField);
        Map<String, DocIdSetIterator> dimDocIdSetIterators = Map.of("field1", d1sndv, "field3", d2sndv);
        Map<String, DocIdSetIterator> metricDocIdSetIterators = Map.of("field2", m1sndv);
        StarTreeValues starTreeValues = new StarTreeValues(
            sf,
            null,
            dimDocIdSetIterators,
            metricDocIdSetIterators,
            Map.of("numSegmentDocs", "6")
        );

        SortedNumericDocValues f2d1sndv = DocValues.emptySortedNumeric();
        SortedNumericDocValues f2d2sndv = DocValues.emptySortedNumeric();
        SortedNumericDocValues f2m1sndv = DocValues.emptySortedNumeric();
        Map<String, DocIdSetIterator> f2dimDocIdSetIterators = Map.of("field1", f2d1sndv, "field3", f2d2sndv);
        Map<String, DocIdSetIterator> f2metricDocIdSetIterators = Map.of("field2", f2m1sndv);
        StarTreeValues starTreeValues2 = new StarTreeValues(
            sf,
            null,
            f2dimDocIdSetIterators,
            f2metricDocIdSetIterators,
            Map.of("numSegmentDocs", "1")
        );
        OnHeapStarTreeBuilder builder = new OnHeapStarTreeBuilder(sf, writeState, mapperService);
        Iterator<StarTreeDocument> starTreeDocumentIterator = builder.mergeStarTrees(List.of(starTreeValues, starTreeValues2));
        /**
         * Asserting following dim / metrics [ dim1, dim2 / Count [ metric] ]
         [0, 0] | [0]
         [1, 1] | [1]
         [2, 2] | [2]
         [3, 3] | [3]
         [4, 4] | [4]
         [null, 5] | [5]
         */
        int count = 0;
        while (starTreeDocumentIterator.hasNext()) {
            count++;
            StarTreeDocument starTreeDocument = starTreeDocumentIterator.next();
            if (starTreeDocument.dimensions[0] == null) {
                assertEquals(5L, (long) starTreeDocument.dimensions[1]);
            }
            assertEquals(starTreeDocument.dimensions[1], starTreeDocument.metrics[0]);
        }
        assertEquals(6, count);
    }

    private static StarTreeField getStarTreeField(MetricStat count) {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(count));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(
            1000,
            new HashSet<>(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );
        return new StarTreeField("sf", dims, metrics, c);
    }

    private Long getLongFromDouble(Double num) {
        if (num == null) {
            return null;
        }
        return NumericUtils.doubleToSortableLong(num);
    }

    private SortedNumericDocValues getSortedNumericMock(List<Long> dimList, List<Integer> docsWithField) {
        return new SortedNumericDocValues() {
            int index = -1;

            @Override
            public long nextValue() {
                return dimList.get(index);
            }

            @Override
            public int docValueCount() {
                return 0;
            }

            @Override
            public boolean advanceExact(int target) {
                return false;
            }

            @Override
            public int docID() {
                return index;
            }

            @Override
            public int nextDoc() {
                if (index == docsWithField.size() - 1) {
                    return NO_MORE_DOCS;
                }
                index++;
                return docsWithField.get(index);
            }

            @Override
            public int advance(int target) {
                return 0;
            }

            @Override
            public long cost() {
                return 0;
            }
        };
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
