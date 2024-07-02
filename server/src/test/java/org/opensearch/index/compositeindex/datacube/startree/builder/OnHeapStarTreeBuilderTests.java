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
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
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
    private List<String> fields = List.of(
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
    private List<Metric> metrics;
    private Directory directory;
    private FieldInfo[] fieldsInfo;
    private StarTreeField compositeField;
    private Map<String, DocValuesProducer> fieldProducerMap;
    private SegmentWriteState writeState;

    @Before
    public void setup() throws IOException {
        dimensionsOrder = List.of(
            new NumericDimension("field1"),
            new NumericDimension("field3"),
            new NumericDimension("field5"),
            new NumericDimension("field8")
        );
        metrics = List.of(
            new Metric("field2", List.of(MetricStat.SUM)),
            new Metric("field4", List.of(MetricStat.SUM)),
            new Metric("field6", List.of(MetricStat.COUNT)),
            new Metric("field9", List.of(MetricStat.MIN)),
            new Metric("field10", List.of(MetricStat.MAX))
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
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, fieldProducerMap, writeState, mapperService);
    }

    public void test_sortAndAggregateStarTreeDocuments() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[j]);
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();
            for (int dim = 0; dim < 4; dim++) {
                assertEquals(expectedStarTreeDocument.dimensions[dim], resultStarTreeDocument.dimensions[dim]);
            }

            for (int met = 0; met < 5; met++) {
                assertEquals(expectedStarTreeDocument.metrics[met], resultStarTreeDocument.metrics[met]);
            }
            numOfAggregatedDocuments++;
        }
        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_sortAndAggregateStarTreeDocuments_nullMetric() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, null, randomDouble(), 8.0, 20.0 });
        StarTreeDocument expectedStarTreeDocument = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = starTreeDocuments[i].metrics[j] != null
                    ? NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[j])
                    : null;
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);

        StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
        for (int dim = 0; dim < 4; dim++) {
            assertEquals(expectedStarTreeDocument.dimensions[dim], resultStarTreeDocument.dimensions[dim]);
        }

        for (int met = 0; met < 5; met++) {
            assertEquals(expectedStarTreeDocument.metrics[met], resultStarTreeDocument.metrics[met]);
        }

        assertThrows(
            "Null metric should have resulted in IllegalStateException",
            IllegalStateException.class,
            segmentStarTreeDocumentIterator::next
        );

    }

    public void test_sortAndAggregateStarTreeDocument_longMaxAndLongMinDimensions() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { Long.MIN_VALUE, 4L, 3L, 4L },
            new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, Long.MAX_VALUE },
            new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 }
        );

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { Long.MIN_VALUE, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, Long.MAX_VALUE }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[j]);
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            for (int dim = 0; dim < 4; dim++) {
                assertEquals(expectedStarTreeDocument.dimensions[dim], resultStarTreeDocument.dimensions[dim]);
            }

            for (int met = 0; met < 5; met++) {
                assertEquals(expectedStarTreeDocument.metrics[met], resultStarTreeDocument.metrics[met]);
            }

            numOfAggregatedDocuments++;
        }

        assertEquals(inorderStarTreeDocuments.size(), numOfAggregatedDocuments);

    }

    public void test_build_DoubleMaxAndDoubleMinMetrics() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Double[] { Double.MAX_VALUE, 10.0, randomDouble(), 8.0, 20.0 }
        );
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Double[] { 14.0, Double.MIN_VALUE, randomDouble(), 6.0, 24.0 }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        List<StarTreeDocument> inorderStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { Double.MAX_VALUE + 9, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, Double.MIN_VALUE + 22, 3L, 6.0, 24.0 })
        );
        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = inorderStarTreeDocuments.iterator();

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[j]);
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        int numOfAggregatedDocuments = 0;
        while (segmentStarTreeDocumentIterator.hasNext() && expectedStarTreeDocumentIterator.hasNext()) {
            StarTreeDocument resultStarTreeDocument = segmentStarTreeDocumentIterator.next();
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocumentIterator.next();

            for (int dim = 0; dim < 4; dim++) {
                assertEquals(expectedStarTreeDocument.dimensions[dim], resultStarTreeDocument.dimensions[dim]);
            }

            for (int met = 0; met < 5; met++) {
                assertEquals(expectedStarTreeDocument.metrics[met], resultStarTreeDocument.metrics[met]);
            }

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
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.HALF_FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder(
            "field10",
            NumberFieldMapper.NumberType.HALF_FLOAT,
            false,
            true
        ).build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, fieldProducerMap, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf1", 12),
                new HalfFloatPoint("hf6", 10),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 8),
                new HalfFloatPoint("field10", 20) }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf2", 10),
                new HalfFloatPoint("hf7", 6),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 12),
                new HalfFloatPoint("field10", 10) }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf3", 14),
                new HalfFloatPoint("hf8", 12),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 6),
                new HalfFloatPoint("field10", 24) }
        );
        starTreeDocuments[3] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf4", 9),
                new HalfFloatPoint("hf9", 4),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 9),
                new HalfFloatPoint("field10", 12) }
        );
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new HalfFloatPoint[] {
                new HalfFloatPoint("hf5", 11),
                new HalfFloatPoint("hf10", 16),
                new HalfFloatPoint("field6", 10),
                new HalfFloatPoint("field9", 8),
                new HalfFloatPoint("field10", 13) }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = (long) HalfFloatPoint.halfFloatToSortableShort(
                    ((HalfFloatPoint) starTreeDocuments[i].metrics[j]).numericValue().floatValue()
                );
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

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
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.FLOAT, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, fieldProducerMap, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 2L, 4L, 3L, 4L },
            new Float[] { 12.0F, 10.0F, randomFloat(), 8.0F, 20.0F }
        );
        starTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 10.0F, 6.0F, randomFloat(), 12.0F, 10.0F }
        );
        starTreeDocuments[2] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 14.0F, 12.0F, randomFloat(), 6.0F, 24.0F }
        );
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Float[] { 9.0F, 4.0F, randomFloat(), 9.0F, 12.0F });
        starTreeDocuments[4] = new StarTreeDocument(
            new Long[] { 3L, 4L, 2L, 1L },
            new Float[] { 11.0F, 16.0F, randomFloat(), 8.0F, 13.0F }
        );

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = (long) NumericUtils.floatToSortableInt((Float) starTreeDocuments[i].metrics[j]);
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

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
        NumberFieldMapper numberFieldMapper4 = new NumberFieldMapper.Builder("field9", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper5 = new NumberFieldMapper.Builder("field10", NumberFieldMapper.NumberType.LONG, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2, numberFieldMapper3, numberFieldMapper4, numberFieldMapper5),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);
        builder = new OnHeapStarTreeBuilder(compositeField, fieldProducerMap, writeState, mapperService);

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 12L, 10L, randomLong(), 8L, 20L });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 10L, 6L, randomLong(), 12L, 10L });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 14L, 12L, randomLong(), 6L, 24L });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Long[] { 9L, 4L, randomLong(), 9L, 12L });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Long[] { 11L, 16L, randomLong(), 8L, 13L });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = (Long) starTreeDocuments[i].metrics[j];
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private static Iterator<StarTreeDocument> getExpectedStarTreeDocumentIterator() {
        List<StarTreeDocument> expectedStarTreeDocuments = List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { -1L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { -1L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { -1L, 4L, -1L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { -1L, 4L, -1L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0 }),
            new StarTreeDocument(new Long[] { -1L, 4L, -1L, -1L }, new Object[] { 56.0, 48.0, 5L, 6.0, 24.0 }),
            new StarTreeDocument(new Long[] { -1L, -1L, -1L, -1L }, new Object[] { 56.0, 48.0, 5L, 6.0, 24.0 })
        );
        return expectedStarTreeDocuments.iterator();
    }

    public void test_build() throws IOException {

        int noOfStarTreeDocuments = 5;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];

        starTreeDocuments[0] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 12.0, 10.0, randomDouble(), 8.0, 20.0 });
        starTreeDocuments[1] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 10.0, 6.0, randomDouble(), 12.0, 10.0 });
        starTreeDocuments[2] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 14.0, 12.0, randomDouble(), 6.0, 24.0 });
        starTreeDocuments[3] = new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Double[] { 9.0, 4.0, randomDouble(), 9.0, 12.0 });
        starTreeDocuments[4] = new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Double[] { 11.0, 16.0, randomDouble(), 8.0, 13.0 });

        StarTreeDocument[] segmentStarTreeDocuments = new StarTreeDocument[noOfStarTreeDocuments];
        for (int i = 0; i < noOfStarTreeDocuments; i++) {
            Long[] metrics = new Long[starTreeDocuments[0].metrics.length];
            for (int j = 0; j < metrics.length; j++) {
                metrics[j] = NumericUtils.doubleToSortableLong((Double) starTreeDocuments[i].metrics[j]);
            }
            segmentStarTreeDocuments[i] = new StarTreeDocument(starTreeDocuments[i].dimensions, metrics);
        }

        Iterator<StarTreeDocument> segmentStarTreeDocumentIterator = builder.sortAndAggregateStarTreeDocuments(segmentStarTreeDocuments);
        builder.build(segmentStarTreeDocumentIterator);

        List<StarTreeDocument> resultStarTreeDocuments = builder.getStarTreeDocuments();
        assertEquals(8, resultStarTreeDocuments.size());

        Iterator<StarTreeDocument> expectedStarTreeDocumentIterator = getExpectedStarTreeDocumentIterator();
        assertStarTreeDocuments(resultStarTreeDocuments, expectedStarTreeDocumentIterator);
    }

    private static void assertStarTreeDocuments(
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

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
