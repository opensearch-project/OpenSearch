/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MappingLookup;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BaseStarTreeBuilderTests extends OpenSearchTestCase {

    private static BaseStarTreeBuilder builder;
    private static MapperService mapperService;
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
    private static SegmentWriteState writeState;
    private static StarTreeField starTreeField;

    private static IndexOutput dataOut;
    private static IndexOutput metaOut;

    @BeforeClass
    public static void setup() throws IOException {

        dimensionsOrder = List.of(
            new NumericDimension("field1"),
            new NumericDimension("field3"),
            new NumericDimension("field5"),
            new NumericDimension("field8")
        );
        metrics = List.of(new Metric("field2", List.of(MetricStat.SUM)), new Metric("field4", List.of(MetricStat.SUM)));

        starTreeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
        );

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);
        directory = newFSDirectory(createTempDir());
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_12_0,
            "test_segment",
            5,
            false,
            false,
            new Lucene912Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        fieldsInfo = new FieldInfo[fields.size()];
        Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
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

        String dataFileName = IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Composite912DocValuesFormat.DATA_EXTENSION
        );
        dataOut = writeState.directory.createOutput(dataFileName, writeState.context);

        String metaFileName = IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Composite912DocValuesFormat.META_EXTENSION
        );
        metaOut = writeState.directory.createOutput(metaFileName, writeState.context);

        mapperService = mock(MapperService.class);
        DocumentMapper documentMapper = mock(DocumentMapper.class);
        when(mapperService.documentMapper()).thenReturn(documentMapper);
        Settings settings = Settings.builder().put(settings(org.opensearch.Version.CURRENT).build()).build();
        NumberFieldMapper numberFieldMapper1 = new NumberFieldMapper.Builder("field2", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        NumberFieldMapper numberFieldMapper2 = new NumberFieldMapper.Builder("field4", NumberFieldMapper.NumberType.DOUBLE, false, true)
            .build(new Mapper.BuilderContext(settings, new ContentPath()));
        MappingLookup fieldMappers = new MappingLookup(
            Set.of(numberFieldMapper1, numberFieldMapper2),
            Collections.emptyList(),
            Collections.emptyList(),
            0,
            null
        );
        when(documentMapper.mappers()).thenReturn(fieldMappers);

        builder = new BaseStarTreeBuilder(metaOut, dataOut, starTreeField, writeState, mapperService) {
            @Override
            public void build(
                List<StarTreeValues> starTreeValuesSubs,
                AtomicInteger fieldNumberAcrossStarTrees,
                DocValuesConsumer starTreeDocValuesConsumer
            ) throws IOException {}

            @Override
            public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {}

            @Override
            public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
                return null;
            }

            @Override
            public List<StarTreeDocument> getStarTreeDocuments() {
                return List.of();
            }

            @Override
            public Long getDimensionValue(int docId, int dimensionId) throws IOException {
                return 0L;
            }

            @Override
            public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
                SequentialDocValuesIterator[] dimensionReaders,
                List<SequentialDocValuesIterator> metricReaders
            ) throws IOException {
                return null;
            }

            @Override
            public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
                throws IOException {
                return null;
            }

            @Override
            Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValues) throws IOException {
                return null;
            }
        };
    }

    public void test_generateMetricAggregatorInfos() throws IOException {
        List<MetricAggregatorInfo> metricAggregatorInfos = builder.generateMetricAggregatorInfos(mapperService);
        List<MetricAggregatorInfo> expectedMetricAggregatorInfos = List.of(
            new MetricAggregatorInfo(MetricStat.SUM, "field2", starTreeField.getName(), NumberFieldMapper.NumberType.DOUBLE),
            new MetricAggregatorInfo(MetricStat.SUM, "field4", starTreeField.getName(), NumberFieldMapper.NumberType.DOUBLE)
        );
        assertEquals(metricAggregatorInfos, expectedMetricAggregatorInfos);
    }

    public void test_reduceStarTreeDocuments() {
        StarTreeDocument starTreeDocument1 = new StarTreeDocument(new Long[] { 1L, 3L, 5L, 8L }, new Double[] { 4.0, 8.0 });
        StarTreeDocument starTreeDocument2 = new StarTreeDocument(new Long[] { 1L, 3L, 5L, 8L }, new Double[] { 10.0, 6.0 });

        StarTreeDocument expectedeMergedStarTreeDocument = new StarTreeDocument(new Long[] { 1L, 3L, 5L, 8L }, new Double[] { 14.0, 14.0 });
        StarTreeDocument mergedStarTreeDocument = builder.reduceStarTreeDocuments(null, starTreeDocument1);
        StarTreeDocument resultStarTreeDocument = builder.reduceStarTreeDocuments(mergedStarTreeDocument, starTreeDocument2);

        assertEquals(resultStarTreeDocument.metrics[0], expectedeMergedStarTreeDocument.metrics[0]);
        assertEquals(resultStarTreeDocument.metrics[1], expectedeMergedStarTreeDocument.metrics[1]);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        dataOut.close();
        metaOut.close();
        directory.close();
    }
}
