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
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.BaseStarTreeBuilder;
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
import org.opensearch.common.settings.Settings;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.fielddata.IndexNumericFieldData;
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
    private static SegmentWriteState state;
    private static StarTreeField starTreeField;

    @BeforeClass
    public static void setup() throws IOException {

        dimensionsOrder = List.of(new Dimension("field1"), new Dimension("field3"), new Dimension("field5"), new Dimension("field8"));
        metrics = List.of(new Metric("field2", List.of(MetricStat.SUM)), new Metric("field4", List.of(MetricStat.SUM)));

        starTreeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
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
        state = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));

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

        builder = new BaseStarTreeBuilder(starTreeField, fieldProducerMap, state, mapperService) {
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
            public long getDimensionValue(int docId, int dimensionId) throws IOException {
                return 0;
            }

            @Override
            public Iterator<StarTreeDocument> sortAndAggregateStarTreeDocument(int numDocs) throws IOException {
                return null;
            }

            @Override
            public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
                throws IOException {
                return null;
            }
        };
    }

    public void test_generateMetricAggregatorInfos() throws IOException {
        List<MetricAggregatorInfo> metricAggregatorInfos = builder.generateMetricAggregatorInfos(mapperService, state);
        List<MetricAggregatorInfo> expectedMetricAggregatorInfos = List.of(
            new MetricAggregatorInfo(MetricStat.SUM, "field2", starTreeField.getName(), IndexNumericFieldData.NumericType.DOUBLE, null),
            new MetricAggregatorInfo(MetricStat.SUM, "field4", starTreeField.getName(), IndexNumericFieldData.NumericType.DOUBLE, null)
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
        directory.close();
    }
}
