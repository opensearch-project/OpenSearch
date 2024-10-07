/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.CompositeIndexConstants;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.mapper.CompositeMappedFieldType.CompositeFieldType.STAR_TREE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class StarTreeBuilderTestCase extends OpenSearchTestCase {
    private final StarTreeFieldConfiguration.StarTreeBuildMode buildMode;
    protected MapperService mapperService;
    protected List<String> fields = List.of();
    protected List<Metric> metrics;
    protected Directory directory;
    protected FieldInfo[] fieldsInfo;
    protected StarTreeField compositeField;
    protected Map<String, DocValuesProducer> fieldProducerMap;
    protected SegmentWriteState writeState;
    protected BaseStarTreeBuilder builder;
    protected IndexOutput dataOut;
    protected IndexOutput metaOut;
    protected DocValuesConsumer docValuesConsumer;
    protected String dataFileName;
    protected String metaFileName;
    protected List<Dimension> dimensionsOrder;

    public StarTreeBuilderTestCase(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        this.buildMode = buildMode;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP },
            new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP }
        );
    }

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
            new Metric("field6", List.of(MetricStat.VALUE_COUNT)),
            new Metric("field9", List.of(MetricStat.MIN)),
            new Metric("field10", List.of(MetricStat.MAX)),
            new Metric("_doc_count", List.of(MetricStat.DOC_COUNT))
        );

        DocValuesProducer docValuesProducer = mock(DocValuesProducer.class);

        compositeField = new StarTreeField(
            "test",
            dimensionsOrder,
            metrics,
            new StarTreeFieldConfiguration(1, Set.of("field8"), getBuildMode())
        );
        directory = newFSDirectory(createTempDir());

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
        writeState = getWriteState(5, UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8));

        dataFileName = IndexFileNames.segmentFileName(
            writeState.segmentInfo.name,
            writeState.segmentSuffix,
            Composite912DocValuesFormat.DATA_EXTENSION
        );
        dataOut = writeState.directory.createOutput(dataFileName, writeState.context);

        metaFileName = IndexFileNames.segmentFileName(
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
        docValuesConsumer = mock(DocValuesConsumer.class);
    }

    protected BaseStarTreeBuilder getStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        switch (buildMode) {
            case ON_HEAP:
                return new OnHeapStarTreeBuilder(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
            case OFF_HEAP:
                return new OffHeapStarTreeBuilder(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
            default:
                throw new IllegalArgumentException("Invalid build mode: " + buildMode);
        }
    }

    protected StarTreeFieldConfiguration.StarTreeBuildMode getBuildMode() {
        return buildMode;
    }

    protected void validateStarTreeFileFormats(
        InMemoryTreeNode rootNode,
        int numDocs,
        StarTreeMetadata expectedStarTreeMetadata,
        List<StarTreeDocument> expectedStarTreeDocuments
    ) throws IOException {
        BuilderTestsUtils.validateStarTreeFileFormats(
            rootNode,
            numDocs,
            expectedStarTreeMetadata,
            expectedStarTreeDocuments,
            dataFileName,
            metaFileName,
            builder,
            compositeField,
            writeState,
            directory
        );

    }

    SegmentWriteState getWriteState(int numDocs, byte[] id) {
        return BuilderTestsUtils.getWriteState(numDocs, id, fieldsInfo, directory);
    }

    SegmentReadState getReadState(int numDocs, List<String> dimensionFields, List<Metric> metrics) {
        return BuilderTestsUtils.getReadState(numDocs, dimensionFields, metrics, compositeField, writeState, directory);
    }

    protected Map<String, String> getAttributes(int numSegmentDocs) {
        return Map.of(CompositeIndexConstants.SEGMENT_DOCS_COUNT, String.valueOf(numSegmentDocs));
    }

    protected List<String> getStarTreeDimensionNames(List<Dimension> dimensionsOrder) {
        List<String> dimensionNames = new ArrayList<>();
        for (Dimension dimension : dimensionsOrder) {
            dimensionNames.add(dimension.getField());
        }
        return dimensionNames;
    }

    protected StarTreeField getStarTreeField(MetricStat count) {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Metric m1 = new Metric("field2", List.of(count));
        List<Dimension> dims = List.of(d1, d2);
        List<Metric> metrics = List.of(m1);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(1000, new HashSet<>(), getBuildMode());
        return new StarTreeField("sf", dims, metrics, c);
    }

    protected StarTreeField getStarTreeFieldWithDocCount(int maxLeafDocs, boolean includeDocCountMetric) {
        Dimension d1 = new NumericDimension("field1");
        Dimension d2 = new NumericDimension("field3");
        Dimension d3 = new NumericDimension("field5");
        Dimension d4 = new NumericDimension("field8");
        List<Dimension> dims = List.of(d1, d2, d3, d4);
        Metric m1 = new Metric("field2", List.of(MetricStat.SUM));
        Metric m2 = null;
        if (includeDocCountMetric) {
            m2 = new Metric("_doc_count", List.of(MetricStat.DOC_COUNT));
        }
        List<Metric> metrics = m2 == null ? List.of(m1) : List.of(m1, m2);
        StarTreeFieldConfiguration c = new StarTreeFieldConfiguration(maxLeafDocs, new HashSet<>(), getBuildMode());
        StarTreeField sf = new StarTreeField("sf", dims, metrics, c);
        return sf;
    }

    protected void assertStarTreeDocuments(
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
            assertEquals(expectedStarTreeDocument.metrics[3], resultStarTreeDocument.metrics[3]);
            assertEquals(expectedStarTreeDocument.metrics[4], resultStarTreeDocument.metrics[4]);
        }
    }

    protected static List<StarTreeDocument> getExpectedStarTreeDocumentIterator() {
        return List.of(
            new StarTreeDocument(new Long[] { 2L, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 2L }),
            new StarTreeDocument(new Long[] { 3L, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, 2L, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, 3L, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 2L }),
            new StarTreeDocument(new Long[] { null, 4L, null, 1L }, new Object[] { 35.0, 34.0, 3L, 6.0, 24.0, 3L }),
            new StarTreeDocument(new Long[] { null, 4L, null, 4L }, new Object[] { 21.0, 14.0, 2L, 8.0, 20.0, 2L }),
            new StarTreeDocument(new Long[] { null, 4L, null, null }, new Object[] { 56.0, 48.0, 5L, 6.0, 24.0, 5L })
        );
    }

    protected long getLongFromDouble(double value) {
        return NumericUtils.doubleToSortableLong(value);
    }

    protected StarTreeMetadata getStarTreeMetadata(List<String> fields, int segmentAggregatedDocCount, int maxLeafDocs, int dataLength) {
        return new StarTreeMetadata(
            "sf",
            STAR_TREE,
            mock(IndexInput.class),
            VERSION_CURRENT,
            builder.numStarTreeNodes,
            fields,
            compositeField.getMetrics(),
            segmentAggregatedDocCount,
            builder.numStarTreeDocs,
            maxLeafDocs,
            Set.of(),
            getBuildMode(),
            0,
            dataLength
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        if (builder != null) {
            builder.close();
        }
        docValuesConsumer.close();
        metaOut.close();
        dataOut.close();
        directory.close();
    }
}
