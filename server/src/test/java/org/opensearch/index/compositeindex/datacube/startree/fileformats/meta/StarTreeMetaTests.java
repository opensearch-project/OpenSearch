/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.fileformats.meta;

import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.CompositeMappedFieldType;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.mapper.CompositeMappedFieldType.CompositeFieldType.STAR_TREE;

public class StarTreeMetaTests extends OpenSearchTestCase {

    private IndexOutput metaOut;
    private IndexInput metaIn;
    private StarTreeField starTreeField;
    private SegmentWriteState writeState;
    private Directory directory;
    private FieldInfo[] fieldsInfo;
    private List<Dimension> dimensionsOrder;
    private List<String> fields = List.of();
    private List<Metric> metrics;
    private List<MetricAggregatorInfo> metricAggregatorInfos = new ArrayList<>();
    private int segmentDocumentCount;
    private long dataFilePointer;
    private long dataFileLength;

    @Before
    public void setup() throws IOException {
        fields = List.of("field1", "field2", "field3", "field4", "field5", "field6", "field7", "field8", "field9", "field10");
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
        writeState = new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));
    }

    public void test_starTreeMetadata() throws IOException {
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
        int maxLeafDocs = randomNonNegativeInt();
        StarTreeFieldConfiguration starTreeFieldConfiguration = new StarTreeFieldConfiguration(
            maxLeafDocs,
            new HashSet<>(),
            StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
        );
        starTreeField = new StarTreeField("star_tree", dimensionsOrder, metrics, starTreeFieldConfiguration);

        for (Metric metric : metrics) {
            for (MetricStat metricType : metric.getMetrics()) {
                MetricAggregatorInfo metricAggregatorInfo = new MetricAggregatorInfo(
                    metricType,
                    metric.getField(),
                    starTreeField.getName(),
                    IndexNumericFieldData.NumericType.DOUBLE
                );
                metricAggregatorInfos.add(metricAggregatorInfo);
            }
        }

        dataFileLength = randomNonNegativeLong();
        dataFilePointer = randomNonNegativeLong();
        segmentDocumentCount = randomNonNegativeInt();
        metaOut = directory.createOutput("star-tree-metadata", IOContext.DEFAULT);
        StarTreeWriter.writeStarTreeMetadata(
            metaOut,
            starTreeField,
            metricAggregatorInfos,
            segmentDocumentCount,
            dataFilePointer,
            dataFileLength
        );
        metaOut.close();

        // reading and asserting the metadata
        metaIn = directory.openInput("star-tree-metadata", IOContext.READONCE);
        assertEquals(COMPOSITE_FIELD_MARKER, metaIn.readLong());
        assertEquals(VERSION_CURRENT, metaIn.readVInt());

        String compositeFieldName = metaIn.readString();
        CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(
            metaIn.readString()
        );

        StarTreeMetadata starTreeMetadata = new StarTreeMetadata(metaIn, compositeFieldName, compositeFieldType);
        assertEquals(starTreeField.getName(), starTreeMetadata.getCompositeFieldName());
        assertEquals(STAR_TREE, starTreeMetadata.getCompositeFieldType());

        assertNotNull(starTreeMetadata);

        for (int i = 0; i < dimensionsOrder.size(); i++) {
            assertEquals(dimensionsOrder.get(i).getField(), starTreeMetadata.getDimensionFields().get(i));
        }

        for (int i = 0; i < metricAggregatorInfos.size(); i++) {
            MetricEntry metricEntry = starTreeMetadata.getMetricEntries().get(i);
            assertEquals(metricAggregatorInfos.get(i).getField(), metricEntry.getMetricFieldName());
            assertEquals(metricAggregatorInfos.get(i).getMetricStat(), metricEntry.getMetricStat());
        }
        assertEquals(segmentDocumentCount, starTreeMetadata.getSegmentAggregatedDocCount(), 0);
        assertEquals(maxLeafDocs, starTreeMetadata.getMaxLeafDocs(), 0);
        assertEquals(
            starTreeFieldConfiguration.getSkipStarNodeCreationInDims().size(),
            starTreeMetadata.getSkipStarNodeCreationInDims().size()
        );
        for (String skipStarNodeCreationInDims : starTreeField.getStarTreeConfig().getSkipStarNodeCreationInDims()) {
            assertTrue(starTreeMetadata.getSkipStarNodeCreationInDims().contains(skipStarNodeCreationInDims));
        }
        assertEquals(starTreeFieldConfiguration.getBuildMode(), starTreeMetadata.getStarTreeBuildMode());
        assertEquals(dataFileLength, starTreeMetadata.getDataLength());
        assertEquals(dataFilePointer, starTreeMetadata.getDataStartFilePointer());

        metaIn.close();

    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        metaOut.close();
        metaIn.close();
        directory.close();
    }

}
