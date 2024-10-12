/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.codec.composite.LuceneDocValuesProducerFactory;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.mapper.FieldValueConverter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;

import static org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils.validateFileFormats;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeDimensionsDocValues;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.apache.lucene.tests.util.LuceneTestCase.newIOContext;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class BuilderTestsUtils {
    public static SequentialDocValuesIterator[] getDimensionIterators(StarTreeDocument[] starTreeDocuments) {
        SequentialDocValuesIterator[] sequentialDocValuesIterators =
            new SequentialDocValuesIterator[starTreeDocuments[0].dimensions.length];
        for (int j = 0; j < starTreeDocuments[0].dimensions.length; j++) {
            List<Long> dimList = new ArrayList<>();
            List<Integer> docsWithField = new ArrayList<>();

            for (int i = 0; i < starTreeDocuments.length; i++) {
                if (starTreeDocuments[i].dimensions[j] != null) {
                    dimList.add(starTreeDocuments[i].dimensions[j]);
                    docsWithField.add(i);
                }
            }
            sequentialDocValuesIterators[j] = new SequentialDocValuesIterator(
                new SortedNumericStarTreeValuesIterator(getSortedNumericMock(dimList, docsWithField))
            );
        }
        return sequentialDocValuesIterators;
    }

    public static List<SequentialDocValuesIterator> getMetricIterators(StarTreeDocument[] starTreeDocuments) {
        List<SequentialDocValuesIterator> sequentialDocValuesIterators = new ArrayList<>();
        for (int j = 0; j < starTreeDocuments[0].metrics.length; j++) {
            List<Long> metricslist = new ArrayList<>();
            List<Integer> docsWithField = new ArrayList<>();

            for (int i = 0; i < starTreeDocuments.length; i++) {
                if (starTreeDocuments[i].metrics[j] != null) {
                    metricslist.add((long) starTreeDocuments[i].metrics[j]);
                    docsWithField.add(i);
                }
            }
            sequentialDocValuesIterators.add(
                new SequentialDocValuesIterator(new SortedNumericStarTreeValuesIterator(getSortedNumericMock(metricslist, docsWithField)))
            );
        }
        return sequentialDocValuesIterators;
    }

    public static SortedNumericDocValues getSortedNumericMock(List<Long> dimList, List<Integer> docsWithField) {
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

    public static SortedSetDocValues getSortedSetMock(List<Long> dimList, List<Integer> docsWithField) {
        return new SortedSetDocValues() {
            int index = -1;

            @Override
            public long nextOrd() throws IOException {
                return dimList.get(index);
            }

            @Override
            public int docValueCount() {
                return 1;
            }

            @Override
            public BytesRef lookupOrd(long l) throws IOException {
                return new BytesRef("dummy" + l);
            }

            @Override
            public long getValueCount() {
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

    public static void validateStarTree(
        InMemoryTreeNode root,
        int totalDimensions,
        int maxLeafDocuments,
        List<StarTreeDocument> starTreeDocuments
    ) {
        Queue<Object[]> queue = new LinkedList<>();
        queue.offer(new Object[] { root, false });
        while (!queue.isEmpty()) {
            Object[] current = queue.poll();
            InMemoryTreeNode node = (InMemoryTreeNode) current[0];
            boolean currentIsStarNode = (boolean) current[1];

            assertNotNull(node);

            // assert dimensions
            if (node.getDimensionId() != StarTreeUtils.ALL) {
                assertTrue(node.getDimensionId() >= 0 && node.getDimensionId() < totalDimensions);
            }

            if (node.getChildren() != null && !node.getChildren().isEmpty()) {
                assertEquals(node.getDimensionId() + 1, node.getChildDimensionId());
                assertTrue(node.getChildDimensionId() < totalDimensions);
                InMemoryTreeNode starNode = null;
                Object[] nonStarNodeCumulativeMetrics = getMetrics(starTreeDocuments);
                for (Map.Entry<Long, InMemoryTreeNode> entry : node.getChildren().entrySet()) {
                    Long childDimensionValue = entry.getKey();
                    InMemoryTreeNode child = entry.getValue();
                    Object[] currMetrics = getMetrics(starTreeDocuments);
                    if (child.getNodeType() != StarTreeNodeType.STAR.getValue()) {
                        // Validate dimension values in documents
                        for (int i = child.getStartDocId(); i < child.getEndDocId(); i++) {
                            StarTreeDocument doc = starTreeDocuments.get(i);
                            int j = 0;
                            addMetrics(doc, currMetrics, j);
                            if (child.getNodeType() != StarTreeNodeType.STAR.getValue()) {
                                Long dimension = doc.dimensions[child.getDimensionId()];
                                assertEquals(childDimensionValue, dimension);
                                if (dimension != null) {
                                    assertEquals(child.getDimensionValue(), (long) dimension);
                                } else {
                                    // TODO : fix this ?
                                    assertEquals(child.getDimensionValue(), StarTreeUtils.ALL);
                                }
                            }
                        }
                        Object[] aggregatedMetrics = starTreeDocuments.get(child.getAggregatedDocId()).metrics;
                        int j = 0;
                        for (Object metric : currMetrics) {
                            /*
                             * TODO : refactor this to handle any data type
                             */
                            if (metric instanceof Double) {
                                nonStarNodeCumulativeMetrics[j] = (double) nonStarNodeCumulativeMetrics[j] + (double) metric;
                                assertEquals((Double) metric, (Double) aggregatedMetrics[j], 0);
                            } else if (metric instanceof Long) {
                                nonStarNodeCumulativeMetrics[j] = (long) nonStarNodeCumulativeMetrics[j] + (long) metric;
                                assertEquals((long) metric, (long) aggregatedMetrics[j]);
                            } else if (metric instanceof Float) {
                                nonStarNodeCumulativeMetrics[j] = (float) nonStarNodeCumulativeMetrics[j] + (float) metric;
                                assertEquals((float) metric, (float) aggregatedMetrics[j], 0);
                            }
                            j++;
                        }
                        queue.offer(new Object[] { child, false });
                    } else {
                        starNode = child;
                    }
                }
                // Add star node to queue
                if (starNode != null) {
                    Object[] starNodeMetrics = getMetrics(starTreeDocuments);
                    for (int i = starNode.getStartDocId(); i < starNode.getEndDocId(); i++) {
                        StarTreeDocument doc = starTreeDocuments.get(i);
                        int j = 0;
                        addMetrics(doc, starNodeMetrics, j);
                    }
                    int j = 0;
                    Object[] aggregatedMetrics = starTreeDocuments.get(starNode.getAggregatedDocId()).metrics;
                    for (Object nonStarNodeCumulativeMetric : nonStarNodeCumulativeMetrics) {
                        assertEquals(nonStarNodeCumulativeMetric, starNodeMetrics[j]);
                        assertEquals(starNodeMetrics[j], aggregatedMetrics[j]);
                        /*
                         * TODO : refactor this to handle any data type
                         */
                        if (nonStarNodeCumulativeMetric instanceof Double) {
                            assertEquals((double) nonStarNodeCumulativeMetric, (double) starNodeMetrics[j], 0);
                            assertEquals((double) nonStarNodeCumulativeMetric, (double) aggregatedMetrics[j], 0);
                        } else if (nonStarNodeCumulativeMetric instanceof Long) {
                            assertEquals((long) nonStarNodeCumulativeMetric, (long) starNodeMetrics[j]);
                            assertEquals((long) nonStarNodeCumulativeMetric, (long) aggregatedMetrics[j]);
                        } else if (nonStarNodeCumulativeMetric instanceof Float) {
                            assertEquals((float) nonStarNodeCumulativeMetric, (float) starNodeMetrics[j], 0);
                            assertEquals((float) nonStarNodeCumulativeMetric, (float) aggregatedMetrics[j], 0);
                        }

                        j++;
                    }
                    assertEquals(-1L, starNode.getDimensionValue());
                    queue.offer(new Object[] { starNode, true });
                }
            } else {
                assertTrue(node.getEndDocId() - node.getStartDocId() <= maxLeafDocuments);
            }

            if (currentIsStarNode) {
                StarTreeDocument prevDoc = null;
                int docCount = 0;
                int docId = node.getStartDocId();
                int dimensionId = node.getDimensionId();

                while (docId < node.getEndDocId()) {
                    StarTreeDocument currentDoc = starTreeDocuments.get(docId);
                    docCount++;

                    // Verify that the dimension at 'dimensionId' is set to STAR_IN_DOC_VALUES_INDEX
                    assertNull(currentDoc.dimensions[dimensionId]);

                    // Verify sorting of documents
                    if (prevDoc != null) {
                        assertTrue(compareDocuments(prevDoc, currentDoc, dimensionId + 1, totalDimensions) <= 0);
                    }
                    prevDoc = currentDoc;
                    docId++;
                }

                // Verify that the number of generated star documents matches the range in the star node
                assertEquals(node.getEndDocId() - node.getStartDocId(), docCount);
            }
        }
    }

    /**
     * TODO : refactor this to handle any data type
     */
    private static void addMetrics(StarTreeDocument doc, Object[] currMetrics, int j) {
        for (Object metric : doc.metrics) {
            if (metric instanceof Double) {
                currMetrics[j] = (double) currMetrics[j] + (double) metric;
            } else if (metric instanceof Long) {
                currMetrics[j] = (long) currMetrics[j] + (long) metric;
            } else if (metric instanceof Float) {
                currMetrics[j] = (float) currMetrics[j] + (float) metric;
            }
            j++;
        }
    }

    private static Object[] getMetrics(List<StarTreeDocument> starTreeDocuments) {
        Object[] nonStarNodeCumulativeMetrics = new Object[starTreeDocuments.get(0).metrics.length];
        for (int i = 0; i < nonStarNodeCumulativeMetrics.length; i++) {
            if (starTreeDocuments.get(0).metrics[i] instanceof Long) {
                nonStarNodeCumulativeMetrics[i] = 0L;
            } else if (starTreeDocuments.get(0).metrics[i] instanceof Double) {
                nonStarNodeCumulativeMetrics[i] = 0.0;
            } else if (starTreeDocuments.get(0).metrics[i] instanceof Float) {
                nonStarNodeCumulativeMetrics[i] = 0.0f;
            }
        }
        return nonStarNodeCumulativeMetrics;
    }

    private static int compareDocuments(StarTreeDocument doc1, StarTreeDocument doc2, int startDim, int endDim) {
        for (int i = startDim; i < endDim; i++) {
            Long val1 = doc1.dimensions[i];
            Long val2 = doc2.dimensions[i];

            if (!Objects.equals(val1, val2)) {
                if (val1 == null) return 1;
                if (val2 == null) return -1;
                return Long.compare(val1, val2);
            }
        }
        return 0;
    }

    public static void validateStarTreeFileFormats(
        InMemoryTreeNode rootNode,
        int numDocs,
        StarTreeMetadata expectedStarTreeMetadata,
        List<StarTreeDocument> expectedStarTreeDocuments,
        String dataFileName,
        String metaFileName,
        BaseStarTreeBuilder builder,
        StarTreeField starTreeField,
        SegmentWriteState writeState,
        Directory directory
    ) throws IOException {

        assertNotNull(rootNode.getChildren());
        assertFalse(rootNode.getChildren().isEmpty());
        SegmentReadState readState = getReadState(
            numDocs,
            expectedStarTreeMetadata.getDimensionFields(),
            expectedStarTreeMetadata.getMetrics(),
            starTreeField,
            writeState,
            directory
        );

        DocValuesProducer compositeDocValuesProducer = LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
            Composite912Codec.COMPOSITE_INDEX_CODEC_NAME,
            readState,
            Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
            Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
            Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
        );

        IndexInput dataIn = readState.directory.openInput(dataFileName, IOContext.DEFAULT);
        IndexInput metaIn = readState.directory.openInput(metaFileName, IOContext.DEFAULT);

        StarTreeValues starTreeValues = new StarTreeValues(expectedStarTreeMetadata, dataIn, compositeDocValuesProducer, readState);
        assertEquals(expectedStarTreeMetadata.getStarTreeDocCount(), starTreeValues.getStarTreeDocumentCount());
        List<FieldValueConverter> fieldValueConverters = new ArrayList<>();
        builder.metricAggregatorInfos.forEach(
            metricAggregatorInfo -> fieldValueConverters.add(metricAggregatorInfo.getValueAggregators().getAggregatedValueType())
        );
        StarTreeDocument[] starTreeDocuments = StarTreeTestUtils.getSegmentsStarTreeDocuments(
            List.of(starTreeValues),
            fieldValueConverters,
            readState.segmentInfo.maxDoc()
        );

        StarTreeDocument[] expectedStarTreeDocumentsArray = expectedStarTreeDocuments.toArray(new StarTreeDocument[0]);
        StarTreeTestUtils.assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocumentsArray);

        validateFileFormats(dataIn, metaIn, rootNode, expectedStarTreeMetadata);

        dataIn.close();
        metaIn.close();
        compositeDocValuesProducer.close();
    }

    public static SegmentReadState getReadState(
        int numDocs,
        Map<String, DocValuesType> dimensionFields,
        List<Metric> metrics,
        StarTreeField compositeField,
        SegmentWriteState writeState,
        Directory directory
    ) {

        int numMetrics = 0;
        for (Metric metric : metrics) {
            numMetrics += metric.getBaseMetrics().size();
        }

        FieldInfo[] fields = new FieldInfo[dimensionFields.size() + numMetrics];

        int i = 0;
        for (String dimension : dimensionFields.keySet()) {
            fields[i] = new FieldInfo(
                fullyQualifiedFieldNameForStarTreeDimensionsDocValues(compositeField.getName(), dimension),
                i,
                false,
                false,
                true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS,
                dimensionFields.get(dimension),
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
            i++;
        }

        for (Metric metric : metrics) {
            for (MetricStat metricStat : metric.getBaseMetrics()) {
                fields[i] = new FieldInfo(
                    fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                        compositeField.getName(),
                        metric.getField(),
                        metricStat.getTypeName()
                    ),
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
                i++;
            }
        }

        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            numDocs,
            false,
            false,
            new Lucene912Codec(),
            new HashMap<>(),
            writeState.segmentInfo.getId(),
            new HashMap<>(),
            null
        );
        return new SegmentReadState(segmentInfo.dir, segmentInfo, new FieldInfos(fields), writeState.context);
    }

    static void traverseStarTree(InMemoryTreeNode root, Map<Integer, Map<Long, Integer>> dimValueToDocIdMap, boolean traverStarNodes) {
        InMemoryTreeNode starTree = root;
        // Use BFS to traverse the star tree
        Queue<InMemoryTreeNode> queue = new ArrayDeque<>();
        queue.add(starTree);
        int currentDimensionId = -1;
        InMemoryTreeNode starTreeNode;
        List<Integer> docIds = new ArrayList<>();
        while ((starTreeNode = queue.poll()) != null) {
            int dimensionId = starTreeNode.getDimensionId();
            if (dimensionId > currentDimensionId) {
                currentDimensionId = dimensionId;
            }

            // store aggregated document of the node
            int docId = starTreeNode.getAggregatedDocId();
            Map<Long, Integer> map = dimValueToDocIdMap.getOrDefault(dimensionId, new HashMap<>());
            if (starTreeNode.getNodeType() == StarTreeNodeType.STAR.getValue()) {
                map.put(Long.MAX_VALUE, docId);
            } else {
                map.put(starTreeNode.getDimensionValue(), docId);
            }
            dimValueToDocIdMap.put(dimensionId, map);

            if (starTreeNode.getChildren() != null
                && (!traverStarNodes || starTreeNode.getNodeType() == StarTreeNodeType.STAR.getValue())) {
                Iterator<InMemoryTreeNode> childrenIterator = starTreeNode.getChildren().values().iterator();
                while (childrenIterator.hasNext()) {
                    InMemoryTreeNode childNode = childrenIterator.next();
                    queue.add(childNode);
                }
            }
        }
    }

    public static SegmentWriteState getWriteState(int numDocs, byte[] id, FieldInfo[] fieldsInfo, Directory directory) {
        FieldInfos fieldInfos = new FieldInfos(fieldsInfo);
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            numDocs,
            false,
            false,
            new Lucene912Codec(),
            new HashMap<>(),
            id,
            new HashMap<>(),
            null
        );
        return new SegmentWriteState(InfoStream.getDefault(), segmentInfo.dir, segmentInfo, fieldInfos, null, newIOContext(random()));
    }
}
