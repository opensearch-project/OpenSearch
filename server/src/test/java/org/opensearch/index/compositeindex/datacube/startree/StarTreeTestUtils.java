/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.lucene.store.IndexInput;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.fileformats.meta.StarTreeMetadata;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.InMemoryTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeFactory;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.CompositeMappedFieldType;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

import static org.opensearch.index.compositeindex.CompositeIndexConstants.COMPOSITE_FIELD_MARKER;
import static org.opensearch.index.compositeindex.datacube.startree.fileformats.StarTreeWriter.VERSION_CURRENT;
import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues;
import static org.opensearch.index.mapper.CompositeMappedFieldType.CompositeFieldType.STAR_TREE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StarTreeTestUtils {

    public static StarTreeDocument[] getSegmentsStarTreeDocuments(
        List<StarTreeValues> starTreeValuesSubs,
        List<StarTreeNumericType> starTreeNumericTypes,
        int numDocs
    ) throws IOException {
        List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
        for (StarTreeValues starTreeValues : starTreeValuesSubs) {
            List<Dimension> dimensionsSplitOrder = starTreeValues.getStarTreeField().getDimensionsOrder();
            SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[dimensionsSplitOrder.size()];

            for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
                String dimension = dimensionsSplitOrder.get(i).getField();
                dimensionReaders[i] = new SequentialDocValuesIterator(starTreeValues.getDimensionDocIdSetIterator(dimension));
            }

            List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
            // get doc id set iterators for metrics
            for (Metric metric : starTreeValues.getStarTreeField().getMetrics()) {
                for (MetricStat metricStat : metric.getMetrics()) {
                    if (metricStat.isDerivedMetric()) {
                        continue;
                    }
                    String metricFullName = fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                        starTreeValues.getStarTreeField().getName(),
                        metric.getField(),
                        metricStat.getTypeName()
                    );
                    metricReaders.add(new SequentialDocValuesIterator(starTreeValues.getMetricDocIdSetIterator(metricFullName)));

                }
            }
            int currentDocId = 0;
            while (currentDocId < numDocs) {
                starTreeDocuments.add(getStarTreeDocument(currentDocId, dimensionReaders, metricReaders, starTreeNumericTypes));
                currentDocId++;
            }
        }
        StarTreeDocument[] starTreeDocumentsArr = new StarTreeDocument[starTreeDocuments.size()];
        return starTreeDocuments.toArray(starTreeDocumentsArr);
    }

    public static StarTreeDocument getStarTreeDocument(
        int currentDocId,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders,
        List<StarTreeNumericType> starTreeNumericTypes
    ) throws IOException {
        Long[] dims = new Long[dimensionReaders.length];
        int i = 0;
        for (SequentialDocValuesIterator dimensionDocValueIterator : dimensionReaders) {
            dimensionDocValueIterator.nextDoc(currentDocId);
            Long val = dimensionDocValueIterator.value(currentDocId);
            dims[i] = val;
            i++;
        }
        i = 0;
        Object[] metrics = new Object[metricReaders.size()];
        for (SequentialDocValuesIterator metricDocValuesIterator : metricReaders) {
            metricDocValuesIterator.nextDoc(currentDocId);
            metrics[i] = toStarTreeNumericTypeValue(metricDocValuesIterator.value(currentDocId), starTreeNumericTypes.get(i));
            i++;
        }
        return new StarTreeDocument(dims, metrics);
    }

    public static Double toStarTreeNumericTypeValue(Long value, StarTreeNumericType starTreeNumericType) {
        try {
            return starTreeNumericType.getDoubleValue(value);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot convert " + value + " to sortable aggregation type", e);
        }
    }

    public static void assertStarTreeDocuments(StarTreeDocument[] starTreeDocuments, StarTreeDocument[] expectedStarTreeDocuments) {

        assertNotNull(starTreeDocuments);
        assertEquals(starTreeDocuments.length, expectedStarTreeDocuments.length);

        for (int i = 0; i < starTreeDocuments.length; i++) {

            StarTreeDocument resultStarTreeDocument = starTreeDocuments[i];
            StarTreeDocument expectedStarTreeDocument = expectedStarTreeDocuments[i];

            assertNotNull(resultStarTreeDocument.dimensions);
            assertNotNull(resultStarTreeDocument.metrics);

            assertEquals(expectedStarTreeDocument.dimensions.length, resultStarTreeDocument.dimensions.length);
            assertEquals(expectedStarTreeDocument.metrics.length, resultStarTreeDocument.metrics.length);

            for (int di = 0; di < resultStarTreeDocument.dimensions.length; di++) {
                assertEquals(expectedStarTreeDocument.dimensions[di], resultStarTreeDocument.dimensions[di]);
            }

            for (int mi = 0; mi < resultStarTreeDocument.metrics.length; mi++) {
                if (expectedStarTreeDocument.metrics[mi] instanceof Long) {
                    assertEquals(((Long) expectedStarTreeDocument.metrics[mi]).doubleValue(), resultStarTreeDocument.metrics[mi]);
                } else {
                    assertEquals(expectedStarTreeDocument.metrics[mi], resultStarTreeDocument.metrics[mi]);
                }
            }
        }
    }

    public static void validateFileFormats(
        IndexInput dataIn,
        IndexInput metaIn,
        InMemoryTreeNode rootNode,
        StarTreeMetadata expectedStarTreeMetadata
    ) throws IOException {
        long magicMarker = metaIn.readLong();
        assertEquals(COMPOSITE_FIELD_MARKER, magicMarker);
        int version = metaIn.readVInt();
        assertEquals(VERSION_CURRENT, version);

        String compositeFieldName = metaIn.readString();
        assertEquals(expectedStarTreeMetadata.getStarTreeFieldName(), compositeFieldName);
        CompositeMappedFieldType.CompositeFieldType compositeFieldType = CompositeMappedFieldType.CompositeFieldType.fromName(
            metaIn.readString()
        );
        assertEquals(STAR_TREE, compositeFieldType);
        StarTreeMetadata resultStarTreeMetadata = new StarTreeMetadata(metaIn, compositeFieldName, compositeFieldType, version);
        assertStarTreeMetadata(expectedStarTreeMetadata, resultStarTreeMetadata);

        IndexInput starTreeIndexInput = dataIn.slice(
            "star-tree data slice for respective star-tree fields",
            resultStarTreeMetadata.getDataStartFilePointer(),
            resultStarTreeMetadata.getDataLength()
        );

        StarTreeNode starTreeNode = StarTreeFactory.createStarTree(starTreeIndexInput, resultStarTreeMetadata);
        Queue<StarTreeNode> expectedTreeNodeQueue = new ArrayDeque<>();
        Queue<InMemoryTreeNode> resultTreeNodeQueue = new ArrayDeque<>();

        expectedTreeNodeQueue.add(starTreeNode);
        resultTreeNodeQueue.add(rootNode);

        while ((starTreeNode = expectedTreeNodeQueue.poll()) != null && (rootNode = resultTreeNodeQueue.poll()) != null) {

            // verify the star node
            assertStarTreeNode(starTreeNode, rootNode);

            Iterator<? extends StarTreeNode> expectedChildrenIterator = starTreeNode.getChildrenIterator();

            List<InMemoryTreeNode> sortedChildren = new ArrayList<>();
            if (rootNode.getChildren() != null) {
                sortedChildren = new ArrayList<>(rootNode.getChildren().values());
            }

            if (starTreeNode.getChildDimensionId() != -1) {
                assertFalse(sortedChildren.isEmpty());
                int childCount = 0;
                boolean childStarNodeAsserted = false;
                while (expectedChildrenIterator.hasNext()) {
                    StarTreeNode child = expectedChildrenIterator.next();
                    InMemoryTreeNode resultChildNode = null;
                    if (!childStarNodeAsserted && rootNode.getChildStarNode() != null) {
                        // check if star tree node exists
                        resultChildNode = rootNode.getChildStarNode();
                        assertNotNull(child);
                        assertNotNull(starTreeNode.getChildStarNode());
                        assertStarTreeNode(child, resultChildNode);
                        childStarNodeAsserted = true;
                    } else {
                        resultChildNode = sortedChildren.get(childCount);
                        assertNotNull(child);
                        assertNotNull(resultChildNode);
                        if (child.getStarTreeNodeType() != StarTreeNodeType.NULL.getValue()) {
                            assertNotNull(starTreeNode.getChildForDimensionValue(child.getDimensionValue()));
                        } else {
                            assertNull(starTreeNode.getChildForDimensionValue(child.getDimensionValue()));
                        }
                        assertStarTreeNode(child, resultChildNode);
                        assertNotEquals(child.getStarTreeNodeType(), StarTreeNodeType.STAR.getValue());
                        childCount++;
                    }

                    expectedTreeNodeQueue.add(child);
                    resultTreeNodeQueue.add(resultChildNode);
                }

                assertEquals(childCount, rootNode.getChildren().size());
            } else {
                assertTrue(rootNode.getChildren().isEmpty());
            }
        }

        assertTrue(expectedTreeNodeQueue.isEmpty());
        assertTrue(resultTreeNodeQueue.isEmpty());

    }

    public static void assertStarTreeNode(StarTreeNode starTreeNode, InMemoryTreeNode treeNode) throws IOException {
        assertEquals(starTreeNode.getDimensionId(), treeNode.getDimensionId());
        assertEquals(starTreeNode.getDimensionValue(), treeNode.getDimensionValue());
        assertEquals(starTreeNode.getStartDocId(), treeNode.getStartDocId());
        assertEquals(starTreeNode.getEndDocId(), treeNode.getEndDocId());
        assertEquals(starTreeNode.getChildDimensionId(), treeNode.getChildDimensionId());
        assertEquals(starTreeNode.getAggregatedDocId(), treeNode.getAggregatedDocId());

        if (starTreeNode.getChildDimensionId() != -1) {
            assertFalse(starTreeNode.isLeaf());
            if (treeNode.getChildren() != null) {
                assertEquals(
                    starTreeNode.getNumChildren(),
                    treeNode.getChildren().values().size() + (treeNode.getChildStarNode() != null ? 1 : 0)
                );
            }
        } else {
            assertTrue(starTreeNode.isLeaf());
        }

    }

    public static void assertStarTreeMetadata(StarTreeMetadata expectedStarTreeMetadata, StarTreeMetadata resultStarTreeMetadata) {

        assertEquals(expectedStarTreeMetadata.getCompositeFieldName(), resultStarTreeMetadata.getCompositeFieldName());
        assertEquals(expectedStarTreeMetadata.getCompositeFieldType(), resultStarTreeMetadata.getCompositeFieldType());
        assertEquals(expectedStarTreeMetadata.getDimensionFields().size(), resultStarTreeMetadata.getDimensionFields().size());
        for (int i = 0; i < expectedStarTreeMetadata.getDimensionFields().size(); i++) {
            assertEquals(expectedStarTreeMetadata.getDimensionFields().get(i), resultStarTreeMetadata.getDimensionFields().get(i));
        }
        assertEquals(expectedStarTreeMetadata.getMetrics().size(), resultStarTreeMetadata.getMetrics().size());

        for (int i = 0; i < expectedStarTreeMetadata.getMetrics().size(); i++) {

            Metric expectedMetric = expectedStarTreeMetadata.getMetrics().get(i);
            Metric resultMetric = resultStarTreeMetadata.getMetrics().get(i);
            assertEquals(expectedMetric.getField(), resultMetric.getField());
            List<MetricStat> metricStats = new ArrayList<>();
            for (MetricStat metricStat : expectedMetric.getMetrics()) {
                if (metricStat.isDerivedMetric()) {
                    continue;
                }
                metricStats.add(metricStat);
            }
            Metric expectedMetricWithoutDerivedMetrics = new Metric(expectedMetric.getField(), metricStats);
            metricStats = new ArrayList<>();
            for (MetricStat metricStat : resultMetric.getMetrics()) {
                if (metricStat.isDerivedMetric()) {
                    continue;
                }
                metricStats.add(metricStat);
            }
            Metric resultantMetricWithoutDerivedMetrics = new Metric(resultMetric.getField(), metricStats);

            // assert base metrics are in order in metadata
            for (int j = 0; j < expectedMetricWithoutDerivedMetrics.getMetrics().size(); j++) {
                assertEquals(
                    expectedMetricWithoutDerivedMetrics.getMetrics().get(j),
                    resultantMetricWithoutDerivedMetrics.getMetrics().get(j)
                );
            }

            // assert all metrics ( including derived metrics are present )
            for (int j = 0; j < expectedMetric.getMetrics().size(); j++) {
                assertTrue(resultMetric.getMetrics().contains(expectedMetric.getMetrics().get(j)));
            }

        }

        assertEquals(expectedStarTreeMetadata.getSegmentAggregatedDocCount(), resultStarTreeMetadata.getSegmentAggregatedDocCount());
        assertEquals(expectedStarTreeMetadata.getStarTreeDocCount(), resultStarTreeMetadata.getStarTreeDocCount());
        assertEquals(expectedStarTreeMetadata.getMaxLeafDocs(), resultStarTreeMetadata.getMaxLeafDocs());
        assertEquals(
            expectedStarTreeMetadata.getSkipStarNodeCreationInDims().size(),
            resultStarTreeMetadata.getSkipStarNodeCreationInDims().size()
        );
        for (String skipDimension : expectedStarTreeMetadata.getSkipStarNodeCreationInDims()) {
            assertTrue(resultStarTreeMetadata.getSkipStarNodeCreationInDims().contains(skipDimension));
        }
        assertEquals(expectedStarTreeMetadata.getStarTreeBuildMode(), resultStarTreeMetadata.getStarTreeBuildMode());
        assertEquals(expectedStarTreeMetadata.getDataStartFilePointer(), resultStarTreeMetadata.getDataStartFilePointer());
        assertEquals(expectedStarTreeMetadata.getDataLength(), resultStarTreeMetadata.getDataLength());
        assertEquals(0, (resultStarTreeMetadata.getDataLength()) % 33);
    }

}
