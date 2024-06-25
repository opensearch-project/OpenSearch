/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.apache.lucene.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregationDescriptor;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.builder.SingleTreeBuilder;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreeDocValuesIteratorAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeBuilderUtils;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base class for star-tree builder
 * @opensearch.experimental
 */
public abstract class BaseStarTreeBuilder implements SingleTreeBuilder {

    private static final Logger logger = LogManager.getLogger(BaseStarTreeBuilder.class);

    public static final int STAR_IN_DOC_VALUES_INDEX = -1;

    protected final Set<Integer> skipStarNodeCreationForDimensions;

    protected final List<MetricAggregationDescriptor> metricAggregationDescriptors;
    protected final int numMetrics;
    protected final int numDimensions;
    protected int numStarTreeDocs;
    protected int totalSegmentDocs;
    protected int numStarTreeNodes;
    protected final int maxLeafDocuments;

    protected final StarTreeBuilderUtils.TreeNode rootNode = getNewNode();

    protected DocIdSetIterator[] dimensionReaders;

    protected Map<String, DocValuesProducer> fieldProducerMap;
    protected DocValuesConsumer docValuesConsumer;

    private final StarTreeDocValuesIteratorAdapter starTreeDocValuesIteratorFactory;
    private final StarTreeField starTreeField;

    /**
     * Constructor for base star-tree builder
     *
     * @param starTreeField     holds the configuration for the star tree
     * @param fieldProducerMap  helps return the doc values iterator for each type based on field name
     * @param docValuesConsumer to consume the new aggregated metrics during flush
     * @param state             stores the segment write state
     * @param mapperService     helps to find the original type of the field
     */
    protected BaseStarTreeBuilder(
        StarTreeField starTreeField,
        Map<String, DocValuesProducer> fieldProducerMap,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {

        logger.debug("Building in base star tree builder");

        this.starTreeField = starTreeField;
        StarTreeFieldConfiguration starTreeFieldSpec = starTreeField.getStarTreeConfig();
        this.docValuesConsumer = docValuesConsumer;
        this.fieldProducerMap = fieldProducerMap;
        this.starTreeDocValuesIteratorFactory = new StarTreeDocValuesIteratorAdapter();

        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        this.numDimensions = dimensionsSplitOrder.size();

        this.skipStarNodeCreationForDimensions = new HashSet<>();
        this.totalSegmentDocs = state.segmentInfo.maxDoc();
        this.dimensionReaders = new DocIdSetIterator[numDimensions];
        Set<String> skipStarNodeCreationForDimensions = starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
            FieldInfo dimensionFieldInfos = state.fieldInfos.fieldInfo(dimension);
            DocValuesType dimensionDocValuesType = state.fieldInfos.fieldInfo(dimension).getDocValuesType();
            dimensionReaders[i] = starTreeDocValuesIteratorFactory.getDocValuesIterator(
                dimensionDocValuesType,
                dimensionFieldInfos,
                fieldProducerMap.get(dimensionFieldInfos.name)
            );
        }

        this.metricAggregationDescriptors = generateMetricStatFieldPairs(mapperService, state);
        this.numMetrics = metricAggregationDescriptors.size();
        this.maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the MetricStatFieldPairs for all the metrics on a field
     *
     * @return list of metric stat mapped with respective fields
     */
    public List<MetricAggregationDescriptor> generateMetricStatFieldPairs(MapperService mapperService, SegmentWriteState state)
        throws IOException {
        List<MetricAggregationDescriptor> metricAggregationDescriptors = new ArrayList<>();
        IndexNumericFieldData.NumericType numericType;
        DocIdSetIterator metricStatReader = null;
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricType : metric.getMetrics()) {
                Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(metric.getField());
                if (fieldMapper instanceof NumberFieldMapper) {
                    numericType = ((NumberFieldMapper) fieldMapper).fieldType().numericType();
                } else {
                    logger.error("metric mapper is not of type number field mapper");
                    throw new IllegalStateException("metric mapper is not of type number field mapper");
                }
                // Ignore the column for COUNT aggregation function
                if (metricType != MetricStat.COUNT) {
                    FieldInfo metricFieldInfos = state.fieldInfos.fieldInfo(metric.getField());
                    DocValuesType metricDocValuesType = metricFieldInfos.getDocValuesType();
                    metricStatReader = starTreeDocValuesIteratorFactory.getDocValuesIterator(
                        metricDocValuesType,
                        metricFieldInfos,
                        fieldProducerMap.get(metricFieldInfos.name)
                    );
                }
                MetricAggregationDescriptor metricAggregationDescriptor = new MetricAggregationDescriptor(
                    metricType,
                    metric.getField(),
                    numericType,
                    metricStatReader
                );
                metricAggregationDescriptors.add(metricAggregationDescriptor);
            }
        }
        return metricAggregationDescriptors;
    }

    /**
     * Adds a document to the star-tree.
     *
     * @param starTreeDocument star tree document to be added
     * @throws IOException if an I/O error occurs while adding the document
     */
    public abstract void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException;

    /**
     * Returns the document of the given document id in the star-tree.
     *
     * @param docId document id
     * @return star tree document
     * @throws IOException if an I/O error occurs while fetching the star-tree document
     */
    public abstract StarTreeDocument getStarTreeDocument(int docId) throws IOException;

    /**
     * Retrieves the list of star-tree documents in the star-tree.
     *
     * @return Star tree documents
     */
    public abstract List<StarTreeDocument> getStarTreeDocuments() throws IOException;

    /**
     * Returns the value of the dimension for the given dimension id and document in the star-tree.
     *
     * @param docId       document id
     * @param dimensionId dimension id
     * @return dimension value
     */
    public abstract long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates the star-tree document in the segment, and returns a star-tree document iterator for all the
     * aggregated star-tree document.
     *
     * @param numDocs number of documents in the given segment
     * @return Iterator for the aggregated star-tree document
     */
    public abstract Iterator<StarTreeDocument> sortMergeAndAggregateStarTreeDocument(int numDocs) throws IOException;

    /**
     * Generates aggregated star-tree document for star-node.
     *
     * @param startDocId  start document id (inclusive) in the star-tree
     * @param endDocId    end document id (exclusive) in the star-tree
     * @param dimensionId dimension id of the star-node
     * @return Iterator for the aggregated star-tree documents
     */
    public abstract Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException;

    /**
     * Returns the star-tree document from the segment
     * @throws IOException when we are unable to build a star tree document from the segment
     */
    protected StarTreeDocument getSegmentStarTreeDocument() throws IOException {
        long[] dimensions = getStarTreeDimensionsFromSegment();
        Object[] metrics = getStarTreeMetricsFromSegment();
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Returns the dimension values for the next document from the segment
     *
     * @return dimension values for each of the star-tree dimension
     * @throws IOException when we are unable to iterate to the next doc for the given dimension readers
     */
    long[] getStarTreeDimensionsFromSegment() throws IOException {
        long[] dimensions = new long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            try {
                dimensionReaders[i].nextDoc();
            } catch (IOException e) {
                logger.error("unable to iterate to next doc", e);
            } catch (NullPointerException e) {
                logger.error("dimension does not have an associated reader", e);
                throw new IllegalStateException("dimension should have a valid associated reader", e);
            } catch (Exception e) {
                logger.error("unable to read the dimension values from the segment", e);
                throw new IllegalStateException("unable to read the dimension values from the segment", e);
            }

            if (starTreeField.getDimensionsOrder().get(i) instanceof DateDimension) {
                dimensions[i] = handleDateDimension(
                    starTreeField.getDimensionsOrder().get(i).getField(),
                    starTreeDocValuesIteratorFactory.getNextOrd(dimensionReaders[i])
                );
            } else {
                dimensions[i] = starTreeDocValuesIteratorFactory.getNextOrd(dimensionReaders[i]);
            }
        }
        return dimensions;
    }

    /**
     * Returns the metric values for the next document from the segment
     *
     * @return metric values for each of the star-tree metric
     * @throws IOException when we are unable to iterate to the next doc for the given metric readers
     */
    private Object[] getStarTreeMetricsFromSegment() throws IOException {
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            // Ignore the column for COUNT aggregation function
            DocIdSetIterator metricStatReader = metricAggregationDescriptors.get(i).getMetricStatReader();
            if (metricStatReader != null) {
                try {
                    metricStatReader.nextDoc();
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                } catch (NullPointerException e) {
                    logger.error("metric does not have an associated reader", e);
                    throw new IllegalStateException("metric should have a valid associated reader", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
                metrics[i] = starTreeDocValuesIteratorFactory.getNextOrd(metricStatReader);
            }
        }
        return metrics;
    }

    /**
     * Merges a star-tree document from the segment into an aggregated star-tree document.
     * A new aggregated star-tree document is created if the aggregated segment document is null.
     *
     * @param aggregatedSegmentDocument aggregated star-tree document
     * @param segmentDocument           segment star-tree document
     * @return merged star-tree document
     */
    protected StarTreeDocument reduceSegmentStarTreeDocuments(
        StarTreeDocument aggregatedSegmentDocument,
        StarTreeDocument segmentDocument
    ) {
        if (aggregatedSegmentDocument == null) {
            long[] dimensions = Arrays.copyOf(segmentDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator metricValueAggregator = metricAggregationDescriptors.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregationDescriptors.get(i).getStarTreeNumericType();
                    metrics[i] = metricValueAggregator.getInitialAggregatedValue((Long) segmentDocument.metrics[i], starTreeNumericType);
                } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
                    logger.error("Cannot parse initial segment doc value", e);
                    throw new IllegalStateException("Cannot parse initial segment doc value [" + segmentDocument.metrics[i] + "]");
                } catch (Exception e) {
                    logger.error("Cannot parse initial segment doc value", e);
                    throw new RuntimeException("Cannot parse initial segment doc value [" + segmentDocument.metrics[i] + "]");
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator metricValueAggregator = metricAggregationDescriptors.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregationDescriptors.get(i).getStarTreeNumericType();
                    aggregatedSegmentDocument.metrics[i] = metricValueAggregator.applySegmentRawValue(
                        aggregatedSegmentDocument.metrics[i],
                        (Long) segmentDocument.metrics[i],
                        starTreeNumericType
                    );
                } catch (IllegalArgumentException | NullPointerException | IllegalStateException e) {
                    logger.error("Cannot apply segment doc value for aggregation", e);
                    throw new IllegalStateException("Cannot apply segment doc value for aggregation [" + segmentDocument.metrics[i] + "]");
                } catch (Exception e) {
                    logger.error("Cannot apply segment doc value for aggregation", e);
                    throw new RuntimeException("Cannot apply segment doc value for aggregation [" + segmentDocument.metrics[i] + "]");
                }
            }
            return aggregatedSegmentDocument;
        }
    }

    /**
     * Merges a star-tree document into an aggregated star-tree document.
     * A new aggregated star-tree document is created if the aggregated document is null.
     *
     * @param aggregatedDocument aggregated star-tree document
     * @param starTreeDocument   segment star-tree document
     * @return merged star-tree document
     */
    public StarTreeDocument reduceStarTreeDocuments(StarTreeDocument aggregatedDocument, StarTreeDocument starTreeDocument) {
        // aggregate the documents
        if (aggregatedDocument == null) {
            long[] dimensions = Arrays.copyOf(starTreeDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    metrics[i] = metricAggregationDescriptors.get(i).getValueAggregators().getAggregatedValue(starTreeDocument.metrics[i]);
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot get aggregated value", e);
                    throw new IllegalArgumentException("Cannot get aggregated value[" + starTreeDocument.metrics[i] + "]");
                } catch (Exception e) {
                    logger.error("Cannot get value for aggregation", e);
                    throw new RuntimeException("Cannot get value for aggregation[" + starTreeDocument.metrics[i] + "]");
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    aggregatedDocument.metrics[i] = metricAggregationDescriptors.get(i)
                        .getValueAggregators()
                        .applyAggregatedValue(starTreeDocument.metrics[i], aggregatedDocument.metrics[i]);
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot apply value to aggregated document for aggregation", e);
                    throw new IllegalArgumentException(
                        "Cannot apply value to aggregated document for aggregation[" + starTreeDocument.metrics[i] + "]"
                    );
                } catch (Exception e) {
                    logger.error("Cannot apply value to aggregated document for aggregation", e);
                    throw new RuntimeException(
                        "Cannot apply value to aggregated document for aggregation [" + starTreeDocument.metrics[i] + "]"
                    );
                }
            }
            return aggregatedDocument;
        }
    }

    /**
     * Builds the star tree using total segment documents
     *
     * @throws IOException when we are unable to build star-tree
     */
    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.debug("Star-tree build is a go with star tree field{}", starTreeField);

        Iterator<StarTreeDocument> starTreeDocumentIterator = sortMergeAndAggregateStarTreeDocument(totalSegmentDocs);
        logger.debug("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        build(starTreeDocumentIterator);
        logger.debug("Finished Building star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    /**
     * Builds the star tree using Star-Tree Document
     *
     * @param starTreeDocumentIterator contains the sorted and aggregated documents
     * @throws IOException when we are unable to build star-tree
     */
    public void build(Iterator<StarTreeDocument> starTreeDocumentIterator) throws IOException {
        int numSegmentStarTreeDocument = totalSegmentDocs;

        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        int numStarTreeDocument = numStarTreeDocs;
        logger.debug("Generated star tree docs : [{}] from segment docs : [{}]", numStarTreeDocument, numSegmentStarTreeDocument);

        if (numStarTreeDocs == 0) {
            // TODO: Uncomment when segment codec is ready
            // StarTreeBuilderUtils.serializeTree(indexOutput, rootNode, dimensionsSplitOrder, numNodes);
            return;
        }

        constructStarTree(rootNode, 0, numStarTreeDocs);
        int numStarTreeDocumentUnderStarNode = numStarTreeDocs - numStarTreeDocument;
        logger.debug(
            "Finished constructing star-tree, got [ {} ] tree nodes and [ {} ] starTreeDocument under star-node",
            numStarTreeNodes,
            numStarTreeDocumentUnderStarNode
        );

        createAggregatedDocs(rootNode);
        int numAggregatedStarTreeDocument = numStarTreeDocs - numStarTreeDocument - numStarTreeDocumentUnderStarNode;
        logger.debug("Finished creating aggregated documents : {}", numAggregatedStarTreeDocument);

        // TODO: When StarTree Codec is ready
        // Create doc values indices in disk
        // Serialize and save in disk
        // Write star tree metadata for off heap implementation

    }

    /**
     * Adds a document to star-tree
     *
     * @param starTreeDocument star-tree document
     * @throws IOException throws an exception if we are unable to add the doc
     */
    private void appendToStarTree(StarTreeDocument starTreeDocument) throws IOException {
        appendStarTreeDocument(starTreeDocument);
        numStarTreeDocs++;
    }

    /**
     * Returns a new star-tree node
     *
     * @return return new star-tree node
     */
    private StarTreeBuilderUtils.TreeNode getNewNode() {
        numStarTreeNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    /**
     * Implements the algorithm to construct a star-tree
     *
     * @param node         star-tree node
     * @param startDocId   start document id
     * @param endDocId     end document id
     * @throws IOException throws an exception if we are unable to construct the tree
     */
    private void constructStarTree(StarTreeBuilderUtils.TreeNode node, int startDocId, int endDocId) throws IOException {

        int childDimensionId = node.dimensionId + 1;
        if (childDimensionId == numDimensions) {
            return;
        }

        // Construct all non-star children nodes
        node.childDimensionId = childDimensionId;
        Map<Long, StarTreeBuilderUtils.TreeNode> children = constructNonStarNodes(startDocId, endDocId, childDimensionId);
        node.children = children;

        // Construct star-node if required
        if (!skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
            children.put((long) StarTreeBuilderUtils.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
        }

        // Further split on child nodes if required
        for (StarTreeBuilderUtils.TreeNode child : children.values()) {
            if (child.endDocId - child.startDocId > maxLeafDocuments) {
                constructStarTree(child, child.startDocId, child.endDocId);
            }
        }
    }

    /**
     * Constructs non star tree nodes
     *
     * @param startDocId  start document id
     * @param endDocId    end document id
     * @param dimensionId id of the dimension in the star tree
     * @return root node with non-star nodes constructed
     * @throws IOException throws an exception if we are unable to construct non-star nodes
     */
    private Map<Long, StarTreeBuilderUtils.TreeNode> constructNonStarNodes(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        Map<Long, StarTreeBuilderUtils.TreeNode> nodes = new HashMap<>();
        int nodeStartDocId = startDocId;
        long nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
        for (int i = startDocId + 1; i < endDocId; i++) {
            long dimensionValue = getDimensionValue(i, dimensionId);
            if (dimensionValue != nodeDimensionValue) {
                StarTreeBuilderUtils.TreeNode child = getNewNode();
                child.dimensionId = dimensionId;
                child.dimensionValue = nodeDimensionValue;
                child.startDocId = nodeStartDocId;
                child.endDocId = i;
                nodes.put(nodeDimensionValue, child);

                nodeStartDocId = i;
                nodeDimensionValue = dimensionValue;
            }
        }
        StarTreeBuilderUtils.TreeNode lastNode = getNewNode();
        lastNode.dimensionId = dimensionId;
        lastNode.dimensionValue = nodeDimensionValue;
        lastNode.startDocId = nodeStartDocId;
        lastNode.endDocId = endDocId;
        nodes.put(nodeDimensionValue, lastNode);
        return nodes;
    }

    /**
     * Constructs star tree nodes
     *
     * @param startDocId  start document id
     * @param endDocId    end document id
     * @param dimensionId id of the dimension in the star tree
     * @return root node with star nodes constructed
     * @throws IOException throws an exception if we are unable to construct non-star nodes
     */
    private StarTreeBuilderUtils.TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        StarTreeBuilderUtils.TreeNode starNode = getNewNode();
        starNode.dimensionId = dimensionId;
        starNode.dimensionValue = StarTreeBuilderUtils.ALL;
        starNode.isStarNode = true;
        starNode.startDocId = numStarTreeDocs;
        Iterator<StarTreeDocument> starTreeDocumentIterator = generateStarTreeDocumentsForStarNode(startDocId, endDocId, dimensionId);
        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        starNode.endDocId = numStarTreeDocs;
        return starNode;
    }

    /**
     * Returns aggregated star-tree document
     *
     * @param node star-tree node
     * @return aggregated star-tree documents
     * @throws IOException throws an exception upon failing to create new aggregated docs based on star tree
     */
    private StarTreeDocument createAggregatedDocs(StarTreeBuilderUtils.TreeNode node) throws IOException {
        StarTreeDocument aggregatedStarTreeDocument = null;
        if (node.children == null) {
            // For leaf node

            if (node.startDocId == node.endDocId - 1) {
                // If it has only one document, use it as the aggregated document
                aggregatedStarTreeDocument = getStarTreeDocument(node.startDocId);
                node.aggregatedDocId = node.startDocId;
            } else {
                // If it has multiple documents, aggregate all of them
                for (int i = node.startDocId; i < node.endDocId; i++) {
                    aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, getStarTreeDocument(i));
                }
                assert aggregatedStarTreeDocument != null;
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numStarTreeDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        } else {
            // For non-leaf node
            if (node.children.containsKey((long) StarTreeBuilderUtils.ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    if (child.isStarNode) {
                        aggregatedStarTreeDocument = createAggregatedDocs(child);
                        node.aggregatedDocId = child.aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, createAggregatedDocs(child));
                }
                assert aggregatedStarTreeDocument != null;
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numStarTreeDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        }
        return aggregatedStarTreeDocument;
    }

    /**
     * Handles the dimension of date time field type
     *
     * @param fieldName name of the field
     * @param val       value of the field
     * @return returns the converted dimension of the field to a particular granularity
     */
    private long handleDateDimension(final String fieldName, final long val) {
        // TODO: handle timestamp granularity
        return val;
    }

    public void close() throws IOException {

    }

}
