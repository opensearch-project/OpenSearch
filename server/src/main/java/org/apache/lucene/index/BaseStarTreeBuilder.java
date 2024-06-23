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
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricStatFieldPair;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregatorFactory;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.builder.SingleTreeBuilder;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreeDocValuesIteratorAdapter;
import org.opensearch.index.compositeindex.datacube.startree.data.StarTreeDocument;
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
 */
public abstract class BaseStarTreeBuilder implements SingleTreeBuilder {

    private static final Logger logger = LogManager.getLogger(BaseStarTreeBuilder.class);

    public static final int STAR_IN_DOC_VALUES_INDEX = -1;

    protected final String[] dimensionsSplitOrder;
    protected final Set<Integer> skipStarNodeCreationForDimensions;
    protected final String[] metrics;

    protected final int numMetrics;
    protected final int numDimensions;
    protected int numStarTreeDocs;
    protected int totalSegmentDocs;
    protected int numStarTreeNodes;
    protected final int maxLeafDocuments;

    protected final StarTreeBuilderUtils.TreeNode rootNode = getNewNode();

    protected DocIdSetIterator[] dimensionReaders;
    protected DocIdSetIterator[] metricReaders;

    protected ValueAggregator[] valueAggregators;
    protected IndexNumericFieldData.NumericType[] numericTypes;
    protected DocValuesConsumer docValuesConsumer;
    protected DocValuesProducer docValuesProducer;

    private final StarTreeDocValuesIteratorAdapter starTreeDocValuesIteratorFactory;
    private final StarTreeField starTreeField;
    private final StarTreeFieldConfiguration starTreeFieldSpec;
    private final List<MetricStatFieldPair> metricStatFieldPairs;
    private final MapperService mapperService;

    /**
     * Constructor for base star-tree builder
     *
     * @param starTreeField     holds the configuration for the star tree
     * @param docValuesProducer helps return the doc values iterator for each type based on field name
     * @param docValuesConsumer to consume the new aggregated metrics during flush
     * @param state             stores the segment state
     * @param mapperService     helps to find the original type of the field
     */
    protected BaseStarTreeBuilder(
        StarTreeField starTreeField,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {

        logger.debug("Building in base star tree builder");

        this.mapperService = mapperService;
        this.starTreeField = starTreeField;
        this.starTreeFieldSpec = starTreeField.getStarTreeConfig();
        this.docValuesConsumer = docValuesConsumer;
        this.docValuesProducer = docValuesProducer;
        this.starTreeDocValuesIteratorFactory = new StarTreeDocValuesIteratorAdapter();

        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        this.numDimensions = dimensionsSplitOrder.size();
        this.dimensionsSplitOrder = new String[numDimensions];

        this.skipStarNodeCreationForDimensions = new HashSet<>();
        this.totalSegmentDocs = state.segmentInfo.maxDoc();
        this.dimensionReaders = new DocIdSetIterator[numDimensions];
        Set<String> skipStarNodeCreationForDimensions = this.starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            this.dimensionsSplitOrder[i] = dimension;
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
            FieldInfo dimensionFieldInfos = state.fieldInfos.fieldInfo(dimension);
            DocValuesType dimensionDocValuesType = state.fieldInfos.fieldInfo(dimension).getDocValuesType();
            dimensionReaders[i] = starTreeDocValuesIteratorFactory.getDocValuesIterator(
                dimensionDocValuesType,
                dimensionFieldInfos,
                docValuesProducer
            );
        }

        this.metricStatFieldPairs = generateMetricStatFieldPairs();
        this.numMetrics = metricStatFieldPairs.size();
        this.metrics = new String[numMetrics];
        this.valueAggregators = new ValueAggregator[numMetrics];
        this.numericTypes = new IndexNumericFieldData.NumericType[numMetrics];
        this.metricReaders = new DocIdSetIterator[numMetrics];

        int index = 0;
        for (MetricStatFieldPair metricStatFieldPair : metricStatFieldPairs) {
            metrics[index] = metricStatFieldPair.toFieldName();
            valueAggregators[index] = ValueAggregatorFactory.getValueAggregator(metricStatFieldPair.getMetricStat());

            Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(metrics[index]);
            if (fieldMapper instanceof NumberFieldMapper) {
                numericTypes[index] = ((NumberFieldMapper) fieldMapper).fieldType().numericType();
            } else {
                numericTypes[index] = IndexNumericFieldData.NumericType.DOUBLE;
            }
            // Ignore the column for COUNT aggregation function
            if (valueAggregators[index].getAggregationType() != MetricStat.COUNT) {
                String metricName = metricStatFieldPair.getField();
                FieldInfo metricFieldInfos = state.fieldInfos.fieldInfo(metricName);
                DocValuesType metricDocValuesType = state.fieldInfos.fieldInfo(metricName).getDocValuesType();
                metricReaders[index] = starTreeDocValuesIteratorFactory.getDocValuesIterator(
                    metricDocValuesType,
                    metricFieldInfos,
                    docValuesProducer
                );
            }
            index++;
        }
        this.maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the MetricStatFieldPairs for all the metrics on a field
     *
     * @return list of metric stat mapped with respective fields
     */
    public List<MetricStatFieldPair> generateMetricStatFieldPairs() {
        List<MetricStatFieldPair> metricStatFieldPairs = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricType : metric.getMetrics()) {
                MetricStatFieldPair metricStatFieldPair = new MetricStatFieldPair(metricType, metric.getField());
                metricStatFieldPairs.add(metricStatFieldPair);
            }
        }
        return metricStatFieldPairs;
    }

    /**
     * Appends a star-tree document to the star-tree.
     *
     * @param starTreeDocument star tree document to be appended
     */
    public abstract void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException;

    /**
     * Returns the star-tree document of the given document id in the star-tree.
     *
     * @param docId Document dd
     * @return Star tree document
     */
    public abstract StarTreeDocument getStarTreeDocument(int docId) throws IOException;

    /**
     * Returns the star-tree document of the given document id in the star-tree.
     *
     * @return Star tree document
     */
    public abstract List<StarTreeDocument> getStarTreeDocuments() throws IOException;

    /**
     * Returns the dimension value of the given document and dimension id in the star-tree.
     *
     * @param docId       Document Id
     * @param dimensionId Dimension Id
     * @return Dimension value
     */
    public abstract long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates the star-tree Document in the segment, and returns a star-tree document iterator for all the
     * aggregated star-tree document.
     *
     * <p>This method reads star-tree document from segment and generates the initial star-tree document for the star-tree.
     *
     * @param numDocs Number of documents in the segment
     * @return Iterator for the aggregated star-tree document
     */
    public abstract Iterator<StarTreeDocument> processSegmentStarTreeDocuments(int numDocs) throws IOException;

    /**
     * Generates aggregated star-tree document for star-node.
     *
     * <p>This method will do the following steps:
     *
     * <ul>
     *   <li>Creates a temporary buffer for the given range of documents
     *   <li>Replaces the value for the given dimension Id to {@code STAR}
     *   <li>Sorts the star-tree document inside the temporary buffer
     *   <li>Aggregates the star-tree document with same dimensions
     *   <li>Returns an iterator for the aggregated star-tree document
     * </ul>
     *
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return Iterator for the aggregated starTreeDocument
     */
    public abstract Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException;

    /**
     * Returns the segment star-tree document
     */
    protected StarTreeDocument getSegmentStarTreeDocument() throws IOException {
        long[] dimensions = getStarTreeDimensionsFromSegment();
        Object[] metrics = getStarTreeMetricsFromSegment();
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Returns the next segment star-tree document for the dimensions
     *
     * @return dimension values for each of the star-tree dimension
     * @throws IOException when we are unable to iterate to the next doc
     */
    long[] getStarTreeDimensionsFromSegment() throws IOException {
        long[] dimensions = new long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            try {
                dimensionReaders[i].nextDoc();
            } catch (IOException e) {
                logger.error("unable to iterate to next doc", e);
            }

            if (starTreeField.getDimensionsOrder().get(i) instanceof DateDimension) {
                dimensions[i] = handleDateDimension(
                    dimensionsSplitOrder[i],
                    starTreeDocValuesIteratorFactory.getNextValue(dimensionReaders[i])
                );
            } else {
                dimensions[i] = starTreeDocValuesIteratorFactory.getNextValue(dimensionReaders[i]);
            }
        }
        return dimensions;
    }

    /**
     * Returns the next segment star-tree document for the metrics
     *
     * @return metric values for each of the star-tree metric
     * @throws IOException when we are unable to iterate to the next doc
     */
    private Object[] getStarTreeMetricsFromSegment() throws IOException {
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            // Ignore the column for COUNT aggregation function
            if (metricReaders[i] != null) {
                try {
                    metricReaders[i].nextDoc();
                } catch (IOException e) {
                    // TODO : handle null values in columns
                    logger.error("unable to iterate to next doc", e);
                }
                metrics[i] = starTreeDocValuesIteratorFactory.getNextValue(metricReaders[i]);
            }
        }
        return metrics;
    }

    /**
     * Merges a segment star-tree document (raw) into the aggregated star-tree document.
     *
     * <p>Will create a new aggregated star-tree document if the current one is {@code null}.
     *
     * @param aggregatedSegmentDocument Aggregated star-tree document
     * @param segmentDocument    Segment star-tree document
     * @return Merged starTreeDocument
     */
    protected StarTreeDocument aggregateSegmentDocuments(
        StarTreeDocument aggregatedSegmentDocument,
        StarTreeDocument segmentDocument
    ) {
        // TODO: HANDLE KEYWORDS LATER!
        if (aggregatedSegmentDocument == null) {
            long[] dimensions = Arrays.copyOf(segmentDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    StarTreeNumericType numericType = StarTreeNumericType.fromNumericType(numericTypes[i]);
                    metrics[i] = valueAggregators[i].getInitialAggregatedValue((Long) segmentDocument.metrics[i], numericType);
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot parse initial aggregated value", e);
                    throw new IllegalArgumentException(
                        "Cannot parse initial aggregated value [" + segmentDocument.metrics[i] + "]"
                    );
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    StarTreeNumericType numericType = StarTreeNumericType.fromNumericType(numericTypes[i]);
                    aggregatedSegmentDocument.metrics[i] = valueAggregators[i].applySegmentRawValue(
                        aggregatedSegmentDocument.metrics[i],
                        (Long) segmentDocument.metrics[i],
                        numericType
                    );
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot apply segment raw value", e);
                    throw new IllegalArgumentException("Cannot aggregate on segment value [" + segmentDocument.metrics[i] + "]");
                }
            }
            return aggregatedSegmentDocument;
        }
    }

    /**
     * Merges a star-tree document (aggregated) into the aggregated document.
     *
     * <p>Will create a new aggregated starTreeDocument if the current one is {@code null}.
     *
     * @param aggregatedDocument Aggregated star-tree document
     * @param starTreeDocument   Star-tree document
     * @return Merged star-tree document
     */
    public StarTreeDocument aggregateDocuments(
        StarTreeDocument aggregatedDocument,
        StarTreeDocument starTreeDocument
    ) {
        // aggregate the documents
        if (aggregatedDocument == null) {
            long[] dimensions = Arrays.copyOf(starTreeDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    metrics[i] = valueAggregators[i].getAggregatedValue(starTreeDocument.metrics[i]);
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot clone aggregated value", e);
                    throw new IllegalArgumentException("Cannot clone aggregated value [" + starTreeDocument.metrics[i] + "]");
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    aggregatedDocument.metrics[i] = valueAggregators[i].applyAggregatedValue(
                        starTreeDocument.metrics[i],
                        aggregatedDocument.metrics[i]
                    );
                } catch (IllegalArgumentException | NullPointerException e) {
                    logger.error("Cannot apply aggregated value", e);
                    throw new IllegalArgumentException("Cannot apply aggregated value [" + starTreeDocument.metrics[i] + "]");
                }
            }
            return aggregatedDocument;
        }
    }

    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.debug("Tree of Aggregations build is a go with config {}", starTreeField);

        Iterator<StarTreeDocument> starTreeDocumentIterator = processSegmentStarTreeDocuments(totalSegmentDocs);
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
     * Appends a starTreeDocument to star tree
     *
     * @param starTreeDocument star-tree document
     * @throws IOException throws an exception if we are unable to append the doc
     */
    private void appendToStarTree(StarTreeDocument starTreeDocument) throws IOException {
        appendStarTreeDocument(starTreeDocument);
        numStarTreeDocs++;
    }

    /**
     * Returns a new node
     *
     * @return return new star-tree node
     */
    private StarTreeBuilderUtils.TreeNode getNewNode() {
        numStarTreeNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    /**
     * Implements the algorithm to construct a star-tree based on star-tree documents
     *
     * @param node       star-tree node
     * @param startDocId start document id
     * @param endDocId   end document id
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
                    aggregatedStarTreeDocument = aggregateDocuments(aggregatedStarTreeDocument, getStarTreeDocument(i));
                }
                assert aggregatedStarTreeDocument != null;
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
                    if (child.dimensionValue == StarTreeBuilderUtils.ALL) {
                        aggregatedStarTreeDocument = createAggregatedDocs(child);
                        node.aggregatedDocId = child.aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    aggregatedStarTreeDocument = aggregateDocuments(aggregatedStarTreeDocument, createAggregatedDocs(child));
                }
                assert aggregatedStarTreeDocument != null;
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
