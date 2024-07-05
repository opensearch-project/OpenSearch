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
import org.apache.lucene.codecs.DocValuesProducer;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreeBuilder;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreeDocValuesIteratorAdapter;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
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
 * Builder for star tree. Defines the algorithm to construct star-tree
 * See {@link StarTreesBuilder} for information around the construction of star-trees based on star-tree fields
 *
 * @opensearch.experimental
 */
public abstract class BaseStarTreeBuilder implements StarTreeBuilder {

    private static final Logger logger = LogManager.getLogger(BaseStarTreeBuilder.class);

    /**
     * Default value for star node
     */
    public static final int STAR_IN_DOC_VALUES_INDEX = -1;

    protected final Set<Integer> skipStarNodeCreationForDimensions;

    protected final List<MetricAggregatorInfo> metricAggregatorInfos;
    protected final int numMetrics;
    protected final int numDimensions;
    protected int numStarTreeDocs;
    protected int totalSegmentDocs;
    protected int numStarTreeNodes;
    protected final int maxLeafDocuments;

    protected final StarTreeBuilderUtils.TreeNode rootNode = getNewNode();

    protected SequentialDocValuesIterator[] dimensionReaders;

    protected Map<String, DocValuesProducer> fieldProducerMap;

    private final StarTreeDocValuesIteratorAdapter starTreeDocValuesIteratorAdapter;
    private final StarTreeField starTreeField;

    /**
     * Reads all the configuration related to dimensions and metrics, builds a star-tree based on the different construction parameters.
     *
     * @param starTreeField    holds the configuration for the star tree
     * @param fieldProducerMap helps return the doc values iterator for each type based on field name
     * @param state            stores the segment write state
     * @param mapperService    helps to find the original type of the field
     */
    protected BaseStarTreeBuilder(
        StarTreeField starTreeField,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {

        logger.debug("Building in base star tree builder");

        this.starTreeField = starTreeField;
        StarTreeFieldConfiguration starTreeFieldSpec = starTreeField.getStarTreeConfig();
        this.fieldProducerMap = fieldProducerMap;
        this.starTreeDocValuesIteratorAdapter = new StarTreeDocValuesIteratorAdapter();

        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        this.numDimensions = dimensionsSplitOrder.size();

        this.skipStarNodeCreationForDimensions = new HashSet<>();
        this.totalSegmentDocs = state.segmentInfo.maxDoc();
        this.dimensionReaders = new SequentialDocValuesIterator[numDimensions];
        Set<String> skipStarNodeCreationForDimensions = starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
            FieldInfo dimensionFieldInfos = state.fieldInfos.fieldInfo(dimension);
            DocValuesType dimensionDocValuesType = dimensionFieldInfos.getDocValuesType();
            dimensionReaders[i] = starTreeDocValuesIteratorAdapter.getDocValuesIterator(
                dimensionDocValuesType,
                dimensionFieldInfos,
                fieldProducerMap.get(dimensionFieldInfos.name)
            );
        }

        this.metricAggregatorInfos = generateMetricAggregatorInfos(mapperService, state);
        this.numMetrics = metricAggregatorInfos.size();
        this.maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the configuration required to perform aggregation for all the metrics on a field
     *
     * @return list of MetricAggregatorInfo
     */
    public List<MetricAggregatorInfo> generateMetricAggregatorInfos(MapperService mapperService, SegmentWriteState state)
        throws IOException {
        List<MetricAggregatorInfo> metricAggregatorInfos = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricType : metric.getMetrics()) {
                IndexNumericFieldData.NumericType numericType;
                SequentialDocValuesIterator metricStatReader;
                Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(metric.getField());
                if (fieldMapper instanceof NumberFieldMapper) {
                    numericType = ((NumberFieldMapper) fieldMapper).fieldType().numericType();
                } else {
                    logger.error("unsupported mapper type");
                    throw new IllegalStateException("unsupported mapper type");
                }

                FieldInfo metricFieldInfos = state.fieldInfos.fieldInfo(metric.getField());
                DocValuesType metricDocValuesType = metricFieldInfos.getDocValuesType();
                if (metricType != MetricStat.COUNT) {
                    // Need not initialize the metric reader with relevant doc id set iterator for COUNT metric type
                    metricStatReader = starTreeDocValuesIteratorAdapter.getDocValuesIterator(
                        metricDocValuesType,
                        metricFieldInfos,
                        fieldProducerMap.get(metricFieldInfos.name)
                    );
                } else {
                    metricStatReader = new SequentialDocValuesIterator();
                }

                MetricAggregatorInfo metricAggregatorInfo = new MetricAggregatorInfo(
                    metricType,
                    metric.getField(),
                    starTreeField.getName(),
                    numericType,
                    metricStatReader
                );
                metricAggregatorInfos.add(metricAggregatorInfo);
            }
        }
        return metricAggregatorInfos;
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
    public abstract List<StarTreeDocument> getStarTreeDocuments();

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
    public abstract Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(int numDocs) throws IOException;

    /**
     * Generates aggregated star-tree documents for star-node.
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
     *
     * @throws IOException when we are unable to build a star tree document from the segment
     */
    protected StarTreeDocument getSegmentStarTreeDocument(int currentDocId) throws IOException {
        Long[] dimensions = getStarTreeDimensionsFromSegment(currentDocId);
        Object[] metrics = getStarTreeMetricsFromSegment(currentDocId);
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Returns the dimension values for the next document from the segment
     *
     * @return dimension values for each of the star-tree dimension
     * @throws IOException when we are unable to iterate to the next doc for the given dimension readers
     */
    Long[] getStarTreeDimensionsFromSegment(int currentDocId) throws IOException {
        Long[] dimensions = new Long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            if (dimensionReaders[i] != null) {
                try {
                    starTreeDocValuesIteratorAdapter.nextDoc(dimensionReaders[i], currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the dimension values from the segment", e);
                    throw new IllegalStateException("unable to read the dimension values from the segment", e);
                }

                dimensions[i] = starTreeDocValuesIteratorAdapter.getNextValue(dimensionReaders[i], currentDocId);
            } else {
                throw new IllegalStateException("dimension readers are empty");
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
    private Object[] getStarTreeMetricsFromSegment(int currentDocId) throws IOException {
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            SequentialDocValuesIterator metricStatReader = metricAggregatorInfos.get(i).getMetricStatReader();
            if (metricStatReader != null) {
                try {
                    starTreeDocValuesIteratorAdapter.nextDoc(metricStatReader, currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
                metrics[i] = starTreeDocValuesIteratorAdapter.getNextValue(metricStatReader, currentDocId);
            } else {
                throw new IllegalStateException("metric readers are empty");
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
            Long[] dimensions = Arrays.copyOf(segmentDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    metrics[i] = metricValueAggregator.getInitialAggregatedValueForSegmentDocValue(
                        getLong(segmentDocument.metrics[i]),
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot parse initial segment doc value", e);
                    throw new IllegalStateException("Cannot parse initial segment doc value [" + segmentDocument.metrics[i] + "]");
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    aggregatedSegmentDocument.metrics[i] = metricValueAggregator.mergeAggregatedValueAndSegmentValue(
                        aggregatedSegmentDocument.metrics[i],
                        getLong(segmentDocument.metrics[i]),
                        starTreeNumericType
                    );
                } catch (Exception e) {
                    logger.error("Cannot apply segment doc value for aggregation", e);
                    throw new IllegalStateException("Cannot apply segment doc value for aggregation [" + segmentDocument.metrics[i] + "]");
                }
            }
            return aggregatedSegmentDocument;
        }
    }

    /**
     * Safely converts the metric value of object type to long.
     *
     * @param metric value of the metric
     * @return converted metric value to long
     */
    private static long getLong(Object metric) {

        Long metricValue = null;
        try {
            if (metric instanceof Long) {
                metricValue = (long) metric;
            } else if (metric != null) {
                metricValue = Long.valueOf(String.valueOf(metric));
            }
        } catch (Exception e) {
            throw new IllegalStateException("unable to cast segment metric", e);
        }

        if (metricValue == null) {
            throw new IllegalStateException("unable to cast segment metric");
        }
        return metricValue;
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
            Long[] dimensions = Arrays.copyOf(starTreeDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    metrics[i] = metricAggregatorInfos.get(i).getValueAggregators().getInitialAggregatedValue(starTreeDocument.metrics[i]);
                } catch (Exception e) {
                    logger.error("Cannot get value for aggregation", e);
                    throw new IllegalStateException("Cannot get value for aggregation[" + starTreeDocument.metrics[i] + "]");
                }
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                try {
                    aggregatedDocument.metrics[i] = metricAggregatorInfos.get(i)
                        .getValueAggregators()
                        .mergeAggregatedValues(starTreeDocument.metrics[i], aggregatedDocument.metrics[i]);
                } catch (Exception e) {
                    logger.error("Cannot apply value to aggregated document for aggregation", e);
                    throw new IllegalStateException(
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
        logger.debug("Star-tree build is a go with star tree field {}", starTreeField.getName());

        Iterator<StarTreeDocument> starTreeDocumentIterator = sortAndAggregateStarTreeDocuments(totalSegmentDocs);
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
     * @param startDocId  start document id (inclusive)
     * @param endDocId    end document id (exclusive)
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
     * @param startDocId  start document id (inclusive)
     * @param endDocId    end document id (exclusive)
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
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = Long.valueOf(STAR_IN_DOC_VALUES_INDEX);
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
                if (null == aggregatedStarTreeDocument) {
                    throw new IllegalStateException("aggregated star-tree document is null after reducing the documents");
                }
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = Long.valueOf(STAR_IN_DOC_VALUES_INDEX);
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
