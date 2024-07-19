/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.MetricAggregatorInfo;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericType;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.TreeNode;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.opensearch.index.compositeindex.datacube.startree.utils.TreeNode.ALL;

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
    public static final Long STAR_IN_DOC_VALUES_INDEX = null;
    protected final Set<Integer> skipStarNodeCreationForDimensions;

    protected final List<MetricAggregatorInfo> metricAggregatorInfos;
    protected final int numMetrics;
    protected final int numDimensions;
    protected int numStarTreeDocs;
    protected int totalSegmentDocs;
    protected int numStarTreeNodes;
    protected final int maxLeafDocuments;

    protected final TreeNode rootNode = getNewNode();

    private final StarTreeField starTreeField;
    private final MapperService mapperService;
    private final SegmentWriteState state;
    static String NUM_SEGMENT_DOCS = "numSegmentDocs";

    /**
     * Reads all the configuration related to dimensions and metrics, builds a star-tree based on the different construction parameters.
     *
     * @param starTreeField    holds the configuration for the star tree
     * @param state            stores the segment write state
     * @param mapperService    helps to find the original type of the field
     */
    protected BaseStarTreeBuilder(StarTreeField starTreeField, SegmentWriteState state, MapperService mapperService) {
        logger.debug("Building star tree : {}", starTreeField.getName());

        this.starTreeField = starTreeField;
        StarTreeFieldConfiguration starTreeFieldSpec = starTreeField.getStarTreeConfig();

        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        this.numDimensions = dimensionsSplitOrder.size();

        this.skipStarNodeCreationForDimensions = new HashSet<>();
        this.totalSegmentDocs = state.segmentInfo.maxDoc();
        this.mapperService = mapperService;
        this.state = state;

        Set<String> skipStarNodeCreationForDimensions = starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
        }

        this.metricAggregatorInfos = generateMetricAggregatorInfos(mapperService);
        this.numMetrics = metricAggregatorInfos.size();
        this.maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the configuration required to perform aggregation for all the metrics on a field
     *
     * @return list of MetricAggregatorInfo
     */
    public List<MetricAggregatorInfo> generateMetricAggregatorInfos(MapperService mapperService) {
        List<MetricAggregatorInfo> metricAggregatorInfos = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricStat : metric.getMetrics()) {
                IndexNumericFieldData.NumericType numericType;
                Mapper fieldMapper = mapperService.documentMapper().mappers().getMapper(metric.getField());
                if (fieldMapper instanceof NumberFieldMapper) {
                    numericType = ((NumberFieldMapper) fieldMapper).fieldType().numericType();
                } else {
                    logger.error("unsupported mapper type");
                    throw new IllegalStateException("unsupported mapper type");
                }

                MetricAggregatorInfo metricAggregatorInfo = new MetricAggregatorInfo(
                    metricStat,
                    metric.getField(),
                    starTreeField.getName(),
                    numericType
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
    public abstract Long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates all the documents in the segment as per the configuration, and returns a star-tree document iterator for all the
     * aggregated star-tree documents.
     *
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders List of docValues readers to read metrics from the segment
     * @return Iterator for the aggregated star-tree document
     */
    public abstract Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException;

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
     * Returns the star-tree document from the segment based on the current doc id
     *
     */
    protected StarTreeDocument getSegmentStarTreeDocument(
        int currentDocId,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        Long[] dimensions = getStarTreeDimensionsFromSegment(currentDocId, dimensionReaders);
        Object[] metrics = getStarTreeMetricsFromSegment(currentDocId, metricReaders);
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Returns the dimension values for the next document from the segment
     *
     * @return dimension values for each of the star-tree dimension
     * @throws IOException when we are unable to iterate to the next doc for the given dimension readers
     */
    Long[] getStarTreeDimensionsFromSegment(int currentDocId, SequentialDocValuesIterator[] dimensionReaders) throws IOException {
        Long[] dimensions = new Long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            if (dimensionReaders[i] != null) {
                try {
                    dimensionReaders[i].nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the dimension values from the segment", e);
                    throw new IllegalStateException("unable to read the dimension values from the segment", e);
                }
                dimensions[i] = dimensionReaders[i].value(currentDocId);
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
    private Object[] getStarTreeMetricsFromSegment(int currentDocId, List<SequentialDocValuesIterator> metricsReaders) throws IOException {
        Object[] metrics = new Object[numMetrics];
        for (int i = 0; i < numMetrics; i++) {
            SequentialDocValuesIterator metricStatReader = metricsReaders.get(i);
            if (metricStatReader != null) {
                try {
                    metricStatReader.nextDoc(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
                metrics[i] = metricStatReader.value(currentDocId);
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
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected StarTreeDocument reduceSegmentStarTreeDocuments(
        StarTreeDocument aggregatedSegmentDocument,
        StarTreeDocument segmentDocument,
        boolean isMerge
    ) {
        if (aggregatedSegmentDocument == null) {
            Long[] dimensions = Arrays.copyOf(segmentDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                try {
                    ValueAggregator metricValueAggregator = metricAggregatorInfos.get(i).getValueAggregators();
                    StarTreeNumericType starTreeNumericType = metricAggregatorInfos.get(i).getAggregatedValueType();
                    if (isMerge) {
                        metrics[i] = metricValueAggregator.getInitialAggregatedValue(segmentDocument.metrics[i]);
                    } else {
                        metrics[i] = metricValueAggregator.getInitialAggregatedValueForSegmentDocValue(getLong(segmentDocument.metrics[i]));
                    }

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
                    if (isMerge) {
                        aggregatedSegmentDocument.metrics[i] = metricValueAggregator.mergeAggregatedValues(
                            segmentDocument.metrics[i],
                            aggregatedSegmentDocument.metrics[i]
                        );
                    } else {
                        aggregatedSegmentDocument.metrics[i] = metricValueAggregator.mergeAggregatedValueAndSegmentValue(
                            aggregatedSegmentDocument.metrics[i],
                            getLong(segmentDocument.metrics[i])
                        );
                    }
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
            return 0;
            // TODO: handle this properly
            // throw new IllegalStateException("unable to cast segment metric");
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
    @SuppressWarnings("unchecked")
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
     * Builds the star tree from the original segment documents
     *
     * @param fieldProducerMap contain s the docValues producer to get docValues associated with each field
     *
     * @throws IOException when we are unable to build star-tree
     */
    public void build(Map<String, DocValuesProducer> fieldProducerMap) throws IOException {
        long startTime = System.currentTimeMillis();
        logger.debug("Star-tree build is a go with star tree field {}", starTreeField.getName());
        if (totalSegmentDocs == 0) {
            logger.debug("No documents found in the segment");
            return;
        }
        List<SequentialDocValuesIterator> metricReaders = getMetricReaders(state, fieldProducerMap);
        List<Dimension> dimensionsSplitOrder = starTreeField.getDimensionsOrder();
        SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[dimensionsSplitOrder.size()];
        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            FieldInfo dimensionFieldInfo = state.fieldInfos.fieldInfo(dimension);
            if (dimensionFieldInfo == null) {
                dimensionFieldInfo = getFieldInfo(dimension);
            }
            dimensionReaders[i] = new SequentialDocValuesIterator(
                fieldProducerMap.get(dimensionFieldInfo.name).getSortedNumeric(dimensionFieldInfo)
            );
        }
        Iterator<StarTreeDocument> starTreeDocumentIterator = sortAndAggregateSegmentDocuments(dimensionReaders, metricReaders);
        logger.debug("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        build(starTreeDocumentIterator);
        logger.debug("Finished Building star-tree in ms : {}", (System.currentTimeMillis() - startTime));
    }

    private static FieldInfo getFieldInfo(String field) {
        return new FieldInfo(
            field,
            1,
            false,
            false,
            false,
            IndexOptions.NONE,
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

    /**
     * Generates the configuration required to perform aggregation for all the metrics on a field
     *
     * @return list of MetricAggregatorInfo
     */
    public List<SequentialDocValuesIterator> getMetricReaders(SegmentWriteState state, Map<String, DocValuesProducer> fieldProducerMap)
        throws IOException {
        List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
        for (Metric metric : this.starTreeField.getMetrics()) {
            for (MetricStat metricStat : metric.getMetrics()) {
                FieldInfo metricFieldInfo = state.fieldInfos.fieldInfo(metric.getField());
                if (metricFieldInfo == null) {
                    metricFieldInfo = getFieldInfo(metric.getField());
                }
                // TODO
                // if (metricStat != MetricStat.COUNT) {
                // Need not initialize the metric reader for COUNT metric type
                SequentialDocValuesIterator metricReader = new SequentialDocValuesIterator(
                    fieldProducerMap.get(metricFieldInfo.name).getSortedNumeric(metricFieldInfo)
                );
                // }

                metricReaders.add(metricReader);
            }
        }
        return metricReaders;
    }

    /**
     * Builds the star tree using Star-Tree Document
     *
     * @param starTreeDocumentIterator contains the sorted and aggregated documents
     * @throws IOException when we are unable to build star-tree
     */
    void build(Iterator<StarTreeDocument> starTreeDocumentIterator) throws IOException {
        int numSegmentStarTreeDocument = totalSegmentDocs;

        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        int numStarTreeDocument = numStarTreeDocs;
        logger.debug("Generated star tree docs : [{}] from segment docs : [{}]", numStarTreeDocument, numSegmentStarTreeDocument);

        if (numStarTreeDocs == 0) {
            // TODO: Uncomment when segment codec and file formats is ready
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

    TreeNode getRootNode() {
        return rootNode;
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
    private TreeNode getNewNode() {
        numStarTreeNodes++;
        return new TreeNode();
    }

    /**
     * Implements the algorithm to construct a star-tree
     *
     * @param node       star-tree node
     * @param startDocId start document id
     * @param endDocId   end document id
     * @throws IOException throws an exception if we are unable to construct the tree
     */
    private void constructStarTree(TreeNode node, int startDocId, int endDocId) throws IOException {

        int childDimensionId = node.dimensionId + 1;
        if (childDimensionId == numDimensions) {
            return;
        }

        // Construct all non-star children nodes
        node.childDimensionId = childDimensionId;
        Map<Long, TreeNode> children = constructNonStarNodes(startDocId, endDocId, childDimensionId);
        node.children = children;

        // Construct star-node if required
        if (!skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
            children.put((long) ALL, constructStarNode(startDocId, endDocId, childDimensionId));
        }

        // Further split on child nodes if required
        for (TreeNode child : children.values()) {
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
    private Map<Long, TreeNode> constructNonStarNodes(int startDocId, int endDocId, int dimensionId) throws IOException {
        Map<Long, TreeNode> nodes = new HashMap<>();
        int nodeStartDocId = startDocId;
        Long nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
        for (int i = startDocId + 1; i < endDocId; i++) {
            Long dimensionValue = getDimensionValue(i, dimensionId);
            if (Objects.equals(dimensionValue, nodeDimensionValue) == false) {
                TreeNode child = getNewNode();
                child.dimensionId = dimensionId;
                child.dimensionValue = nodeDimensionValue != null ? nodeDimensionValue : ALL;
                child.startDocId = nodeStartDocId;
                child.endDocId = i;
                nodes.put(nodeDimensionValue, child);

                nodeStartDocId = i;
                nodeDimensionValue = dimensionValue;
            }
        }
        TreeNode lastNode = getNewNode();
        lastNode.dimensionId = dimensionId;
        lastNode.dimensionValue = nodeDimensionValue != null ? nodeDimensionValue : ALL;
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
    private TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        TreeNode starNode = getNewNode();
        starNode.dimensionId = dimensionId;
        starNode.dimensionValue = ALL;
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
    private StarTreeDocument createAggregatedDocs(TreeNode node) throws IOException {
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
                    aggregatedStarTreeDocument.dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numStarTreeDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        } else {
            // For non-leaf node
            if (node.children.containsKey((long) ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (TreeNode child : node.children.values()) {
                    if (child.isStarNode) {
                        aggregatedStarTreeDocument = createAggregatedDocs(child);
                        node.aggregatedDocId = child.aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                if (node.children.values().size() == 1) {
                    for (TreeNode child : node.children.values()) {
                        aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, createAggregatedDocs(child));
                        node.aggregatedDocId = child.aggregatedDocId;
                    }
                } else {
                    for (TreeNode child : node.children.values()) {
                        aggregatedStarTreeDocument = reduceStarTreeDocuments(aggregatedStarTreeDocument, createAggregatedDocs(child));
                    }
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

    abstract Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValues) throws IOException;
}
