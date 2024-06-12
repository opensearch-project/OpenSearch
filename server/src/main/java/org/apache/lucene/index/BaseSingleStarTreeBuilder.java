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
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.opensearch.index.compositeindex.CompositeField;
import org.opensearch.index.compositeindex.DateDimension;
import org.opensearch.index.compositeindex.Dimension;
import org.opensearch.index.compositeindex.Metric;
import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.index.compositeindex.StarTreeFieldSpec;
import org.opensearch.index.compositeindex.startree.aggregators.MetricTypeFieldPair;
import org.opensearch.index.compositeindex.startree.aggregators.ValueAggregator;
import org.opensearch.index.compositeindex.startree.aggregators.ValueAggregatorFactory;
import org.opensearch.index.compositeindex.startree.builder.SingleTreeBuilder;
import org.opensearch.index.compositeindex.startree.builder.StarTreeDocValuesIteratorFactory;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocValues;
import org.opensearch.index.compositeindex.startree.data.StarTreeDocument;
import org.opensearch.index.compositeindex.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.startree.utils.StarTreeBuilderUtils;
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
 * Base class for star tree builder
 */
public abstract class BaseSingleStarTreeBuilder implements SingleTreeBuilder {

    // TODO: STAR_TREE_CODEC will be moved to CodecService once the Star Tree Codec is defined
    public static final String STAR_TREE_CODEC = "startreecodec";

    private static final Logger logger = LogManager.getLogger(BaseSingleStarTreeBuilder.class);

    public static final int STAR_IN_DOC_VALUES_INDEX = 0;

    protected final String[] dimensionsSplitOrder;
    protected final Set<Integer> skipStarNodeCreationForDimensions;
    protected final String[] metrics;

    protected final int numMetrics;
    protected final int numDimensions;
    protected int numDocs;
    protected int totalDocs;
    protected int numNodes;
    protected final int maxLeafDocuments;

    protected final StarTreeBuilderUtils.TreeNode rootNode = getNewNode();

    // TODO: This will be initialized with OnHeap / OffHeap Implementations (Commented it's occurrences for now)
    // private IndexOutput indexOutput;
    protected DocIdSetIterator[] dimensionReaders;
    protected DocIdSetIterator[] metricReaders;

    protected ValueAggregator[] valueAggregators;
    protected DocValuesConsumer docValuesConsumer;
    protected DocValuesProducer docValuesProducer;

    private final StarTreeDocValuesIteratorFactory starTreeDocValuesIteratorFactory;
    private final CompositeField compositeField;
    private final StarTreeFieldSpec starTreeFieldSpec;
    private final List<MetricTypeFieldPair> metricTypeFieldPairs;
    private final SegmentWriteState segmentWriteState;

    /**
     * Constructor for base star tree builder
     * @param compositeField holds the configuration for the star tree
     * @param docValuesProducer helps return the doc values iterator for each type based on field name
     * @param docValuesConsumer to consume the new aggregated metrics during flush
     * @param state stores the segment state
     */
    protected BaseSingleStarTreeBuilder(
        CompositeField compositeField,
        DocValuesProducer docValuesProducer,
        DocValuesConsumer docValuesConsumer,
        SegmentWriteState state
    ) throws IOException {

        logger.info("Building in base star tree builder");

        // String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
        // logger.info("Star tree file name : {}", docFileName);

        // indexOutput = state.directory.createOutput(docFileName, state.context);
        // CodecUtil.writeIndexHeader(indexOutput, STAR_TREE_CODEC, 0, state.segmentInfo.getId(), state.segmentSuffix);

        starTreeDocValuesIteratorFactory = new StarTreeDocValuesIteratorFactory();
        this.compositeField = compositeField;
        this.starTreeFieldSpec = (StarTreeFieldSpec) (compositeField.getSpec());
        this.segmentWriteState = state;
        this.docValuesConsumer = docValuesConsumer;
        this.docValuesProducer = docValuesProducer;

        List<Dimension> dimensionsSplitOrder = compositeField.getDimensionsOrder();
        numDimensions = dimensionsSplitOrder.size();
        this.dimensionsSplitOrder = new String[numDimensions];
        // let's see how can populate this
        skipStarNodeCreationForDimensions = new HashSet<>();
        totalDocs = state.segmentInfo.maxDoc();
        dimensionReaders = new DocIdSetIterator[numDimensions];
        Set<String> skipStarNodeCreationForDimensions = this.starTreeFieldSpec.getSkipStarNodeCreationInDims();

        for (int i = 0; i < numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i).getField();
            this.dimensionsSplitOrder[i] = dimension;
            if (skipStarNodeCreationForDimensions.contains(dimensionsSplitOrder.get(i).getField())) {
                this.skipStarNodeCreationForDimensions.add(i);
            }
            FieldInfo dimensionFieldInfos = state.fieldInfos.fieldInfo(dimension);
            DocValuesType dimensionDocValuesType = state.fieldInfos.fieldInfo(dimension).getDocValuesType();
            dimensionReaders[i] = starTreeDocValuesIteratorFactory.createIterator(
                dimensionDocValuesType,
                dimensionFieldInfos,
                docValuesProducer
            );
        }

        this.metricTypeFieldPairs = generateAggregationFunctionColumnPairs();
        numMetrics = metricTypeFieldPairs.size();
        metrics = new String[numMetrics];
        valueAggregators = new ValueAggregator[numMetrics];
        metricReaders = new DocIdSetIterator[numMetrics];

        int index = 0;
        for (MetricTypeFieldPair metricTypeFieldPair : metricTypeFieldPairs) {
            metrics[index] = metricTypeFieldPair.toFieldName();
            valueAggregators[index] = ValueAggregatorFactory.getValueAggregator(metricTypeFieldPair.getMetricType());
            // Ignore the column for COUNT aggregation function
            if (valueAggregators[index].getAggregationType() != MetricType.COUNT) {
                String metricName = metricTypeFieldPair.getField();
                FieldInfo metricFieldInfos = state.fieldInfos.fieldInfo(metricName);
                DocValuesType metricDocValuesType = state.fieldInfos.fieldInfo(metricName).getDocValuesType();
                metricReaders[index] = starTreeDocValuesIteratorFactory.createIterator(
                    metricDocValuesType,
                    metricFieldInfos,
                    docValuesProducer
                );
            }
            index++;
        }
        maxLeafDocuments = starTreeFieldSpec.maxLeafDocs();
    }

    /**
     * Generates the AggregationFunctionColumnPairs for all the metrics on a field
     */
    public List<MetricTypeFieldPair> generateAggregationFunctionColumnPairs() {
        List<MetricTypeFieldPair> metricTypeFieldPairs = new ArrayList<>();
        for (Metric metric : this.compositeField.getMetrics()) {
            for (MetricType metricType : metric.getMetrics()) {
                MetricTypeFieldPair metricTypeFieldPair = new MetricTypeFieldPair(metricType, metric.getField());
                metricTypeFieldPairs.add(metricTypeFieldPair);
            }
        }
        return metricTypeFieldPairs;
    }

    /**
     * Appends a starTreeDocument to the star-tree.
     *
     * @param starTreeDocument StarTreeDocument to be appended
     */
    public abstract void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException;

    /**
     * Returns the starTreeDocument of the given document Id in the star-tree.
     *
     * @param docId Document Id
     * @return Star-tree Document
     */
    public abstract StarTreeDocument getStarTreeDocument(int docId) throws IOException;

    /**
     * Returns the dimension value of the given document and dimension Id in the star-tree.
     *
     * @param docId       Document Id
     * @param dimensionId Dimension Id
     * @return Dimension value
     */
    public abstract long getDimensionValue(int docId, int dimensionId) throws IOException;

    /**
     * Sorts and aggregates the starTreeDocument in the segment, and returns a starTreeDocument iterator for all the
     * aggregated starTreeDocument.
     *
     * <p>This method reads starTreeDocument from segment and generates the initial starTreeDocument for the star-tree.
     *
     * @param numDocs Number of documents in the segment
     * @return Iterator for the aggregated starTreeDocument
     */
    public abstract Iterator<StarTreeDocument> sortAndAggregateSegmentStarTreeDocument(int numDocs) throws IOException;

    /**
     * Generates aggregated starTreeDocument for star-node.
     *
     * <p>This method will do the following steps:
     *
     * <ul>
     *   <li>Creates a temporary buffer for the given range of documents
     *   <li>Replaces the value for the given dimension Id to {@code STAR}
     *   <li>Sorts the starTreeDocument inside the temporary buffer
     *   <li>Aggregates the starTreeDocument with same dimensions
     *   <li>Returns an iterator for the aggregated starTreeDocument
     * </ul>
     *
     * @param startDocId  Start document Id in the star-tree
     * @param endDocId    End document Id (exclusive) in the star-tree
     * @param dimensionId Dimension Id of the star-node
     * @return Iterator for the aggregated starTreeDocument
     */
    public abstract Iterator<StarTreeDocument> generateStarTreeForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException;

    /**
     * Returns the next segment starTreeDocument for the dimensions
     */
    long[] getNextSegmentStarTreeDocumentDimensions() throws IOException {
        long[] dimensions = new long[numDimensions];
        for (int i = 0; i < numDimensions; i++) {
            try {
                dimensionReaders[i].nextDoc();
            } catch (IOException e) {
                logger.error("unable to iterate to next doc", e);
            }

            if (compositeField.getDimensionsOrder().get(i) instanceof DateDimension) {
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
     * Returns the next segment starTreeDocument
     */
    protected StarTreeDocument getNextSegmentStarTreeDocument() throws IOException {
        long[] dimensions = getNextSegmentStarTreeDocumentDimensions();

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
        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Merges a segment starTreeDocument (raw) into the aggregated starTreeDocument.
     *
     * <p>Will create a new aggregated starTreeDocument if the current one is {@code null}.
     *
     * @param aggregatedStarTreeDocument Aggregated starTreeDocument
     * @param segmentStarTreeDocument    Segment starTreeDocument
     * @return Merged starTreeDocument
     */
    protected StarTreeDocument mergeSegmentStarTreeDocument(
        StarTreeDocument aggregatedStarTreeDocument,
        StarTreeDocument segmentStarTreeDocument
    ) {
        // TODO: HANDLE KEYWORDS LATER!
        if (aggregatedStarTreeDocument == null) {
            long[] dimensions = Arrays.copyOf(segmentStarTreeDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                metrics[i] = valueAggregators[i].getInitialAggregatedValue(segmentStarTreeDocument.metrics[i]);
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                aggregatedStarTreeDocument.metrics[i] = valueAggregators[i].applyRawValue(
                    aggregatedStarTreeDocument.metrics[i],
                    segmentStarTreeDocument.metrics[i]
                );
            }
            return aggregatedStarTreeDocument;
        }
    }

    /**
     * Merges a star-tree starTreeDocument (aggregated) into the aggregated starTreeDocument.
     *
     * <p>Will create a new aggregated starTreeDocument if the current one is {@code null}.
     *
     * @param aggregatedStarTreeDocument Aggregated starTreeDocument
     * @param starTreeStarTreeDocument   Star-tree Document
     * @return Merged starTreeDocument
     */
    public StarTreeDocument mergeStarTreeDocument(StarTreeDocument aggregatedStarTreeDocument, StarTreeDocument starTreeStarTreeDocument) {
        if (aggregatedStarTreeDocument == null) {
            long[] dimensions = Arrays.copyOf(starTreeStarTreeDocument.dimensions, numDimensions);
            Object[] metrics = new Object[numMetrics];
            for (int i = 0; i < numMetrics; i++) {
                metrics[i] = valueAggregators[i].cloneAggregatedValue(starTreeStarTreeDocument.metrics[i]);
            }
            return new StarTreeDocument(dimensions, metrics);
        } else {
            for (int i = 0; i < numMetrics; i++) {
                aggregatedStarTreeDocument.metrics[i] = valueAggregators[i].applyAggregatedValue(
                    starTreeStarTreeDocument.metrics[i],
                    aggregatedStarTreeDocument.metrics[i]
                );
            }
            return aggregatedStarTreeDocument;
        }
    }

    public abstract void build(List<StarTreeDocValues> starTreeDocValues) throws IOException;

    public void build() throws IOException {
        long startTime = System.currentTimeMillis();
        logger.info("Tree of Aggregations build is a go with config {}", compositeField);

        Iterator<StarTreeDocument> starTreeDocumentIterator = sortAndAggregateSegmentStarTreeDocument(totalDocs);
        logger.info("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        build(starTreeDocumentIterator);
        logger.info("Finished Building TOA in ms : {}", (System.currentTimeMillis() - startTime));
    }

    /**
     * Builds the star tree using Star-Tree Document
     */
    public void build(Iterator<StarTreeDocument> starTreeDocumentIterator) throws IOException {
        int numSegmentStarTreeDocument = totalDocs;

        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        int numStarTreeDocument = numDocs;
        logger.info("Generated star tree docs : [{}] from segment docs : [{}]", numStarTreeDocument, numSegmentStarTreeDocument);

        if (numDocs == 0) {
            // StarTreeBuilderUtils.serializeTree(indexOutput, rootNode, dimensionsSplitOrder, numNodes);
            return;
        }

        constructStarTree(rootNode, 0, numDocs);
        int numStarTreeDocumentUnderStarNode = numDocs - numStarTreeDocument;
        logger.info(
            "Finished constructing star-tree, got [ {} ] tree nodes and [ {} ] starTreeDocument under star-node",
            numNodes,
            numStarTreeDocumentUnderStarNode
        );

        createAggregatedDocs(rootNode);
        int numAggregatedStarTreeDocument = numDocs - numStarTreeDocument - numStarTreeDocumentUnderStarNode;
        logger.info("Finished creating aggregated documents : {}", numAggregatedStarTreeDocument);

        // Create doc values indices in disk
        createSortedDocValuesIndices(docValuesConsumer);

        // Serialize and save in disk
        // StarTreeBuilderUtils.serializeTree(indexOutput, rootNode, dimensionsSplitOrder, numNodes);

        // TODO: Write star tree metadata for off heap implementation

    }

    /**
     * Appends a starTreeDocument to star tree
     */
    private void appendToStarTree(StarTreeDocument starTreeDocument) throws IOException {
        appendStarTreeDocument(starTreeDocument);
        numDocs++;
    }

    /**
     * Returns a new node
     */
    private StarTreeBuilderUtils.TreeNode getNewNode() {
        numNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    /**
     * Implements the algorithm to construct a star tree
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
            children.put(StarTreeNode.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
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
     */
    private StarTreeBuilderUtils.TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId) throws IOException {
        StarTreeBuilderUtils.TreeNode starNode = getNewNode();
        starNode.dimensionId = dimensionId;
        starNode.dimensionValue = StarTreeNode.ALL;
        starNode.startDocId = numDocs;
        Iterator<StarTreeDocument> starTreeDocumentIterator = generateStarTreeForStarNode(startDocId, endDocId, dimensionId);
        while (starTreeDocumentIterator.hasNext()) {
            appendToStarTree(starTreeDocumentIterator.next());
        }
        starNode.endDocId = numDocs;
        return starNode;
    }

    /**
     * Returns aggregated starTreeDocument
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
                    aggregatedStarTreeDocument = mergeStarTreeDocument(aggregatedStarTreeDocument, getStarTreeDocument(i));
                }
                assert aggregatedStarTreeDocument != null;
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        } else {
            // For non-leaf node
            if (node.children.containsKey(StarTreeNode.ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    if (child.dimensionValue == StarTreeNode.ALL) {
                        aggregatedStarTreeDocument = createAggregatedDocs(child);
                        node.aggregatedDocId = child.aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node.children.values()) {
                    aggregatedStarTreeDocument = mergeStarTreeDocument(aggregatedStarTreeDocument, createAggregatedDocs(child));
                }
                assert aggregatedStarTreeDocument != null;
                for (int i = node.dimensionId + 1; i < numDimensions; i++) {
                    aggregatedStarTreeDocument.dimensions[i] = STAR_IN_DOC_VALUES_INDEX;
                }
                node.aggregatedDocId = numDocs;
                appendToStarTree(aggregatedStarTreeDocument);
            }
        }
        return aggregatedStarTreeDocument;
    }

    /**
     * Create sorted doc values indices
     */
    private void createSortedDocValuesIndices(DocValuesConsumer docValuesConsumer) throws IOException {
        List<StarTreeDocValuesWriter> dimensionWriters = new ArrayList<>();
        List<StarTreeDocValuesWriter> metricWriters = new ArrayList<>();
        FieldInfo[] dimensionFieldInfoList = new FieldInfo[dimensionReaders.length];
        FieldInfo[] metricFieldInfoList = new FieldInfo[metricReaders.length];

        // star tree index field number
        int fieldNum = 0;
        for (int i = 0; i < dimensionReaders.length; i++) {
            FieldInfo originalDimensionFieldInfo = segmentWriteState.fieldInfos.fieldInfo(dimensionsSplitOrder[i]);
            final FieldInfo fi = new FieldInfo(
                dimensionsSplitOrder[i] + "_dim",
                fieldNum,
                false,
                originalDimensionFieldInfo.omitsNorms(),
                originalDimensionFieldInfo.hasPayloads(),
                originalDimensionFieldInfo.getIndexOptions(),
                originalDimensionFieldInfo.getDocValuesType(),
                -1,
                originalDimensionFieldInfo.attributes(),
                originalDimensionFieldInfo.getPointDimensionCount(),
                originalDimensionFieldInfo.getPointIndexDimensionCount(),
                originalDimensionFieldInfo.getPointNumBytes(),
                originalDimensionFieldInfo.getVectorDimension(),
                originalDimensionFieldInfo.getVectorEncoding(),
                originalDimensionFieldInfo.getVectorSimilarityFunction(),
                false,
                originalDimensionFieldInfo.isParentField()
            );
            dimensionFieldInfoList[i] = fi;
            StarTreeDocValuesWriter starTreeDimensionDocValuesWriter = new StarTreeDocValuesWriter(
                originalDimensionFieldInfo.getDocValuesType(),
                getDocValuesWriter(originalDimensionFieldInfo.getDocValuesType(), fi, Counter.newCounter())
            );
            dimensionWriters.add(starTreeDimensionDocValuesWriter);
            fieldNum++;
        }
        for (int i = 0; i < metricReaders.length; i++) {
            FieldInfo originalMetricFieldInfo = segmentWriteState.fieldInfos.fieldInfo(metrics[i]);
            FieldInfo fi = new FieldInfo(
                metrics[i] + "_metric",
                fieldNum,
                false,
                originalMetricFieldInfo.omitsNorms(),
                originalMetricFieldInfo.hasPayloads(),
                originalMetricFieldInfo.getIndexOptions(),
                originalMetricFieldInfo.getDocValuesType(),
                -1,
                originalMetricFieldInfo.attributes(),
                originalMetricFieldInfo.getPointDimensionCount(),
                originalMetricFieldInfo.getPointIndexDimensionCount(),
                originalMetricFieldInfo.getPointNumBytes(),
                originalMetricFieldInfo.getVectorDimension(),
                originalMetricFieldInfo.getVectorEncoding(),
                originalMetricFieldInfo.getVectorSimilarityFunction(),
                false,
                originalMetricFieldInfo.isParentField()
            );
            metricFieldInfoList[i] = fi;
            StarTreeDocValuesWriter starTreeMetricDocValuesWriter = new StarTreeDocValuesWriter(
                originalMetricFieldInfo.getDocValuesType(),
                getDocValuesWriter(originalMetricFieldInfo.getDocValuesType(), fi, Counter.newCounter())
            );
            metricWriters.add(starTreeMetricDocValuesWriter);
            fieldNum++;
        }

        Map<String, Map<Long, BytesRef>> ordinalToSortedSetDocValueMap = new HashMap<>();
        for (int docId = 0; docId < numDocs; docId++) {
            StarTreeDocument starTreeDocument = getStarTreeDocument(docId);
            for (int i = 0; i < starTreeDocument.dimensions.length; i++) {
                long val = starTreeDocument.dimensions[i];
                StarTreeDocValuesWriter starTreeDocValuesWriter = dimensionWriters.get(i);
                switch (starTreeDocValuesWriter.getDocValuesType()) {
                    case SORTED_SET:
                        if (val == -1) continue;
                        ((SortedSetDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(
                            docId,
                            getSortedSetDocValueBytes(val, i, ordinalToSortedSetDocValueMap)
                        );
                        break;
                    case SORTED_NUMERIC:
                        ((SortedNumericDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(docId, val);
                        break;
                    default:
                        throw new IllegalStateException("Unsupported doc values type");
                }
            }
            for (int i = 0; i < starTreeDocument.metrics.length; i++) {
                try {
                    Number parse = NumberFieldMapper.NumberType.LONG.parse(starTreeDocument.metrics[i], true);
                    StarTreeDocValuesWriter starTreeDocValuesWriter = metricWriters.get(i);
                    ((SortedNumericDocValuesWriter) starTreeDocValuesWriter.getDocValuesWriter()).addValue(docId, parse.longValue());
                } catch (IllegalArgumentException e) {
                    logger.info("could not parse the value, exiting creation of star tree");
                }
            }
        }

        getStarTreeDocValueProducers(docValuesConsumer, dimensionWriters, dimensionFieldInfoList, dimensionReaders);
        getStarTreeDocValueProducers(docValuesConsumer, metricWriters, metricFieldInfoList, metricReaders);
    }

    private BytesRef getSortedSetDocValueBytes(long val, int counter, Map<String, Map<Long, BytesRef>> ordinalToSortedSetDocValueMap)
        throws IOException {
        String dimensionName = dimensionsSplitOrder[counter];
        if (!ordinalToSortedSetDocValueMap.containsKey(dimensionName)) {
            ordinalToSortedSetDocValueMap.put(dimensionName, new HashMap<>());
        }
        BytesRef bytes;
        if (ordinalToSortedSetDocValueMap.get(dimensionName).containsKey(val)) {
            bytes = ordinalToSortedSetDocValueMap.get(dimensionName).get(val);
        } else {
            bytes = ((SortedSetDocValues) dimensionReaders[counter]).lookupOrd(val);
            ordinalToSortedSetDocValueMap.get(dimensionName).put(val, BytesRef.deepCopyOf(bytes));
        }
        return bytes;
    }

    /**
     * Consumes the newly created aggregated metric fields
     */
    private void getStarTreeDocValueProducers(
        DocValuesConsumer docValuesConsumer,
        List<StarTreeDocValuesWriter> docValuesWriters,
        FieldInfo[] fieldInfoList,
        DocIdSetIterator[] readers
    ) throws IOException {
        for (int i = 0; i < readers.length; i++) {
            final int counter = i;
            DocValuesProducer docValuesProducer = new EmptyDocValuesProducer() {
                @Override
                public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
                    return ((SortedNumericDocValuesWriter) docValuesWriters.get(counter).getDocValuesWriter()).getDocValues();
                }

                @Override
                public SortedSetDocValues getSortedSet(FieldInfo field) {
                    return ((SortedSetDocValuesWriter) docValuesWriters.get(counter).getDocValuesWriter()).getDocValues();
                }
            };
            docValuesConsumer.addSortedNumericField(fieldInfoList[i], docValuesProducer);
        }
    }

    /**
     * Returns the respective doc values writer based on doc values type
     */
    DocValuesWriter<?> getDocValuesWriter(DocValuesType docValuesType, FieldInfo fi, Counter counter) {
        final ByteBlockPool.Allocator byteBlockAllocator = new ByteBlockPool.DirectTrackingAllocator(counter);
        final ByteBlockPool docValuesBytePool = new ByteBlockPool(byteBlockAllocator);
        switch (docValuesType) {
            case SORTED_SET:
                return new SortedSetDocValuesWriter(fi, counter, docValuesBytePool);
            case SORTED_NUMERIC:
                return new SortedNumericDocValuesWriter(fi, counter);
            default:
                throw new IllegalArgumentException("Unsupported DocValuesType: " + docValuesType);
        }
    }

    /**
     * Handles the dimension of date time field type
     */
    private long handleDateDimension(final String fieldName, final long val) {
        // TODO: handle timestamp granularity
        return val;
    }

    public void close() throws IOException {
        // boolean success = false;
        // try {
        // if (indexOutput != null) {
        // indexOutput.writeInt(-1);
        // CodecUtil.writeFooter(indexOutput); // write checksum
        // }
        // success = true;
        // } catch (Exception e) {
        // throw new RuntimeException(e);
        // } finally {
        // if (success) {
        // IOUtils.close(indexOutput);
        // } else {
        // IOUtils.closeWhileHandlingException(indexOutput);
        // }
        // indexOutput = null;
        // }
    }

}
