/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opensearch.index.codec.freshstartree.builder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;
import org.opensearch.index.codec.freshstartree.aggregator.AggregationFunctionColumnPair;
import org.opensearch.index.codec.freshstartree.aggregator.AggregationFunctionType;
import org.opensearch.index.codec.freshstartree.aggregator.ValueAggregator;
import org.opensearch.index.codec.freshstartree.aggregator.ValueAggregatorFactory;
import org.opensearch.index.codec.freshstartree.codec.StarTreeAggregatedValues;
import org.opensearch.index.codec.freshstartree.codec.StarTreeDocValuesWriter;
import org.opensearch.index.codec.freshstartree.node.StarTreeNode;
import org.opensearch.index.codec.freshstartree.util.BufferedAggregatedDocValues;


/** Base class for star tree builder */
public abstract class BaseSingleTreeBuilder {
    public static final int STAR_IN_DOC_VALUES_INDEX = -1;
    final static int SECOND = 1000;
    final static int MINUTE = 60 * SECOND;
    final static int HOUR = 60 * 60 * SECOND;
    final static int DAY = 24 * HOUR;
    final static int YEAR = 365 * DAY;
    private static final Logger logger = LogManager.getLogger(BaseSingleTreeBuilder.class);
    final int _numDimensions;
    final String[] _dimensionsSplitOrder;
    final Set<Integer> _skipStarNodeCreationForDimensions;
    final int _numMetrics;
    // Name of the function-column pairs
    final String[] _metrics;
    final int _maxLeafRecords;
    int _numDocs;
    int _totalDocs;
    int _numNodes;
    final StarTreeBuilderUtils.TreeNode _rootNode = getNewNode();
    IndexOutput indexOutput;
    SortedNumericDocValues[] _dimensionReaders;
    SortedNumericDocValues[] _metricReaders;
    ValueAggregator[] _valueAggregators;
    DocValuesConsumer _docValuesConsumer;

    BaseSingleTreeBuilder(IndexOutput output, List<String> dimensionsSplitOrder,
        Map<String, SortedNumericDocValues> docValuesMap, int maxDoc, DocValuesConsumer docValuesConsumer,
        SegmentWriteState state)
        throws IOException {

        String docFileName = IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, "stttree");
        indexOutput = state.directory.createOutput(docFileName, state.context);
        CodecUtil.writeIndexHeader(indexOutput, "STARTreeCodec", 0, state.segmentInfo.getId(), state.segmentSuffix);
        dimensionsSplitOrder = new ArrayList<>();
        // dimensionsSplitOrder.add("hour");
        dimensionsSplitOrder.add("day");
        dimensionsSplitOrder.add("month");
        dimensionsSplitOrder.add("year");
        dimensionsSplitOrder.add("status");
        _numDimensions = dimensionsSplitOrder.size();
        _dimensionsSplitOrder = new String[_numDimensions];
        _skipStarNodeCreationForDimensions = new HashSet<>();
        _totalDocs = state.segmentInfo.maxDoc();
        _docValuesConsumer = docValuesConsumer;
        List<String> functionColumnPairList = new ArrayList<>();

        // TODO : pass function column pair - Remove hardcoding
        functionColumnPairList.add("SUM__status");
        List<AggregationFunctionColumnPair> aggregationSpecs = new ArrayList<>();
        aggregationSpecs.add(AggregationFunctionColumnPair.fromColumnName("SUM__status"));
        //aggregationSpecs.add(AggregationFunctionColumnPair.fromColumnName("COUNT__status"));

        _dimensionReaders = new SortedNumericDocValues[_numDimensions];
        Set<String> skipStarNodeCreationForDimensions = new HashSet<>();
        for (int i = 0; i < _numDimensions; i++) {
            String dimension = dimensionsSplitOrder.get(i);
            //logger.info("Dimension split order : {}", dimension);
            _dimensionsSplitOrder[i] = dimension;
            if (skipStarNodeCreationForDimensions.contains(dimension)) {
                _skipStarNodeCreationForDimensions.add(i);
            }
            _dimensionReaders[i] = docValuesMap.get(dimension + "_dim");
        }
        _numMetrics = aggregationSpecs.size();
        _metrics = new String[_numMetrics];
        _valueAggregators = new ValueAggregator[_numMetrics];

        int index = 0;
        _metricReaders = new SortedNumericDocValues[_numMetrics];
        for (AggregationFunctionColumnPair aggrPair : aggregationSpecs) {
            AggregationFunctionColumnPair functionColumnPair = aggrPair;
            _metrics[index] = functionColumnPair.toColumnName() + "_" + functionColumnPair.getFunctionType().getName();
            _valueAggregators[index] = ValueAggregatorFactory.getValueAggregator(functionColumnPair.getFunctionType());
            // Ignore the column for COUNT aggregation function
            if (_valueAggregators[index].getAggregationType() != AggregationFunctionType.COUNT) {
                String column = functionColumnPair.getColumn();
                _metricReaders[index] =
                    docValuesMap.get(column + "_" + functionColumnPair.getFunctionType().getName() + "_metric");
            }

            index++;
        }

        // TODO : Removing hardcoding
        _maxLeafRecords = 100; // builderConfig.getMaxLeafRecords();
    }

    private void constructStarTree(StarTreeBuilderUtils.TreeNode node, int startDocId, int endDocId)
        throws IOException {

        int childDimensionId = node._dimensionId + 1;
        if (childDimensionId == _numDimensions) {
            return;
        }

        // Construct all non-star children nodes
        node._childDimensionId = childDimensionId;
        Map<Long, StarTreeBuilderUtils.TreeNode> children =
            constructNonStarNodes(startDocId, endDocId, childDimensionId);
        node._children = children;

        // Construct star-node if required
        if (!_skipStarNodeCreationForDimensions.contains(childDimensionId) && children.size() > 1) {
            children.put(StarTreeNode.ALL, constructStarNode(startDocId, endDocId, childDimensionId));
        }

        // Further split on child nodes if required
        for (StarTreeBuilderUtils.TreeNode child : children.values()) {
            if (child._endDocId - child._startDocId > _maxLeafRecords) {
                constructStarTree(child, child._startDocId, child._endDocId);
            }
        }
    }

    private Map<Long, StarTreeBuilderUtils.TreeNode> constructNonStarNodes(int startDocId, int endDocId,
        int dimensionId)
        throws IOException {
        Map<Long, StarTreeBuilderUtils.TreeNode> nodes = new HashMap<>();
        int nodeStartDocId = startDocId;
        long nodeDimensionValue = getDimensionValue(startDocId, dimensionId);
        for (int i = startDocId + 1; i < endDocId; i++) {
            long dimensionValue = getDimensionValue(i, dimensionId);
            // System.out.println("Dim value : " + dimensionValue );
            if (dimensionValue != nodeDimensionValue) {
                StarTreeBuilderUtils.TreeNode child = getNewNode();
                child._dimensionId = dimensionId;
                child._dimensionValue = nodeDimensionValue;
                child._startDocId = nodeStartDocId;
                child._endDocId = i;
                nodes.put(nodeDimensionValue, child);

                nodeStartDocId = i;
                nodeDimensionValue = dimensionValue;
            }
        }
        StarTreeBuilderUtils.TreeNode lastNode = getNewNode();
        lastNode._dimensionId = dimensionId;
        lastNode._dimensionValue = nodeDimensionValue;
        lastNode._startDocId = nodeStartDocId;
        lastNode._endDocId = endDocId;
        nodes.put(nodeDimensionValue, lastNode);
        return nodes;
    }

    private StarTreeBuilderUtils.TreeNode constructStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        StarTreeBuilderUtils.TreeNode starNode = getNewNode();
        starNode._dimensionId = dimensionId;
        starNode._dimensionValue = StarTreeNode.ALL;
        starNode._startDocId = _numDocs;
        Iterator<Record> recordIterator = generateRecordsForStarNode(startDocId, endDocId, dimensionId);
        while (recordIterator.hasNext()) {
            appendToStarTree(recordIterator.next());
        }
        starNode._endDocId = _numDocs;
        return starNode;
    }

    public abstract void build(List<StarTreeAggregatedValues> aggrList)
        throws IOException;

    public void build()
        throws IOException {
        // TODO: get total docs
        int numSegmentRecords = _totalDocs;

        long startTime = System.currentTimeMillis();
        Iterator<Record> recordIterator = sortAndAggregateSegmentRecords(numSegmentRecords);
        logger.info("Sorting and aggregating star-tree in ms : {}", (System.currentTimeMillis() - startTime));
        //    System.out.println(
        //        "== =============Finished sorting and aggregating star-tree in ms : " +
        // (System.currentTimeMillis()
        //            - startTime));

        build(recordIterator, false);
    }

    public void build(Iterator<Record> recordIterator, boolean isMerge)
        throws IOException {
        int numSegmentRecords = _totalDocs;

        while (recordIterator.hasNext()) {
            appendToStarTree(recordIterator.next());
        }
        int numStarTreeRecords = _numDocs;
        logger.info("Generated star tree records number : [{}] from segment records : [{}]", numStarTreeRecords,
            numSegmentRecords);
        if (_numDocs == 0) {
            StarTreeBuilderUtils.serializeTree(indexOutput, _rootNode, _dimensionsSplitOrder, _numNodes);
            return;
        }
        constructStarTree(_rootNode, 0, _numDocs);
        int numRecordsUnderStarNode = _numDocs - numStarTreeRecords;
        logger.info("Finished constructing star-tree, got [ {} ] tree nodes and [ {} ] records under star-node",
            _numNodes, numRecordsUnderStarNode);

        createAggregatedDocs(_rootNode);
        int numAggregatedRecords = _numDocs - numStarTreeRecords - numRecordsUnderStarNode;
        logger.info("Finished creating aggregated documents, got aggregated records : {}", numAggregatedRecords);

        // Create doc values indices in disk
        createDocValuesIndices(_docValuesConsumer);

        // Serialize and save in disk
        StarTreeBuilderUtils.serializeTree(indexOutput, _rootNode, _dimensionsSplitOrder, _numNodes);
    }

    private void createDocValuesIndices(DocValuesConsumer docValuesConsumer)
        throws IOException {
        PackedLongValues.Builder[] pendingDimArr = new PackedLongValues.Builder[_dimensionReaders.length];
        PackedLongValues.Builder[] pendingMetricArr = new PackedLongValues.Builder[_metricReaders.length];

        FieldInfo[] dimFieldInfoArr = new FieldInfo[_dimensionReaders.length];
        FieldInfo[] metricFieldInfoArr = new FieldInfo[_metricReaders.length];
        int fieldNum = 0;

        for (int i = 0; i < _dimensionReaders.length; i++) {
            pendingDimArr[fieldNum] = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
            dimFieldInfoArr[fieldNum] = new FieldInfo(_dimensionsSplitOrder[i] + "_dim", fieldNum, false, false, true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1,
                Collections.emptyMap(), 0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
            fieldNum++;
        }

        for (int i = 0; i < _metricReaders.length; i++) {
            pendingMetricArr[i] = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
            metricFieldInfoArr[i] = new FieldInfo(_metrics[i] + "_metric", fieldNum, false, false, true,
                IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, DocValuesType.NUMERIC, -1,
                Collections.emptyMap(), 0, 0, 0, 0, VectorEncoding.FLOAT32, VectorSimilarityFunction.EUCLIDEAN, false);
            fieldNum++;
        }

        DocsWithFieldSet docsWithField = new DocsWithFieldSet();

        for (int docId = 0; docId < _numDocs; docId++) {
            Record record = getStarTreeRecord(docId);
            for (int i = 0; i < record._dimensions.length; i++) {
                long val = record._dimensions[i];
                pendingDimArr[i].add(val);
            }
            for (int i = 0; i < record._metrics.length; i++) {
                switch (_valueAggregators[i].getAggregatedValueType()) {
                    case LONG:
                        long val = (long) record._metrics[i];
                        pendingMetricArr[i].add(val);
                        break;
                    // TODO: support this
                    case DOUBLE:
                        // double doubleval = (double) record._metrics[i];
                        // break;
                    case FLOAT:
                    case INT:
                    default:
                        throw new IllegalStateException("Unsupported value type");
                }
            }
            docsWithField.add(docId);
        }
        for (int i = 0; i < _dimensionReaders.length; i++) {
            final int finalI = i;
            DocValuesProducer a1 = new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field)
                    throws IOException {

                    return new BufferedAggregatedDocValues(pendingDimArr[finalI].build(), docsWithField.iterator());
                }
            };
            docValuesConsumer.addNumericField(dimFieldInfoArr[i], a1);
        }

        for (int i = 0; i < _metricReaders.length; i++) {
            final int finalI = i;
            DocValuesProducer a1 = new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field)
                    throws IOException {

                    return new BufferedAggregatedDocValues(pendingMetricArr[finalI].build(), docsWithField.iterator());
                }
            };
            docValuesConsumer.addNumericField(metricFieldInfoArr[i], a1);
        }
    }

    private StarTreeBuilderUtils.TreeNode getNewNode() {
        _numNodes++;
        return new StarTreeBuilderUtils.TreeNode();
    }

    private void appendToStarTree(Record record)
        throws IOException {
        // TODO : uncomment this for sanity
//        boolean star = true;
//        for(long dim : record._dimensions) {
//            if(dim != StarTreeNode.ALL) {
//                star = false;
//                break;
//            }
//        }
//        if(star) {
//            System.out.println("======Overall sum =====" + (long) record._metrics[0]);
//        }
        appendRecord(record);
        _numDocs++;
    }

    /**
     * Appends a record to the star-tree.
     *
     * @param record Record to be appended
     */
    abstract void appendRecord(Record record)
        throws IOException;

    /**
     * Returns the record of the given document Id in the star-tree.
     *
     * @param docId Document Id
     * @return Star-tree record
     */
    abstract Record getStarTreeRecord(int docId)
        throws IOException;

    /**
     * Returns the dimension value of the given document and dimension Id in the star-tree.
     *
     * @param docId Document Id
     * @param dimensionId Dimension Id
     * @return Dimension value
     */
    abstract long getDimensionValue(int docId, int dimensionId)
        throws IOException;

    /**
     * Sorts and aggregates the records in the segment, and returns a record iterator for all the
     * aggregated records.
     *
     * <p>This method reads records from segment and generates the initial records for the star-tree.
     *
     * @param numDocs Number of documents in the segment
     * @return Iterator for the aggregated records
     */
    abstract Iterator<Record> sortAndAggregateSegmentRecords(int numDocs)
        throws IOException;

    /**
     * Generates aggregated records for star-node.
     *
     * <p>This method will do the following steps:
     *
     * <ul>
     *   <li>Creates a temporary buffer for the given range of documents
     *   <li>Replaces the value for the given dimension Id to {@code STAR}
     *   <li>Sorts the records inside the temporary buffer
     *   <li>Aggregates the records with same dimensions
     *   <li>Returns an iterator for the aggregated records
     * </ul>
     *
     * @param startDocId Start document Id in the star-tree
     * @param endDocId End document Id (exclusive) in the star-tree
     * @param dimensionId Dimension Id of the star-node
     * @return Iterator for the aggregated records
     */
    abstract Iterator<Record> generateRecordsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException;

    private Record createAggregatedDocs(StarTreeBuilderUtils.TreeNode node)
        throws IOException {
        Record aggregatedRecord = null;
        if (node._children == null) {
            // For leaf node

            if (node._startDocId == node._endDocId - 1) {
                // If it has only one document, use it as the aggregated document
                aggregatedRecord = getStarTreeRecord(node._startDocId);
                node._aggregatedDocId = node._startDocId;
            } else {
                // If it has multiple documents, aggregate all of them
                for (int i = node._startDocId; i < node._endDocId; i++) {
                    aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, getStarTreeRecord(i));
                }
                assert aggregatedRecord != null;
                for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
                    aggregatedRecord._dimensions[i] =
                        STAR_IN_DOC_VALUES_INDEX; // StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
                }
                node._aggregatedDocId = _numDocs;
                appendToStarTree(aggregatedRecord);
            }
        } else {
            // For non-leaf node

            if (node._children.containsKey(StarTreeNode.ALL)) {
                // If it has star child, use the star child aggregated document directly
                for (StarTreeBuilderUtils.TreeNode child : node._children.values()) {
                    if (child._dimensionValue == StarTreeNode.ALL) {
                        aggregatedRecord = createAggregatedDocs(child);
                        node._aggregatedDocId = child._aggregatedDocId;
                    } else {
                        createAggregatedDocs(child);
                    }
                }
            } else {
                // If no star child exists, aggregate all aggregated documents from non-star children
                for (StarTreeBuilderUtils.TreeNode child : node._children.values()) {
                    aggregatedRecord = mergeStarTreeRecord(aggregatedRecord, createAggregatedDocs(child));
                }
                assert aggregatedRecord != null;
                for (int i = node._dimensionId + 1; i < _numDimensions; i++) {
                    aggregatedRecord._dimensions[i] =
                        STAR_IN_DOC_VALUES_INDEX; // StarTreeV2Constants.STAR_IN_FORWARD_INDEX;
                }
                node._aggregatedDocId = _numDocs;
                appendToStarTree(aggregatedRecord);
            }
        }
        return aggregatedRecord;
    }

    /**
     * Merges a segment record (raw) into the aggregated record.
     *
     * <p>Will create a new aggregated record if the current one is {@code null}.
     *
     * @param aggregatedRecord Aggregated record
     * @param segmentRecord Segment record
     * @return Merged record
     */
    Record mergeSegmentRecord(Record aggregatedRecord, Record segmentRecord) {
        if (aggregatedRecord == null) {
            long[] dimensions = new long[_numDimensions];
            for (int i = 0; i < _numDimensions; i++) {
                dimensions[i] = segmentRecord._dimensions[i];
            }
            Object[] metrics = new Object[_numMetrics];
            for (int i = 0; i < _numMetrics; i++) {
                // TODO: fill this
                metrics[i] = _valueAggregators[i].getInitialAggregatedValue((Long) segmentRecord._metrics[i]);
            }
            return new Record(dimensions, metrics);
        } else {
            for (int i = 0; i < _numMetrics; i++) {
                aggregatedRecord._metrics[i] = _valueAggregators[i].applyRawValue((Long) aggregatedRecord._metrics[i],
                    (Long) segmentRecord._metrics[i]);
            }
            return aggregatedRecord;
        }
    }

    /**
     * Merges a star-tree record (aggregated) into the aggregated record.
     *
     * <p>Will create a new aggregated record if the current one is {@code null}.
     *
     * @param aggregatedRecord Aggregated record
     * @param starTreeRecord Star-tree record
     * @return Merged record
     */
    Record mergeStarTreeRecord(Record aggregatedRecord, Record starTreeRecord) {
        if (aggregatedRecord == null) {
            long[] dimensions = new long[_numDimensions];
            for (int i = 0; i < _numDimensions; i++) {
                dimensions[i] = starTreeRecord._dimensions[i];
            }
            Object[] metrics = new Object[_numMetrics];
            for (int i = 0; i < _numMetrics; i++) {
                metrics[i] = _valueAggregators[i].cloneAggregatedValue((Long) starTreeRecord._metrics[i]);
            }
            return new Record(dimensions, metrics);
        } else {
            for (int i = 0; i < _numMetrics; i++) {
                aggregatedRecord._metrics[i] =
                    _valueAggregators[i].applyAggregatedValue((Long) starTreeRecord._metrics[i],
                        (Long) aggregatedRecord._metrics[i]);
            }
            return aggregatedRecord;
        }
    }

    Record getNextSegmentRecord()
        throws IOException {
        long[] dimensions = getNextSegmentRecordDimensions();
        Object[] metrics = new Object[_numMetrics];
        for (int i = 0; i < _numMetrics; i++) {
            // Ignore the column for COUNT aggregation function
            if (_metricReaders[i] != null) {
                _metricReaders[i].nextDoc();
                metrics[i] = _metricReaders[i].nextValue();
            }
        }
        return new Record(dimensions, metrics);
    }

    private long getTimeStampVal(final String fieldName, final long val) {

        switch (fieldName) {
            case "minute":
                return val / MINUTE;
            case "hour":
                return val / HOUR;
            case "day":
                return val / DAY;
            case "year":
                return val / YEAR;
            default:
                return val;
        }
    }

    long[] getNextSegmentRecordDimensions()
        throws IOException {
        long[] dimensions = new long[_numDimensions];
        for (int i = 0; i < _numDimensions; i++) {
            _dimensionReaders[i].nextDoc();
            dimensions[i] = getTimeStampVal(_dimensionsSplitOrder[i], _dimensionReaders[i].nextValue());
        }
        return dimensions;
    }

    public void close()
        throws IOException {
        boolean success = false;
        try {
            if (indexOutput != null) {
                indexOutput.writeInt(-1);
                CodecUtil.writeFooter(indexOutput); // write checksum
            }
            success = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
            //      System.out.println(e.getMessage());
        } finally {
            if (success) {
                IOUtils.close(indexOutput);
            } else {
                IOUtils.closeWhileHandlingException(indexOutput);
            }
            indexOutput = null;
        }
    }

    /** Star tree record */
    public static class Record {
        final long[] _dimensions;
        final Object[] _metrics;

        public Record(long[] dimensions, Object[] metrics) {
            _dimensions = dimensions;
            _metrics = metrics;
        }

        @Override
        public String toString() {
            return Arrays.toString(_dimensions) + " | " + Arrays.toString(_metrics);
        }
    }
}
