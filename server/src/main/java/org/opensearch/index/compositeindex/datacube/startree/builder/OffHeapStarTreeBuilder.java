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
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.store.TrackingDirectoryWrapper;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.codec.composite.datacube.startree.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.aggregators.numerictype.StarTreeNumericTypeConverters;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Off-heap implementation of the star tree builder.
 *
 * <p>
 * Segment documents are stored in a single file named 'segment.documents' for sorting and aggregation. A document ID array is created,
 * and the document IDs in the array are swapped during sorting based on the actual segment document values in the file.
 * <p>
 * Star tree documents are stored in multiple 'star-tree.documents' files. The algorithm works as follows:
 * <ol>
 * <li> Initially, aggregated documents are created based on the segment documents.</li>
 * <li> Further, star tree documents are generated (e.g., in the {@code generateStarTreeDocumentsForStarNode} method) by reading the current
 * aggregated documents and creating new aggregated star tree documents, which are appended to the 'star-tree.documents' files. </li>
 * <li> This process is repeated until all combinations of star tree documents are generated. </li>
 * </ol>
 * <p>In cases where previously written star tree documents need to be read from the 'star-tree.documents' files, the current
 * 'star-tree.documents' file is closed, and the values are read. Then, the derived values gets appended to a new 'star-tree.documents' file.
 * This is necessary because Lucene maintains immutability of data, and an {@code IndexOutput} cannot be kept open while creating an
 * {@code IndexInput} on the same file, as all file contents may not be visible in the reader. Therefore, the {@code IndexOutput} must be
 * closed to ensure all data can be read before creating an {@code IndexInput}. Additionally, an {@code IndexOutput} cannot be reopened,
 * so a new file is created for the new star tree documents.
 * <p>The set of 'star-tree.documents' files is maintained, and a tracker array is used to keep track of the start document ID for each file.
 * Once the number of files reaches a set threshold, the files are merged.

 @opensearch.experimental
 **/
@ExperimentalApi
public class OffHeapStarTreeBuilder extends BaseStarTreeBuilder {
    private static final Logger logger = LogManager.getLogger(OffHeapStarTreeBuilder.class);
    private static final String SEGMENT_DOC_FILE_NAME = "segment.documents";
    private static final String STAR_TREE_DOC_FILE_NAME = "star-tree.documents";
    // TODO : Should this be via settings ?
    private static final int DEFAULT_FILE_COUNT_MERGE_THRESHOLD = 5;
    private final int fileCountMergeThreshold;
    private final List<Integer> starTreeDocumentOffsets;
    private int numReadableStarTreeDocuments;
    final IndexOutput segmentDocsFileOutput;
    private IndexOutput starTreeDocsFileOutput;
    private IndexInput starTreeDocsFileInput;
    private IndexInput segmentDocsFileInput;
    private RandomAccessInput segmentRandomInput;
    private RandomAccessInput starTreeDocsFileRandomInput;
    private final SegmentWriteState state;
    private final Map<String, Integer> fileToEndDocIdMap;
    private int starTreeFileCount = -1;
    private int prevStartDocId = Integer.MAX_VALUE;
    private int currBytes = 0;
    private int docSizeInBytes = -1;
    private final TrackingDirectoryWrapper tmpDirectory;

    /**
     * Builds star tree based on star tree field configuration consisting of dimensions, metrics and star tree index
     * specific configuration.
     *
     * @param starTreeField holds the configuration for the star tree
     * @param state         stores the segment write state
     * @param mapperService helps to find the original type of the field
     */
    protected OffHeapStarTreeBuilder(StarTreeField starTreeField, SegmentWriteState state, MapperService mapperService) throws IOException {
        this(starTreeField, state, mapperService, DEFAULT_FILE_COUNT_MERGE_THRESHOLD);
    }

    /**
     * Builds star tree based on star tree field configuration consisting of dimensions, metrics and star tree index
     * specific configuration.
     *
     * @param starTreeField holds the configuration for the star tree
     * @param state         stores the segment write state
     * @param mapperService helps to find the original type of the field
     * @param fileThreshold threshold for number of files after which we merge the files
     */
    protected OffHeapStarTreeBuilder(StarTreeField starTreeField, SegmentWriteState state, MapperService mapperService, int fileThreshold)
        throws IOException {
        super(starTreeField, state, mapperService);
        this.fileCountMergeThreshold = fileThreshold;
        this.state = state;
        this.tmpDirectory = new TrackingDirectoryWrapper(state.directory);
        fileToEndDocIdMap = new LinkedHashMap<>(); // maintain order
        try {
            starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
            segmentDocsFileOutput = tmpDirectory.createTempOutput(SEGMENT_DOC_FILE_NAME, state.segmentSuffix, state.context);
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
            IOUtils.close(this);
            throw e;
        }
        starTreeDocumentOffsets = new ArrayList<>();
    }

    /**
     * Creates a new star tree document temporary file to store star tree documents.
     */
    IndexOutput createStarTreeDocumentsFileOutput() throws IOException {
        starTreeFileCount++;
        return tmpDirectory.createTempOutput(STAR_TREE_DOC_FILE_NAME + starTreeFileCount, state.segmentSuffix, state.context);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        int bytes = writeStarTreeDocument(starTreeDocument, starTreeDocsFileOutput, true);
        if (docSizeInBytes == -1) {
            docSizeInBytes = bytes;
        }
        assert docSizeInBytes == bytes;
        starTreeDocumentOffsets.add(currBytes);
        currBytes += bytes;
    }

    /**
     * Builds star tree based on the star tree values from multiple segments
     *
     * @param starTreeValuesSubs contains the star tree values from multiple segments
     */
    @Override
    public void build(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        try {
            build(mergeStarTrees(starTreeValuesSubs));
        } finally {
            try {
                for (String file : tmpDirectory.getCreatedFiles()) {
                    tmpDirectory.deleteFile(file);
                }
            } catch (final IOException ignored) {}
        }
    }

    /**
     * Sorts and aggregates the star-tree documents from multiple segments and builds star tree based on the newly
     * aggregated star-tree documents
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return iterator of star tree documents
     */
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        int docBytesLength = 0;
        int numDocs = 0;
        int[] sortedDocIds;
        try {
            for (StarTreeValues starTreeValues : starTreeValuesSubs) {
                List<Dimension> dimensionsSplitOrder = starTreeValues.getStarTreeField().getDimensionsOrder();
                SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[starTreeValues.getStarTreeField()
                    .getDimensionsOrder()
                    .size()];
                for (int i = 0; i < dimensionsSplitOrder.size(); i++) {
                    String dimension = dimensionsSplitOrder.get(i).getField();
                    dimensionReaders[i] = new SequentialDocValuesIterator(starTreeValues.getDimensionDocValuesIteratorMap().get(dimension));
                }
                List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
                for (Map.Entry<String, DocIdSetIterator> metricDocValuesEntry : starTreeValues.getMetricDocValuesIteratorMap().entrySet()) {
                    metricReaders.add(new SequentialDocValuesIterator(metricDocValuesEntry.getValue()));
                }
                int currentDocId = 0;
                int numSegmentDocs = Integer.parseInt(
                    starTreeValues.getAttributes().getOrDefault(NUM_SEGMENT_DOCS, String.valueOf(DocIdSetIterator.NO_MORE_DOCS))
                );
                while (currentDocId < numSegmentDocs) {
                    StarTreeDocument starTreeDocument = getStarTreeDocument(currentDocId, dimensionReaders, metricReaders);
                    int bytes = writeStarTreeDocument(starTreeDocument, segmentDocsFileOutput, true);
                    numDocs++;
                    docBytesLength = bytes;
                    currentDocId++;
                }
            }
            sortedDocIds = new int[numDocs];
            for (int i = 0; i < numDocs; i++) {
                sortedDocIds[i] = i;
            }
        } finally {
            segmentDocsFileOutput.close();
        }

        if (numDocs == 0) {
            return Collections.emptyIterator();
        }

        return sortAndReduceDocuments(sortedDocIds, numDocs, docBytesLength, true);
    }

    /**
     * Sorts and reduces the star tree documents based on the dimensions during flush flow
     */
    private Iterator<StarTreeDocument> sortAndReduceDocuments(int[] sortedDocIds, int numDocs, int docBytesLength) throws IOException {
        return sortAndReduceDocuments(sortedDocIds, numDocs, docBytesLength, false);
    }

    /**
     * Sorts and reduces the star tree documents based on the dimensions
     */
    private Iterator<StarTreeDocument> sortAndReduceDocuments(int[] sortedDocIds, int numDocs, int docBytesLength, boolean isMerge)
        throws IOException {
        try {
            segmentDocsFileInput = tmpDirectory.openInput(segmentDocsFileOutput.getName(), state.context);
            final long documentBytes = docBytesLength;
            segmentRandomInput = segmentDocsFileInput.randomAccessSlice(0, segmentDocsFileInput.length());
            if (sortedDocIds == null || sortedDocIds.length == 0) {
                logger.debug("Sorted doc ids array is null");
                return Collections.emptyIterator();
            }
            new IntroSorter() {
                private long[] dimensions;

                @Override
                protected void swap(int i, int j) {
                    int temp = sortedDocIds[i];
                    sortedDocIds[i] = sortedDocIds[j];
                    sortedDocIds[j] = temp;
                }

                @Override
                protected void setPivot(int i) {
                    long offset = (long) sortedDocIds[i] * documentBytes;
                    dimensions = new long[starTreeField.getDimensionsOrder().size()];
                    try {
                        for (int j = 0; j < dimensions.length; j++) {
                            dimensions[j] = segmentRandomInput.readLong(offset + (long) j * Long.BYTES);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Sort documents failed ", e);
                    }
                }

                @Override
                protected int comparePivot(int j) {
                    long offset = (long) sortedDocIds[j] * documentBytes;
                    try {
                        for (int i = 0; i < dimensions.length; i++) {
                            long dimension = segmentRandomInput.readLong(offset + (long) i * Long.BYTES);
                            if (dimensions[i] != dimension) {
                                return Long.compare(dimensions[i], dimension);
                            }
                        }
                    } catch (IOException e) {
                        throw new RuntimeException("Sort documents failed ", e);
                    }
                    return 0;
                }
            }.sort(0, numDocs);

            // Create an iterator for aggregated documents
            IndexInput finalSegmentDocsFileInput = segmentDocsFileInput;
            return new Iterator<StarTreeDocument>() {
                boolean _hasNext = true;
                StarTreeDocument currentDocument;

                {
                    currentDocument = getSegmentStarTreeDocument(sortedDocIds[0], documentBytes, isMerge);
                }

                int _docId = 1;

                @Override
                public boolean hasNext() {
                    return _hasNext;
                }

                @Override
                public StarTreeDocument next() {
                    StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentDocument, isMerge);
                    while (_docId < numDocs) {
                        StarTreeDocument doc;
                        try {
                            doc = getSegmentStarTreeDocument(sortedDocIds[_docId++], documentBytes, isMerge);
                        } catch (IOException e) {
                            throw new RuntimeException("Reducing documents failed ", e);
                        }
                        if (!Arrays.equals(doc.dimensions, next.dimensions)) {
                            currentDocument = doc;
                            return next;
                        } else {
                            next = reduceSegmentStarTreeDocuments(next, doc, isMerge);
                        }
                    }
                    _hasNext = false;
                    IOUtils.closeWhileHandlingException(finalSegmentDocsFileInput);
                    try {
                        tmpDirectory.deleteFile(segmentDocsFileOutput.getName());
                    } catch (final IOException ignored) {}
                    return next;
                }
            };
        } catch (IOException ex) {
            IOUtils.closeWhileHandlingException(segmentDocsFileInput);
            throw ex;
        }
    }

    /**
     * Get segment star tree document from the segment.documents file
     */
    public StarTreeDocument getSegmentStarTreeDocument(int docID, long documentBytes, boolean isMerge) throws IOException {
        return readStarTreeDocument(segmentRandomInput, docID * documentBytes, isMerge);
    }

    /**
     * Get star tree document for the given docId from the star-tree documents file
     */
    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        ensureDocumentReadable(docId);
        return readStarTreeDocument(starTreeDocsFileRandomInput, starTreeDocumentOffsets.get(docId), true);
    }

    // This should be only used for testing
    @Override
    public List<StarTreeDocument> getStarTreeDocuments() throws IOException {
        List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
        for (int i = 0; i < numStarTreeDocs; i++) {
            starTreeDocuments.add(getStarTreeDocument(i));
        }
        return starTreeDocuments;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        ensureDocumentReadable(docId);
        return starTreeDocsFileRandomInput.readLong((starTreeDocumentOffsets.get(docId) + ((long) dimensionId * Long.BYTES)));
    }

    /**
     * Sorts and aggregates all the documents of the segment based on dimension and metrics configuration
     *
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders    List of docValues readers to read metrics from the segment
     * @return Iterator of star-tree documents
     */
    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        // Write all dimensions for segment documents into the buffer, and sort all documents using an int
        // array
        int documentBytesLength = 0;
        int[] sortedDocIds = new int[totalSegmentDocs];
        for (int i = 0; i < totalSegmentDocs; i++) {
            sortedDocIds[i] = i;
        }

        try {
            for (int i = 0; i < totalSegmentDocs; i++) {
                StarTreeDocument document = getSegmentStarTreeDocument(i, dimensionReaders, metricReaders);
                documentBytesLength = writeStarTreeDocument(document, segmentDocsFileOutput, false);
            }
        } finally {
            segmentDocsFileOutput.close();
        }

        // Create an iterator for aggregated documents
        return sortAndReduceDocuments(sortedDocIds, totalSegmentDocs, documentBytesLength);
    }

    /**
     * Generates a star-tree for a given star-node
     *
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return iterator for star-tree documents of star-node
     * @throws IOException throws when unable to generate star-tree for star-node
     */
    @Override
    public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        // End doc id is not inclusive but start doc is inclusive
        // Hence we need to check if buffer is readable till endDocId - 1
        ensureDocumentReadable(endDocId - 1);

        // Sort all documents using an int array
        int numDocs = endDocId - startDocId;
        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = startDocId + i;
        }
        new IntroSorter() {
            private long[] dimensions;

            @Override
            protected void swap(int i, int j) {
                int temp = sortedDocIds[i];
                sortedDocIds[i] = sortedDocIds[j];
                sortedDocIds[j] = temp;
            }

            @Override
            protected void setPivot(int i) {
                long offset = starTreeDocumentOffsets.get(sortedDocIds[i]);
                dimensions = new long[starTreeField.getDimensionsOrder().size()];
                try {
                    for (int j = dimensionId + 1; j < dimensions.length; j++) {
                        dimensions[j] = starTreeDocsFileRandomInput.readLong(offset + (long) j * Long.BYTES);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed ", e);
                }
            }

            @Override
            protected int comparePivot(int j) {
                long offset = starTreeDocumentOffsets.get(sortedDocIds[j]);
                try {
                    for (int i = dimensionId + 1; i < dimensions.length; i++) {
                        long dimension = starTreeDocsFileRandomInput.readLong(offset + (long) i * Long.BYTES);
                        if (dimensions[i] != dimension) {
                            return Long.compare(dimensions[i], dimension);
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Sort documents failed ", e);
                }
                return 0;
            }
        }.sort(0, numDocs);

        // Create an iterator for aggregated documents
        return new Iterator<StarTreeDocument>() {
            boolean _hasNext = true;
            StarTreeDocument _currentdocument = getStarTreeDocument(sortedDocIds[0]);
            int _docId = 1;

            private boolean hasSameDimensions(StarTreeDocument document1, StarTreeDocument document2) {
                for (int i = dimensionId + 1; i < starTreeField.getDimensionsOrder().size(); i++) {
                    if (!Objects.equals(document1.dimensions[i], document2.dimensions[i])) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return _hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceStarTreeDocuments(null, _currentdocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (_docId < numDocs) {
                    StarTreeDocument document;
                    try {
                        document = getStarTreeDocument(sortedDocIds[_docId++]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!hasSameDimensions(document, _currentdocument)) {
                        _currentdocument = document;
                        return next;
                    } else {
                        next = reduceStarTreeDocuments(next, document);
                    }
                }
                _hasNext = false;
                return next;
            }
        };
    }

    /**
     * Write the star tree document to file associated with dimensions and metrics
     */
    int writeStarTreeDocument(StarTreeDocument starTreeDocument, IndexOutput output, boolean isAggregatedDoc) throws IOException {
        int numBytes = writeDimensions(starTreeDocument, output);
        numBytes += writeMetrics(starTreeDocument, output, isAggregatedDoc);
        return numBytes;
    }

    /**
     * Write dimensions to file
     */
    int writeDimensions(StarTreeDocument starTreeDocument, IndexOutput output) throws IOException {
        int numBytes = 0;
        // Initialize to set bits for dimensions which are null
        byte dimensionNullBitSet = 0;
        List<Byte> dimensionsByteBitSets = new ArrayList<>();
        for (int i = 0; i < starTreeDocument.dimensions.length; i++) {
            if (i % 8 == 0 && i > 0) {
                dimensionsByteBitSets.add(dimensionNullBitSet);
                dimensionNullBitSet = 0;
            }
            if (starTreeDocument.dimensions[i] == null) {
                // Set the corresponding bit in dimensionNullBitSet to 1 (present)
                dimensionNullBitSet |= (byte) (1 << (i % 8));
                starTreeDocument.dimensions[i] = 0L;
            }
            output.writeLong(starTreeDocument.dimensions[i]);
            numBytes += Long.BYTES;
        }
        dimensionsByteBitSets.add(dimensionNullBitSet);
        for (Byte dimBitSet : dimensionsByteBitSets) {
            output.writeByte(dimBitSet);
            numBytes += Byte.BYTES;
        }
        return numBytes;
    }

    /**
     * Write star tree document metrics to file
     */
    private int writeMetrics(StarTreeDocument starTreeDocument, IndexOutput output, boolean isAggregatedDoc) throws IOException {
        int numBytes = 0;
        List<Byte> byteBitSets = new ArrayList<>();
        byte metricsNullBitSet = 0;
        for (int i = 0; i < starTreeDocument.metrics.length; i++) {
            if (i % 8 == 0 && i > 0) {
                output.writeByte(metricsNullBitSet);
                byteBitSets.add(metricsNullBitSet);
                metricsNullBitSet = 0;
            }
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    if (starTreeDocument.metrics[i] == null) {
                        starTreeDocument.metrics[i] = 0L;
                        metricsNullBitSet |= (byte) (1 << (i % 8));
                    }
                    output.writeLong((Long) starTreeDocument.metrics[i]);
                    numBytes += Long.BYTES;
                    break;
                case DOUBLE:
                    if (isAggregatedDoc) {
                        if (starTreeDocument.metrics[i] == null) {
                            starTreeDocument.metrics[i] = 0.0;
                            metricsNullBitSet |= (byte) (1 << (i % 8));
                        }
                        long val = NumericUtils.doubleToSortableLong((Double) starTreeDocument.metrics[i]);
                        output.writeLong(val);
                        numBytes += Long.BYTES;
                    } else {
                        if (starTreeDocument.metrics[i] == null) {
                            starTreeDocument.metrics[i] = 0L;
                            metricsNullBitSet |= (byte) (1 << (i % 8));
                        }
                        output.writeLong((Long) starTreeDocument.metrics[i]);
                        numBytes += Long.BYTES;
                    }
                    break;
                case INT:
                case FLOAT:
                default:
                    throw new IllegalStateException("Unsupported metric type");
            }
        }
        byteBitSets.add(metricsNullBitSet);
        for (Byte bitSet : byteBitSets) {
            output.writeByte(bitSet);
            numBytes += Byte.BYTES;
        }
        return numBytes;
    }

    /**
     * Reads the star tree document from file with given offset
     *
     * @param input   RandomAccessInput
     * @param offset  Offset in the file
     * @param shouldReadAggregatedDocs boolean to indicate if aggregated star tree docs should be read
     * @return StarTreeDocument
     * @throws IOException IOException in case of I/O errors
     */
    private StarTreeDocument readStarTreeDocument(RandomAccessInput input, long offset, boolean shouldReadAggregatedDocs)
        throws IOException {
        int dimSize = starTreeField.getDimensionsOrder().size();
        Long[] dimensions = new Long[dimSize];
        offset = readDimensions(dimensions, input, offset);

        int numMetrics = getNumMetrics();
        Object[] metrics = new Object[numMetrics];
        offset = readMetrics(input, offset, numMetrics, metrics, shouldReadAggregatedDocs);

        return new StarTreeDocument(dimensions, metrics);
    }

    /**
     * Read dimensions from file
     */
    long readDimensions(Long[] dimensions, RandomAccessInput input, long offset) throws IOException {
        for (int i = 0; i < dimensions.length; i++) {
            try {
                dimensions[i] = input.readLong(offset);
            } catch (Exception e) {
                logger.error("Error reading dimension value at offset {} for dimension {}", offset, i);
                throw e;
            }
            offset += Long.BYTES;
        }
        offset += setNullValuesForDimensions(input, offset, dimensions);
        return offset;
    }

    /**
     * Read star tree metrics from file
     */
    private long readMetrics(RandomAccessInput input, long offset, int numMetrics, Object[] metrics, boolean shouldReadAggregatedDocs)
        throws IOException {
        for (int i = 0; i < numMetrics; i++) {
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    metrics[i] = input.readLong(offset);
                    offset += Long.BYTES;
                    break;
                case DOUBLE:
                    long val = input.readLong(offset);
                    if (shouldReadAggregatedDocs) {
                        metrics[i] = StarTreeNumericTypeConverters.sortableLongtoDouble(val);
                    } else {
                        metrics[i] = val;
                    }
                    offset += Long.BYTES;
                    break;
                default:
                    throw new IllegalStateException("Unsupported metric type");
            }
        }
        offset += setNullForMetrics(input, offset, numMetrics, metrics);
        return offset;
    }

    private int getNumMetrics() {
        int numMetrics = 0;
        for (Metric metric : starTreeField.getMetrics()) {
            numMetrics += metric.getMetrics().size();
        }
        return numMetrics;
    }

    /**
     * Sets null/identity equivalent for the metrics when applicable based on the bitset
     */
    int setNullForMetrics(RandomAccessInput input, long offset, int numMetrics, Object[] metrics) throws IOException {
        int numBytes = 0;
        byte nullMetricsBitSet = input.readByte(offset + numBytes);
        numBytes += Byte.BYTES;
        for (int i = 0; i < numMetrics; i++) {
            if (i > 0 && i % 8 == 0) {
                nullMetricsBitSet = input.readByte(offset + numBytes);
                numBytes += Byte.BYTES;
            }
            boolean isMetricNull;
            switch (metricAggregatorInfos.get(i).getValueAggregators().getAggregatedValueType()) {
                case LONG:
                    isMetricNull = (nullMetricsBitSet & (1 << (i % 8))) != 0;
                    if (isMetricNull) {
                        // TODO : get the identity value of metrics here
                        metrics[i] = 0L;
                    }
                    break;
                case DOUBLE:
                    isMetricNull = (nullMetricsBitSet & (1 << (i % 8))) != 0;
                    if (isMetricNull) {
                        // TODO : get the identity value of metrics here
                        metrics[i] = 0;
                    }
                    break;

                case FLOAT:
                case INT:
                default:
                    throw new IllegalStateException();
            }
        }
        return numBytes;
    }

    /**
     * Set null values for dimension based on the bitset
     */
    private int setNullValuesForDimensions(RandomAccessInput input, long offset, Long[] dimensions) throws IOException {
        int numBytes = 0;
        byte nullDimensionsBitSet = input.readByte(offset + numBytes);
        numBytes += Byte.BYTES;
        for (int i = 0; i < dimensions.length; i++) {
            if (i > 0 && i % 8 == 0) {
                nullDimensionsBitSet = input.readByte(offset + numBytes);
                numBytes += Byte.BYTES;
            }
            boolean isDimensionNull = (nullDimensionsBitSet & (1L << (i % 8))) != 0;
            if (isDimensionNull) {
                dimensions[i] = null;
            }
        }
        return numBytes;
    }

    /**
     * Load the correct StarTreeDocuments file based on the docId
     */
    private void ensureDocumentReadable(int docId) throws IOException {
        ensureDocumentReadable(docId, true);
    }

    /**
     * Load the correct StarTreeDocuments file based on the docId
     *
     * @param  docId requested doc id
     * @param shouldCreateFileOutput this flag is used to indicate whether to create a new file output which is not needed during file format write operation
     */
    private void ensureDocumentReadable(int docId, boolean shouldCreateFileOutput) throws IOException {
        if (docId >= prevStartDocId && docId < numReadableStarTreeDocuments) {
            return;
        }
        IOUtils.closeWhileHandlingException(starTreeDocsFileInput);
        starTreeDocsFileInput = null;
        /*
         * If docId is less then the _numDocs , then we need to find a previous file associated with doc id
         * The fileToByteSizeMap is in the following format
         * file1 -> 521
         * file2 -> 780
         * which represents that file1 contains all docs till "520".
         *
         * "prevStartDocId" tracks the "start doc id" of the range in the present 'star-tree.documents' file
         * "numReadableStarTreeDocuments" tracks the "end doc id + 1" of the range of docs in the present file
         *
         * IMPORTANT : This is case where the requested file is not the file which is being currently written to
         */
        try {
            if (docId < numStarTreeDocs) {
                int prevStartDocId = 0;
                for (Map.Entry<String, Integer> entry : fileToEndDocIdMap.entrySet()) {
                    if (docId < entry.getValue()) {
                        starTreeDocsFileInput = tmpDirectory.openInput(entry.getKey(), state.context);
                        starTreeDocsFileRandomInput = starTreeDocsFileInput.randomAccessSlice(
                            starTreeDocsFileInput.getFilePointer(),
                            starTreeDocsFileInput.length() - starTreeDocsFileInput.getFilePointer()
                        );
                        numReadableStarTreeDocuments = entry.getValue();
                        break;
                    }
                    prevStartDocId = entry.getValue();
                }
                this.prevStartDocId = prevStartDocId;
            }

            if (starTreeDocsFileInput != null) {
                return;
            }
        } catch (IOException ex) {
            if (starTreeDocsFileOutput != null) {
                IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
            }
        }
        /*
         * This is the case where the requested document id is in the currently open star.documents file
         * So we close the current file, create/open a new file and read the current file.
         */
        if (starTreeDocsFileOutput != null) {
            IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
        }
        currBytes = 0;
        if (starTreeDocsFileOutput != null) {
            fileToEndDocIdMap.put(starTreeDocsFileOutput.getName(), numStarTreeDocs);
        }

        if (shouldCreateFileOutput) {
            starTreeDocsFileOutput = createStarTreeDocumentsFileOutput();
        }

        // Check if we need to merge files
        if (fileToEndDocIdMap.size() >= fileCountMergeThreshold) {
            mergeFiles();
        }

        if (starTreeDocsFileRandomInput != null) {
            starTreeDocsFileRandomInput = null;
        }
        try {
            int prevStartDocId = 0;
            for (Map.Entry<String, Integer> fileToEndDocId : fileToEndDocIdMap.entrySet()) {
                if (docId <= fileToEndDocId.getValue() - 1) {
                    starTreeDocsFileInput = tmpDirectory.openInput(fileToEndDocId.getKey(), state.context);
                    starTreeDocsFileRandomInput = starTreeDocsFileInput.randomAccessSlice(
                        starTreeDocsFileInput.getFilePointer(),
                        starTreeDocsFileInput.length() - starTreeDocsFileInput.getFilePointer()
                    );
                    numReadableStarTreeDocuments = fileToEndDocId.getValue();
                    break;
                }
                prevStartDocId = fileToEndDocId.getValue();
            }
            this.prevStartDocId = prevStartDocId;
        } catch (IOException e) {
            IOUtils.close(this);
            throw e;
        }
    }

    /**
     * Merge temporary star tree files once the number of files reach threshold
     */
    private void mergeFiles() throws IOException {
        try (IndexOutput mergedOutput = createStarTreeDocumentsFileOutput()) {
            long st = System.currentTimeMillis();

            long mergeBytes = 0L;
            for (Map.Entry<String, Integer> entry : fileToEndDocIdMap.entrySet()) {
                IndexInput input = tmpDirectory.openInput(entry.getKey(), state.context);
                mergedOutput.copyBytes(input, input.length());
                mergeBytes += input.length();
                input.close();
            }
            logger.debug(
                "Created merge file : {} in : {} ms with size of : {} KB",
                starTreeDocsFileOutput.getName(),
                System.currentTimeMillis() - st,
                mergeBytes / 1024
            );
            // Delete the old files
            for (String fileName : fileToEndDocIdMap.keySet()) {
                tmpDirectory.deleteFile(fileName);
            }
            // Clear the fileToByteSizeMap and add the merged file
            fileToEndDocIdMap.clear();
            fileToEndDocIdMap.put(mergedOutput.getName(), numStarTreeDocs);
        }

        int curr = 0;
        for (int i = 0; i < starTreeDocumentOffsets.size(); i++) {
            starTreeDocumentOffsets.set(i, curr);
            curr += docSizeInBytes;
        }

    }

    /**
     * Close the open segment files, star tree document files and associated data in/outputs.
     * Delete all the temporary segment files and star tree document files
     *
     * @throws IOException IOException in case of I/O errors
     */
    @Override
    public void close() throws IOException {
        try {
            if (starTreeDocsFileOutput != null) {
                IOUtils.closeWhileHandlingException(starTreeDocsFileOutput);
                try {
                    tmpDirectory.deleteFile(starTreeDocsFileOutput.getName());
                } catch (IOException ignored) {}
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeWhileHandlingException(starTreeDocsFileInput, segmentDocsFileInput, starTreeDocsFileOutput, segmentDocsFileOutput);
        }
        try {
            if (this.segmentDocsFileOutput != null) {
                // Delete all temporary segment document files
                tmpDirectory.deleteFile(segmentDocsFileOutput.getName());
            }
        } catch (IOException ignored) {}
        // Delete all temporary star tree document files
        for (String file : fileToEndDocIdMap.keySet()) {
            try {
                tmpDirectory.deleteFile(file);
            } catch (IOException ignored) {}
        }
        super.close();
    }
}
