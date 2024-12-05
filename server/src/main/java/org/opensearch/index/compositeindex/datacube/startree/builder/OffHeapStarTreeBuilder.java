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
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeDocumentsSorter;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Off-heap implementation of the star tree builder.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OffHeapStarTreeBuilder extends BaseStarTreeBuilder {
    private static final Logger logger = LogManager.getLogger(OffHeapStarTreeBuilder.class);
    private final StarTreeDocsFileManager starTreeDocumentFileManager;
    private final SegmentDocsFileManager segmentDocumentFileManager;

    /**
     * Builds star tree based on star tree field configuration consisting of dimensions, metrics and star tree index
     * specific configuration.
     *
     * @param metaOut       an index output to write star-tree metadata
     * @param dataOut       an index output to write star-tree data
     * @param starTreeField holds the configuration for the star tree
     * @param state         stores the segment write state
     * @param mapperService helps to find the original type of the field
     */
    protected OffHeapStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState state,
        MapperService mapperService
    ) throws IOException {
        super(metaOut, dataOut, starTreeField, state, mapperService);
        segmentDocumentFileManager = new SegmentDocsFileManager(state, starTreeField, metricAggregatorInfos, numDimensions);
        try {
            starTreeDocumentFileManager = new StarTreeDocsFileManager(state, starTreeField, metricAggregatorInfos, numDimensions);
        } catch (IOException e) {
            IOUtils.closeWhileHandlingException(segmentDocumentFileManager);
            throw e;
        }

    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        starTreeDocumentFileManager.writeStarTreeDocument(starTreeDocument, true);
    }

    /**
     * Builds star tree based on the star tree values from multiple segments
     *
     * @param starTreeValuesSubs contains the star tree values from multiple segments
     */
    @Override
    public void build(
        List<StarTreeValues> starTreeValuesSubs,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer
    ) throws IOException {
        boolean success = false;
        try {
            build(mergeStarTrees(starTreeValuesSubs), fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
            success = true;
        } finally {
            starTreeDocumentFileManager.deleteFiles(success);
            segmentDocumentFileManager.deleteFiles(success);
        }
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
        // Write all dimensions for segment documents into the buffer,
        // and sort all documents using an int array
        int[] sortedDocIds = new int[totalSegmentDocs];
        for (int i = 0; i < totalSegmentDocs; i++) {
            sortedDocIds[i] = i;
        }
        try {
            for (int i = 0; i < totalSegmentDocs; i++) {
                StarTreeDocument document = getSegmentStarTreeDocumentWithMetricFieldValues(i, dimensionReaders, metricReaders);
                segmentDocumentFileManager.writeStarTreeDocument(document, false);
            }
        } catch (IOException ex) {
            segmentDocumentFileManager.close();
            throw ex;
        }
        // Create an iterator for aggregated documents
        return sortAndReduceDocuments(sortedDocIds, totalSegmentDocs, false);
    }

    /**
     * Returns the star-tree document from the segment based on the current doc id
     */
    StarTreeDocument getSegmentStarTreeDocumentWithMetricFieldValues(
        int currentDocId,
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        Long[] dimensions = getStarTreeDimensionsFromSegment(currentDocId, dimensionReaders);
        Object[] metricValues = getStarTreeMetricFieldValuesFromSegment(currentDocId, metricReaders);
        return new StarTreeDocument(dimensions, metricValues);
    }

    /**
     * Returns the metric field values for the star-tree document from the segment based on the current doc id
     */
    private Object[] getStarTreeMetricFieldValuesFromSegment(int currentDocId, List<SequentialDocValuesIterator> metricReaders) {
        Object[] metricValues = new Object[starTreeField.getMetrics().size()];
        for (int i = 0; i < starTreeField.getMetrics().size(); i++) {
            if (starTreeField.getMetrics().get(i).getBaseMetrics().isEmpty()) continue;
            SequentialDocValuesIterator metricReader = metricReaders.get(i);
            if (metricReader != null) {
                try {
                    metricReader.nextEntry(currentDocId);
                    metricValues[i] = metricReader.value(currentDocId);
                } catch (IOException e) {
                    logger.error("unable to iterate to next doc", e);
                    throw new RuntimeException("unable to iterate to next doc", e);
                } catch (Exception e) {
                    logger.error("unable to read the metric values from the segment", e);
                    throw new IllegalStateException("unable to read the metric values from the segment", e);
                }
            } else {
                throw new IllegalStateException("metric reader is empty");
            }
        }
        return metricValues;
    }

    /**
     * Sorts and aggregates the star-tree documents from multiple segments and builds star tree based on the newly
     * aggregated star-tree documents
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return iterator of star tree documents
     */
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs) throws IOException {
        int numDocs = 0;
        int[] docIds;
        this.isMerge = true;
        Map<String, OrdinalMap> ordinalMaps = getOrdinalMaps(starTreeValuesSubs);
        try {
            int seg = 0;
            for (StarTreeValues starTreeValues : starTreeValuesSubs) {
                SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[numDimensions];
                List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
                AtomicInteger numSegmentDocs = new AtomicInteger();
                setReadersAndNumSegmentDocsDuringMerge(dimensionReaders, metricReaders, numSegmentDocs, starTreeValues);
                int currentDocId = 0;
                Map<String, LongValues> longValuesMap = new LinkedHashMap<>();
                for (Map.Entry<String, OrdinalMap> entry : ordinalMaps.entrySet()) {
                    longValuesMap.put(entry.getKey(), entry.getValue().getGlobalOrds(seg));
                }
                while (currentDocId < numSegmentDocs.get()) {
                    StarTreeDocument starTreeDocument = getStarTreeDocument(currentDocId, dimensionReaders, metricReaders, longValuesMap);
                    segmentDocumentFileManager.writeStarTreeDocument(starTreeDocument, true);
                    numDocs++;
                    currentDocId++;
                }
                seg++;
            }
            docIds = new int[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docIds[i] = i;
            }
        } catch (IOException ex) {
            segmentDocumentFileManager.close();
            throw ex;
        }

        if (numDocs == 0) {
            return Collections.emptyIterator();
        }

        return sortAndReduceDocuments(docIds, numDocs, true);
    }

    /**
     * Sorts and reduces the star tree documents based on the dimensions
     */
    private Iterator<StarTreeDocument> sortAndReduceDocuments(int[] sortedDocIds, int numDocs, boolean isMerge) throws IOException {
        try {
            if (sortedDocIds == null || sortedDocIds.length == 0) {
                logger.debug("Sorted doc ids array is null");
                return Collections.emptyIterator();
            }
            try {
                StarTreeDocumentsSorter.sort(sortedDocIds, -1, numDocs, index -> {
                    try {
                        return segmentDocumentFileManager.readDimensions(sortedDocIds[index]);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            } catch (UncheckedIOException ex) {
                // Unwrap UncheckedIOException and throw as IOException
                if (ex.getCause() != null) {
                    throw ex.getCause();
                }
                throw ex;
            }
            final StarTreeDocument currentDocument = segmentDocumentFileManager.readStarTreeDocument(sortedDocIds[0], isMerge);
            // Create an iterator for aggregated documents
            return new Iterator<StarTreeDocument>() {
                StarTreeDocument tempCurrentDocument = currentDocument;
                boolean hasNext = true;
                int docId = 1;

                @Override
                public boolean hasNext() {
                    return hasNext;
                }

                @Override
                public StarTreeDocument next() {
                    StarTreeDocument next = reduceSegmentStarTreeDocuments(null, tempCurrentDocument, isMerge);
                    while (docId < numDocs) {
                        StarTreeDocument doc;
                        try {
                            doc = segmentDocumentFileManager.readStarTreeDocument(sortedDocIds[docId++], isMerge);
                        } catch (IOException e) {
                            throw new RuntimeException("Reducing documents failed ", e);
                        }
                        if (!Arrays.equals(doc.dimensions, next.dimensions)) {
                            tempCurrentDocument = doc;
                            return next;
                        } else {
                            next = reduceSegmentStarTreeDocuments(next, doc, isMerge);
                        }
                    }
                    hasNext = false;
                    try {
                        segmentDocumentFileManager.close();
                    } catch (IOException ex) {
                        logger.error("Closing segment documents file failed", ex);
                    }
                    return next;
                }
            };
        } catch (IOException ex) {
            IOUtils.closeWhileHandlingException(segmentDocumentFileManager);
            throw ex;
        }
    }

    /**
     * Get star tree document for the given docId from the star-tree.documents file
     */
    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        return starTreeDocumentFileManager.readStarTreeDocument(docId, true);
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
        return starTreeDocumentFileManager.getDimensionValue(docId, dimensionId);
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
        // Sort all documents using an int array
        int numDocs = endDocId - startDocId;
        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = startDocId + i;
        }
        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, index -> {
            try {
                return starTreeDocumentFileManager.readDimensions(sortedDocIds[index]);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        // Create an iterator for aggregated documents
        return new Iterator<StarTreeDocument>() {
            boolean hasNext = true;
            StarTreeDocument currentDocument = getStarTreeDocument(sortedDocIds[0]);
            int docId = 1;

            private boolean hasSameDimensions(StarTreeDocument document1, StarTreeDocument document2) {
                for (int i = dimensionId + 1; i < numDimensions; i++) {
                    if (!Objects.equals(document1.dimensions[i], document2.dimensions[i])) {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                StarTreeDocument next = reduceStarTreeDocuments(null, currentDocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (docId < numDocs) {
                    StarTreeDocument document;
                    try {
                        document = getStarTreeDocument(sortedDocIds[docId++]);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                    if (!hasSameDimensions(document, currentDocument)) {
                        currentDocument = document;
                        return next;
                    } else {
                        next = reduceStarTreeDocuments(next, document);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(starTreeDocumentFileManager, segmentDocumentFileManager);
        super.close();
    }
}
