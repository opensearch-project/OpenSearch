/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.LongValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.SequentialDocValuesIterator;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * On heap single tree builder
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OnHeapStarTreeBuilder extends BaseStarTreeBuilder {

    private final List<StarTreeDocument> starTreeDocuments = new ArrayList<>();

    /**
     * Constructor for OnHeapStarTreeBuilder
     *
     * @param metaOut an index output to write star-tree metadata
     * @param dataOut an index output to write star-tree data
     * @param starTreeField     star-tree field
     * @param segmentWriteState segment write state
     * @param mapperService     helps with the numeric type of field
     */
    public OnHeapStarTreeBuilder(
        IndexOutput metaOut,
        IndexOutput dataOut,
        StarTreeField starTreeField,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        super(metaOut, dataOut, starTreeField, segmentWriteState, mapperService);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) {
        starTreeDocuments.add(starTreeDocument);
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) {
        return starTreeDocuments.get(docId);
    }

    @Override
    public List<StarTreeDocument> getStarTreeDocuments() {
        return starTreeDocuments;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) {
        return starTreeDocuments.get(docId).dimensions[dimensionId];
    }

    /**
     * Sorts and aggregates all the documents of the segment based on dimension and metrics configuration
     *
     * @param dimensionReaders List of docValues readers to read dimensions from the segment
     * @param metricReaders List of docValues readers to read metrics from the segment
     * @return Iterator of star-tree documents
     *
     */
    @Override
    public Iterator<StarTreeDocument> sortAndAggregateSegmentDocuments(
        SequentialDocValuesIterator[] dimensionReaders,
        List<SequentialDocValuesIterator> metricReaders
    ) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[totalSegmentDocs];
        for (int currentDocId = 0; currentDocId < totalSegmentDocs; currentDocId++) {
            // TODO : we can save empty iterator for dimensions which are not part of segment
            starTreeDocuments[currentDocId] = getSegmentStarTreeDocument(currentDocId, dimensionReaders, metricReaders);
        }
        return sortAndAggregateStarTreeDocuments(starTreeDocuments, false);
    }

    @Override
    public void build(
        List<StarTreeValues> starTreeValuesSubs,
        AtomicInteger fieldNumberAcrossStarTrees,
        DocValuesConsumer starTreeDocValuesConsumer,
        MergeState mergeState,
        Map<String, SortedSetDocValues> sortedSetDocValuesMap
    ) throws IOException {
        setSortedSetDocValuesMap(sortedSetDocValuesMap);
        build(mergeStarTrees(starTreeValuesSubs, mergeState), fieldNumberAcrossStarTrees, starTreeDocValuesConsumer);
    }

    /**
     * Sorts and aggregates the star-tree documents from multiple segments and builds star tree based on the newly
     * aggregated star-tree documents
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return iterator of star tree documents
     */
    @Override
    Iterator<StarTreeDocument> mergeStarTrees(List<StarTreeValues> starTreeValuesSubs, MergeState mergeState) throws IOException {
        return sortAndAggregateStarTreeDocuments(getSegmentsStarTreeDocuments(starTreeValuesSubs, mergeState), true);
    }

    /**
     * Returns an array of all the starTreeDocuments from all the segments
     * We only take the non-star documents from all the segments.
     *
     * @param starTreeValuesSubs StarTreeValues from multiple segments
     * @return array of star tree documents
     */
    StarTreeDocument[] getSegmentsStarTreeDocuments(List<StarTreeValues> starTreeValuesSubs, MergeState mergeState) throws IOException {
        List<StarTreeDocument> starTreeDocuments = new ArrayList<>();
        Map<String, OrdinalMap> ordinalMaps = getOrdinalMaps(starTreeValuesSubs, mergeState);
        int seg = 0;
        for (StarTreeValues starTreeValues : starTreeValuesSubs) {
            SequentialDocValuesIterator[] dimensionReaders = new SequentialDocValuesIterator[numDimensions];
            List<SequentialDocValuesIterator> metricReaders = new ArrayList<>();
            AtomicInteger numSegmentDocs = new AtomicInteger();
            setReadersAndNumSegmentDocs(dimensionReaders, metricReaders, numSegmentDocs, starTreeValues);
            int currentDocId = 0;
            Map<String, LongValues> longValuesMap = new LinkedHashMap<>();
            for (Map.Entry<String, OrdinalMap> entry : ordinalMaps.entrySet()) {
                longValuesMap.put(entry.getKey(), entry.getValue().getGlobalOrds(seg));
            }
            while (currentDocId < numSegmentDocs.get()) {
                starTreeDocuments.add(getStarTreeDocument(currentDocId, dimensionReaders, metricReaders, longValuesMap));
                currentDocId++;
            }
            seg++;
        }
        StarTreeDocument[] starTreeDocumentsArr = new StarTreeDocument[starTreeDocuments.size()];
        return starTreeDocuments.toArray(starTreeDocumentsArr);
    }

    /**
     * Sort, aggregates and merges the star-tree documents
     *
     * @param starTreeDocuments star-tree documents
     * @return iterator for star-tree documents
     */
    Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {

        // sort all the documents
        sortStarTreeDocumentsFromDimensionId(starTreeDocuments, 0);

        // merge the documents
        return mergeStarTreeDocuments(starTreeDocuments, isMerge);
    }

    /**
     * Merges the star-tree documents
     *
     * @param starTreeDocuments star-tree documents
     * @return iterator to aggregate star-tree documents
     */
    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments, boolean isMerge) {
        return new Iterator<>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            // starting from 1 since we have already fetched the 0th document
            int docId = 1;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                // aggregate as we move on to the next doc
                StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentStarTreeDocument, isMerge);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId];
                    docId++;
                    if (Arrays.equals(starTreeDocument.dimensions, next.dimensions) == false) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceSegmentStarTreeDocuments(next, starTreeDocument, isMerge);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    /**
     * Generates a star-tree for a given star-node
     *
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return iterator for star-tree documents of star-node
     */
    @Override
    public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId) {
        int numDocs = endDocId - startDocId;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
        }

        // sort star tree documents from given dimension id (as previous dimension ids have already been processed)
        sortStarTreeDocumentsFromDimensionId(starTreeDocuments, dimensionId + 1);

        return new Iterator<StarTreeDocument>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            private boolean hasSameDimensions(StarTreeDocument starTreeDocument1, StarTreeDocument starTreeDocument2) {
                for (int i = dimensionId + 1; i < numDimensions; i++) {
                    if (!Objects.equals(starTreeDocument1.dimensions[i], starTreeDocument2.dimensions[i])) {
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
                StarTreeDocument next = reduceStarTreeDocuments(null, currentStarTreeDocument);
                next.dimensions[dimensionId] = STAR_IN_DOC_VALUES_INDEX;
                while (docId < numDocs) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId];
                    docId++;
                    if (!hasSameDimensions(starTreeDocument, currentStarTreeDocument)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceStarTreeDocuments(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    /**
     * Sorts the star-tree documents from the given dimension id
     *
     * @param starTreeDocuments star-tree documents
     * @param dimensionId id of the dimension
     */
    private void sortStarTreeDocumentsFromDimensionId(StarTreeDocument[] starTreeDocuments, int dimensionId) {
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = dimensionId; i < numDimensions; i++) {
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    if (o1.dimensions[i] == null && o2.dimensions[i] == null) {
                        return 0;
                    }
                    if (o1.dimensions[i] == null) {
                        return 1;
                    }
                    if (o2.dimensions[i] == null) {
                        return -1;
                    }
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });
    }
}
