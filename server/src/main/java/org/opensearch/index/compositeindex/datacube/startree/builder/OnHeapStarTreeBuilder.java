/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BaseStarTreeBuilder;
import org.apache.lucene.index.SegmentWriteState;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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
     * @param starTreeField     star-tree field
     * @param fieldProducerMap  helps with document values producer for a particular field
     * @param segmentWriteState segment write state
     * @param mapperService     helps with the numeric type of field
     * @throws IOException throws an exception we are unable to construct an onheap star-tree
     */
    public OnHeapStarTreeBuilder(
        StarTreeField starTreeField,
        Map<String, DocValuesProducer> fieldProducerMap,
        SegmentWriteState segmentWriteState,
        MapperService mapperService
    ) throws IOException {
        super(starTreeField, fieldProducerMap, segmentWriteState, mapperService);
    }

    @Override
    public void appendStarTreeDocument(StarTreeDocument starTreeDocument) throws IOException {
        starTreeDocuments.add(starTreeDocument);
    }

    @Override
    public StarTreeDocument getStarTreeDocument(int docId) throws IOException {
        return starTreeDocuments.get(docId);
    }

    @Override
    public List<StarTreeDocument> getStarTreeDocuments() {
        return starTreeDocuments;
    }

    @Override
    public Long getDimensionValue(int docId, int dimensionId) throws IOException {
        return starTreeDocuments.get(docId).dimensions[dimensionId];
    }

    @Override
    public Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(int numDocs) throws IOException {
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int currentDocId = 0; currentDocId < numDocs; currentDocId++) {
            starTreeDocuments[currentDocId] = getSegmentStarTreeDocument(currentDocId);
        }
        return sortAndAggregateStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Sort, aggregates and merges the star-tree documents
     * @param starTreeDocuments star-tree documents
     * @return iterator for star-tree documents
     * @throws IOException throws when unable to sort, merge and aggregate star-tree documents
     */
    public Iterator<StarTreeDocument> sortAndAggregateStarTreeDocuments(StarTreeDocument[] starTreeDocuments) throws IOException {

        // sort the documents
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = 0; i < numDimensions; i++) {
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });

        // merge the documents
        return mergeStarTreeDocuments(starTreeDocuments);
    }

    /**
     * Merges the star-tree documents
     * @param starTreeDocuments star-tree documents
     * @return iterator to aggregate star-tree documents
     */
    private Iterator<StarTreeDocument> mergeStarTreeDocuments(StarTreeDocument[] starTreeDocuments) {
        return new Iterator<>() {
            boolean hasNext = true;
            StarTreeDocument currentStarTreeDocument = starTreeDocuments[0];
            int docId = 1;

            @Override
            public boolean hasNext() {
                return hasNext;
            }

            @Override
            public StarTreeDocument next() {
                // aggregate as we move on to the next doc
                StarTreeDocument next = reduceSegmentStarTreeDocuments(null, currentStarTreeDocument);
                while (docId < starTreeDocuments.length) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
                    if (!Arrays.equals(starTreeDocument.dimensions, next.dimensions)) {
                        currentStarTreeDocument = starTreeDocument;
                        return next;
                    } else {
                        next = reduceSegmentStarTreeDocuments(next, starTreeDocument);
                    }
                }
                hasNext = false;
                return next;
            }
        };
    }

    /**
     * Generates a star-tree for a given star-node
     * @param startDocId  Start document id in the star-tree
     * @param endDocId    End document id (exclusive) in the star-tree
     * @param dimensionId Dimension id of the star-node
     * @return iterator for star-tree documents of star-node
     * @throws IOException throws when unable to generate star-tree for star-node
     */
    @Override
    public Iterator<StarTreeDocument> generateStarTreeDocumentsForStarNode(int startDocId, int endDocId, int dimensionId)
        throws IOException {
        int numDocs = endDocId - startDocId;
        StarTreeDocument[] starTreeDocuments = new StarTreeDocument[numDocs];
        for (int i = 0; i < numDocs; i++) {
            starTreeDocuments[i] = getStarTreeDocument(startDocId + i);
        }
        Arrays.sort(starTreeDocuments, (o1, o2) -> {
            for (int i = dimensionId + 1; i < numDimensions; i++) {
                if (!Objects.equals(o1.dimensions[i], o2.dimensions[i])) {
                    return Long.compare(o1.dimensions[i], o2.dimensions[i]);
                }
            }
            return 0;
        });
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
                next.dimensions[dimensionId] = Long.valueOf(STAR_IN_DOC_VALUES_INDEX);
                while (docId < numDocs) {
                    StarTreeDocument starTreeDocument = starTreeDocuments[docId++];
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
}
