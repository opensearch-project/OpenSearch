/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.builder;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.utils.CoordinatedDocumentReader;

import java.io.IOException;

/**
 * A factory class to return respective doc values iterator based on the doc volues type.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeDocValuesIteratorAdapter {

    /**
     * Creates an iterator for the given doc values type and field using the doc values producer
     */
    public CoordinatedDocumentReader getDocValuesIterator(DocValuesType type, FieldInfo field, DocValuesProducer producer)
        throws IOException {
        switch (type) {
            case SORTED_NUMERIC:
                return new CoordinatedDocumentReader(producer.getSortedNumeric(field));
            default:
                throw new IllegalArgumentException("Unsupported DocValuesType: " + type);
        }
    }

    /**
     * Returns the next value for the given iterator
     */
    public Long getNextValue(CoordinatedDocumentReader coordinatedDocumentReader, int currentDocId) throws IOException {
        if (coordinatedDocumentReader.getDocIdSetIterator() instanceof SortedNumericDocValues) {
            SortedNumericDocValues sortedNumericDocValues = (SortedNumericDocValues) coordinatedDocumentReader.getDocIdSetIterator();
            if (coordinatedDocumentReader.getDocId() < 0 || coordinatedDocumentReader.getDocId() == DocIdSetIterator.NO_MORE_DOCS) {
                throw new IllegalStateException("invalid doc id to fetch the next value");
            }

            if (coordinatedDocumentReader.getDocValue() == null) {
                coordinatedDocumentReader.setDocValue(sortedNumericDocValues.nextValue());
                return coordinatedDocumentReader.getDocValue();
            }

            if (coordinatedDocumentReader.getDocId() == currentDocId) {
                Long nextValue = coordinatedDocumentReader.getDocValue();
                coordinatedDocumentReader.setDocValue(null);
                return nextValue;
            } else {
                return null;
            }
        } else {
            throw new IllegalStateException("Unsupported Iterator: " + coordinatedDocumentReader.getDocIdSetIterator().toString());
        }
    }

    /**
     * Moves to the next doc in the iterator
     * Returns the doc id for the next document from the given iterator
     */
    public int nextDoc(CoordinatedDocumentReader iterator, int currentDocId) throws IOException {
        if (iterator.getDocValue() != null) {
            return iterator.getDocId();
        }
        iterator.setDocId(iterator.getDocIdSetIterator().nextDoc());
        iterator.setDocValue(this.getNextValue(iterator, currentDocId));
        return iterator.getDocId();
    }

}
