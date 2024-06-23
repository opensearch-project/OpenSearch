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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * A factory class to return respective doc values iterator based on the doc volues type.
 * @opensearch.experimental
 */
public class StarTreeDocValuesIteratorFactory implements DocValuesIteratorFactory {

    @Override
    public DocIdSetIterator createIterator(DocValuesType type, FieldInfo field, DocValuesProducer producer) throws IOException {
        switch (type) {
            case SORTED_SET:
                return producer.getSortedSet(field);
            case SORTED_NUMERIC:
                return producer.getSortedNumeric(field);
            default:
                throw new IllegalArgumentException("Unsupported DocValuesType: " + type);
        }
    }

    @Override
    public long getNextValue(DocIdSetIterator iterator) throws IOException {
        if (iterator instanceof SortedSetDocValues) {
            return ((SortedSetDocValues) iterator).nextOrd();
        } else if (iterator instanceof SortedNumericDocValues) {
            return ((SortedNumericDocValues) iterator).nextValue();
        } else {
            throw new IllegalArgumentException("Unsupported Iterator: " + iterator.toString());
        }
    }

    @Override
    public int nextDoc(DocIdSetIterator iterator) throws IOException {
        return iterator.nextDoc();
    }

}
