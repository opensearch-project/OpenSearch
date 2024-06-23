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
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

/**
 * An interface to support iterators for various doc values types.
 * @opensearch.experimental
 */
public interface DocValuesIteratorFactory {

    /**
     * Creates an iterator for the given doc values type and field using the doc values producer
     */
    DocIdSetIterator createIterator(DocValuesType type, FieldInfo field, DocValuesProducer producer) throws IOException;

    /**
     * Returns the next value for the given iterator
     */
    long getNextValue(DocIdSetIterator iterator) throws IOException;

    /**
     * Returns the doc id  for the next document from the given iterator
     */
    int nextDoc(DocIdSetIterator iterator) throws IOException;
}
