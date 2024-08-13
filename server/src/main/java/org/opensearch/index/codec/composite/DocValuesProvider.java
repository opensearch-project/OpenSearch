/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

/**
 * An interface that provides access to document values for a specific field.
 *
 * @opensearch.experimental
 */
public interface DocValuesProvider {

    /**
     * Returns the sorted numeric document values for the specified field.
     *
     * @param fieldName The name of the field for which to retrieve the sorted numeric document values.
     * @return The sorted numeric document values for the specified field.
     * @throws IOException If an error occurs while retrieving the sorted numeric document values.
     */
    SortedNumericDocValues getSortedNumeric(String fieldName) throws IOException;

    /**
     * Returns the DocValuesProducer instance.
     *
     * @return The DocValuesProducer instance.
     */
    DocValuesProducer getDocValuesProducer();
}
