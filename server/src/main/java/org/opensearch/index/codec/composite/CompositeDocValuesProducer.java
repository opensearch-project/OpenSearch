/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesProducer;

import java.io.Closeable;

/**
 * An interface that provides access to document values for a specific field.
 *
 * @opensearch.experimental
 */
public interface CompositeDocValuesProducer extends Closeable {

    /**
     * Returns the DocValuesProducer instance.
     *
     * @return The DocValuesProducer instance.
     */
    DocValuesProducer getDocValuesProducer();
}
