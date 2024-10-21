/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

import org.apache.lucene.search.DocIdSetIterator;

/**
 * Base wrapper class for DocValuesWriter.
 */
public abstract class DocValuesWriterWrapper<T extends DocIdSetIterator> {
    public abstract T getDocValues();
}
