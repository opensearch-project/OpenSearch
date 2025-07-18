/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.approximate;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterDocIdSetIterator;

public class ApproximateConjunctionDISI extends FilterDocIdSetIterator {
    /**
     * Sole constructor.
     *
     * @param in
     */
    public ApproximateConjunctionDISI(DocIdSetIterator in) {
        super(in);
    }
}
