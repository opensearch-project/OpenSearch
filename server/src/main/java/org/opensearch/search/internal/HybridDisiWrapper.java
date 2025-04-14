/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.Scorer;

/**
 * Wrapper for DisiWrapper, saves state of sub-queries for performance reasons
 */
public class HybridDisiWrapper extends DisiWrapper {

    // index of disi wrapper sub-query object when its part of the hybrid query
    private final int subQueryIndex;

    public HybridDisiWrapper(Scorer scorer, int subQueryIndex) {
        super(scorer, false);
        this.subQueryIndex = subQueryIndex;
    }

    public int getSubQueryIndex() {
        return subQueryIndex;
    }
}
