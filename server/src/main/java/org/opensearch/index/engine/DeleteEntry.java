/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;

public class DeleteEntry {
    private final Term term;

    public DeleteEntry(Term term) {
        this.term = term;
    }

    public Term getTerm() {
        return term;
    }
}
