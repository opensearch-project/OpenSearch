/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils.iterator;

import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * Wrapper iterator class for StarTree index to traverse through SortedNumericDocValues
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SortedSetStarTreeValuesIterator extends StarTreeValuesIterator {

    public SortedSetStarTreeValuesIterator(DocIdSetIterator docIdSetIterator) {
        super(docIdSetIterator);
    }

    public long nextOrd() throws IOException {
        return ((SortedSetDocValues) docIdSetIterator).nextOrd();
    }

    public int docValueCount() {
        return ((SortedSetDocValues) docIdSetIterator).docValueCount();
    }

    public BytesRef lookupOrd(long ord) throws IOException {
        return ((SortedSetDocValues) docIdSetIterator).lookupOrd(ord);
    }

    public long getValueCount() {
        return ((SortedSetDocValues) docIdSetIterator).getValueCount();
    }

    public long lookupTerm(BytesRef key) throws IOException {
        return ((SortedSetDocValues) docIdSetIterator).lookupTerm(key);
    }

    public TermsEnum termsEnum() throws IOException {
        return ((SortedSetDocValues) docIdSetIterator).termsEnum();
    }

    public TermsEnum intersect(CompiledAutomaton automaton) throws IOException {
        return ((SortedSetDocValues) docIdSetIterator).intersect(automaton);
    }
}
