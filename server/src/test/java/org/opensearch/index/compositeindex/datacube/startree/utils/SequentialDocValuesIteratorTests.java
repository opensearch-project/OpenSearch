/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.index.fielddata.AbstractNumericDocValues;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class SequentialDocValuesIteratorTests extends OpenSearchTestCase {

    public void test_sequentialDocValuesIterator() {
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator(new AbstractNumericDocValues() {
            @Override
            public long longValue() throws IOException {
                return 0;
            }

            @Override
            public boolean advanceExact(int i) throws IOException {
                return false;
            }

            @Override
            public int docID() {
                return 0;
            }
        });

        assertTrue(sequentialDocValuesIterator.getDocIdSetIterator() instanceof AbstractNumericDocValues);
        assertEquals(sequentialDocValuesIterator.getDocId(), 0);
    }

    public void test_sequentialDocValuesIterator_default() {
        SequentialDocValuesIterator sequentialDocValuesIterator = new SequentialDocValuesIterator();
        assertTrue(sequentialDocValuesIterator.getDocIdSetIterator() instanceof SortedNumericDocValues);
    }

}
