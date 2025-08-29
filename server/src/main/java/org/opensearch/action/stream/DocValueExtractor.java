/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.stream;


import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;

public final class DocValueExtractor {

    private DocValueExtractor() {
    }


    public static Object getSortFieldValue(LeafReaderContext leaf, int localDoc, MappedFieldType fieldType) throws IOException {
        Object value = getDocValue(leaf, localDoc, fieldType);
        if (value == null) {
            throw new IllegalStateException("DocValues for sorting field [" + fieldType.name() +
                "] returned null for doc: " + localDoc);
        }
        return value;
    }

    public static Object getDocValue(LeafReaderContext leaf, int localDoc, MappedFieldType fieldType)
        throws IOException {
        final String fieldName = fieldType.name();
        NumericDocValues ndv = leaf.reader().getNumericDocValues(fieldName);
        if (ndv != null && ndv.advanceExact(localDoc)) {
            return ndv.longValue();
        }
        SortedDocValues sdv = leaf.reader().getSortedDocValues(fieldName);
        if (sdv != null && sdv.advanceExact(localDoc)) {
            return sdv.lookupOrd(sdv.ordValue()).utf8ToString();
        }
        SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues(fieldName);
        if (sndv != null && sndv.advanceExact(localDoc)) {
            return sndv.nextValue();
        }
        SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(fieldName);
        if (ssdv != null && ssdv.advanceExact(localDoc)) {
            long ord = ssdv.nextOrd();
            if (ord != SortedSetDocValues.NO_MORE_ORDS) { // TODO 'NO_MORE_ORDS' is deprecated
                return ssdv.lookupOrd(ord).utf8ToString();
            }
        }
        return null;
    }
}
