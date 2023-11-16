/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.fieldcomparator;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.util.BitSet;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.search.comparators.HalfFloatComparator;
import org.opensearch.search.MultiValueMode;

import java.io.IOException;

/**
 * Comparator source for half_float values.
 *
 * @opensearch.internal
 */
public class HalfFloatValuesComparatorSource extends FloatValuesComparatorSource {
    private final IndexNumericFieldData indexFieldData;

    public HalfFloatValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(indexFieldData, missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final float fMissingValue = (Float) missingObject(missingValue, reversed);
        return new HalfFloatComparator(numHits, fieldname, fMissingValue, reversed, enableSkipping && this.enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new HalfFloatLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return HalfFloatValuesComparatorSource.this.getNumericDocValues(context, fMissingValue).getRawFloatValues();
                    }
                };
            }
        };
    }

    private NumericDoubleValues getNumericDocValues(LeafReaderContext context, float missingValue) throws IOException {
        final SortedNumericDoubleValues values = indexFieldData.load(context).getDoubleValues();
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        } else {
            final BitSet rootDocs = nested.rootDocs(context);
            final DocIdSetIterator innerDocs = nested.innerDocs(context);
            final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
            return sortMode.select(values, missingValue, rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
        }
    }
}
