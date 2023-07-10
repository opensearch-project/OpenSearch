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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSet;
import org.opensearch.common.Nullable;
import org.opensearch.common.Numbers;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.search.comparators.UnsignedLongComparator;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;
import java.math.BigInteger;

/**
 * Comparator source for unsigned long values.
 *
 * @opensearch.internal
 */
public class UnsignedLongValuesComparatorSource extends IndexFieldData.XFieldComparatorSource {

    private final IndexNumericFieldData indexFieldData;

    public UnsignedLongValuesComparatorSource(
        IndexNumericFieldData indexFieldData,
        @Nullable Object missingValue,
        MultiValueMode sortMode,
        Nested nested
    ) {
        super(missingValue, sortMode, nested);
        this.indexFieldData = indexFieldData;
    }

    @Override
    public SortField.Type reducedType() {
        return SortField.Type.LONG;
    }

    private SortedNumericDocValues loadDocValues(LeafReaderContext context) {
        final LeafNumericFieldData data = indexFieldData.load(context);
        SortedNumericDocValues values = data.getLongValues();
        return values;
    }

    private NumericDocValues getNumericDocValues(LeafReaderContext context, BigInteger missingValue) throws IOException {
        final SortedNumericDocValues values = loadDocValues(context);
        if (nested == null) {
            return FieldData.replaceMissing(sortMode.select(values), missingValue);
        }
        final BitSet rootDocs = nested.rootDocs(context);
        final DocIdSetIterator innerDocs = nested.innerDocs(context);
        final int maxChildren = nested.getNestedSort() != null ? nested.getNestedSort().getMaxChildren() : Integer.MAX_VALUE;
        return sortMode.select(values, missingValue.longValue(), rootDocs, innerDocs, context.reader().maxDoc(), maxChildren);
    }

    @Override
    public Object missingObject(Object missingValue, boolean reversed) {
        if (sortMissingFirst(missingValue) || sortMissingLast(missingValue)) {
            final boolean min = sortMissingFirst(missingValue) ^ reversed;
            return min ? Numbers.MIN_UNSIGNED_LONG_VALUE : Numbers.MAX_UNSIGNED_LONG_VALUE;
        } else {
            if (missingValue instanceof Number) {
                return ((Number) missingValue);
            } else {
                return new BigInteger(missingValue.toString());
            }
        }
    }

    @Override
    public FieldComparator<?> newComparator(String fieldname, int numHits, boolean enableSkipping, boolean reversed) {
        assert indexFieldData == null || fieldname.equals(indexFieldData.getFieldName());

        final BigInteger ulMissingValue = (BigInteger) missingObject(missingValue, reversed);
        return new UnsignedLongComparator(numHits, fieldname, null, reversed, enableSkipping && this.enableSkipping) {
            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                return new UnsignedLongLeafComparator(context) {
                    @Override
                    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field) throws IOException {
                        return UnsignedLongValuesComparatorSource.this.getNumericDocValues(context, ulMissingValue);
                    }
                };
            }
        };
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        return new BucketedSort.ForUnsignedLongs(bigArrays, sortOrder, format, bucketSize, extra) {
            private final BigInteger ulMissingValue = (BigInteger) missingObject(missingValue, sortOrder == SortOrder.DESC);

            @Override
            public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                return new Leaf(ctx) {
                    private final NumericDocValues docValues = getNumericDocValues(ctx, ulMissingValue);
                    private long docValue;

                    @Override
                    protected boolean advanceExact(int doc) throws IOException {
                        if (docValues.advanceExact(doc)) {
                            docValue = docValues.longValue();
                            return true;
                        }
                        return false;
                    }

                    @Override
                    protected long docValue() {
                        return docValue;
                    }
                };
            }
        };
    }

}
