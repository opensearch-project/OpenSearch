/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.LeafFieldData;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

import java.io.IOException;

/**
 * Exposes a flat object's subfield as string values, filtering related field values.
 *
 * @opensearch.internal
 */
@InternalApi
public final class FlatObjectIndexFieldData implements IndexFieldData<LeafFieldData> {
    private final String fieldName;
    private final String docValuesField;
    private final BytesRef prefix;

    public FlatObjectIndexFieldData(String fieldName, String docValuesField, String prefix) {
        this.fieldName = fieldName;
        this.docValuesField = docValuesField;
        this.prefix = new BytesRef(prefix);
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.BYTES;
    }

    @Override
    public LeafFieldData load(LeafReaderContext context) {
        return new LeafFieldData() {
            @Override
            public SortedBinaryDocValues getBytesValues() {
                final SortedSetDocValues values;
                try {
                    values = DocValues.getSortedSet(context.reader(), docValuesField);
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot load doc values for [" + fieldName + "]", e);
                }
                return new SortedBinaryDocValues() {
                    private long[] ordinals = new long[0];
                    private final BytesRef scratch = new BytesRef();
                    private int count;
                    private int index;

                    @Override
                    public boolean advanceExact(int doc) throws IOException {
                        count = 0;
                        index = 0;
                        if (values.advanceExact(doc)) {
                            int valueCount = values.docValueCount();
                            for (int i = 0; i < valueCount; i++) {
                                long ordinal = values.nextOrd();
                                BytesRef value = values.lookupOrd(ordinal);
                                if (StringHelper.startsWith(value, prefix)) {
                                    ordinals = ArrayUtil.grow(ordinals, count + 1);
                                    ordinals[count++] = ordinal;
                                }
                            }
                        }
                        return count > 0;
                    }

                    @Override
                    public int docValueCount() {
                        return count;
                    }

                    @Override
                    public BytesRef nextValue() throws IOException {
                        BytesRef value = values.lookupOrd(ordinals[index++]);
                        scratch.bytes = value.bytes;
                        scratch.offset = value.offset + prefix.length;
                        scratch.length = value.length - prefix.length;
                        return scratch;
                    }
                };
            }

            @Override
            public ScriptDocValues<?> getScriptValues() {
                return new ScriptDocValues.Strings(getBytesValues());
            }

            @Override
            public long ramBytesUsed() {
                return 0;
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public LeafFieldData loadDirect(LeafReaderContext context) {
        return load(context);
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        return new SortField(fieldName, new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested), reverse);
    }

    @Override
    public BucketedSort newBucketedSort(
        BigArrays bigArrays,
        Object missingValue,
        MultiValueMode sortMode,
        XFieldComparatorSource.Nested nested,
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra
    ) {
        throw new IllegalArgumentException("only supported on numeric fields");
    }
}
