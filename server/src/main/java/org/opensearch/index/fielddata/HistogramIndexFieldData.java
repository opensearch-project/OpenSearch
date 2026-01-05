/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.index.fielddata;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.SortField;
import org.opensearch.common.util.BigArrays;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.aggregations.support.ValuesSourceType;
import org.opensearch.search.sort.BucketedSort;
import org.opensearch.search.sort.SortOrder;

public class HistogramIndexFieldData implements IndexFieldData<HistogramLeafFieldData> {
    private final String fieldName;

    public HistogramIndexFieldData(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return CoreValuesSourceType.HISTOGRAM;
    }

    @Override
    public HistogramLeafFieldData load(LeafReaderContext context) {
        return new HistogramLeafFieldData(context.reader(), fieldName);
    }

    @Override
    public HistogramLeafFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public SortField wideSortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        return IndexFieldData.super.wideSortField(missingValue, sortMode, nested, reverse);
    }

    @Override
    public BucketedSort newBucketedSort(BigArrays bigArrays, Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, SortOrder sortOrder, DocValueFormat format, int bucketSize, BucketedSort.ExtraData extra) {
        return null;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, XFieldComparatorSource.Nested nested, boolean reverse) {
        throw new UnsupportedOperationException("Histogram fields do not support sorting");
    }
}

