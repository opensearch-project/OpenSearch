/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.IndexSorter;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.Function;

/**
 * Wrapper for {@link SortedSetOrdinalsIndexFieldData} which disables pruning optimization for
 * sorting. Used in {@link WildcardFieldMapper}.
 *
 * @opensearch.internal
 */
public class NonPruningSortedSetOrdinalsIndexFieldData extends SortedSetOrdinalsIndexFieldData {

    /**
     * Builder for non-pruning sorted set ordinals
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction;
        private final ValuesSourceType valuesSourceType;

        public Builder(String name, ValuesSourceType valuesSourceType) {
            this(name, AbstractLeafOrdinalsFieldData.DEFAULT_SCRIPT_FUNCTION, valuesSourceType);
        }

        public Builder(String name, Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction, ValuesSourceType valuesSourceType) {
            this.name = name;
            this.scriptFunction = scriptFunction;
            this.valuesSourceType = valuesSourceType;
        }

        @Override
        public NonPruningSortedSetOrdinalsIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new NonPruningSortedSetOrdinalsIndexFieldData(cache, name, valuesSourceType, breakerService, scriptFunction);
        }
    }

    public NonPruningSortedSetOrdinalsIndexFieldData(
        IndexFieldDataCache cache,
        String fieldName,
        ValuesSourceType valuesSourceType,
        CircuitBreakerService breakerService,
        Function<SortedSetDocValues, ScriptDocValues<?>> scriptFunction
    ) {
        super(cache, fieldName, valuesSourceType, breakerService, scriptFunction);
    }

    @Override
    public SortField sortField(@Nullable Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        XFieldComparatorSource source = new BytesRefFieldComparatorSource(this, missingValue, sortMode, nested);
        source.disableSkipping();
        /*
        Check if we can use a simple {@link SortedSetSortField} compatible with index sorting and
        returns a custom sort field otherwise.
        */
        if (nested != null
            || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
            || (source.sortMissingLast(missingValue) == false && source.sortMissingFirst(missingValue) == false)) {
            return new NonPruningSortField(new SortField(getFieldName(), source, reverse));
        }
        SortField sortField = new NonPruningSortField(
            new SortedSetSortField(
                getFieldName(),
                reverse,
                sortMode == MultiValueMode.MAX ? SortedSetSelector.Type.MAX : SortedSetSelector.Type.MIN
            )
        );
        sortField.setMissingValue(
            source.sortMissingLast(missingValue) ^ reverse ? SortedSetSortField.STRING_LAST : SortedSetSortField.STRING_FIRST
        );
        return sortField;
    }

    /**
     * {@link SortField} implementation which delegates calls to another {@link SortField}.
     *
     */
    public abstract class FilteredSortField extends SortField {
        protected final SortField delegate;

        protected FilteredSortField(SortField sortField) {
            super(sortField.getField(), sortField.getType());
            this.delegate = sortField;
        }

        @Override
        public Object getMissingValue() {
            return delegate.getMissingValue();
        }

        @Override
        public void setMissingValue(Object missingValue) {
            delegate.setMissingValue(missingValue);
        }

        @Override
        public String getField() {
            return delegate.getField();
        }

        @Override
        public Type getType() {
            return delegate.getType();
        }

        @Override
        public boolean getReverse() {
            return delegate.getReverse();
        }

        @Override
        public FieldComparatorSource getComparatorSource() {
            return delegate.getComparatorSource();
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public boolean equals(Object o) {
            return delegate.equals(o);
        }

        @Override
        public int hashCode() {
            return delegate.hashCode();
        }

        @Override
        public void setBytesComparator(Comparator<BytesRef> b) {
            delegate.setBytesComparator(b);
        }

        @Override
        public Comparator<BytesRef> getBytesComparator() {
            return delegate.getBytesComparator();
        }

        @Override
        public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
            return delegate.getComparator(numHits, pruning);
        }

        @Override
        public SortField rewrite(IndexSearcher searcher) throws IOException {
            return delegate.rewrite(searcher);
        }

        @Override
        public boolean needsScores() {
            return delegate.needsScores();
        }

        @Override
        public IndexSorter getIndexSorter() {
            return delegate.getIndexSorter();
        }

        @Deprecated
        @Override
        public void setOptimizeSortWithIndexedData(boolean optimizeSortWithIndexedData) {
            delegate.setOptimizeSortWithIndexedData(optimizeSortWithIndexedData);
        }

        @Deprecated
        @Override
        public boolean getOptimizeSortWithIndexedData() {
            return delegate.getOptimizeSortWithIndexedData();
        }

        @Deprecated
        @Override
        public void setOptimizeSortWithPoints(boolean optimizeSortWithPoints) {
            delegate.setOptimizeSortWithPoints(optimizeSortWithPoints);
        }

        @Deprecated
        @Override
        public boolean getOptimizeSortWithPoints() {
            return delegate.getOptimizeSortWithPoints();
        }
    }

    /**
     * {@link SortField} extension which disables pruning in the comparator.
     *
     * @opensearch.internal
     */
    public final class NonPruningSortField extends FilteredSortField {

        private NonPruningSortField(SortField sortField) {
            super(sortField);
        }

        public static Type readType(DataInput in) throws IOException {
            return SortField.readType(in);
        }

        @Override
        public FieldComparator<?> getComparator(int numHits, Pruning pruning) {
            // explictly disable pruning
            return delegate.getComparator(numHits, Pruning.NONE);
        }

        public SortField getDelegate() {
            return delegate;
        }
    }
}
