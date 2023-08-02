/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.fielddata.plain;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.time.DateUtils;
import org.opensearch.index.fielddata.FieldData;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.LeafNumericFieldData;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.opensearch.index.mapper.DocValueFetcher;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * FieldData backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 *
 * @opensearch.internal
 */
public class SortedNumericIndexFieldData extends IndexNumericFieldData {

    /**
     * Builder for sorted numeric index field data
     *
     * @opensearch.internal
     */
    public static class Builder implements IndexFieldData.Builder {
        private final String name;
        private final NumericType numericType;

        public Builder(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
        }

        @Override
        public SortedNumericIndexFieldData build(IndexFieldDataCache cache, CircuitBreakerService breakerService) {
            return new SortedNumericIndexFieldData(name, numericType);
        }
    }

    private final NumericType numericType;
    protected final String fieldName;
    protected final ValuesSourceType valuesSourceType;

    public SortedNumericIndexFieldData(String fieldName, NumericType numericType) {
        this.fieldName = fieldName;
        this.numericType = Objects.requireNonNull(numericType);
        this.valuesSourceType = numericType.getValuesSourceType();
    }

    @Override
    public final String getFieldName() {
        return fieldName;
    }

    @Override
    public ValuesSourceType getValuesSourceType() {
        return valuesSourceType;
    }

    @Override
    protected boolean sortRequiresCustomComparator() {
        return numericType == NumericType.HALF_FLOAT || numericType == NumericType.UNSIGNED_LONG;
    }

    @Override
    protected XFieldComparatorSource dateComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        if (numericType == NumericType.DATE_NANOSECONDS) {
            // converts date_nanos values to millisecond resolution
            return new LongValuesComparatorSource(
                this,
                missingValue,
                sortMode,
                nested,
                dvs -> convertNumeric(dvs, DateUtils::toMilliSeconds)
            );
        }
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    protected XFieldComparatorSource dateNanosComparatorSource(Object missingValue, MultiValueMode sortMode, Nested nested) {
        if (numericType == NumericType.DATE) {
            // converts date values to nanosecond resolution
            return new LongValuesComparatorSource(
                this,
                missingValue,
                sortMode,
                nested,
                dvs -> convertNumeric(dvs, DateUtils::toNanoSeconds)
            );
        }
        return new LongValuesComparatorSource(this, missingValue, sortMode, nested);
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    @Override
    public LeafNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public LeafNumericFieldData load(LeafReaderContext context) {
        final LeafReader reader = context.reader();
        final String field = fieldName;

        switch (numericType) {
            case HALF_FLOAT:
                return new SortedNumericHalfFloatFieldData(reader, field);
            case FLOAT:
                return new SortedNumericFloatFieldData(reader, field);
            case DOUBLE:
                return new SortedNumericDoubleFieldData(reader, field);
            case DATE_NANOSECONDS:
                return new NanoSecondFieldData(reader, field, numericType);
            case UNSIGNED_LONG:
                return new SortedNumericUnsignedLongFieldData(reader, field);
            default:
                return new SortedNumericLongFieldData(reader, field, numericType);
        }
    }

    /**
     * A small helper class that can be configured to load nanosecond field data either in nanosecond resolution retaining the original
     * values or in millisecond resolution converting the nanosecond values to milliseconds
     */
    public final class NanoSecondFieldData extends LeafLongFieldData {

        private final LeafReader reader;
        private final String fieldName;

        NanoSecondFieldData(LeafReader reader, String fieldName, NumericType numericType) {
            super(0L, numericType);
            this.reader = reader;
            this.fieldName = fieldName;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            return convertNumeric(getLongValuesAsNanos(), DateUtils::toMilliSeconds);
        }

        public SortedNumericDocValues getLongValuesAsNanos() {
            try {
                return DocValues.getSortedNumeric(reader, fieldName);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public DocValueFetcher.Leaf getLeafValueFetcher(DocValueFormat format) {
            DocValueFormat nanosFormat = DocValueFormat.withNanosecondResolution(format);
            SortedNumericDocValues values = getLongValuesAsNanos();
            return new DocValueFetcher.Leaf() {
                @Override
                public boolean advanceExact(int docId) throws IOException {
                    return values.advanceExact(docId);
                }

                @Override
                public int docValueCount() throws IOException {
                    return values.docValueCount();
                }

                @Override
                public Object nextValue() throws IOException {
                    return nanosFormat.format(values.nextValue());
                }
            };
        }
    }

    /**
     * FieldData implementation for integral types.
     * <p>
     * Order of values within a document is consistent with
     * {@link Long#compareTo(Long)}.
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link DocValues#unwrapSingleton(SortedNumericDocValues)} will return
     * the underlying single-valued NumericDocValues representation.
     *
     * @opensearch.internal
     */
    static final class SortedNumericLongFieldData extends LeafLongFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericLongFieldData(LeafReader reader, String field, NumericType numericType) {
            super(0L, numericType);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            try {
                return DocValues.getSortedNumeric(reader, field);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * FieldData implementation for 16-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 15) & 0x7fff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     *
     * @opensearch.internal
     */
    static final class SortedNumericHalfFloatFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericHalfFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleHalfFloatValues(single));
                } else {
                    return new MultiHalfFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 16-bit float per document.
     *
     * @opensearch.internal
     */
    static final class SingleHalfFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleHalfFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 16-bit floats per document.
     *
     * @opensearch.internal
     */
    static final class MultiHalfFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiHalfFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 32-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 31) & 0x7fffffff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     *
     * @opensearch.internal
     */
    static final class SortedNumericFloatFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleFloatValues(single));
                } else {
                    return new MultiFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 32-bit float per document.
     *
     * @opensearch.internal
     */
    static final class SingleFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 32-bit floats per document.
     *
     * @opensearch.internal
     */
    static final class MultiFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 64-bit double values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Double#compareTo(Double)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 63) & 0x7fffffffffffffffL}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     *
     * @opensearch.internal
     */
    static final class SortedNumericDoubleFieldData extends LeafDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericDoubleFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                return FieldData.sortableLongBitsToDoubles(raw);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 64-bit double per document.
     *
     * @opensearch.internal
     */
    static final class SortedNumericUnsignedLongFieldData implements LeafNumericFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericUnsignedLongFieldData(LeafReader reader, String field) {
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            try {
                return DocValues.getSortedNumeric(reader, field);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                return FieldData.unsignedLongToDoubles(raw);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public final ScriptDocValues<BigInteger> getScriptValues() {
            return new ScriptDocValues.UnsignedLongs(getLongValues());
        }

        @Override
        public final SortedBinaryDocValues getBytesValues() {
            return FieldData.toUnsignedString(getLongValues());
        }

        @Override
        public long ramBytesUsed() {
            return 0L;
        }

        @Override
        public void close() {}
    }
}
