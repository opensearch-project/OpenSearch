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

package org.opensearch.search.aggregations.support;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Rounding;
import org.opensearch.common.Rounding.Prepared;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.lucene.ScorerAware;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.index.fielddata.AbstractSortingNumericDocValues;
import org.opensearch.index.fielddata.DocValueBits;
import org.opensearch.index.fielddata.GeoShapeValue;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexGeoPointFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.LeafOrdinalsFieldData;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.index.fielddata.SortingBinaryDocValues;
import org.opensearch.index.fielddata.SortingNumericDoubleValues;
import org.opensearch.index.fielddata.plain.AbstractGeoShapeIndexFieldData;
import org.opensearch.index.mapper.RangeType;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.support.ValuesSource.Bytes.WithScript.BytesValues;
import org.opensearch.search.aggregations.support.values.ScriptBytesValues;
import org.opensearch.search.aggregations.support.values.ScriptDoubleValues;
import org.opensearch.search.aggregations.support.values.ScriptLongValues;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.LongUnaryOperator;

/**
 * Base class for a ValuesSource; the primitive data for an agg
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class ValuesSource {

    /**
     * Get the current {@link BytesValues}.
     */
    public abstract SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException;

    public abstract DocValueBits docsWithValue(LeafReaderContext context) throws IOException;

    /** Whether this values source needs scores. */
    public boolean needsScores() {
        return false;
    }

    /**
     * Build a function prepares rounding values to be called many times.
     * <p>
     * This returns a {@linkplain Function} because auto date histogram will
     * need to call it many times over the course of running the aggregation.
     */
    public abstract Function<Rounding, Rounding.Prepared> roundingPreparer(IndexReader reader) throws IOException;

    /**
     * Check if this values source supports using global ordinals
     */
    public boolean hasGlobalOrdinals() {
        return false;
    }

    /**
     * Range type
     *
     * @opensearch.internal
     */
    public static class Range extends ValuesSource {
        private final RangeType rangeType;
        protected final IndexFieldData<?> indexFieldData;

        public Range(IndexFieldData<?> indexFieldData, RangeType rangeType) {
            this.indexFieldData = indexFieldData;
            this.rangeType = rangeType;
        }

        @Override
        public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
            return indexFieldData.load(context).getBytesValues();
        }

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final SortedBinaryDocValues bytes = bytesValues(context);
            return org.opensearch.index.fielddata.FieldData.docsWithValue(bytes);
        }

        @Override
        public Function<Rounding, Prepared> roundingPreparer(IndexReader reader) throws IOException {
            // TODO lookup the min and max rounding when appropriate
            return Rounding::prepareForUnknown;
        }

        public RangeType rangeType() {
            return rangeType;
        }
    }

    /**
     * Bytes type
     *
     * @opensearch.internal
     */
    public abstract static class Bytes extends ValuesSource {

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final SortedBinaryDocValues bytes = bytesValues(context);
            return org.opensearch.index.fielddata.FieldData.docsWithValue(bytes);
        }

        @Override
        public final Function<Rounding, Rounding.Prepared> roundingPreparer(IndexReader reader) throws IOException {
            throw new AggregationExecutionException("can't round a [BYTES]");
        }

        /**
         * Provides ordinals for bytes
         *
         * @opensearch.internal
         */
        public abstract static class WithOrdinals extends Bytes {

            public static final WithOrdinals EMPTY = new WithOrdinals() {

                @Override
                public SortedSetDocValues ordinalsValues(LeafReaderContext context) {
                    return DocValues.emptySortedSet();
                }

                @Override
                public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
                    return DocValues.emptySortedSet();
                }

                @Override
                public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                    return org.opensearch.index.fielddata.FieldData.emptySortedBinary();
                }

                @Override
                public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException {
                    return LongUnaryOperator.identity();
                }

            };

            @Override
            public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
                final SortedSetDocValues ordinals = ordinalsValues(context);
                return org.opensearch.index.fielddata.FieldData.docsWithValue(ordinals);
            }

            public abstract SortedSetDocValues ordinalsValues(LeafReaderContext context) throws IOException;

            public abstract SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) throws IOException;

            /**
             * Whether this values source is able to provide a mapping between global and segment ordinals,
             * by returning the underlying {@link OrdinalMap}. If this method returns false, then calling
             * {@link #globalOrdinalsMapping} will result in an {@link UnsupportedOperationException}.
             */
            public boolean supportsGlobalOrdinalsMapping() {
                return true;
            }

            @Override
            public boolean hasGlobalOrdinals() {
                return true;
            }

            /** Returns a mapping from segment ordinals to global ordinals. */
            public abstract LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException;

            public long globalMaxOrd(IndexSearcher indexSearcher) throws IOException {
                IndexReader indexReader = indexSearcher.getIndexReader();
                if (indexReader.leaves().isEmpty()) {
                    return 0;
                } else {
                    LeafReaderContext atomicReaderContext = indexReader.leaves().get(0);
                    SortedSetDocValues values = globalOrdinalsValues(atomicReaderContext);
                    return values.getValueCount();
                }
            }

            /**
             * Field data for the bytes values source
             *
             * @opensearch.internal
             */
            public static class FieldData extends WithOrdinals {

                protected final IndexOrdinalsFieldData indexFieldData;

                public FieldData(IndexOrdinalsFieldData indexFieldData) {
                    this.indexFieldData = indexFieldData;
                }

                @Override
                public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                    final LeafOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getBytesValues();
                }

                @Override
                public SortedSetDocValues ordinalsValues(LeafReaderContext context) {
                    final LeafOrdinalsFieldData atomicFieldData = indexFieldData.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }

                @Override
                public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) {
                    final IndexOrdinalsFieldData global = indexFieldData.loadGlobal((DirectoryReader) context.parent.reader());
                    final LeafOrdinalsFieldData atomicFieldData = global.load(context);
                    return atomicFieldData.getOrdinalsValues();
                }

                @Override
                public boolean supportsGlobalOrdinalsMapping() {
                    return indexFieldData.supportsGlobalOrdinalsMapping();
                }

                @Override
                public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException {
                    final IndexOrdinalsFieldData global = indexFieldData.loadGlobal((DirectoryReader) context.parent.reader());
                    final OrdinalMap map = global.getOrdinalMap();
                    if (map == null) {
                        // segments and global ordinals are the same
                        return LongUnaryOperator.identity();
                    }
                    final org.apache.lucene.util.LongValues segmentToGlobalOrd = map.getGlobalOrds(context.ord);
                    return segmentToGlobalOrd::get;
                }
            }
        }

        /**
         * Field data without ordinals
         *
         * @opensearch.internal
         */
        public static class FieldData extends Bytes {

            protected final IndexFieldData<?> indexFieldData;

            public FieldData(IndexFieldData<?> indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

        }

        /**
         * {@link ValuesSource} implementation for stand alone scripts returning a Bytes value
         *
         * @opensearch.internal
         */
        public static class Script extends Bytes {

            private final AggregationScript.LeafFactory script;

            public Script(AggregationScript.LeafFactory script) {
                this.script = script;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.newInstance(context));
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }
        }

        // No need to implement ReaderContextAware here, the delegate already takes care of updating data structures
        /**
         * {@link ValuesSource} subclass for Bytes fields with a Value Script applied
         *
         * @opensearch.internal
         */
        public static class WithScript extends Bytes {

            private final ValuesSource delegate;
            private final AggregationScript.LeafFactory script;

            public WithScript(ValuesSource delegate, AggregationScript.LeafFactory script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new BytesValues(delegate.bytesValues(context), script.newInstance(context));
            }

            /**
             * Bytes values
             *
             * @opensearch.internal
             */
            static class BytesValues extends SortingBinaryDocValues implements ScorerAware {

                private final SortedBinaryDocValues bytesValues;
                private final AggregationScript script;

                BytesValues(SortedBinaryDocValues bytesValues, AggregationScript script) {
                    this.bytesValues = bytesValues;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int doc) throws IOException {
                    if (bytesValues.advanceExact(doc)) {
                        count = bytesValues.docValueCount();
                        grow();
                        script.setDocument(doc);
                        for (int i = 0; i < count; ++i) {
                            final BytesRef value = bytesValues.nextValue();
                            script.setNextAggregationValue(value.utf8ToString());
                            Object run = script.execute();
                            CollectionUtils.ensureNoSelfReferences(run, "ValuesSource.BytesValues script");
                            values[i].copyChars(run.toString());
                        }
                        sort();
                        return true;
                    } else {
                        count = 0;
                        grow();
                        return false;
                    }
                }
            }
        }
    }

    /**
     * Numeric values source type
     *
     * @opensearch.internal
     */
    public abstract static class Numeric extends ValuesSource {

        public static final Numeric EMPTY = new Numeric() {

            @Override
            public boolean isFloatingPoint() {
                return false;
            }

            @Override
            public boolean isBigInteger() {
                return false;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return DocValues.emptySortedNumeric();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return org.opensearch.index.fielddata.FieldData.emptySortedNumericDoubles();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.opensearch.index.fielddata.FieldData.emptySortedBinary();
            }

        };

        /** Whether the underlying data is floating-point or not. */
        public abstract boolean isFloatingPoint();

        /** Whether the underlying data is big integer or not. */
        public abstract boolean isBigInteger();

        /** Get the current {@link SortedNumericDocValues}. */
        public abstract SortedNumericDocValues longValues(LeafReaderContext context) throws IOException;

        /** Get the current {@link SortedNumericDoubleValues}. */
        public abstract SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException;

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            if (isFloatingPoint() || isBigInteger()) {
                final SortedNumericDoubleValues values = doubleValues(context);
                return org.opensearch.index.fielddata.FieldData.docsWithValue(values);
            } else {
                final SortedNumericDocValues values = longValues(context);
                return org.opensearch.index.fielddata.FieldData.docsWithValue(values);
            }
        }

        @Override
        public Function<Rounding, Prepared> roundingPreparer(IndexReader reader) throws IOException {
            return Rounding::prepareForUnknown;
        }

        /**
         * {@link ValuesSource} subclass for Numeric fields with a Value Script applied
         *
         * @opensearch.internal
         */
        public static class WithScript extends Numeric {

            private final Numeric delegate;
            private final AggregationScript.LeafFactory script;

            public WithScript(Numeric delegate, AggregationScript.LeafFactory script) {
                this.delegate = delegate;
                this.script = script;
            }

            @Override
            public boolean isFloatingPoint() {
                return true; // even if the underlying source produces longs, scripts can change them to doubles
            }

            @Override
            public boolean isBigInteger() {
                return false; /* always fall back to floating point */
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new Bytes.WithScript.BytesValues(delegate.bytesValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new LongValues(delegate.longValues(context), script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new DoubleValues(delegate.doubleValues(context), script.newInstance(context));
            }

            /**
             * Long values source type
             *
             * @opensearch.internal
             */
            static class LongValues extends AbstractSortingNumericDocValues implements ScorerAware {

                private final SortedNumericDocValues longValues;
                private final AggregationScript script;

                LongValues(SortedNumericDocValues values, AggregationScript script) {
                    this.longValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (longValues.advanceExact(target)) {
                        resize(longValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(longValues.nextValue());
                            values[i] = script.runAsLong();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }

            /**
             * Double values source type
             *
             * @opensearch.internal
             */
            static class DoubleValues extends SortingNumericDoubleValues implements ScorerAware {

                private final SortedNumericDoubleValues doubleValues;
                private final AggregationScript script;

                DoubleValues(SortedNumericDoubleValues values, AggregationScript script) {
                    this.doubleValues = values;
                    this.script = script;
                }

                @Override
                public void setScorer(Scorable scorer) {
                    script.setScorer(scorer);
                }

                @Override
                public boolean advanceExact(int target) throws IOException {
                    if (doubleValues.advanceExact(target)) {
                        resize(doubleValues.docValueCount());
                        script.setDocument(target);
                        for (int i = 0; i < docValueCount(); ++i) {
                            script.setNextAggregationValue(doubleValues.nextValue());
                            values[i] = script.runAsDouble();
                        }
                        sort();
                        return true;
                    }
                    return false;
                }
            }
        }

        /**
         * Field data for numerics
         *
         * @opensearch.internal
         */
        public static class FieldData extends Numeric {

            protected final IndexNumericFieldData indexFieldData;

            public FieldData(IndexNumericFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public boolean isFloatingPoint() {
                return indexFieldData.getNumericType().isFloatingPoint();
            }

            @Override
            public boolean isBigInteger() {
                return indexFieldData.getNumericType() == NumericType.UNSIGNED_LONG;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) {
                return indexFieldData.load(context).getLongValues();
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) {
                return indexFieldData.load(context).getDoubleValues();
            }
        }

        /**
         * {@link ValuesSource} implementation for stand alone scripts returning a Numeric value
         *
         * @opensearch.internal
         */
        public static class Script extends Numeric {
            private final AggregationScript.LeafFactory script;
            private final ValueType scriptValueType;

            public Script(AggregationScript.LeafFactory script, ValueType scriptValueType) {
                this.script = script;
                this.scriptValueType = scriptValueType;
            }

            @Override
            public boolean isFloatingPoint() {
                return scriptValueType != null ? scriptValueType == ValueType.DOUBLE : true;
            }

            @Override
            public boolean isBigInteger() {
                return scriptValueType != null ? scriptValueType == ValueType.UNSIGNED_LONG : false;
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                return new ScriptLongValues(script.newInstance(context));
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                return new ScriptDoubleValues(script.newInstance(context));
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return new ScriptBytesValues(script.newInstance(context));
            }

            @Override
            public boolean needsScores() {
                return script.needs_score();
            }
        }

    }

    /**
     * Geo point values source
     *
     * @opensearch.internal
     */
    public abstract static class GeoPoint extends ValuesSource {

        public static final GeoPoint EMPTY = new GeoPoint() {

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return org.opensearch.index.fielddata.FieldData.emptyMultiGeoPoints();
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.opensearch.index.fielddata.FieldData.emptySortedBinary();
            }

        };

        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) throws IOException {
            final MultiGeoPointValues geoPoints = geoPointValues(context);
            return org.opensearch.index.fielddata.FieldData.docsWithValue(geoPoints);
        }

        @Override
        public final Function<Rounding, Rounding.Prepared> roundingPreparer(IndexReader reader) throws IOException {
            throw new AggregationExecutionException("can't round a [GEO_POINT]");
        }

        public abstract MultiGeoPointValues geoPointValues(LeafReaderContext context);

        /**
         * Field data for geo point values source
         *
         * @opensearch.internal
         */
        public static class Fielddata extends GeoPoint {

            protected final IndexGeoPointFieldData indexFieldData;

            public Fielddata(IndexGeoPointFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) {
                return indexFieldData.load(context).getBytesValues();
            }

            public org.opensearch.index.fielddata.MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                return indexFieldData.load(context).getGeoPointValues();
            }
        }
    }

    /**
     * The primitive data type for doing an aggregation on the GeoShape
     */
    public abstract static class GeoShape extends ValuesSource {
        public static final GeoShape EMPTY = new GeoShape() {
            /**
             * This provides the {@link GeoShapeValue} after reading from LeafReaderContext
             *
             * @param context {@link LeafReaderContext}
             * @return {@link GeoShapeValue}
             */
            @Override
            public GeoShapeValue getGeoShapeValues(LeafReaderContext context) {
                return org.opensearch.index.fielddata.FieldData.emptyGeoShape();
            }

            /**
             * Get the current {@link BytesValues}.
             */
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return org.opensearch.index.fielddata.FieldData.emptySortedBinary();
            }
        };

        /**
         * This is getting used in the {@link org.opensearch.search.aggregations.bucket.missing.MissingAggregator}
         * @param context {@link LeafReaderContext}
         * @return DocValueBits
         */
        @Override
        public DocValueBits docsWithValue(LeafReaderContext context) {
            final GeoShapeValue geoShapeValue = getGeoShapeValues(context);
            return org.opensearch.index.fielddata.FieldData.docsWithValue(geoShapeValue);
        }

        @Override
        public Function<Rounding, Prepared> roundingPreparer(IndexReader reader) {
            throw new AggregationExecutionException("can't round a [GEO_SHAPE]");
        }

        /**
         * This provides the {@link GeoShapeValue} after reading from LeafReaderContext
         * @param context {@link LeafReaderContext}
         * @return {@link GeoShapeValue}
         */
        public abstract GeoShapeValue getGeoShapeValues(LeafReaderContext context);

        /**
         * Field data for geo shape values source
         *
         * @opensearch.internal
         */
        public static class FieldData extends GeoShape {

            protected final AbstractGeoShapeIndexFieldData indexFieldData;

            public FieldData(AbstractGeoShapeIndexFieldData indexFieldData) {
                this.indexFieldData = indexFieldData;
            }

            /**
             * Get the current {@link BytesValues}.
             *
             * @param context {@link LeafReaderContext}
             * @return SortedBinaryDocValues
             */
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return indexFieldData.load(context).getBytesValues();
            }

            /**
             * This provides the {@link GeoShapeValue} after reading from LeafReaderContext
             *
             * @param context {@link LeafReaderContext}
             * @return {@link GeoShapeValue}
             */
            @Override
            public GeoShapeValue getGeoShapeValues(LeafReaderContext context) {
                return indexFieldData.load(context).getGeoShapeValue();
            }
        }
    }
}
