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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.fielddata.AbstractSortedNumericDocValues;
import org.opensearch.index.fielddata.AbstractSortedSetDocValues;
import org.opensearch.index.fielddata.GeoShapeValue;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.function.LongUnaryOperator;

/**
 * Utility class that allows to return views of {@link ValuesSource}s that
 * replace the missing value with a configured value.
 *
 * @opensearch.internal
 */
public enum MissingValues {
    ;

    // TODO: we could specialize the single value case

    public static ValuesSource.Bytes replaceMissing(final ValuesSource.Bytes valuesSource, final BytesRef missing) {
        return new ValuesSource.Bytes() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                SortedBinaryDocValues values = valuesSource.bytesValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public String toString() {
                return "anon ValuesSource.Bytes of [" + super.toString() + "]";
            }
        };
    }

    public static SortedBinaryDocValues replaceMissing(final SortedBinaryDocValues values, final BytesRef missing) {
        return new SortedBinaryDocValues() {

            private int count;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc)) {
                    count = values.docValueCount();
                } else {
                    count = 0;
                }
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public int docValueCount() {
                return count == 0 ? 1 : count;
            }

            @Override
            public BytesRef nextValue() throws IOException {
                if (count > 0) {
                    return values.nextValue();
                } else {
                    return missing;
                }
            }

            @Override
            public String toString() {
                return "anon SortedBinaryDocValues of [" + super.toString() + "]";
            }
        };
    }

    public static ValuesSource.Numeric replaceMissing(final ValuesSource.Numeric valuesSource, final Number missing) {
        final boolean missingIsFloat = missing.doubleValue() % 1 != 0;
        final boolean isFloatingPoint = valuesSource.isFloatingPoint() || missingIsFloat;
        final boolean isBigInteger = valuesSource.isBigInteger() && !isFloatingPoint;
        return new ValuesSource.Numeric() {

            @Override
            public boolean isFloatingPoint() {
                return isFloatingPoint;
            }

            @Override
            public boolean isBigInteger() {
                return isBigInteger;
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }

            @Override
            public SortedNumericDocValues longValues(LeafReaderContext context) throws IOException {
                final SortedNumericDocValues values = valuesSource.longValues(context);
                return replaceMissing(values, missing.longValue());
            }

            @Override
            public SortedNumericDoubleValues doubleValues(LeafReaderContext context) throws IOException {
                final SortedNumericDoubleValues values = valuesSource.doubleValues(context);
                return replaceMissing(values, missing.doubleValue());
            }

            @Override
            public String toString() {
                return "anon ValuesSource.Numeric of [" + super.toString() + "]";
            }
        };
    }

    static SortedNumericDocValues replaceMissing(final SortedNumericDocValues values, final long missing) {
        return new AbstractSortedNumericDocValues() {

            private int count;

            @Override
            public long nextValue() throws IOException {
                if (count > 0) {
                    return values.nextValue();
                } else {
                    return missing;
                }
            }

            @Override
            public int docValueCount() {
                return count == 0 ? 1 : count;
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc)) {
                    count = values.docValueCount();
                } else {
                    count = 0;
                }
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public String toString() {
                return "anon SortedNumericDocValues of [" + super.toString() + "]";
            }

        };
    }

    static SortedNumericDoubleValues replaceMissing(final SortedNumericDoubleValues values, final double missing) {
        return new SortedNumericDoubleValues() {

            private int count;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc)) {
                    count = values.docValueCount();
                } else {
                    count = 0;
                }
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public double nextValue() throws IOException {
                if (count > 0) {
                    return values.nextValue();
                } else {
                    return missing;
                }
            }

            @Override
            public int docValueCount() {
                return count == 0 ? 1 : count;
            }

            @Override
            public String toString() {
                return "anon SortedNumericDoubleValues of [" + super.toString() + "]";
            }

            @Override
            public int advance(int target) throws IOException {
                return values.advance(target);
            }
        };
    }

    public static ValuesSource.Bytes replaceMissing(final ValuesSource.Bytes.WithOrdinals valuesSource, final BytesRef missing) {
        return new ValuesSource.Bytes.WithOrdinals() {
            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                SortedBinaryDocValues values = valuesSource.bytesValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public SortedSetDocValues ordinalsValues(LeafReaderContext context) throws IOException {
                SortedSetDocValues values = valuesSource.ordinalsValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public SortedSetDocValues globalOrdinalsValues(LeafReaderContext context) throws IOException {
                SortedSetDocValues values = valuesSource.globalOrdinalsValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public LongUnaryOperator globalOrdinalsMapping(LeafReaderContext context) throws IOException {
                return getGlobalMapping(
                    valuesSource.ordinalsValues(context),
                    valuesSource.globalOrdinalsValues(context),
                    valuesSource.globalOrdinalsMapping(context),
                    missing
                );
            }

            @Override
            public String toString() {
                return "anon ValuesSource.Bytes.WithOrdinals of [" + super.toString() + "]";
            }

        };
    }

    static SortedSetDocValues replaceMissing(final SortedSetDocValues values, final BytesRef missing) throws IOException {
        final long missingOrd = values.lookupTerm(missing);
        if (missingOrd >= 0) {
            // The value already exists
            return replaceMissingOrd(values, missingOrd);
        } else {
            final long insertedOrd = -1 - missingOrd;
            return insertOrd(values, insertedOrd, missing);
        }
    }

    static SortedSetDocValues replaceMissingOrd(final SortedSetDocValues values, final long missingOrd) {
        return new AbstractSortedSetDocValues() {

            private boolean hasOrds;
            private long nextMissingOrd;

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                return values.lookupOrd(ord);
            }

            @Override
            public long getValueCount() {
                return values.getValueCount();
            }

            @Override
            public long nextOrd() throws IOException {
                if (hasOrds) {
                    return values.nextOrd();
                } else {
                    // we want to return the next missing ord but set this to
                    // NO_MORE_DOCS so on the next call we indicate there are no
                    // more values
                    long ordToReturn = nextMissingOrd;
                    nextMissingOrd = SortedSetDocValues.NO_MORE_DOCS;
                    return ordToReturn;
                }
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                hasOrds = values.advanceExact(doc);
                nextMissingOrd = missingOrd;
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public String toString() {
                return "anon AbstractSortedDocValues of [" + super.toString() + "]";
            }

        };
    }

    static SortedSetDocValues insertOrd(final SortedSetDocValues values, final long insertedOrd, final BytesRef missingValue) {
        return new AbstractSortedSetDocValues() {

            private boolean hasOrds;
            private long nextMissingOrd;

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                if (ord < insertedOrd) {
                    return values.lookupOrd(ord);
                } else if (ord > insertedOrd) {
                    return values.lookupOrd(ord - 1);
                } else {
                    return missingValue;
                }
            }

            @Override
            public long getValueCount() {
                return 1 + values.getValueCount();
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public long nextOrd() throws IOException {
                if (hasOrds) {
                    final long ord = values.nextOrd();
                    if (ord < insertedOrd) {
                        return ord;
                    } else if (ord == SortedSetDocValues.NO_MORE_DOCS /* no more docs */) {
                        return SortedSetDocValues.NO_MORE_DOCS;
                    } else {
                        return ord + 1;
                    }
                } else {
                    // we want to return the next missing ord but set this to
                    // NO_MORE_DOCS so on the next call we indicate there are no
                    // more values
                    long ordToReturn = nextMissingOrd;
                    nextMissingOrd = SortedSetDocValues.NO_MORE_DOCS;
                    return ordToReturn;
                }
            }

            @Override
            public boolean advanceExact(int doc) throws IOException {
                hasOrds = values.advanceExact(doc);
                nextMissingOrd = insertedOrd;
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public String toString() {
                return "anon AbstractSortedDocValues of [" + super.toString() + "]";
            }
        };
    }

    static LongUnaryOperator getGlobalMapping(
        SortedSetDocValues values,
        SortedSetDocValues globalValues,
        LongUnaryOperator segmentToGlobalOrd,
        BytesRef missing
    ) throws IOException {
        final long missingGlobalOrd = globalValues.lookupTerm(missing);
        final long missingSegmentOrd = values.lookupTerm(missing);

        if (missingSegmentOrd >= 0) {
            // the missing value exists in the segment, nothing to do
            return segmentToGlobalOrd;
        } else if (missingGlobalOrd >= 0) {
            // the missing value exists in another segment, but not the current one
            final long insertedSegmentOrd = -1L - missingSegmentOrd;
            final long insertedGlobalOrd = missingGlobalOrd;
            return segmentOrd -> {
                if (insertedSegmentOrd == segmentOrd) {
                    return insertedGlobalOrd;
                } else if (insertedSegmentOrd > segmentOrd) {
                    return segmentToGlobalOrd.applyAsLong(segmentOrd);
                } else {
                    return segmentToGlobalOrd.applyAsLong(segmentOrd - 1);
                }
            };
        } else {
            // the missing value exists neither in this segment nor in another segment
            final long insertedSegmentOrd = -1L - missingSegmentOrd;
            final long insertedGlobalOrd = -1L - missingGlobalOrd;
            return segmentOrd -> {
                if (insertedSegmentOrd == segmentOrd) {
                    return insertedGlobalOrd;
                } else if (insertedSegmentOrd > segmentOrd) {
                    return segmentToGlobalOrd.applyAsLong(segmentOrd);
                } else {
                    return 1 + segmentToGlobalOrd.applyAsLong(segmentOrd - 1);
                }
            };
        }
    }

    public static ValuesSource.GeoPoint replaceMissing(final ValuesSource.GeoPoint valuesSource, final GeoPoint missing) {
        return new ValuesSource.GeoPoint() {

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }

            @Override
            public MultiGeoPointValues geoPointValues(LeafReaderContext context) {
                final MultiGeoPointValues values = valuesSource.geoPointValues(context);
                return replaceMissing(values, missing);
            }

            @Override
            public String toString() {
                return "anon ValuesSource.GeoPoint of [" + super.toString() + "]";
            }
        };
    }

    static MultiGeoPointValues replaceMissing(final MultiGeoPointValues values, final GeoPoint missing) {
        return new MultiGeoPointValues() {

            private int count;

            @Override
            public boolean advanceExact(int doc) throws IOException {
                if (values.advanceExact(doc)) {
                    count = values.docValueCount();
                } else {
                    count = 0;
                }
                // always return true because we want to return a value even if
                // the document does not have a value
                return true;
            }

            @Override
            public int docValueCount() {
                return count == 0 ? 1 : count;
            }

            @Override
            public GeoPoint nextValue() throws IOException {
                if (count > 0) {
                    return values.nextValue();
                } else {
                    return missing;
                }
            }

            @Override
            public String toString() {
                return "anon MultiGeoPointValues of [" + super.toString() + "]";
            }
        };
    }

    /**
     * Replace the missing value provided in the param while iterating over a ValuesSource which doesn't have the
     * value for the field.
     *
     * @param missing Value to be returned if doc doesn't contain the data for the field.
     * @return {@link ValuesSource.GeoShape}
     */
    public static ValuesSource.GeoShape replaceMissing(final ValuesSource.GeoShape valuesSource, final Geometry missing) {
        return new ValuesSource.GeoShape() {
            @Override
            public GeoShapeValue getGeoShapeValues(LeafReaderContext context) {
                final GeoShapeValue currentValueSourceValue = valuesSource.getGeoShapeValues(context);
                return new GeoShapeValue.MissingGeoShapeValue(currentValueSourceValue, missing);
            }

            @Override
            public SortedBinaryDocValues bytesValues(LeafReaderContext context) throws IOException {
                return replaceMissing(valuesSource.bytesValues(context), new BytesRef(missing.toString()));
            }
        };
    }
}
