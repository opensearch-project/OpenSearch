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

package org.opensearch.index.mapper;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.exc.InputCoercionException;

import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.sandbox.document.BigIntegerPoint;
import org.apache.lucene.sandbox.document.HalfFloatPoint;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.Explicit;
import org.opensearch.common.Numbers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.document.SortedUnsignedLongDocValuesRangeQuery;
import org.opensearch.index.document.SortedUnsignedLongDocValuesSetQuery;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.opensearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} for numeric types: byte, short, int, long, float and double.
 *
 * @opensearch.internal
 */
public class NumberFieldMapper extends ParametrizedFieldMapper {

    public static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", true, Property.IndexScope);

    private static NumberFieldMapper toType(FieldMapper in) {
        return (NumberFieldMapper) in;
    }

    /**
     * Builder for the number field mappers
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Explicit<Boolean>> ignoreMalformed;
        private final Parameter<Explicit<Boolean>> coerce;

        private final Parameter<Number> nullValue;

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final NumberType type;

        public Builder(String name, NumberType type, Settings settings) {
            this(name, type, IGNORE_MALFORMED_SETTING.get(settings), COERCE_SETTING.get(settings));
        }

        public static Builder docValuesOnly(String name, NumberType type) {
            Builder builder = new Builder(name, type, false, false);
            builder.indexed.setValue(false);
            return builder;
        }

        public Builder(String name, NumberType type, boolean ignoreMalformedByDefault, boolean coerceByDefault) {
            super(name);
            this.type = type;
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            this.nullValue = new Parameter<>(
                "null_value",
                false,
                () -> null,
                (n, c, o) -> o == null ? null : type.parse(o, false),
                m -> toType(m).nullValue
            ).acceptsNull();
        }

        Builder nullValue(Number number) {
            this.nullValue.setValue(number);
            return this;
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(indexed, hasDocValues, stored, ignoreMalformed, coerce, nullValue, meta);
        }

        @Override
        public NumberFieldMapper build(BuilderContext context) {
            MappedFieldType ft = new NumberFieldType(buildFullName(context), this);
            return new NumberFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    /**
     * Type of number
     *
     * @opensearch.internal
     */
    public enum NumberType implements NumericPointEncoder {
        HALF_FLOAT("half_float", NumericType.HALF_FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                validateParsed(result);
                return result;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return HalfFloatPoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[HalfFloatPoint.BYTES];
                HalfFloatPoint.encodeDimension(value.floatValue(), point, 0);
                return point;
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                float v = parse(value, false);
                if (isSearchable && hasDocValues) {
                    Query query = HalfFloatPoint.newExactQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery(field, HalfFloatPoint.halfFloatToSortableShort(v));
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, HalfFloatPoint.halfFloatToSortableShort(v));
                }
                return HalfFloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                float[] v = new float[values.size()];
                long points[] = new long[v.length];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                    if (hasDocValues) {
                        points[i] = HalfFloatPoint.halfFloatToSortableShort(v[i]);
                    }
                }
                if (isSearchable && hasDocValues) {
                    Query query = HalfFloatPoint.newSetQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowSetQuery(field, points);
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, points);
                }
                return HalfFloatPoint.newSetQuery(field, v);

            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower) {
                        l = HalfFloatPoint.nextDown(l);
                    }
                    l = HalfFloatPoint.nextUp(l);
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper) {
                        u = HalfFloatPoint.nextUp(u);
                    }
                    u = HalfFloatPoint.nextDown(u);
                }
                if (isSearchable && hasDocValues) {
                    Query query = HalfFloatPoint.newRangeQuery(field, l, u);
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(
                        field,
                        HalfFloatPoint.halfFloatToSortableShort(l),
                        HalfFloatPoint.halfFloatToSortableShort(u)
                    );
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowRangeQuery(
                        field,
                        HalfFloatPoint.halfFloatToSortableShort(l),
                        HalfFloatPoint.halfFloatToSortableShort(u)
                    );
                }
                return HalfFloatPoint.newRangeQuery(field, l, u);
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new HalfFloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, HalfFloatPoint.halfFloatToSortableShort(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return Float.parseFloat(value);
            }

            private void validateParsed(float value) {
                if (!Float.isFinite(HalfFloatPoint.sortableShortToHalfFloat(HalfFloatPoint.halfFloatToSortableShort(value)))) {
                    throw new IllegalArgumentException("[half_float] supports only finite values, but got [" + value + "]");
                }
            }
        },
        FLOAT("float", NumericType.FLOAT) {
            @Override
            public Float parse(Object value, boolean coerce) {
                final float result;

                if (value instanceof Number) {
                    result = ((Number) value).floatValue();
                } else {
                    if (value instanceof BytesRef) {
                        value = ((BytesRef) value).utf8ToString();
                    }
                    result = Float.parseFloat(value.toString());
                }
                validateParsed(result);
                return result;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return FloatPoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Float.BYTES];
                FloatPoint.encodeDimension(value.floatValue(), point, 0);
                return point;
            }

            @Override
            public Float parse(XContentParser parser, boolean coerce) throws IOException {
                float parsed = parser.floatValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                float v = parse(value, false);
                if (isSearchable && hasDocValues) {
                    Query query = FloatPoint.newExactQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery(field, NumericUtils.floatToSortableInt(v));
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, NumericUtils.floatToSortableInt(v));
                }
                return FloatPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                float[] v = new float[values.size()];
                long points[] = new long[v.length];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                    if (hasDocValues) {
                        points[i] = NumericUtils.floatToSortableInt(v[i]);
                    }
                }
                if (isSearchable && hasDocValues) {
                    return new IndexOrDocValuesQuery(
                        FloatPoint.newSetQuery(field, v),
                        SortedNumericDocValuesField.newSlowSetQuery(field, points)
                    );
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, points);
                }
                return FloatPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                float l = Float.NEGATIVE_INFINITY;
                float u = Float.POSITIVE_INFINITY;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, false);
                    if (includeLower == false) {
                        l = FloatPoint.nextUp(l);
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, false);
                    if (includeUpper == false) {
                        u = FloatPoint.nextDown(u);
                    }
                }
                if (isSearchable && hasDocValues) {
                    Query query = FloatPoint.newRangeQuery(field, l, u);
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(
                        field,
                        NumericUtils.floatToSortableInt(l),
                        NumericUtils.floatToSortableInt(u)
                    );
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowRangeQuery(
                        field,
                        NumericUtils.floatToSortableInt(l),
                        NumericUtils.floatToSortableInt(u)
                    );
                }
                return FloatPoint.newRangeQuery(field, l, u);
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new FloatPoint(name, value.floatValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, NumericUtils.floatToSortableInt(value.floatValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.floatValue()));
                }
                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return Float.parseFloat(value);
            }

            private void validateParsed(float value) {
                if (!Float.isFinite(value)) {
                    throw new IllegalArgumentException("[float] supports only finite values, but got [" + value + "]");
                }
            }
        },
        DOUBLE("double", NumericType.DOUBLE) {
            @Override
            public Double parse(Object value, boolean coerce) {
                double parsed = objectToDouble(value);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return DoublePoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Double.BYTES];
                DoublePoint.encodeDimension(value.doubleValue(), point, 0);
                return point;
            }

            @Override
            public Double parse(XContentParser parser, boolean coerce) throws IOException {
                double parsed = parser.doubleValue(coerce);
                validateParsed(parsed);
                return parsed;
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                double v = parse(value, false);
                if (isSearchable && hasDocValues) {
                    Query query = DoublePoint.newExactQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery(field, NumericUtils.doubleToSortableLong(v));
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, NumericUtils.doubleToSortableLong(v));
                }
                return DoublePoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                double[] v = new double[values.size()];
                long points[] = new long[v.length];
                for (int i = 0; i < values.size(); ++i) {
                    v[i] = parse(values.get(i), false);
                    if (hasDocValues) {
                        points[i] = NumericUtils.doubleToSortableLong(v[i]);
                    }
                }
                if (isSearchable && hasDocValues) {
                    return new IndexOrDocValuesQuery(
                        DoublePoint.newSetQuery(field, v),
                        SortedNumericDocValuesField.newSlowSetQuery(field, points)
                    );
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, points);
                }
                return DoublePoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                return doubleRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    if (isSearchable && hasDocValues) {
                        Query query = DoublePoint.newRangeQuery(field, l, u);
                        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(
                            field,
                            NumericUtils.doubleToSortableLong(l),
                            NumericUtils.doubleToSortableLong(u)
                        );
                        return new IndexOrDocValuesQuery(query, dvQuery);
                    }
                    if (hasDocValues) {
                        return SortedNumericDocValuesField.newSlowRangeQuery(
                            field,
                            NumericUtils.doubleToSortableLong(l),
                            NumericUtils.doubleToSortableLong(u)
                        );
                    }
                    return DoublePoint.newRangeQuery(field, l, u);
                });
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new DoublePoint(name, value.doubleValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, NumericUtils.doubleToSortableLong(value.doubleValue())));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.doubleValue()));
                }
                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return Double.parseDouble(value);
            }

            private void validateParsed(double value) {
                if (!Double.isFinite(value)) {
                    throw new IllegalArgumentException("[double] supports only finite values, but got [" + value + "]");
                }
            }
        },
        BYTE("byte", NumericType.BYTE) {
            @Override
            public Byte parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Byte.MIN_VALUE || doubleValue > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }

                return (byte) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).byteValue();
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Integer.BYTES];
                IntPoint.encodeDimension(value.intValue(), point, 0);
                return point;
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                int value = parser.intValue(coerce);
                if (value < Byte.MIN_VALUE || value > Byte.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a byte");
                }
                return (short) value;
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                return INTEGER.termQuery(field, value, hasDocValues, isSearchable);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                return INTEGER.termsQuery(field, values, hasDocValues, isSearchable);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues, isSearchable, context);
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.byteValue();
            }

            @Override
            Number valueForSearch(String value) {
                return Byte.parseByte(value);
            }
        },
        SHORT("short", NumericType.SHORT) {
            @Override
            public Short parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Short.MIN_VALUE || doubleValue > Short.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for a short");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }

                return (short) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return INTEGER.parsePoint(value).shortValue();
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Integer.BYTES];
                IntPoint.encodeDimension(value.intValue(), point, 0);
                return point;
            }

            @Override
            public Short parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.shortValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                return INTEGER.termQuery(field, value, hasDocValues, isSearchable);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                return INTEGER.termsQuery(field, values, hasDocValues, isSearchable);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                return INTEGER.rangeQuery(field, lowerTerm, upperTerm, includeLower, includeUpper, hasDocValues, isSearchable, context);
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                return INTEGER.createFields(name, value, indexed, docValued, stored);
            }

            @Override
            Number valueForSearch(Number value) {
                return value.shortValue();
            }

            @Override
            Number valueForSearch(String value) {
                return Short.parseShort(value);
            }
        },
        INTEGER("integer", NumericType.INT) {
            @Override
            public Integer parse(Object value, boolean coerce) {
                double doubleValue = objectToDouble(value);

                if (doubleValue < Integer.MIN_VALUE || doubleValue > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException("Value [" + value + "] is out of range for an integer");
                }
                if (!coerce && doubleValue % 1 != 0) {
                    throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
                }

                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }

                return (int) doubleValue;
            }

            @Override
            public Number parsePoint(byte[] value) {
                return IntPoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Integer.BYTES];
                IntPoint.encodeDimension(value.intValue(), point, 0);
                return point;
            }

            @Override
            public Integer parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.intValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                int v = parse(value, true);
                if (isSearchable && hasDocValues) {
                    Query query = IntPoint.newExactQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery(field, v);
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, v);
                }
                return IntPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                int[] v = new int[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                long points[] = new long[v.length];
                if (hasDocValues) {
                    for (int i = 0; i < v.length; i++) {
                        points[i] = v[i];
                    }
                }
                if (isSearchable && hasDocValues) {
                    return new IndexOrDocValuesQuery(
                        IntPoint.newSetQuery(field, v),
                        SortedNumericDocValuesField.newSlowSetQuery(field, points)
                    );
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, points);
                }
                return IntPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                int l = Integer.MIN_VALUE;
                int u = Integer.MAX_VALUE;
                if (lowerTerm != null) {
                    l = parse(lowerTerm, true);
                    // if the lower bound is decimal:
                    // - if the bound is positive then we increment it:
                    // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                    // - if the bound is negative then we leave it as is:
                    // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                    boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                    if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                        if (l == Integer.MAX_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        ++l;
                    }
                }
                if (upperTerm != null) {
                    u = parse(upperTerm, true);
                    boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                    if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                        if (u == Integer.MIN_VALUE) {
                            return new MatchNoDocsQuery();
                        }
                        --u;
                    }
                }
                if (isSearchable && hasDocValues) {
                    Query query = IntPoint.newRangeQuery(field, l, u);
                    Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                    query = new IndexOrDocValuesQuery(query, dvQuery);
                    if (context.indexSortedOnField(field)) {
                        query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                    }
                    return query;
                }
                if (hasDocValues) {
                    Query query = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                    if (context.indexSortedOnField(field)) {
                        query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                    }
                    return query;
                }
                return IntPoint.newRangeQuery(field, l, u);
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new IntPoint(name, value.intValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.intValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.intValue()));
                }
                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return Integer.parseInt(value);
            }
        },
        LONG("long", NumericType.LONG) {
            @Override
            public Long parse(Object value, boolean coerce) {
                return objectToLong(value, coerce);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return LongPoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[Long.BYTES];
                LongPoint.encodeDimension(value.longValue(), point, 0);
                return point;
            }

            @Override
            public Long parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.longValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                long v = parse(value, true);
                if (isSearchable && hasDocValues) {
                    Query query = LongPoint.newExactQuery(field, v);
                    Query dvQuery = SortedNumericDocValuesField.newSlowExactQuery(field, v);
                    return new IndexOrDocValuesQuery(query, dvQuery);

                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowExactQuery(field, v);

                }
                return LongPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable) {
                long[] v = new long[values.size()];

                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }
                if (isSearchable && hasDocValues) {
                    return new IndexOrDocValuesQuery(
                        LongPoint.newSetQuery(field, v),
                        SortedNumericDocValuesField.newSlowSetQuery(field, v)
                    );
                }
                if (hasDocValues) {
                    return SortedNumericDocValuesField.newSlowSetQuery(field, v);

                }
                return LongPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                return longRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    if (isSearchable && hasDocValues) {
                        Query query = LongPoint.newRangeQuery(field, l, u);
                        Query dvQuery = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                        query = new IndexOrDocValuesQuery(query, dvQuery);
                        if (context.indexSortedOnField(field)) {
                            query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                        }
                        return query;
                    }
                    if (hasDocValues) {
                        Query query = SortedNumericDocValuesField.newSlowRangeQuery(field, l, u);
                        if (context.indexSortedOnField(field)) {
                            query = new IndexSortSortedNumericDocValuesRangeQuery(field, l, u, query);
                        }
                        return query;
                    }
                    return LongPoint.newRangeQuery(field, l, u);

                });
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                if (indexed) {
                    fields.add(new LongPoint(name, value.longValue()));
                }
                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, value.longValue()));
                }
                if (stored) {
                    fields.add(new StoredField(name, value.longValue()));
                }
                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return Long.parseLong(value);
            }
        },
        UNSIGNED_LONG("unsigned_long", NumericType.UNSIGNED_LONG) {
            @Override
            public BigInteger parse(Object value, boolean coerce) {
                return objectToUnsignedLong(value, coerce);
            }

            @Override
            public Number parsePoint(byte[] value) {
                return BigIntegerPoint.decodeDimension(value, 0);
            }

            @Override
            public byte[] encodePoint(Number value) {
                byte[] point = new byte[BigIntegerPoint.BYTES];
                BigIntegerPoint.encodeDimension(objectToUnsignedLong(value, false, true), point, 0);
                return point;
            }

            @Override
            public BigInteger parse(XContentParser parser, boolean coerce) throws IOException {
                return parser.bigIntegerValue(coerce);
            }

            @Override
            public Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable) {
                if (hasDecimalPart(value)) {
                    return Queries.newMatchNoDocsQuery("Value [" + value + "] has a decimal part");
                }
                BigInteger v = parse(value, true);
                if (isSearchable && hasDocValues) {
                    Query query = BigIntegerPoint.newExactQuery(field, v);
                    Query dvQuery = SortedUnsignedLongDocValuesSetQuery.newSlowExactQuery(field, v);
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocValues) {
                    return SortedUnsignedLongDocValuesSetQuery.newSlowExactQuery(field, v);
                }
                return BigIntegerPoint.newExactQuery(field, v);
            }

            @Override
            public Query termsQuery(String field, List<Object> values, boolean hasDocvalues, boolean isSearchable) {
                BigInteger[] v = new BigInteger[values.size()];
                int upTo = 0;

                for (int i = 0; i < values.size(); i++) {
                    Object value = values.get(i);
                    if (!hasDecimalPart(value)) {
                        v[upTo++] = parse(value, true);
                    }
                }

                if (upTo == 0) {
                    return Queries.newMatchNoDocsQuery("All values have a decimal part");
                }
                if (upTo != v.length) {
                    v = Arrays.copyOf(v, upTo);
                }

                if (isSearchable && hasDocvalues) {
                    Query query = BigIntegerPoint.newSetQuery(field, v);
                    Query dvQuery = SortedUnsignedLongDocValuesSetQuery.newSlowSetQuery(field, v);
                    return new IndexOrDocValuesQuery(query, dvQuery);
                }
                if (hasDocvalues) {
                    return SortedUnsignedLongDocValuesSetQuery.newSlowSetQuery(field, v);
                }
                return BigIntegerPoint.newSetQuery(field, v);
            }

            @Override
            public Query rangeQuery(
                String field,
                Object lowerTerm,
                Object upperTerm,
                boolean includeLower,
                boolean includeUpper,
                boolean hasDocValues,
                boolean isSearchable,
                QueryShardContext context
            ) {
                return unsignedLongRangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, (l, u) -> {
                    if (isSearchable && hasDocValues) {
                        Query query = BigIntegerPoint.newRangeQuery(field, l, u);
                        Query dvQuery = SortedUnsignedLongDocValuesRangeQuery.newSlowRangeQuery(field, l, u);
                        return new IndexOrDocValuesQuery(query, dvQuery);
                    }
                    if (hasDocValues) {
                        return SortedUnsignedLongDocValuesRangeQuery.newSlowRangeQuery(field, l, u);
                    }
                    return BigIntegerPoint.newRangeQuery(field, l, u);
                });
            }

            @Override
            public List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored) {
                List<Field> fields = new ArrayList<>();
                final BigInteger v = Numbers.toUnsignedLongExact(value);

                if (indexed) {
                    fields.add(new BigIntegerPoint(name, v));
                }

                if (docValued) {
                    fields.add(new SortedNumericDocValuesField(name, v.longValue()));
                }

                if (stored) {
                    fields.add(new StoredField(name, v.toString()));
                }

                return fields;
            }

            @Override
            Number valueForSearch(String value) {
                return new BigInteger(value);
            }
        };

        private final String name;
        private final NumericType numericType;
        private final TypeParser parser;

        NumberType(String name, NumericType numericType) {
            this.name = name;
            this.numericType = numericType;
            this.parser = new TypeParser((n, c) -> new Builder(n, this, c.getSettings()));
        }

        /** Get the associated type name. */
        public final String typeName() {
            return name;
        }

        /** Get the associated numeric type */
        public final NumericType numericType() {
            return numericType;
        }

        public final TypeParser parser() {
            return parser;
        }

        public abstract Query termQuery(String field, Object value, boolean hasDocValues, boolean isSearchable);

        public abstract Query termsQuery(String field, List<Object> values, boolean hasDocValues, boolean isSearchable);

        public abstract Query rangeQuery(
            String field,
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            boolean hasDocValues,
            boolean isSearchable,
            QueryShardContext context
        );

        public abstract Number parse(XContentParser parser, boolean coerce) throws IOException;

        public abstract Number parse(Object value, boolean coerce);

        public abstract Number parsePoint(byte[] value);

        public abstract List<Field> createFields(String name, Number value, boolean indexed, boolean docValued, boolean stored);

        abstract Number valueForSearch(String value);

        Number valueForSearch(Number value) {
            return value;
        }

        /**
         * Returns true if the object is a number and has a decimal part
         */
        public static boolean hasDecimalPart(Object number) {
            if (number instanceof Number) {
                double doubleValue = ((Number) number).doubleValue();
                return doubleValue % 1 != 0;
            }
            if (number instanceof BytesRef) {
                number = ((BytesRef) number).utf8ToString();
            }
            if (number instanceof String) {
                return Double.parseDouble((String) number) % 1 != 0;
            }
            return false;
        }

        /**
         * Returns -1, 0, or 1 if the value is lower than, equal to, or greater than 0
         */
        static double signum(Object value) {
            if (value instanceof Number) {
                double doubleValue = ((Number) value).doubleValue();
                return Math.signum(doubleValue);
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return Math.signum(Double.parseDouble(value.toString()));
        }

        /**
         * Converts an Object to a double by checking it against known types first
         */
        public static double objectToDouble(Object value) {
            double doubleValue;

            if (value instanceof Number) {
                doubleValue = ((Number) value).doubleValue();
            } else if (value instanceof BytesRef) {
                doubleValue = Double.parseDouble(((BytesRef) value).utf8ToString());
            } else {
                doubleValue = Double.parseDouble(value.toString());
            }

            return doubleValue;
        }

        /**
         * Converts and Object to a {@code long} by checking it against known
         * types and checking its range.
         */
        public static long objectToLong(Object value, boolean coerce) {
            if (value instanceof Long) {
                return (Long) value;
            }

            double doubleValue = objectToDouble(value);
            // this check does not guarantee that value is inside MIN_VALUE/MAX_VALUE because values up to 9223372036854776832 will
            // be equal to Long.MAX_VALUE after conversion to double. More checks ahead.
            if (doubleValue < Long.MIN_VALUE || doubleValue > Long.MAX_VALUE) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for a long");
            }
            if (!coerce && doubleValue % 1 != 0) {
                throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
            }

            // longs need special handling so we don't lose precision while parsing
            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            return Numbers.toLong(stringValue, coerce);
        }

        public static BigInteger objectToUnsignedLong(Object value, boolean coerce) {
            return objectToUnsignedLong(value, coerce, false);
        }

        /**
         * Converts an Object to a {@code BigInteger} by checking it against known
         * types and checking its range.
         *
         * @param lenientBound if true, use MIN or MAX if the value is out of bound
         */
        public static BigInteger objectToUnsignedLong(Object value, boolean coerce, boolean lenientBound) {
            if (value instanceof Long) {
                return Numbers.toUnsignedBigInteger(((Long) value).longValue());
            }

            double doubleValue = objectToDouble(value);
            if (lenientBound) {
                if (doubleValue < Numbers.MIN_UNSIGNED_LONG_VALUE.doubleValue()) {
                    return Numbers.MIN_UNSIGNED_LONG_VALUE;
                }
                if (doubleValue > Numbers.MAX_UNSIGNED_LONG_VALUE.doubleValue()) {
                    return Numbers.MAX_UNSIGNED_LONG_VALUE;
                }
            }
            if (doubleValue < Numbers.MIN_UNSIGNED_LONG_VALUE.doubleValue()
                || doubleValue > Numbers.MAX_UNSIGNED_LONG_VALUE.doubleValue()) {
                throw new IllegalArgumentException("Value [" + value + "] is out of range for an unsigned long");
            }
            if (!coerce && doubleValue % 1 != 0) {
                throw new IllegalArgumentException("Value [" + value + "] has a decimal part");
            }

            String stringValue = (value instanceof BytesRef) ? ((BytesRef) value).utf8ToString() : value.toString();
            return Numbers.toUnsignedLong(stringValue, coerce);
        }

        public static Query doubleRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Double, Double, Query> builder
        ) {
            double l = Double.NEGATIVE_INFINITY;
            double u = Double.POSITIVE_INFINITY;
            if (lowerTerm != null) {
                l = objectToDouble(lowerTerm);
                if (includeLower == false) {
                    l = DoublePoint.nextUp(l);
                }
            }
            if (upperTerm != null) {
                u = objectToDouble(upperTerm);
                if (includeUpper == false) {
                    u = DoublePoint.nextDown(u);
                }
            }
            return builder.apply(l, u);
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query longRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<Long, Long, Query> builder
        ) {
            long l = Long.MIN_VALUE;
            long u = Long.MAX_VALUE;
            if (lowerTerm != null) {
                l = objectToLong(lowerTerm, true);
                // if the lower bound is decimal:
                // - if the bound is positive then we increment it:
                // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                // - if the bound is negative then we leave it as is:
                // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                    if (l == Long.MAX_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    ++l;
                }
            }
            if (upperTerm != null) {
                u = objectToLong(upperTerm, true);
                boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                    if (u == Long.MIN_VALUE) {
                        return new MatchNoDocsQuery();
                    }
                    --u;
                }
            }
            return builder.apply(l, u);
        }

        /**
         * Processes query bounds into {@code long}s and delegates the
         * provided {@code builder} to build a range query.
         */
        public static Query unsignedLongRangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            BiFunction<BigInteger, BigInteger, Query> builder
        ) {
            BigInteger l = Numbers.MIN_UNSIGNED_LONG_VALUE;
            BigInteger u = Numbers.MAX_UNSIGNED_LONG_VALUE;
            if (lowerTerm != null) {
                l = objectToUnsignedLong(lowerTerm, true);
                // if the lower bound is decimal:
                // - if the bound is positive then we increment it:
                // if lowerTerm=1.5 then the (inclusive) bound becomes 2
                // - if the bound is negative then we leave it as is:
                // if lowerTerm=-1.5 then the (inclusive) bound becomes -1 due to the call to longValue
                boolean lowerTermHasDecimalPart = hasDecimalPart(lowerTerm);
                if ((lowerTermHasDecimalPart == false && includeLower == false) || (lowerTermHasDecimalPart && signum(lowerTerm) > 0)) {
                    if (l.compareTo(Numbers.MAX_UNSIGNED_LONG_VALUE) == 0) {
                        return new MatchNoDocsQuery();
                    }
                    l = l.add(BigInteger.ONE);
                }
            }
            if (upperTerm != null) {
                u = objectToUnsignedLong(upperTerm, true);
                boolean upperTermHasDecimalPart = hasDecimalPart(upperTerm);
                if ((upperTermHasDecimalPart == false && includeUpper == false) || (upperTermHasDecimalPart && signum(upperTerm) < 0)) {
                    if (u.compareTo(Numbers.MAX_UNSIGNED_LONG_VALUE) == 0) {
                        return new MatchNoDocsQuery();
                    }
                    u = u.subtract(BigInteger.ONE);
                }
            }
            return builder.apply(l, u);
        }
    }

    /**
     * Field type for numeric fields
     *
     * @opensearch.internal
     */
    public static class NumberFieldType extends SimpleMappedFieldType implements NumericPointEncoder {

        private final NumberType type;
        private final boolean coerce;
        private final Number nullValue;

        public NumberFieldType(
            String name,
            NumberType type,
            boolean isSearchable,
            boolean isStored,
            boolean hasDocValues,
            boolean coerce,
            Number nullValue,
            Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.type = Objects.requireNonNull(type);
            this.coerce = coerce;
            this.nullValue = nullValue;
            this.setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);     // allows number fields in significant text aggs - do we need this?
        }

        NumberFieldType(String name, Builder builder) {
            this(
                name,
                builder.type,
                builder.indexed.getValue(),
                builder.stored.getValue(),
                builder.hasDocValues.getValue(),
                builder.coerce.getValue().value(),
                builder.nullValue.getValue(),
                builder.meta.getValue()
            );
        }

        public NumberFieldType(String name, NumberType type) {
            this(name, type, true, false, true, true, null, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return type.name;
        }

        public NumberType numberType() {
            return type;
        }

        public NumericType numericType() {
            return type.numericType();
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            Query query = type.termQuery(name(), value, hasDocValues(), isSearchable());
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List values, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            Query query = type.termsQuery(name(), values, hasDocValues(), isSearchable());
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            Query query = type.rangeQuery(
                name(),
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                hasDocValues(),
                isSearchable(),
                context
            );
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Function<byte[], Number> pointReaderIfPossible() {
            if (isSearchable()) {
                return this::parsePoint;
            }
            return null;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), type.numericType());
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof String) {
                return type.valueForSearch((String) value);
            } else {
                return type.valueForSearch((Number) value);
            }
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Object parseSourceValue(Object value) {
                    if (value.equals("")) {
                        return nullValue;
                    }
                    return type.parse(value, coerce);
                }
            };
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            if (format == null) {
                if (type == NumberType.UNSIGNED_LONG) {
                    return DocValueFormat.UNSIGNED_LONG;
                } else {
                    return DocValueFormat.RAW;
                }
            } else {
                return new DocValueFormat.Decimal(format);
            }
        }

        public Number parsePoint(byte[] value) {
            return type.parsePoint(value);
        }

        @Override
        public byte[] encodePoint(Number value) {
            return type.encodePoint(value);
        }
    }

    private final NumberType type;

    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;
    private final Explicit<Boolean> ignoreMalformed;
    private final Explicit<Boolean> coerce;
    private final Number nullValue;

    private final boolean ignoreMalformedByDefault;
    private final boolean coerceByDefault;

    private NumberFieldMapper(String simpleName, MappedFieldType mappedFieldType, MultiFields multiFields, CopyTo copyTo, Builder builder) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.type = builder.type;
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.stored = builder.stored.getValue();
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.coerce = builder.coerce.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformed.getDefaultValue().value();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
    }

    boolean coerce() {
        return coerce.value();
    }

    boolean ignoreMalformed() {
        return ignoreMalformed.value();
    }

    @Override
    public NumberFieldType fieldType() {
        return (NumberFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().type.typeName();
    }

    @Override
    protected NumberFieldMapper clone() {
        return (NumberFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        Object value;
        Number numericValue = null;
        if (context.externalValueSet()) {
            value = context.externalValue();
        } else if (parser.currentToken() == Token.VALUE_NULL) {
            value = null;
        } else if (coerce.value() && parser.currentToken() == Token.VALUE_STRING && parser.textLength() == 0) {
            value = null;
        } else {
            try {
                numericValue = fieldType().type.parse(parser, coerce.value());
            } catch (InputCoercionException | IllegalArgumentException | JsonParseException e) {
                if (ignoreMalformed.value() && parser.currentToken().isValue()) {
                    context.addIgnoredField(mappedFieldType.name());
                    return;
                } else {
                    throw e;
                }
            }
            value = numericValue;
        }

        if (value == null) {
            value = nullValue;
        }

        if (value == null) {
            return;
        }

        if (numericValue == null) {
            numericValue = fieldType().type.parse(value, coerce.value());
        }

        context.doc().addAll(fieldType().type.createFields(fieldType().name(), numericValue, indexed, hasDocValues, stored));

        if (hasDocValues == false && (stored || indexed)) {
            createFieldNamesField(context);
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), type, ignoreMalformedByDefault, coerceByDefault).init(this);
    }
}
