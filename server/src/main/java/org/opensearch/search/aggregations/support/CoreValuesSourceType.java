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

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Rounding;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.geometry.Geometry;
import org.opensearch.geometry.utils.WellKnownText;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexGeoPointFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.index.fielddata.IndexOrdinalsFieldData;
import org.opensearch.index.fielddata.plain.AbstractGeoShapeIndexFieldData;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.RangeFieldMapper;
import org.opensearch.script.AggregationScript;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * {@link CoreValuesSourceType} holds the {@link ValuesSourceType} implementations for the core aggregations package.
 *
 * @opensearch.internal
 */
public enum CoreValuesSourceType implements ValuesSourceType {

    NUMERIC() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Numeric.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Numeric.Script(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {

            if ((fieldContext.indexFieldData() instanceof IndexNumericFieldData) == false) {
                // TODO: Is this the correct exception type here?
                throw new IllegalArgumentException(
                    "Expected numeric type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }

            ValuesSource.Numeric dataSource = new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData());
            if (script != null) {
                // Value script case
                dataSource = new ValuesSource.Numeric.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            Number missing = docValueFormat.parseDouble(rawMissing.toString(), false, now);
            return MissingValues.replaceMissing((ValuesSource.Numeric) valuesSource, missing);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            /* TODO: this silently ignores a timezone argument, whereas NumberFieldType#docValueFormat throws if given a time zone.
                     Before we can solve this, we need to resolve https://github.com/elastic/elasticsearch/issues/47469 which deals
                     with the fact that the same formatter is used for input and output values.  We want to support a use case in SQL
                     (and elsewhere) that allows for passing a long value milliseconds since epoch into date aggregations.  In that case,
                     the timezone is sensible as part of the bucket key format.
             */
            if (format == null) {
                return DocValueFormat.RAW;
            } else {
                return new DocValueFormat.Decimal(format);
            }

        }
    },
    BYTES() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.Bytes.WithOrdinals.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return new ValuesSource.Bytes.Script(script);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            final IndexFieldData<?> indexFieldData = fieldContext.indexFieldData();
            ValuesSource dataSource;
            if (indexFieldData instanceof IndexOrdinalsFieldData) {
                dataSource = new ValuesSource.Bytes.WithOrdinals.FieldData((IndexOrdinalsFieldData) indexFieldData);
            } else {
                dataSource = new ValuesSource.Bytes.FieldData(indexFieldData);
            }
            if (script != null) {
                // Again, what's the difference between WithScript and Script?
                dataSource = new ValuesSource.Bytes.WithScript(dataSource, script);
            }
            return dataSource;
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            final BytesRef missing = docValueFormat.parseBytesRef(rawMissing.toString());
            if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                return MissingValues.replaceMissing((ValuesSource.Bytes.WithOrdinals) valuesSource, missing);
            } else {
                return MissingValues.replaceMissing((ValuesSource.Bytes) valuesSource, missing);
            }
        }
    },
    GEOPOINT() {
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.GeoPoint.EMPTY;
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (!(fieldContext.indexFieldData() instanceof IndexGeoPointFieldData)) {
                // TODO: Is this the correct exception type here?
                throw new IllegalArgumentException(
                    "Expected geo_point type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }

            return new ValuesSource.GeoPoint.Fielddata((IndexGeoPointFieldData) fieldContext.indexFieldData());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            // TODO: also support the structured formats of geo points
            final GeoPoint missing = new GeoPoint(rawMissing.toString());
            return MissingValues.replaceMissing((ValuesSource.GeoPoint) valuesSource, missing);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.GEOHASH;
        }
    },
    GEO_SHAPE() {
        /**
         * Called when an aggregation is operating over a known empty set (usually because the field isn't specified), this method allows for
         * returning a no-op implementation.  All ValuesSource should implement this method.
         *
         * @return - Empty specialization of the base ValuesSource
         */
        @Override
        public ValuesSource getEmpty() {
            return ValuesSource.GeoShape.EMPTY;
        }

        /**
         * Returns the type-specific sub class for a script data source. ValuesSource that do not support scripts should throw
         * AggregationExecutionException.  Note that this method is called when a script is
         * operating without an underlying field.  Scripts operating over fields are handled by the script argument to getField below.
         *
         * @param script          - The script being wrapped
         * @param scriptValueType - The expected output type of the script
         * @return - Script specialization of the base ValuesSource
         */
        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException(
                String.format(Locale.ROOT, "value source of type [%s] is not supported by scripts", this.value())
            );
        }

        /**
         * Return a ValuesSource wrapping a field for the given type. All ValuesSource must
         * implement this method.
         *
         * @param fieldContext - The field being wrapped
         * @param script       - Optional script that might be applied over the field
         * @return - Field specialization of the base ValuesSource
         */
        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            if (!(fieldContext.indexFieldData() instanceof AbstractGeoShapeIndexFieldData)) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "Expected geo_shape type on field [%s], but got [%s]",
                        fieldContext.field(),
                        fieldContext.fieldType().typeName()
                    )
                );
            }
            return new ValuesSource.GeoShape.FieldData((AbstractGeoShapeIndexFieldData) fieldContext.indexFieldData());
        }

        /**
         * Apply the given missing value to an already-constructed ValuesSource. The parameter rawMissing is
         * the value which has been supplied by the user. We support String representation only for the
         * rawMissing for now.
         *
         * @param valuesSource   - The original ValuesSource
         * @param rawMissing     - The missing value we got from the parser, typically a string or number
         * @param docValueFormat - The format to use for further parsing the user supplied value, e.g. a date format
         * @param now            - Used in conjunction with the formatter, should return the current time in milliseconds
         * @return - Wrapper over the provided ValuesSource to apply the given missing value
         */
        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            // TODO: also support the structured formats of geo shapes
            try {
                final Geometry geometry = WellKnownText.INSTANCE.fromWKT((String) rawMissing);
                return MissingValues.replaceMissing((ValuesSource.GeoShape) valuesSource, geometry);
            } catch (Exception e) {
                throw new OpenSearchParseException(
                    String.format(Locale.ROOT, "Unable to parse the missing value [%s] provided in the input.", rawMissing),
                    e
                );
            }
        }

        /**
         * This method provides a hook for specifying a type-specific formatter. When ValuesSourceConfig can resolve a
         * MappedFieldType, it prefers to get the formatter from there. Only when a field can't be
         * resolved (which is to say script cases and unmapped field cases), it will fall back to calling this method on whatever
         * ValuesSourceType it was able to resolve to.
         * <p>
         * For geoshape field we may never hit this function till we have aggregations which are only geo_shape
         * specific and not present on geo_points, as we use default CoreValueSource types for Geo based aggregations
         * as GEOPOINT
         *
         * @param format - User supplied format string (Optional)
         * @param tz     - User supplied time zone (Optional)
         * @return - A formatter object, configured with the passed in settings if appropriate.
         */
        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.RAW;
        }
    },
    RANGE() {
        @Override
        public ValuesSource getEmpty() {
            // TODO: Is this the correct exception type here?
            throw new IllegalArgumentException("Can't deal with unmapped ValuesSource type " + this.value());
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            throw new AggregationExecutionException("value source of type [" + this.value() + "] is not supported by scripts");
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            MappedFieldType fieldType = fieldContext.fieldType();

            if (fieldType instanceof RangeFieldMapper.RangeFieldType == false) {
                // TODO: Is this the correct exception type here?
                throw new IllegalStateException("Asked for range ValuesSource, but field is of type " + fieldType.name());
            }
            RangeFieldMapper.RangeFieldType rangeFieldType = (RangeFieldMapper.RangeFieldType) fieldType;
            return new ValuesSource.Range(fieldContext.indexFieldData(), rangeFieldType.rangeType());
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            throw new IllegalArgumentException("Can't apply missing values on a " + valuesSource.getClass());
        }
    },
    IP() {
        @Override
        public ValuesSource getEmpty() {
            return BYTES.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return BYTES.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return BYTES.getField(fieldContext, script);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return BYTES.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.IP;
        }
    },
    DATE() {
        @Override
        public ValuesSource getEmpty() {
            return NUMERIC.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return NUMERIC.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            ValuesSource.Numeric dataSource = fieldData(fieldContext);
            if (script != null) {
                // Value script case
                return new ValuesSource.Numeric.WithScript(dataSource, script);
            }
            return dataSource;
        }

        private ValuesSource.Numeric fieldData(FieldContext fieldContext) {
            if ((fieldContext.indexFieldData() instanceof IndexNumericFieldData) == false) {
                throw new IllegalArgumentException(
                    "Expected numeric type on field [" + fieldContext.field() + "], but got [" + fieldContext.fieldType().typeName() + "]"
                );
            }
            if (fieldContext.fieldType().isSearchable() == false || fieldContext.fieldType() instanceof DateFieldType == false) {
                /*
                 * We can't implement roundingPreparer in these cases because
                 * we can't look up the min and max date without both the
                 * search index (the first test) and the resolution which is
                 * on the DateFieldType.
                 */
                return new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData());
            }
            return new ValuesSource.Numeric.FieldData((IndexNumericFieldData) fieldContext.indexFieldData()) {
                /**
                 * Proper dates get a real implementation of
                 * {@link #roundingPreparer(IndexReader)}. If the field is
                 * configured with a script or a missing value then we'll
                 * wrap this without delegating so those fields will ignore
                 * this implementation. Which is correct.
                 */
                @Override
                public Function<Rounding, Rounding.Prepared> roundingPreparer(IndexReader reader) throws IOException {
                    DateFieldType dft = (DateFieldType) fieldContext.fieldType();
                    byte[] min = PointValues.getMinPackedValue(reader, fieldContext.field());
                    if (min == null) {
                        // There aren't any indexes values so we don't need to optimize.
                        return Rounding::prepareForUnknown;
                    }
                    byte[] max = PointValues.getMaxPackedValue(reader, fieldContext.field());
                    long minUtcMillis = dft.resolution().parsePointAsMillis(min);
                    long maxUtcMillis = dft.resolution().parsePointAsMillis(max);
                    return rounding -> rounding.prepare(minUtcMillis, maxUtcMillis);
                }
            };
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return new DocValueFormat.DateTime(
                format == null ? DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER : DateFormatter.forPattern(format),
                tz == null ? ZoneOffset.UTC : tz,
                // If we were just looking at fields, we could read the resolution from the field settings, but we need to deal with script
                // output, which has no way to indicate the resolution, so we need to default to something. Milliseconds is the standard.
                DateFieldMapper.Resolution.MILLISECONDS
            );
        }
    },
    BOOLEAN() {
        @Override
        public ValuesSource getEmpty() {
            return NUMERIC.getEmpty();
        }

        @Override
        public ValuesSource getScript(AggregationScript.LeafFactory script, ValueType scriptValueType) {
            return NUMERIC.getScript(script, scriptValueType);
        }

        @Override
        public ValuesSource getField(FieldContext fieldContext, AggregationScript.LeafFactory script) {
            return NUMERIC.getField(fieldContext, script);
        }

        @Override
        public ValuesSource replaceMissing(ValuesSource valuesSource, Object rawMissing, DocValueFormat docValueFormat, LongSupplier now) {
            return NUMERIC.replaceMissing(valuesSource, rawMissing, docValueFormat, now);
        }

        @Override
        public DocValueFormat getFormatter(String format, ZoneId tz) {
            return DocValueFormat.BOOLEAN;
        }
    };

    public static ValuesSourceType fromString(String name) {
        return valueOf(name.trim().toUpperCase(Locale.ROOT));
    }

    public String value() {
        return name().toLowerCase(Locale.ROOT);
    }

    @Override
    public String typeName() {
        return value();
    }

    CoreValuesSourceType() {}

    /** List containing all members of the enumeration. */
    public static final List<ValuesSourceType> ALL_CORE = Arrays.asList(CoreValuesSourceType.values());
}
