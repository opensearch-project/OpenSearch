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
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANYDa
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.StoredValue;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Explicit;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.network.InetAddresses;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.util.LocaleUtils;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.BinaryIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.index.query.RangeQueryBuilder.GTE_FIELD;
import static org.opensearch.index.query.RangeQueryBuilder.GT_FIELD;
import static org.opensearch.index.query.RangeQueryBuilder.LTE_FIELD;
import static org.opensearch.index.query.RangeQueryBuilder.LT_FIELD;

/**
 * A {@link FieldMapper} for indexing numeric and date ranges, and creating queries
 *
 * @opensearch.internal
 */
public class RangeFieldMapper extends ParametrizedFieldMapper {
    public static final boolean DEFAULT_INCLUDE_UPPER = true;
    public static final boolean DEFAULT_INCLUDE_LOWER = true;

    /**
     * Default parameters for range fields
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final Explicit<Boolean> COERCE = new Explicit<>(true, false);
        public static final DateFormatter DATE_FORMATTER = DateFieldMapper.getDefaultDateTimeFormatter();
    }

    // this is private since it has a different default
    static final Setting<Boolean> COERCE_SETTING = Setting.boolSetting("index.mapping.coerce", true, Setting.Property.IndexScope);

    private static RangeFieldMapper toType(FieldMapper in) {
        return (RangeFieldMapper) in;
    }

    /**
     * Builder for range fields
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).index, true);
        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> store = Parameter.storeParam(m -> toType(m).store, false);
        private final Parameter<Explicit<Boolean>> coerce;
        private final Parameter<String> format = Parameter.stringParam(
            "format",
            false,
            m -> toType(m).format,
            Defaults.DATE_FORMATTER.pattern()
        );
        private final Parameter<Locale> locale = new Parameter<>(
            "locale",
            false,
            () -> Locale.ROOT,
            (n, c, o) -> LocaleUtils.parse(o.toString()),
            m -> toType(m).locale
        );
        private final Parameter<Float> boost = Parameter.boostParam();
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        private final RangeType type;
        private final Version indexCreatedVersion;
        private final boolean ignoreMalformedByDefault;
        private final Parameter<Explicit<Boolean>> ignoreMalformed;

        public Builder(String name, RangeType type, Settings settings) {
            this(
                name,
                type,
                COERCE_SETTING.get(settings),
                IGNORE_MALFORMED_SETTING.get(settings),
                hasIndexCreated(settings) ? IndexMetadata.indexCreated(settings) : null
            );
        }

        public Builder(String name, RangeType type, boolean coerceByDefault, Version indexCreatedVersion) {
            this(name, type, coerceByDefault, false /* ignoreMalformedByDefault */, indexCreatedVersion);
        }

        public Builder(
            String name,
            RangeType type,
            boolean coerceByDefault,
            boolean ignoreMalformedByDefault,
            Version indexCreatedVersion
        ) {
            super(name);
            this.type = type;
            this.coerce = Parameter.explicitBoolParam("coerce", true, m -> toType(m).coerce, coerceByDefault);
            this.indexCreatedVersion = indexCreatedVersion;
            this.ignoreMalformedByDefault = ignoreMalformedByDefault;
            this.ignoreMalformed = Parameter.explicitBoolParam(
                "ignore_malformed",
                true,
                m -> toType(m).ignoreMalformed,
                ignoreMalformedByDefault
            );
            if (this.type != RangeType.DATE) {
                format.neverSerialize();
                locale.neverSerialize();
            }
        }

        public void docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
        }

        Builder format(String format) {
            this.format.setValue(format);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(index, hasDocValues, store, coerce, format, locale, boost, meta, ignoreMalformed);
        }

        protected RangeFieldType setupFieldType(BuilderContext context) {
            if (format.isConfigured()) {
                if (type != RangeType.DATE) {
                    throw new IllegalArgumentException(
                        "field ["
                            + name()
                            + "] of type [range]"
                            + " should not define a dateTimeFormatter unless it is a "
                            + RangeType.DATE
                            + " type"
                    );
                }

                // The builder context may not have index created version, falling back to indexCreatedVersion
                // property of this mapper builder.
                DateFormatter dateTimeFormatter = DateFormatter.forPattern(format.getValue()).withLocale(locale.getValue());
                return new RangeFieldType(
                    buildFullName(context),
                    index.getValue(),
                    store.getValue(),
                    hasDocValues.getValue(),
                    dateTimeFormatter,
                    coerce.getValue().value(),
                    meta.getValue()
                );
            }
            if (type == RangeType.DATE) {
                return new RangeFieldType(
                    buildFullName(context),
                    index.getValue(),
                    store.getValue(),
                    hasDocValues.getValue(),
                    Defaults.DATE_FORMATTER,
                    coerce.getValue().value(),
                    meta.getValue()
                );
            }
            return new RangeFieldType(
                buildFullName(context),
                type,
                index.getValue(),
                store.getValue(),
                hasDocValues.getValue(),
                coerce.getValue().value(),
                meta.getValue()
            );
        }

        @Override
        public RangeFieldMapper build(BuilderContext context) {
            RangeFieldType ft = setupFieldType(context);
            ft.setBoost(boost.getValue());
            return new RangeFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), type, this);
        }
    }

    /**
     * Field type for range fields
     *
     * @opensearch.internal
     */
    public static final class RangeFieldType extends MappedFieldType {
        protected final RangeType rangeType;
        protected final DateFormatter dateTimeFormatter;
        protected final DateMathParser dateMathParser;
        protected final boolean coerce;

        public RangeFieldType(
            String name,
            RangeType type,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            boolean coerce,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            assert type != RangeType.DATE;
            this.rangeType = Objects.requireNonNull(type);
            dateTimeFormatter = null;
            dateMathParser = null;
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.coerce = coerce;
        }

        public RangeFieldType(String name, RangeType type) {
            this(name, type, true, false, true, false, Collections.emptyMap());
        }

        public RangeFieldType(
            String name,
            boolean indexed,
            boolean stored,
            boolean hasDocValues,
            DateFormatter formatter,
            boolean coerce,
            Map<String, String> meta
        ) {
            super(name, indexed, stored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.rangeType = RangeType.DATE;
            this.dateTimeFormatter = Objects.requireNonNull(formatter);
            this.dateMathParser = dateTimeFormatter.toDateMathParser();
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.coerce = coerce;
        }

        public RangeFieldType(String name, DateFormatter formatter) {
            this(name, true, false, true, formatter, false, Collections.emptyMap());
        }

        public RangeType rangeType() {
            return rangeType;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new BinaryIndexFieldData.Builder(name(), CoreValuesSourceType.RANGE);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            DateFormatter defaultFormatter = dateTimeFormatter();
            DateFormatter formatter = format != null
                ? DateFormatter.forPattern(format).withLocale(defaultFormatter.locale())
                : defaultFormatter;

            return new SourceValueFetcher(name(), context) {

                @Override
                @SuppressWarnings("unchecked")
                protected Object parseSourceValue(Object value) {
                    RangeType rangeType = rangeType();
                    if (!(value instanceof Map)) {
                        assert rangeType == RangeType.IP;
                        Tuple<InetAddress, Integer> ipRange = InetAddresses.parseCidr(value.toString());
                        return InetAddresses.toCidrString(ipRange.v1(), ipRange.v2());
                    }

                    Map<String, Object> range = (Map<String, Object>) value;
                    Map<String, Object> parsedRange = new HashMap<>();
                    for (Map.Entry<String, Object> entry : range.entrySet()) {
                        Object parsedValue = rangeType.parseValue(entry.getValue(), coerce, dateMathParser);
                        Object formattedValue = rangeType.formatValue(parsedValue, formatter);
                        parsedRange.put(entry.getKey(), formattedValue);
                    }
                    return parsedRange;
                }
            };
        }

        @Override
        public String typeName() {
            return rangeType.name;
        }

        public DateFormatter dateTimeFormatter() {
            return dateTimeFormatter;
        }

        protected DateMathParser dateMathParser() {
            return dateMathParser;
        }

        @Override
        public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
            if (rangeType == RangeType.DATE) {
                DateFormatter dateTimeFormatter = this.dateTimeFormatter;
                if (format != null) {
                    dateTimeFormatter = DateFormatter.forPattern(format).withLocale(dateTimeFormatter.locale());
                }
                if (timeZone == null) {
                    timeZone = ZoneOffset.UTC;
                }
                // the resolution here is always set to milliseconds, as aggregations use this formatter mainly and those are always in
                // milliseconds. The only special case here is docvalue fields, which are handled somewhere else
                return new DocValueFormat.DateTime(dateTimeFormatter, timeZone, DateFieldMapper.Resolution.MILLISECONDS);
            }
            return super.docValueFormat(format, timeZone);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            Query query = rangeQuery(value, value, true, true, ShapeRelation.INTERSECTS, null, null, context);
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query rangeQuery(
            Object lowerTerm,
            Object upperTerm,
            boolean includeLower,
            boolean includeUpper,
            ShapeRelation relation,
            ZoneId timeZone,
            DateMathParser parser,
            QueryShardContext context
        ) {
            failIfNotIndexed();
            if (parser == null) {
                parser = dateMathParser();
            }
            return rangeType.rangeQuery(
                name(),
                hasDocValues(),
                lowerTerm,
                upperTerm,
                includeLower,
                includeUpper,
                relation,
                timeZone,
                parser,
                context
            );
        }
    }

    private final RangeType type;
    private final boolean index;
    private final boolean hasDocValues;
    private final boolean store;
    private final Explicit<Boolean> coerce;
    private final String format;
    private final Locale locale;

    private final boolean coerceByDefault;
    private final Version indexCreatedVersion;
    private final Explicit<Boolean> ignoreMalformed;
    private final boolean ignoreMalformedByDefault;

    private RangeFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        RangeType type,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.type = type;
        this.index = builder.index.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.store = builder.store.getValue();
        this.coerce = builder.coerce.getValue();
        this.format = builder.format.getValue();
        this.locale = builder.locale.getValue();
        this.coerceByDefault = builder.coerce.getDefaultValue().value();
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.ignoreMalformed = builder.ignoreMalformed.getValue();
        this.ignoreMalformedByDefault = builder.ignoreMalformedByDefault;
    }

    boolean coerce() {
        return coerce.value();
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), type, coerceByDefault, ignoreMalformedByDefault, indexCreatedVersion).init(this);
    }

    @Override
    public RangeFieldType fieldType() {
        return (RangeFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return fieldType().typeName();
    }

    @Override
    protected RangeFieldMapper clone() {
        return (RangeFieldMapper) super.clone();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        Range range;
        if (context.externalValueSet()) {
            range = context.parseExternalValue(Range.class);
        } else {
            XContentParser parser = context.parser();
            final XContentParser.Token start = parser.currentToken();
            if (start == XContentParser.Token.VALUE_NULL) {
                return;
            } else if (start == XContentParser.Token.START_OBJECT) {
                RangeFieldType fieldType = fieldType();
                RangeType rangeType = fieldType.rangeType;
                String fieldName = null;
                Object from = rangeType.minValue();
                Object to = rangeType.maxValue();
                boolean includeFrom = DEFAULT_INCLUDE_LOWER;
                boolean includeTo = DEFAULT_INCLUDE_UPPER;
                XContentParser.Token token;
                boolean rangeIsMalformed = false;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else {
                        try {
                            if (fieldName.equals(GT_FIELD.getPreferredName())) {
                                includeFrom = false;
                                if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                    from = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                                }
                            } else if (fieldName.equals(GTE_FIELD.getPreferredName())) {
                                includeFrom = true;
                                if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                    from = rangeType.parseFrom(fieldType, parser, coerce.value(), includeFrom);
                                }
                            } else if (fieldName.equals(LT_FIELD.getPreferredName())) {
                                includeTo = false;
                                if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                    to = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                                }
                            } else if (fieldName.equals(LTE_FIELD.getPreferredName())) {
                                includeTo = true;
                                if (parser.currentToken() != XContentParser.Token.VALUE_NULL) {
                                    to = rangeType.parseTo(fieldType, parser, coerce.value(), includeTo);
                                }
                            } else {
                                throw new MapperParsingException(
                                    "error parsing field [" + name() + "], with unknown parameter [" + fieldName + "]"
                                );
                            }
                        } catch (final IllegalArgumentException e) {
                            // We have to consume the JSON object in full
                            if (ignoreMalformed().value()) {
                                rangeIsMalformed = true;
                            } else {
                                throw e;
                            }
                        }
                    }
                }

                if (rangeIsMalformed) {
                    context.addIgnoredField(fieldType().name());
                    return;
                }

                range = new Range(rangeType, from, to, includeFrom, includeTo);
            } else if (fieldType().rangeType == RangeType.IP && start == XContentParser.Token.VALUE_STRING) {
                try {
                    range = parseIpRangeFromCidr(parser);
                } catch (IllegalArgumentException e) {
                    if (ignoreMalformed().value()) {
                        context.addIgnoredField(fieldType().name());
                        return;
                    } else {
                        throw e;
                    }
                }
            } else {
                throw new MapperParsingException(
                    "error parsing field [" + name() + "], expected an object but got " + parser.currentName()
                );
            }
        }
        context.doc().addAll(fieldType().rangeType.createFields(context, name(), range, index, hasDocValues, store));

        if (hasDocValues == false && (index || store)) {
            createFieldNamesField(context);
        }
    }

    private static Range parseIpRangeFromCidr(final XContentParser parser) throws IOException {
        final Tuple<InetAddress, Integer> cidr = InetAddresses.parseCidr(parser.text());
        // create the lower value by zeroing out the host portion, upper value by filling it with all ones.
        byte[] lower = cidr.v1().getAddress();
        byte[] upper = lower.clone();
        for (int i = cidr.v2(); i < 8 * lower.length; i++) {
            int m = 1 << 7 - (i & 7);
            lower[i >> 3] &= ~m;
            upper[i >> 3] |= m;
        }
        try {
            return new Range(RangeType.IP, InetAddress.getByAddress(lower), InetAddress.getByAddress(upper), true, true);
        } catch (UnknownHostException bogus) {
            throw new AssertionError(bogus);
        }
    }

    @Override
    protected Explicit<Boolean> ignoreMalformed() {
        return ignoreMalformed;
    }

    /**
     * Class defining a range
     *
     * @opensearch.internal
     */
    public static class Range {
        RangeType type;
        Object from;
        Object to;
        private final boolean includeFrom;
        private final boolean includeTo;

        public Range(RangeType type, Object from, Object to, boolean includeFrom, boolean includeTo) {
            this.type = type;
            this.from = from;
            this.to = to;
            this.includeFrom = includeFrom;
            this.includeTo = includeTo;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Range range = (Range) o;
            return includeFrom == range.includeFrom
                && includeTo == range.includeTo
                && type == range.type
                && from.equals(range.from)
                && to.equals(range.to);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, from, to, includeFrom, includeTo);
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(includeFrom ? '[' : '(');
            Object f = includeFrom || from.equals(type.minValue()) ? from : type.nextDown(from);
            Object t = includeTo || to.equals(type.maxValue()) ? to : type.nextUp(to);
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress) f) : f.toString());
            sb.append(" : ");
            sb.append(type == RangeType.IP ? InetAddresses.toAddrString((InetAddress) t) : t.toString());
            sb.append(includeTo ? ']' : ')');
            return sb.toString();
        }

        public Object getFrom() {
            return from;
        }

        public Object getTo() {
            return to;
        }
    }

    /**
     * Doc values field for binary ranges
     *
     * @opensearch.internal
     */
    static class BinaryRangesDocValuesField extends CustomDocValuesField {

        private final Set<Range> ranges;
        private final RangeType rangeType;

        BinaryRangesDocValuesField(String name, Range range, RangeType rangeType) {
            super(name);
            this.rangeType = rangeType;
            ranges = new HashSet<>();
            add(range);
        }

        void add(Range range) {
            ranges.add(range);
        }

        @Override
        public BytesRef binaryValue() {
            try {
                return rangeType.encodeRanges(ranges);
            } catch (IOException e) {
                throw new OpenSearchException("failed to encode ranges", e);
            }
        }

        @Override
        public StoredValue storedValue() {
            return null;
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }
    }
}
