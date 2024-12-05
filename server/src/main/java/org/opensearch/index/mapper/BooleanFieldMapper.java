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

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Booleans;
import org.opensearch.common.Nullable;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexNumericFieldData.NumericType;
import org.opensearch.index.fielddata.plain.SortedNumericIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * A field mapper for boolean fields.
 *
 * @opensearch.internal
 */
public class BooleanFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "boolean";

    /**
     * Default parameters for the boolean field mapper
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.freeze();
        }
    }

    /**
     * Values that can be used for this field mapper
     *
     * @opensearch.internal
     */
    public static class Values {
        public static final BytesRef TRUE = new BytesRef("T");
        public static final BytesRef FALSE = new BytesRef("F");
    }

    private static BooleanFieldMapper toType(FieldMapper in) {
        return (BooleanFieldMapper) in;
    }

    /**
     * Builder for this field mapper
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> docValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).stored, false);

        private final Parameter<Boolean> nullValue = new Parameter<>(
            "null_value",
            false,
            () -> null,
            (n, c, o) -> o == null ? null : XContentMapValues.nodeBooleanValue(o),
            m -> toType(m).nullValue
        ).acceptsNull();

        private final Parameter<Float> boost = Parameter.boostParam();
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(meta, boost, docValues, indexed, nullValue, stored);
        }

        @Override
        public BooleanFieldMapper build(BuilderContext context) {
            MappedFieldType ft = new BooleanFieldType(
                buildFullName(context),
                indexed.getValue(),
                stored.getValue(),
                docValues.getValue(),
                nullValue.getValue(),
                meta.getValue()
            );
            ft.setBoost(boost.getValue());
            return new BooleanFieldMapper(name, ft, multiFieldsBuilder.build(this, context), copyTo.build(), this);
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * Field type for boolean field mapper
     *
     * @opensearch.internal
     */
    public static final class BooleanFieldType extends TermBasedFieldType {

        private final Boolean nullValue;

        public BooleanFieldType(
            String name,
            boolean isSearchable,
            boolean isStored,
            boolean hasDocValues,
            Boolean nullValue,
            Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            this.nullValue = nullValue;
        }

        public BooleanFieldType(String name) {
            this(name, true, false, true, false, Collections.emptyMap());
        }

        public BooleanFieldType(String name, boolean searchable) {
            this(name, searchable, false, true, false, Collections.emptyMap());
        }

        public BooleanFieldType(String name, boolean searchable, boolean hasDocValues) {
            this(name, searchable, false, hasDocValues, false, Collections.emptyMap());
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected Boolean parseSourceValue(Object value) {
                    if (value instanceof Boolean) {
                        return (Boolean) value;
                    } else {
                        String textValue = value.toString();
                        return Booleans.parseBooleanStrict(textValue, false);
                    }
                }
            };
        }

        @Override
        public BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return Values.FALSE;
            }
            if (value instanceof Boolean) {
                return ((Boolean) value) ? Values.TRUE : Values.FALSE;
            }
            String sValue;
            if (value instanceof BytesRef) {
                sValue = ((BytesRef) value).utf8ToString();
            } else {
                sValue = value.toString();
            }
            switch (sValue) {
                case "true":
                    return Values.TRUE;
                case "false":
                    return Values.FALSE;
                default:
                    throw new IllegalArgumentException("Can't parse boolean value [" + sValue + "], expected [true] or [false]");
            }
        }

        @Override
        public Boolean valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            switch (value.toString()) {
                case "F":
                    return false;
                case "T":
                    return true;
                default:
                    throw new IllegalArgumentException("Expected [T] or [F] but got [" + value + "]");
            }
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedNumericIndexFieldData.Builder(name(), NumericType.BOOLEAN);
        }

        @Override
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            return DocValueFormat.BOOLEAN;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            if (!isSearchable()) {
                return SortedNumericDocValuesField.newSlowExactQuery(name(), Values.TRUE.bytesEquals(indexedValueForSearch(value)) ? 1 : 0);
            }
            Query query = new TermQuery(new Term(name(), indexedValueForSearch(value)));
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            int distinct = 0;
            Set<?> distinctValues = new HashSet<>(values);
            for (Object value : distinctValues) {
                if (Values.TRUE.equals(indexedValueForSearch(value))) {
                    distinct |= 2;
                } else if (Values.FALSE.equals(indexedValueForSearch(value))) {
                    distinct |= 1;
                }
                if (distinct == 3) {
                    return this.existsQuery(context);
                }
            }
            switch (distinct) {
                case 1:
                    return termQuery("false", context);
                case 2:
                    return termQuery("true", context);
            }

            return new MatchNoDocsQuery("Values did not contain True or False");
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            if (lowerTerm == null) {
                lowerTerm = false;
                includeLower = true;

            }
            if (upperTerm == null) {
                upperTerm = true;
                includeUpper = true;

            }

            lowerTerm = indexedValueForSearch(lowerTerm);
            upperTerm = indexedValueForSearch(upperTerm);

            if (lowerTerm == upperTerm) {
                if (!includeLower || !includeUpper) {
                    return new MatchNoDocsQuery();
                }
                return termQuery(lowerTerm.equals(Values.TRUE), context);
            }

            if (lowerTerm.equals(Values.TRUE)) {
                return new MatchNoDocsQuery();
            }
            if (!includeLower && !includeUpper) {
                return new MatchNoDocsQuery();
            } else if (!includeLower) {
                return termQuery(true, context);
            } else if (!includeUpper) {
                return termQuery(false, context);
            } else {
                return this.existsQuery(context);
            }

        }
    }

    private final Boolean nullValue;
    private final boolean indexed;
    private final boolean hasDocValues;
    private final boolean stored;

    protected BooleanFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        this.nullValue = builder.nullValue.getValue();
        this.stored = builder.stored.getValue();
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.docValues.getValue();
    }

    @Override
    public BooleanFieldType fieldType() {
        return (BooleanFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        if (indexed == false && stored == false && hasDocValues == false) {
            return;
        }

        Boolean value = context.parseExternalValue(Boolean.class);
        if (value == null) {
            XContentParser.Token token = context.parser().currentToken();
            if (token == XContentParser.Token.VALUE_NULL) {
                if (nullValue != null) {
                    value = nullValue;
                }
            } else {
                value = context.parser().booleanValue();
            }
        }

        if (value == null) {
            return;
        }
        if (indexed) {
            context.doc().add(new Field(fieldType().name(), value ? "T" : "F", Defaults.FIELD_TYPE));
        }
        if (stored) {
            context.doc().add(new StoredField(fieldType().name(), value ? "T" : "F"));
        }
        if (hasDocValues) {
            context.doc().add(new SortedNumericDocValuesField(fieldType().name(), value ? 1 : 0));
        } else {
            createFieldNamesField(context);
        }
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
