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

import com.ibm.icu.text.Collator;
import com.ibm.icu.text.RawCollationKey;
import com.ibm.icu.text.RuleBasedCollator;
import com.ibm.icu.util.ULocale;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.IndexableBinaryStringTools;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

public class ICUCollationKeywordFieldMapper extends FieldMapper {

    public static final String CONTENT_TYPE = "icu_collation_keyword";

    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

        public static final int IGNORE_ABOVE = Integer.MAX_VALUE;
    }

    public static final class CollationFieldType extends StringFieldType {
        private final Collator collator;
        private final String nullValue;
        private final int ignoreAbove;

        public CollationFieldType(
            String name,
            boolean isSearchable,
            boolean isStored,
            boolean hasDocValues,
            Collator collator,
            String nullValue,
            int ignoreAbove,
            Map<String, String> meta
        ) {
            super(name, isSearchable, isStored, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.collator = collator;
            this.nullValue = nullValue;
            this.ignoreAbove = ignoreAbove;
        }

        public CollationFieldType(String name, boolean searchable, Collator collator) {
            this(name, searchable, false, true, collator, null, Integer.MAX_VALUE, Collections.emptyMap());
        }

        public CollationFieldType(String name, Collator collator) {
            this(name, true, false, true, collator, null, Integer.MAX_VALUE, Collections.emptyMap());
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
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    if (keywordValue.length() > ignoreAbove) {
                        return null;
                    }
                    return keywordValue;
                }
            };
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }

            if (collator != null) {
                RawCollationKey key = collator.getRawCollationKey(value.toString(), null);
                return new BytesRef(key.bytes, 0, key.size);
            } else {
                throw new IllegalStateException("collator is null");
            }
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            org.apache.lucene.search.MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[fuzzy] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            throw new UnsupportedOperationException("[prefix] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[wildcard] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            throw new UnsupportedOperationException("[regexp] queries are not supported on [" + CONTENT_TYPE + "] fields.");
        }

        public static DocValueFormat COLLATE_FORMAT = new DocValueFormat() {
            @Override
            public String getWriteableName() {
                return "collate";
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public String format(BytesRef value) {
                int encodedLength = IndexableBinaryStringTools.getEncodedLength(value.bytes, value.offset, value.length);
                char[] encoded = new char[encodedLength];
                IndexableBinaryStringTools.encode(value.bytes, value.offset, value.length, encoded, 0, encodedLength);
                return new String(encoded, 0, encodedLength);
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                char[] encoded = value.toCharArray();
                int decodedLength = IndexableBinaryStringTools.getDecodedLength(encoded, 0, encoded.length);
                byte[] decoded = new byte[decodedLength];
                IndexableBinaryStringTools.decode(encoded, 0, encoded.length, decoded, 0, decodedLength);
                return new BytesRef(decoded);
            }
        };

        @Override
        public DocValueFormat docValueFormat(final String format, final ZoneId timeZone) {
            return COLLATE_FORMAT;
        }
    }

    public static class Builder extends FieldMapper.Builder<Builder> {
        private String rules = null;
        private String language = null;
        private String country = null;
        private String variant = null;
        private String strength = null;
        private String decomposition = null;
        private String alternate = null;
        private boolean caseLevel = false;
        private String caseFirst = null;
        private boolean numeric = false;
        private String variableTop = null;
        private boolean hiraganaQuaternaryMode = false;
        protected int ignoreAbove = Defaults.IGNORE_ABOVE;
        protected String nullValue;

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public Builder indexOptions(IndexOptions indexOptions) {
            if (indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) > 0) {
                throw new IllegalArgumentException(
                    "The [" + CONTENT_TYPE + "] field does not support positions, got [index_options]=" + indexOptionToString(indexOptions)
                );
            }

            return super.indexOptions(indexOptions);
        }

        public Builder ignoreAbove(int ignoreAbove) {
            if (ignoreAbove < 0) {
                throw new IllegalArgumentException("[ignore_above] must be positive, got " + ignoreAbove);
            }
            this.ignoreAbove = ignoreAbove;
            return this;
        }

        public String rules() {
            return rules;
        }

        public Builder rules(final String rules) {
            this.rules = rules;
            return this;
        }

        public String language() {
            return language;
        }

        public Builder language(final String language) {
            this.language = language;
            return this;
        }

        public String country() {
            return country;
        }

        public Builder country(final String country) {
            this.country = country;
            return this;
        }

        public String variant() {
            return variant;
        }

        public Builder variant(final String variant) {
            this.variant = variant;
            return this;
        }

        public String strength() {
            return strength;
        }

        public Builder strength(final String strength) {
            this.strength = strength;
            return this;
        }

        public String decomposition() {
            return decomposition;
        }

        public Builder decomposition(final String decomposition) {
            this.decomposition = decomposition;
            return this;
        }

        public String alternate() {
            return alternate;
        }

        public Builder alternate(final String alternate) {
            this.alternate = alternate;
            return this;
        }

        public boolean caseLevel() {
            return caseLevel;
        }

        public Builder caseLevel(final boolean caseLevel) {
            this.caseLevel = caseLevel;
            return this;
        }

        public String caseFirst() {
            return caseFirst;
        }

        public Builder caseFirst(final String caseFirst) {
            this.caseFirst = caseFirst;
            return this;
        }

        public boolean numeric() {
            return numeric;
        }

        public Builder numeric(final boolean numeric) {
            this.numeric = numeric;
            return this;
        }

        public String variableTop() {
            return variableTop;
        }

        public Builder variableTop(final String variableTop) {
            this.variableTop = variableTop;
            return this;
        }

        public boolean hiraganaQuaternaryMode() {
            return hiraganaQuaternaryMode;
        }

        public Builder hiraganaQuaternaryMode(final boolean hiraganaQuaternaryMode) {
            this.hiraganaQuaternaryMode = hiraganaQuaternaryMode;
            return this;
        }

        public Builder nullValue(String nullValue) {
            this.nullValue = nullValue;
            return this;
        }

        public Collator buildCollator() {
            Collator collator;
            if (rules != null) {
                try {
                    collator = new RuleBasedCollator(rules);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse collation rules", e);
                }
            } else {
                if (language != null) {
                    ULocale locale;
                    if (country != null) {
                        if (variant != null) {
                            locale = new ULocale(language, country, variant);
                        } else {
                            locale = new ULocale(language, country);
                        }
                    } else {
                        locale = new ULocale(language);
                    }
                    collator = Collator.getInstance(locale);
                } else {
                    collator = Collator.getInstance(ULocale.ROOT);
                }
            }

            // set the strength flag, otherwise it will be the default.
            if (strength != null) {
                if (strength.equalsIgnoreCase("primary")) {
                    collator.setStrength(Collator.PRIMARY);
                } else if (strength.equalsIgnoreCase("secondary")) {
                    collator.setStrength(Collator.SECONDARY);
                } else if (strength.equalsIgnoreCase("tertiary")) {
                    collator.setStrength(Collator.TERTIARY);
                } else if (strength.equalsIgnoreCase("quaternary")) {
                    collator.setStrength(Collator.QUATERNARY);
                } else if (strength.equalsIgnoreCase("identical")) {
                    collator.setStrength(Collator.IDENTICAL);
                } else {
                    throw new IllegalArgumentException("Invalid strength: " + strength);
                }
            }

            // set the decomposition flag, otherwise it will be the default.
            if (decomposition != null) {
                if (decomposition.equalsIgnoreCase("no")) {
                    collator.setDecomposition(Collator.NO_DECOMPOSITION);
                } else if (decomposition.equalsIgnoreCase("canonical")) {
                    collator.setDecomposition(Collator.CANONICAL_DECOMPOSITION);
                } else {
                    throw new IllegalArgumentException("Invalid decomposition: " + decomposition);
                }
            }

            // expert options: concrete subclasses are always a RuleBasedCollator
            RuleBasedCollator rbc = (RuleBasedCollator) collator;
            if (alternate != null) {
                if (alternate.equalsIgnoreCase("shifted")) {
                    rbc.setAlternateHandlingShifted(true);
                } else if (alternate.equalsIgnoreCase("non-ignorable")) {
                    rbc.setAlternateHandlingShifted(false);
                } else {
                    throw new IllegalArgumentException("Invalid alternate: " + alternate);
                }
            }

            if (caseLevel) {
                rbc.setCaseLevel(true);
            }

            if (caseFirst != null) {
                if (caseFirst.equalsIgnoreCase("lower")) {
                    rbc.setLowerCaseFirst(true);
                } else if (caseFirst.equalsIgnoreCase("upper")) {
                    rbc.setUpperCaseFirst(true);
                } else {
                    throw new IllegalArgumentException("Invalid caseFirst: " + caseFirst);
                }
            }

            if (numeric) {
                rbc.setNumericCollation(true);
            }

            if (variableTop != null) {
                rbc.setVariableTop(variableTop);
            }

            if (hiraganaQuaternaryMode) {
                rbc.setHiraganaQuaternary(true);
            }

            // freeze so thread-safe
            return collator.freeze();
        }

        @Override
        public ICUCollationKeywordFieldMapper build(BuilderContext context) {
            final Collator collator = buildCollator();
            CollationFieldType ft = new CollationFieldType(
                buildFullName(context),
                indexed,
                fieldType.stored(),
                hasDocValues,
                collator,
                nullValue,
                ignoreAbove,
                meta
            );
            return new ICUCollationKeywordFieldMapper(
                name,
                fieldType,
                ft,
                multiFieldsBuilder.build(this, context),
                copyTo,
                rules,
                language,
                country,
                variant,
                strength,
                decomposition,
                alternate,
                caseLevel,
                caseFirst,
                numeric,
                variableTop,
                hiraganaQuaternaryMode,
                ignoreAbove,
                collator,
                nullValue
            );
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);
            TypeParsers.parseField(builder, name, node, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                switch (fieldName) {
                    case "null_value":
                        if (fieldNode == null) {
                            throw new MapperParsingException("Property [null_value] cannot be null.");
                        }
                        builder.nullValue(fieldNode.toString());
                        iterator.remove();
                        break;
                    case "ignore_above":
                        builder.ignoreAbove(XContentMapValues.nodeIntegerValue(fieldNode, -1));
                        iterator.remove();
                        break;
                    case "norms":
                        builder.omitNorms(!XContentMapValues.nodeBooleanValue(fieldNode, "norms"));
                        iterator.remove();
                        break;
                    case "rules":
                        builder.rules(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "language":
                        builder.language(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "country":
                        builder.country(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "variant":
                        builder.variant(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "strength":
                        builder.strength(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "decomposition":
                        builder.decomposition(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "alternate":
                        builder.alternate(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "case_level":
                        builder.caseLevel(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "case_first":
                        builder.caseFirst(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "numeric":
                        builder.numeric(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    case "variable_top":
                        builder.variableTop(XContentMapValues.nodeStringValue(fieldNode, null));
                        iterator.remove();
                        break;
                    case "hiragana_quaternary_mode":
                        builder.hiraganaQuaternaryMode(XContentMapValues.nodeBooleanValue(fieldNode, false));
                        iterator.remove();
                        break;
                    default:
                        break;
                }
            }

            return builder;
        }
    }

    private final String rules;
    private final String language;
    private final String country;
    private final String variant;
    private final String strength;
    private final String decomposition;
    private final String alternate;
    private final boolean caseLevel;
    private final String caseFirst;
    private final boolean numeric;
    private final String variableTop;
    private final boolean hiraganaQuaternaryMode;
    private int ignoreAbove;
    private final Collator collator;
    private final String nullValue;

    protected ICUCollationKeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        String rules,
        String language,
        String country,
        String variant,
        String strength,
        String decomposition,
        String alternate,
        boolean caseLevel,
        String caseFirst,
        boolean numeric,
        String variableTop,
        boolean hiraganaQuaternaryMode,
        int ignoreAbove,
        Collator collator,
        String nullValue
    ) {
        super(simpleName, fieldType, mappedFieldType, multiFields, copyTo);
        assert collator.isFrozen();
        this.rules = rules;
        this.language = language;
        this.country = country;
        this.variant = variant;
        this.strength = strength;
        this.decomposition = decomposition;
        this.alternate = alternate;
        this.caseLevel = caseLevel;
        this.caseFirst = caseFirst;
        this.numeric = numeric;
        this.variableTop = variableTop;
        this.hiraganaQuaternaryMode = hiraganaQuaternaryMode;
        this.ignoreAbove = ignoreAbove;
        this.collator = collator;
        this.nullValue = nullValue;
    }

    @Override
    public CollationFieldType fieldType() {
        return (CollationFieldType) super.fieldType();
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        ICUCollationKeywordFieldMapper icuMergeWith = (ICUCollationKeywordFieldMapper) other;
        if (!Objects.equals(collator, icuMergeWith.collator)) {
            conflicts.add("mapper [" + name() + "] has different [collator]");
        }
        if (!Objects.equals(rules, icuMergeWith.rules)) {
            conflicts.add("Cannot update parameter [rules] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(language, icuMergeWith.language)) {
            conflicts.add("Cannot update parameter [language] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(country, icuMergeWith.country)) {
            conflicts.add("Cannot update parameter [country] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(variant, icuMergeWith.variant)) {
            conflicts.add("Cannot update parameter [variant] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(strength, icuMergeWith.strength)) {
            conflicts.add("Cannot update parameter [strength] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(decomposition, icuMergeWith.decomposition)) {
            conflicts.add("Cannot update parameter [decomposition] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(alternate, icuMergeWith.alternate)) {
            conflicts.add("Cannot update parameter [alternate] for [" + CONTENT_TYPE + "]");
        }

        if (caseLevel != icuMergeWith.caseLevel) {
            conflicts.add("Cannot update parameter [case_level] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(caseFirst, icuMergeWith.caseFirst)) {
            conflicts.add("Cannot update parameter [case_first] for [" + CONTENT_TYPE + "]");
        }

        if (numeric != icuMergeWith.numeric) {
            conflicts.add("Cannot update parameter [numeric] for [" + CONTENT_TYPE + "]");
        }

        if (!Objects.equals(variableTop, icuMergeWith.variableTop)) {
            conflicts.add("Cannot update parameter [variable_top] for [" + CONTENT_TYPE + "]");
        }

        if (hiraganaQuaternaryMode != icuMergeWith.hiraganaQuaternaryMode) {
            conflicts.add("Cannot update parameter [hiragana_quaternary_mode] for [" + CONTENT_TYPE + "]");
        }

        this.ignoreAbove = icuMergeWith.ignoreAbove;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        super.doXContentBody(builder, includeDefaults, params);
        if (fieldType.indexOptions() != IndexOptions.NONE && (includeDefaults || fieldType.indexOptions() != IndexOptions.DOCS)) {
            builder.field("index_options", indexOptionToString(fieldType.indexOptions()));
        }
        if (nullValue != null) {
            builder.field("null_value", nullValue);
        }
        if (includeDefaults || fieldType.omitNorms() != KeywordFieldMapper.Defaults.FIELD_TYPE.omitNorms()) {
            builder.field("norms", fieldType.omitNorms() == false);
        }
        if (includeDefaults || rules != null) {
            builder.field("rules", rules);
        }

        if (includeDefaults || language != null) {
            builder.field("language", language);
        }

        if (includeDefaults || country != null) {
            builder.field("country", country);
        }

        if (includeDefaults || variant != null) {
            builder.field("variant", variant);
        }

        if (includeDefaults || strength != null) {
            builder.field("strength", strength);
        }

        if (includeDefaults || decomposition != null) {
            builder.field("decomposition", decomposition);
        }

        if (includeDefaults || alternate != null) {
            builder.field("alternate", alternate);
        }

        if (includeDefaults || caseLevel) {
            builder.field("case_level", caseLevel);
        }

        if (includeDefaults || caseFirst != null) {
            builder.field("case_first", caseFirst);
        }

        if (includeDefaults || numeric) {
            builder.field("numeric", numeric);
        }

        if (includeDefaults || variableTop != null) {
            builder.field("variable_top", variableTop);
        }

        if (includeDefaults || hiraganaQuaternaryMode) {
            builder.field("hiragana_quaternary_mode", hiraganaQuaternaryMode);
        }

        if (includeDefaults || ignoreAbove != Defaults.IGNORE_ABOVE) {
            builder.field("ignore_above", ignoreAbove);
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            XContentParser parser = context.parser();
            if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
                value = nullValue;
            } else {
                value = parser.textOrNull();
            }
        }

        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        RawCollationKey key = collator.getRawCollationKey(value, null);
        final BytesRef binaryValue = new BytesRef(key.bytes, 0, key.size);

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(mappedFieldType.name(), binaryValue, fieldType);
            context.doc().add(field);
        }

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        } else if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            createFieldNamesField(context);
        }
    }

}
