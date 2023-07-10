/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Iterators;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.JsonToStringXContentParser;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A field mapper for flat_objects.
 * This mapper accepts JSON object and treat as string fields in one index.
 * @opensearch.internal
 */
public final class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "flat_object";
    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";
    private static final String DOT_SYMBOL = ".";
    private static final String EQUAL_SYMBOL = "=";

    /**
     * In flat_object field mapper, field type is similar to keyword field type
     * Cannot be tokenized, can OmitNorms, and can setIndexOption.
     * @opensearch.internal
     */
    public static class Defaults {
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.freeze();
        }

    }

    @Override
    public MappedFieldType keyedFieldType(String key) {
        return new FlatObjectFieldType(this.name() + DOT_SYMBOL + key, this.name());
    }

    /**
     * FlatObjectFieldType is the parent field type.
     */
    public static class FlatObjectField extends Field {

        public FlatObjectField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }

    }

    /**
     * The builder for the flat_object field mapper using default parameters
     * @opensearch.internal
     */
    public static class Builder extends FieldMapper.Builder<Builder> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
        }

        private FlatObjectFieldType buildFlatObjectFieldType(BuilderContext context, FieldType fieldType) {
            return new FlatObjectFieldType(buildFullName(context), fieldType);
        }

        /**
         * ValueFieldMapper is the subfield type for values in the Json.
         * use a {@link KeywordFieldMapper.KeywordField}
         */
        private ValueFieldMapper buildValueFieldMapper(BuilderContext context, FieldType fieldType, FlatObjectFieldType fft) {
            String fullName = buildFullName(context);
            FieldType vft = new FieldType(fieldType);
            KeywordFieldMapper.KeywordFieldType valueFieldType = new KeywordFieldMapper.KeywordFieldType(fullName + VALUE_SUFFIX, vft);

            fft.setValueFieldType(valueFieldType);
            return new ValueFieldMapper(vft, valueFieldType);
        }

        /**
         * ValueAndPathFieldMapper is the subfield type for path=value format in the Json.
         * also use a {@link KeywordFieldMapper.KeywordField}
         */
        private ValueAndPathFieldMapper buildValueAndPathFieldMapper(BuilderContext context, FieldType fieldType, FlatObjectFieldType fft) {
            String fullName = buildFullName(context);
            FieldType vft = new FieldType(fieldType);
            KeywordFieldMapper.KeywordFieldType ValueAndPathFieldType = new KeywordFieldMapper.KeywordFieldType(
                fullName + VALUE_AND_PATH_SUFFIX,
                vft
            );
            fft.setValueAndPathFieldType(ValueAndPathFieldType);
            return new ValueAndPathFieldMapper(vft, ValueAndPathFieldType);
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            FlatObjectFieldType fft = buildFlatObjectFieldType(context, fieldtype);
            return new FlatObjectFieldMapper(
                name,
                Defaults.FIELD_TYPE,
                fft,
                buildValueFieldMapper(context, fieldtype, fft),
                buildValueAndPathFieldMapper(context, fieldtype, fft),
                CopyTo.empty(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    /**
     * Creates a new TypeParser for flatObjectFieldMapper that does not use ParameterizedFieldMapper
     */
    public static class TypeParser implements Mapper.TypeParser {
        private final BiFunction<String, ParserContext, Builder> builderFunction;

        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            return builder;
        }
    }

    /**
     * flat_object fields type contains its own fieldType, one valueFieldType and one valueAndPathFieldType
     * @opensearch.internal
     */
    public static final class FlatObjectFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;

        private final String mappedFieldTypeName;

        private KeywordFieldMapper.KeywordFieldType valueFieldType;

        private KeywordFieldMapper.KeywordFieldType valueAndPathFieldType;

        public FlatObjectFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.mappedFieldTypeName = null;
        }

        public FlatObjectFieldType(String name, FieldType fieldType) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                false,
                true,
                new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.mappedFieldTypeName = null;
        }

        public FlatObjectFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, false, true, new TextSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.mappedFieldTypeName = null;
        }

        public FlatObjectFieldType(String name, String mappedFieldTypeName) {
            super(
                name,
                true,
                false,
                true,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.mappedFieldTypeName = mappedFieldTypeName;
        }

        void setValueFieldType(KeywordFieldMapper.KeywordFieldType valueFieldType) {
            this.valueFieldType = valueFieldType;
        }

        void setValueAndPathFieldType(KeywordFieldMapper.KeywordFieldType ValueAndPathFieldType) {
            this.valueAndPathFieldType = ValueAndPathFieldType;
        }

        public KeywordFieldMapper.KeywordFieldType getValueFieldType() {
            return this.valueFieldType;
        }

        public KeywordFieldMapper.KeywordFieldType getValueAndPathFieldType() {
            return this.valueAndPathFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        NamedAnalyzer normalizer() {
            return indexAnalyzer();
        }

        /**
         *
         * Fielddata is an in-memory data structure that is used for aggregations, sorting, and scripting.
         * @param fullyQualifiedIndexName the name of the index this field-data is build for
         * @param searchLookup a {@link SearchLookup} supplier to allow for accessing other fields values in the context of runtime fields
         * @return IndexFieldData.Builder
         */
        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            failIfNoDocValues();
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String flatObjectKeywordValue = value.toString();

                    if (flatObjectKeywordValue.length() > ignoreAbove) {
                        return null;
                    }

                    NamedAnalyzer normalizer = normalizer();
                    if (normalizer == null) {
                        return flatObjectKeywordValue;
                    }

                    try {
                        return normalizeValue(normalizer, name(), flatObjectKeywordValue);
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
            };
        }

        @Override
        public Object valueForDisplay(Object value) {
            if (value == null) {
                return null;
            }
            // flat_objects are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // flat_object analyzer with the default attribute source which encodes terms using UTF8
                // in that case we skip normalization, which may be slow if there many terms need to
                // parse (eg. large terms query) since Analyzer.normalize involves things like creating
                // attributes through reflection
                // This if statement will be used whenever a normalizer is NOT configured
                return super.indexedValueForSearch(value);
            }

            if (value == null) {
                return null;
            }
            value = inputToString(value);
            return getTextSearchInfo().getSearchAnalyzer().normalize(name(), value.toString());
        }

        /**
         * redirect queries with rewrite value to rewriteSearchValue and directSubFieldName
         */
        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {

            String searchValueString = inputToString(value);
            String directSubFieldName = directSubfield();
            String rewriteSearchValue = rewriteValue(searchValueString);

            failIfNotIndexed();
            Query query;
            query = new TermQuery(new Term(directSubFieldName, indexedValueForSearch(rewriteSearchValue)));
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            String directedSearchFieldName = directSubfield();
            BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                String rewriteValues = rewriteValue(inputToString(values.get(i)));

                bytesRefs[i] = indexedValueForSearch(new BytesRef(rewriteValues));

            }

            return new TermInSetQuery(directedSearchFieldName, bytesRefs);
        }

        /**
         * To direct search fields, if a dot path was used in search query,
         * then direct to flatObjectFieldName._valueAndPath subfield,
         * else, direct to flatObjectFieldName._value subfield.
         * @return directedSubFieldName
         */
        public String directSubfield() {
            if (mappedFieldTypeName == null) {
                return new StringBuilder().append(this.name()).append(VALUE_SUFFIX).toString();
            } else {
                return new StringBuilder().append(this.mappedFieldTypeName).append(VALUE_AND_PATH_SUFFIX).toString();
            }
        }

        /**
         * If the search key has mappedFieldTypeName as prefix,
         * then the dot path was used in search query,
         * then rewrite the searchValueString as the format "dotpath=value",
         * @return rewriteSearchValue
         */
        public String rewriteValue(String searchValueString) {
            if (!hasMappedFieldTyeNameInQueryFieldName(name())) {
                return searchValueString;
            } else {
                String rewriteSearchValue = new StringBuilder().append(name()).append(EQUAL_SYMBOL).append(searchValueString).toString();
                return rewriteSearchValue;
            }

        }

        private boolean hasMappedFieldTyeNameInQueryFieldName(String input) {
            String prefix = this.mappedFieldTypeName;
            if (prefix == null) {
                return false;
            }
            if (!input.startsWith(prefix)) {
                return false;
            }
            String rest = input.substring(prefix.length());

            if (rest.isEmpty()) {
                return false;
            } else {
                return true;
            }
        }

        private String inputToString(Object inputValue) {
            if (inputValue instanceof Integer) {
                String inputToString = Integer.toString((Integer) inputValue);
                return inputToString;
            } else if (inputValue instanceof Float) {
                String inputToString = Float.toString((Float) inputValue);
                return inputToString;
            } else if (inputValue instanceof Boolean) {
                String inputToString = Boolean.toString((Boolean) inputValue);
                return inputToString;
            } else if (inputValue instanceof Short) {
                String inputToString = Short.toString((Short) inputValue);
                return inputToString;
            } else if (inputValue instanceof Long) {
                String inputToString = Long.toString((Long) inputValue);
                return inputToString;
            } else if (inputValue instanceof Double) {
                String inputToString = Double.toString((Double) inputValue);
                return inputToString;
            } else if (inputValue instanceof BytesRef) {
                String inputToString = (((BytesRef) inputValue).utf8ToString());
                return inputToString;
            } else if (inputValue instanceof String) {
                String inputToString = (String) inputValue;
                return inputToString;
            } else if (inputValue instanceof Version) {
                String inputToString = inputValue.toString();
                return inputToString;
            } else {
                // default to cast toString
                return inputValue.toString();
            }
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            String directSubfield = directSubfield();
            String rewriteValue = rewriteValue(value);

            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[prefix] queries cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false. For optimised prefix queries on text "
                        + "fields please enable [index_prefixes]."
                );
            }
            failIfNotIndexed();
            if (method == null) {
                method = MultiTermQuery.CONSTANT_SCORE_REWRITE;
            }
            if (caseInsensitive) {
                return AutomatonQueries.caseInsensitivePrefixQuery((new Term(directSubfield, indexedValueForSearch(rewriteValue))), method);
            }
            return new PrefixQuery(new Term(directSubfield, indexedValueForSearch(rewriteValue)), method);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            String directSubfield = directSubfield();
            String rewriteUpperTerm = rewriteValue(inputToString(upperTerm));
            String rewriteLowerTerm = rewriteValue(inputToString(lowerTerm));
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[range] queries on [text] or [keyword] fields cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false."
                );
            }
            failIfNotIndexed();
            return new TermRangeQuery(
                directSubfield,
                lowerTerm == null ? null : indexedValueForSearch(rewriteLowerTerm),
                upperTerm == null ? null : indexedValueForSearch(rewriteUpperTerm),
                includeLower,
                includeUpper
            );
        }

        /**
         * if there is dot path. query the field name in flatObject parent field (mappedFieldTypeName).
         * else query in _field_names system field
         */
        @Override
        public Query existsQuery(QueryShardContext context) {
            String searchKey;
            String searchField;
            if (hasMappedFieldTyeNameInQueryFieldName(name())) {
                searchKey = this.mappedFieldTypeName;
                searchField = name();
            } else {
                searchKey = FieldNamesFieldMapper.NAME;
                searchField = name();
            }
            return new TermQuery(new Term(searchKey, indexedValueForSearch(searchField)));
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            QueryShardContext context
        ) {
            // flat_object field types are always normalized, so ignore case sensitivity and force normalize the wildcard
            // query text
            throw new QueryShardException(
                context,
                "Can only use wildcard queries on keyword and text fields - not on [" + name() + "] which is of type [" + typeName() + "]"
            );

        }

    }

    private final ValueFieldMapper valueFieldMapper;
    private final ValueAndPathFieldMapper valueAndPathFieldMapper;

    FlatObjectFieldMapper(
        String simpleName,
        FieldType fieldType,
        FlatObjectFieldType mappedFieldType,
        ValueFieldMapper valueFieldMapper,
        ValueAndPathFieldMapper valueAndPathFieldMapper,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, fieldType, mappedFieldType, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.fieldType = fieldType;
        this.valueFieldMapper = valueFieldMapper;
        this.valueAndPathFieldMapper = valueAndPathFieldMapper;
        this.mappedFieldType = mappedFieldType;
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {

    }

    @Override
    public FlatObjectFieldType fieldType() {
        return (FlatObjectFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String fieldName = null;

        if (context.externalValueSet()) {
            String value = context.externalValue().toString();
            parseValueAddFields(context, value, fieldType().name());
        } else {
            JsonToStringXContentParser JsonToStringParser = new JsonToStringXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                context,
                fieldType().name()
            );
            /**
             * JsonToStringParser is the main parser class to transform JSON into stringFields in a XContentParser
             * It reads the JSON object and parsed to a list of string
             */
            XContentParser parser = JsonToStringParser.parseObject();

            XContentParser.Token currentToken;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                switch (currentToken) {
                    case FIELD_NAME:
                        fieldName = parser.currentName();
                        break;
                    case VALUE_STRING:
                        String value = parser.textOrNull();
                        parseValueAddFields(context, value, fieldName);
                        break;
                }

            }

        }

    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        if (valueFieldMapper != null) {
            subIterators.add(valueFieldMapper);
        }
        if (valueAndPathFieldMapper != null) {
            subIterators.add(valueAndPathFieldMapper);
        }
        if (subIterators.size() == 0) {
            return super.iterator();
        }
        @SuppressWarnings("unchecked")
        Iterator<Mapper> concat = Iterators.concat(super.iterator(), subIterators.iterator());
        return concat;
    }

    /**
     * parseValueAddFields method will store data to Lucene.
     * the JsonToStringXContentParser returns XContentParser with 3 string fields
     * fieldName, fieldName._value, fieldName._valueAndPath.
     * parseValueAddFields recognized string by the stringfield name,
     * fieldName will be store through the parent FlatObjectFieldMapper,which contains all the keys
     * fieldName._value will be store through the valueFieldMapper, which contains the values of the Json Object
     * fieldName._valueAndPath will be store through the valueAndPathFieldMapper, which contains the "path=values" format
     */
    private void parseValueAddFields(ParseContext context, String value, String fieldName) throws IOException {

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, name(), value);
        }

        String[] valueTypeList = fieldName.split("\\._");
        String valueType = "._" + valueTypeList[valueTypeList.length - 1];

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            // convert to utf8 only once before feeding postings/dv/stored fields

            final BytesRef binaryValue = new BytesRef(fieldType().name() + DOT_SYMBOL + value);
            Field field = new FlatObjectField(fieldType().name(), binaryValue, fieldType);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
            if (fieldName.equals(fieldType().name())) {
                context.doc().add(field);
            }
            if (valueType.equals(VALUE_SUFFIX)) {
                if (valueFieldMapper != null) {
                    valueFieldMapper.addField(context, value);
                }
            }
            if (valueType.equals(VALUE_AND_PATH_SUFFIX)) {
                if (valueAndPathFieldMapper != null) {
                    valueAndPathFieldMapper.addField(context, value);
                }
            }

            if (fieldType().hasDocValues()) {
                if (fieldName.equals(fieldType().name())) {
                    context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
                }
                if (valueType.equals(VALUE_SUFFIX)) {
                    if (valueFieldMapper != null) {
                        context.doc().add(new SortedSetDocValuesField(fieldType().name() + VALUE_SUFFIX, binaryValue));
                    }
                }
                if (valueType.equals(VALUE_AND_PATH_SUFFIX)) {
                    if (valueAndPathFieldMapper != null) {
                        context.doc().add(new SortedSetDocValuesField(fieldType().name() + VALUE_AND_PATH_SUFFIX, binaryValue));
                    }
                }
            }

        }

    }

    private static String normalizeValue(NamedAnalyzer normalizer, String field, String value) throws IOException {
        String normalizerErrorMessage = "The normalization token stream is "
            + "expected to produce exactly 1 token, but got 0 for analyzer "
            + normalizer
            + " and input \""
            + value
            + "\"";
        try (TokenStream ts = normalizer.tokenStream(field, value)) {
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            if (ts.incrementToken() == false) {
                throw new IllegalStateException(normalizerErrorMessage);
            }
            final String newValue = termAtt.toString();
            if (ts.incrementToken()) {
                throw new IllegalStateException(normalizerErrorMessage);
            }
            ts.end();
            return newValue;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private static final class ValueAndPathFieldMapper extends FieldMapper {

        protected ValueAndPathFieldMapper(FieldType fieldType, KeywordFieldMapper.KeywordFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            final BytesRef binaryValue = new BytesRef(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                Field field = new KeywordFieldMapper.KeywordField(fieldType().name(), binaryValue, fieldType);

                context.doc().add(field);

                if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                    createFieldNamesField(context);
                }
            }
        }

        @Override
        protected void parseCreateField(ParseContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected String contentType() {
            return "valueAndPath";
        }

        @Override
        public String toString() {
            return fieldType().toString();
        }

    }

    private static final class ValueFieldMapper extends FieldMapper {

        protected ValueFieldMapper(FieldType fieldType, KeywordFieldMapper.KeywordFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            final BytesRef binaryValue = new BytesRef(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                Field field = new KeywordFieldMapper.KeywordField(fieldType().name(), binaryValue, fieldType);
                context.doc().add(field);

                if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                    createFieldNamesField(context);
                }
            }
        }

        @Override
        protected void parseCreateField(ParseContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void mergeOptions(FieldMapper other, List<String> conflicts) {

        }

        @Override
        protected String contentType() {
            return "value";
        }

        @Override
        public String toString() {
            return fieldType().toString();
        }
    }

}
