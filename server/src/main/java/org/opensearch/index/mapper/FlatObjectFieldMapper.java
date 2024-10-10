/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

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
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
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

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            FlatObjectFieldType fft = buildFlatObjectFieldType(context, fieldtype);
            return new FlatObjectFieldMapper(name, Defaults.FIELD_TYPE, fft, CopyTo.empty());
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
            return builderFunction.apply(name, parserContext);
        }
    }

    /**
     * flat_object fields type contains its own fieldType, one valueFieldType and one valueAndPathFieldType
     * @opensearch.internal
     */
    public static final class FlatObjectFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;

        private final String rootFieldTypeName;

        private KeywordFieldMapper.KeywordFieldType valueFieldType;

        private KeywordFieldMapper.KeywordFieldType valueAndPathFieldType;

        public FlatObjectFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, false, true, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.rootFieldTypeName = null;
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
            this.rootFieldTypeName = null;
        }

        public FlatObjectFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, false, true, new TextSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.rootFieldTypeName = null;
        }

        public FlatObjectFieldType(String name, String rootFieldTypeName) {
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
            this.rootFieldTypeName = rootFieldTypeName;
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
            if (isSubfield()) {
                return super.fielddataBuilder(fullyQualifiedIndexName, searchLookup);
            } else {
                return new SortedSetOrdinalsIndexFieldData.Builder(name() + VALUE_SUFFIX, CoreValuesSourceType.BYTES);
            }
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
                        return KeywordFieldMapper.normalizeValue(normalizer, name(), flatObjectKeywordValue);
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
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return getTextSearchInfo().getSearchAnalyzer().normalize(name(), value.toString());
        }

        /**
         * redirect queries with rewrite value to rewriteSearchValue and directSubFieldName
         */
        @Override
        public Query termQuery(Object value, @Nullable QueryShardContext context) {
            failIfNotIndexed();
            final String searchValue = searchValue(inputToString(value));
            Query query = new TermQuery(new Term(searchField(), indexedValueForSearch(searchValue)));
            if (boost() != 1f) {
                query = new BoostQuery(query, boost());
            }
            return query;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexed();
            final BytesRef[] bytesRefs = new BytesRef[values.size()];
            for (int i = 0; i < bytesRefs.length; i++) {
                final String searchValue = searchValue(inputToString(values.get(i)));
                bytesRefs[i] = indexedValueForSearch(new BytesRef(searchValue));
            }

            return new TermInSetQuery(searchField(), bytesRefs);
        }

        /**
         * To direct search fields, if a dot path was used in search query,
         * then direct to flatObjectFieldName._valueAndPath subfield,
         * else, direct to flatObjectFieldName._value subfield.
         * @return directedSubFieldName
         */
        String searchField() {
            return isSubfield() ? rootFieldTypeName + VALUE_AND_PATH_SUFFIX : name() + VALUE_SUFFIX;
        }

        /**
         * If the search key has mappedFieldTypeName as prefix,
         * then the dot path was used in search query,
         * then rewrite the searchValueString as the format "dotpath=value",
         * @return rewriteSearchValue
         */
        String searchValue(String searchValueString) {
            return isSubfield() ? name() + EQUAL_SYMBOL + searchValueString : searchValueString;
        }

        private boolean isSubfield() {
            return rootFieldTypeName != null;
        }

        private String inputToString(Object inputValue) {
            if (inputValue instanceof BytesRef) {
                return ((BytesRef) inputValue).utf8ToString();
            } else {
                return inputValue.toString();
            }
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
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
            final String searchField = searchField();
            final String searchValue = searchValue(value);
            if (caseInsensitive) {
                return AutomatonQueries.caseInsensitivePrefixQuery((new Term(searchField, indexedValueForSearch(searchValue))), method);
            }
            return new PrefixQuery(new Term(searchField, indexedValueForSearch(searchValue)), method);
        }

        @Override
        public Query rangeQuery(Object lowerTerm, Object upperTerm, boolean includeLower, boolean includeUpper, QueryShardContext context) {
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[range] queries on [text] or [keyword] fields cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false."
                );
            }
            failIfNotIndexed();
            final String searchField = searchField();
            final String rewriteUpperTerm = searchValue(inputToString(upperTerm));
            final String rewriteLowerTerm = searchValue(inputToString(lowerTerm));
            return new TermRangeQuery(
                searchField,
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
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
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

    private final String valueFieldName;
    private final String valueAndPathFieldName;

    FlatObjectFieldMapper(String simpleName, FieldType fieldType, FlatObjectFieldType mappedFieldType, CopyTo copyTo) {
        super(simpleName, fieldType, mappedFieldType, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.fieldType = fieldType;
        this.mappedFieldType = mappedFieldType;
        this.valueFieldName = mappedFieldType.name() + VALUE_SUFFIX;
        this.valueAndPathFieldName = mappedFieldType.name() + VALUE_AND_PATH_SUFFIX;
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
        XContentParser ctxParser = context.parser();
        if (fieldType().isSearchable() == false && fieldType().isStored() == false && fieldType().hasDocValues() == false) {
            ctxParser.skipChildren();
            return;
        }
        if (ctxParser.currentToken() != XContentParser.Token.VALUE_NULL) {
            if (ctxParser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new ParsingException(
                    ctxParser.getTokenLocation(),
                    "[" + this.name() + "] unexpected token [" + ctxParser.currentToken() + "] in flat_object field value"
                );
            }
            parseObject(ctxParser, context);
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    private void parseObject(XContentParser parser, ParseContext context) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        parser.nextToken(); // Skip the outer START_OBJECT. Need to return on END_OBJECT.

        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        HashSet<String> leafPaths = new HashSet<>();
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseToken(parser, context, path, leafPaths);
        }

        // create FieldNamesField for exist query
        createFieldNamesField(context, leafPaths);
    }

    private void createFieldNamesField(ParseContext context, HashSet<String> leafPaths) {
        FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = context.docMapper()
            .metadataMapper(FieldNamesFieldMapper.class)
            .fieldType();
        if (fieldNamesFieldType != null && fieldNamesFieldType.isEnabled()) {
            final HashSet<String> fieldNames = new HashSet<>();
            for (String path : leafPaths) {
                for (String field : FieldNamesFieldMapper.extractFieldNames(path)) {
                    fieldNames.add(field);
                }
            }
            for (String fieldName : fieldNames) {
                context.doc().add(new Field(FieldNamesFieldMapper.NAME, fieldName, FieldNamesFieldMapper.Defaults.FIELD_TYPE));
            }
        }
    }

    private void parseToken(XContentParser parser, ParseContext context, Deque<String> path, HashSet<String> pathSet) throws IOException {
        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
            final String currentFieldName = parser.currentName();
            path.addLast(currentFieldName); // Pushing onto the stack *must* be matched by pop
            parser.nextToken(); // advance to the value of fieldName
            parseToken(parser, context, path, pathSet); // parse the value for fieldName (which will be an array, an object,
            // or a primitive value)
            path.removeLast(); // Here is where we pop fieldName from the stack (since we're done with the value of fieldName)
            // Note that whichever other branch we just passed through has already ended with nextToken(), so we
            // don't need to call it.
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                parseToken(parser, context, path, pathSet);
            }
            parser.nextToken();
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                parseToken(parser, context, path, pathSet);
            }
            parser.nextToken();
        } else {
            String value = parseValue(parser);
            if (value == null || value.length() > fieldType().ignoreAbove) {
                parser.nextToken();
                return;
            }
            NamedAnalyzer normalizer = fieldType().normalizer();
            if (normalizer != null) {
                value = KeywordFieldMapper.normalizeValue(normalizer, name(), value);
            }
            final String leafPath = Strings.collectionToDelimitedString(path, ".");
            pathSet.add(leafPath);
            final String valueAndPath = leafPath + EQUAL_SYMBOL + value;
            if (fieldType().isSearchable() || fieldType().isStored()) {
                context.doc().add(new KeywordFieldMapper.KeywordField(valueFieldName, new BytesRef(value), fieldType));
                context.doc().add(new KeywordFieldMapper.KeywordField(valueAndPathFieldName, new BytesRef(valueAndPath), fieldType));
            }
            if (fieldType().hasDocValues()) {
                // TODO: remove prefix 'fieldTypeName + DOT_SYMBOL'
                context.doc().add(new SortedSetDocValuesField(valueFieldName, new BytesRef(name() + DOT_SYMBOL + value)));
                context.doc().add(new SortedSetDocValuesField(valueAndPathFieldName, new BytesRef(name() + DOT_SYMBOL + valueAndPath)));
            }
            parser.nextToken();
        }
    }

    private static String parseValue(XContentParser parser) throws IOException {
        switch (parser.currentToken()) {
            case VALUE_BOOLEAN:
            case VALUE_NUMBER:
            case VALUE_STRING:
            case VALUE_NULL:
                return parser.textOrNull();
            // Handle other token types as needed
            default:
                throw new ParsingException(parser.getTokenLocation(), "Unexpected value token type [" + parser.currentToken() + "]");
        }
    }

}
