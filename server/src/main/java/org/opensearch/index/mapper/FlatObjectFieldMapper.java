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
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.opensearch.index.mapper.FlatObjectFieldMapper.FlatObjectFieldType.getKeywordFieldType;
import static org.opensearch.index.mapper.KeywordFieldMapper.normalizeValue;
import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;
import static org.apache.lucene.search.MultiTermQuery.DOC_VALUES_REWRITE;

/**
 * A field mapper for flat_objects.
 * This mapper accepts JSON object and treat as string fields in one index.
 * @opensearch.internal
 */
public final class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "flat_object";
    public static final Object DOC_VALUE_NO_MATCH = new Object();

    static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    static final String VALUE_SUFFIX = "._value";
    static final String DOT_SYMBOL = ".";
    static final String EQUAL_SYMBOL = "=";

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
        return new FlatObjectFieldType(
            Strings.isNullOrEmpty(key) ? this.name() : (this.name() + DOT_SYMBOL + key),
            this.name(),
            valueFieldType,
            valueAndPathFieldType
        );
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

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            boolean isSearchable = true;
            boolean hasDocValue = true;
            KeywordFieldType valueFieldType = getKeywordFieldType(buildFullName(context), VALUE_SUFFIX, isSearchable, hasDocValue);
            KeywordFieldType valueAndPathFieldType = getKeywordFieldType(
                buildFullName(context),
                VALUE_AND_PATH_SUFFIX,
                isSearchable,
                hasDocValue
            );
            FlatObjectFieldType fft = new FlatObjectFieldType(buildFullName(context), null, valueFieldType, valueAndPathFieldType);

            return new FlatObjectFieldMapper(name, Defaults.FIELD_TYPE, fft);
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
        private final String rootFieldName;
        private final KeywordFieldType valueFieldType;
        private final KeywordFieldType valueAndPathFieldType;

        public FlatObjectFieldType(String name, String rootFieldName, boolean isSearchable, boolean hasDocValues) {
            this(
                name,
                rootFieldName,
                getKeywordFieldType(rootFieldName == null ? name : rootFieldName, VALUE_SUFFIX, isSearchable, hasDocValues),
                getKeywordFieldType(rootFieldName == null ? name : rootFieldName, VALUE_AND_PATH_SUFFIX, isSearchable, hasDocValues)
            );
        }

        public FlatObjectFieldType(
            String name,
            String rootFieldName,
            KeywordFieldType valueFieldType,
            KeywordFieldType valueAndPathFieldType
        ) {
            super(
                name,
                valueFieldType.isSearchable(),
                false,
                valueFieldType.hasDocValues(),
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            assert rootFieldName == null || (name.length() >= rootFieldName.length() && name.startsWith(rootFieldName));
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
            this.rootFieldName = rootFieldName;
            this.valueFieldType = valueFieldType;
            this.valueAndPathFieldType = valueAndPathFieldType;
        }

        static KeywordFieldType getKeywordFieldType(String rootField, String suffix, boolean isSearchable, boolean hasDocValue) {
            return new KeywordFieldType(rootField + suffix, isSearchable, hasDocValue, Collections.emptyMap()) {
                @Override
                protected String rewriteForDocValue(Object value) {
                    assert value instanceof String;
                    return getDVPrefix(rootField) + value;
                }
            };
        }

        public KeywordFieldType getValueFieldType() {
            return this.valueFieldType;
        }

        public KeywordFieldType getValueAndPathFieldType() {
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
            return new SortedSetOrdinalsIndexFieldData.Builder(valueFieldType().name(), CoreValuesSourceType.BYTES);
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
        public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
            }
            if (timeZone != null) {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones"
                );
            }
            if (rootFieldName != null) {
                return new FlatObjectDocValueFormat(getDVPrefix(rootFieldName) + getPathPrefix(name()));
            } else {
                throw new IllegalArgumentException(
                    "Field [" + name() + "] of type [" + typeName() + "] does not support doc_value in root field"
                );
            }
        }

        @Override
        public boolean isAggregatable() {
            return false;
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

        private KeywordFieldType valueFieldType() {
            return (rootFieldName == null) ? valueFieldType : valueAndPathFieldType;
        }

        @Override
        public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().termQueryCaseInsensitive(rewriteSearchValue(value), context);
        }

        /**
         * redirect queries with rewrite value to rewriteSearchValue and directSubFieldName
         */
        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().termQuery(rewriteSearchValue(value), context);
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            List<String> parsedValues = new ArrayList<>(values.size());
            for (Object value : values) {
                parsedValues.add(rewriteSearchValue(value));
            }

            return valueFieldType().termsQuery(parsedValues, context);
        }

        /**
         * To direct search fields, if a dot path was used in search query,
         * then direct to flatObjectFieldName._valueAndPath subfield,
         * else, direct to flatObjectFieldName._value subfield.
         * @return directedSubFieldName
         */
        public String getSearchField() {
            return isSubField() ? rootFieldName + VALUE_AND_PATH_SUFFIX : name() + VALUE_SUFFIX;
        }

        /**
         * If the search key has mappedFieldTypeName as prefix,
         * then the dot path was used in search query,
         * then rewrite the searchValueString as the format "dotpath=value",
         * @return rewriteSearchValue
         */
        public String rewriteSearchValue(Object value) {
            if (value instanceof BytesRef) {
                value = ((BytesRef) value).utf8ToString();
            }
            return isSubField() ? getPathPrefix(name()) + value : value.toString();
        }

        boolean isSubField() {
            return rootFieldName != null;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().prefixQuery(rewriteSearchValue(value), method, caseInsensitive, context);
        }

        @Override
        public Query regexpQuery(
            String value,
            int syntaxFlags,
            int matchFlags,
            int maxDeterminizedStates,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().regexpQuery(rewriteSearchValue(value), syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
        }

        @Override
        public Query fuzzyQuery(
            Object value,
            Fuzziness fuzziness,
            int prefixLength,
            int maxExpansions,
            boolean transpositions,
            @Nullable MultiTermQuery.RewriteMethod method,
            QueryShardContext context
        ) {
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().fuzzyQuery(
                rewriteSearchValue(value),
                fuzziness,
                prefixLength,
                maxExpansions,
                transpositions,
                method,
                context
            );
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
            failIfNotIndexedAndNoDocValues();

            if ((lowerTerm != null && upperTerm != null)) {
                return valueFieldType().rangeQuery(
                    rewriteSearchValue(lowerTerm),
                    rewriteSearchValue(upperTerm),
                    includeLower,
                    includeUpper,
                    context
                );
            }

            // when either the upper term or lower term is null,
            // we can't delegate to valueFieldType() and need to process the prefix ourselves
            Query indexQuery = null;
            Query dvQuery = null;
            if (isSearchable()) {
                if (isSubField() == false) {
                    indexQuery = new TermRangeQuery(
                        getSearchField(),
                        lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                        upperTerm == null ? null : indexedValueForSearch(upperTerm),
                        includeLower,
                        includeUpper
                    );
                } else {
                    Automaton a1 = PrefixQuery.toAutomaton(indexedValueForSearch(getPathPrefix(name())));
                    BytesRef lowerTermBytes = lowerTerm == null ? null : indexedValueForSearch(rewriteSearchValue(lowerTerm));
                    BytesRef upperTermBytes = upperTerm == null ? null : indexedValueForSearch(rewriteSearchValue(upperTerm));
                    Automaton a2 = TermRangeQuery.toAutomaton(lowerTermBytes, upperTermBytes, includeLower, includeUpper);
                    Automaton termAutomaton = Operations.intersection(a1, a2);
                    indexQuery = new AutomatonQuery(new Term(getSearchField()), termAutomaton, true);
                }
            }
            if (hasDocValues()) {
                String dvPrefix = isSubField() ? getDVPrefix(rootFieldName) : getDVPrefix(name());
                String prefix = dvPrefix + (isSubField() ? getPathPrefix(name()) : "");
                Automaton a1 = PrefixQuery.toAutomaton(indexedValueForSearch(prefix));
                BytesRef lowerDvBytes = lowerTerm == null ? null : indexedValueForSearch(dvPrefix + rewriteSearchValue(lowerTerm));
                BytesRef upperDvBytes = upperTerm == null ? null : indexedValueForSearch(dvPrefix + rewriteSearchValue(upperTerm));
                Automaton a2 = TermRangeQuery.toAutomaton(lowerDvBytes, upperDvBytes, includeLower, includeUpper);
                Automaton dvAutomaton = Operations.intersection(a1, a2);
                dvQuery = new AutomatonQuery(new Term(getSearchField()), dvAutomaton, true, DOC_VALUES_REWRITE);
            }

            assert indexQuery != null || dvQuery != null;
            return indexQuery == null ? dvQuery : (dvQuery == null ? indexQuery : new IndexOrDocValuesQuery(indexQuery, dvQuery));
        }

        /**
         * if there is dot path. query the field name in flatObject parent field (mappedFieldTypeName).
         * else query in _field_names system field
         */
        @Override
        public Query existsQuery(QueryShardContext context) {
            String searchKey;
            String searchField;
            if (isSubField()) {
                return rangeQuery(null, null, true, true, context);
            } else {
                if (hasDocValues()) {
                    return new FieldExistsQuery(name());
                } else {
                    searchKey = FieldNamesFieldMapper.NAME;
                    searchField = name();
                }
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
            failIfNotIndexedAndNoDocValues();
            return valueFieldType().wildcardQuery(rewriteSearchValue(value), method, caseInsensitve, context);
        }

        /**
         * A doc_value formatter for flat_object field.
         */
        public class FlatObjectDocValueFormat implements DocValueFormat {
            private static final String NAME = "flat_object";
            private final String prefix;

            public FlatObjectDocValueFormat(String prefix) {
                this.prefix = prefix;
            }

            @Override
            public String getWriteableName() {
                return NAME;
            }

            @Override
            public void writeTo(StreamOutput out) {}

            @Override
            public Object format(BytesRef value) {
                String parsedValue = value.utf8ToString();
                if (parsedValue.startsWith(prefix) == false) {
                    return DOC_VALUE_NO_MATCH;
                }
                return parsedValue.substring(prefix.length());
            }

            @Override
            public BytesRef parseBytesRef(String value) {
                return new BytesRef((String) valueFieldType.rewriteForDocValue(rewriteSearchValue(value)));
            }
        }
    }

    private final KeywordFieldType valueFieldType;
    private final KeywordFieldType valueAndPathFieldType;

    FlatObjectFieldMapper(String simpleName, FieldType fieldType, FlatObjectFieldType mappedFieldType) {
        super(simpleName, fieldType, mappedFieldType, CopyTo.empty());
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        valueFieldType = mappedFieldType.valueFieldType;
        valueAndPathFieldType = mappedFieldType.valueAndPathFieldType;
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

    private void parseObject(XContentParser parser, ParseContext context) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT;
        parser.nextToken(); // Skip the outer START_OBJECT. Need to return on END_OBJECT.

        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        HashSet<String> pathParts = new HashSet<>();
        while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseToken(parser, context, path, pathParts);
        }

        createPathFields(context, pathParts);
    }

    private void createPathFields(ParseContext context, HashSet<String> pathParts) {
        for (String part : pathParts) {
            final BytesRef value = new BytesRef(name() + DOT_SYMBOL + part);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                context.doc().add(new Field(name(), value, fieldType));
            }
            if (fieldType().hasDocValues()) {
                context.doc().add(new SortedSetDocValuesField(name(), value));
            } else {
                createFieldNamesField(context);
            }
        }
    }

    private static String getDVPrefix(String rootFieldName) {
        return rootFieldName + DOT_SYMBOL;
    }

    private static String getPathPrefix(String path) {
        return path + EQUAL_SYMBOL;
    }

    private void parseToken(XContentParser parser, ParseContext context, Deque<String> path, HashSet<String> pathParts) throws IOException {
        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
            final String currentFieldName = parser.currentName();
            path.addLast(currentFieldName); // Pushing onto the stack *must* be matched by pop
            parser.nextToken(); // advance to the value of fieldName
            parseToken(parser, context, path, pathParts); // parse the value for fieldName (which will be an array, an object,
            // or a primitive value)
            path.removeLast(); // Here is where we pop fieldName from the stack (since we're done with the value of fieldName)
            // Note that whichever other branch we just passed through has already ended with nextToken(), so we
            // don't need to call it.
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                parseToken(parser, context, path, pathParts);
            }
            parser.nextToken();
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                parseToken(parser, context, path, pathParts);
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
                value = normalizeValue(normalizer, name(), value);
            }
            final String leafPath = Strings.collectionToDelimitedString(path, ".");
            final String valueAndPath = getPathPrefix(leafPath) + value;
            if (fieldType().isSearchable() || fieldType().isStored()) {
                context.doc().add(new Field(valueFieldType.name(), new BytesRef(value), fieldType));
                context.doc().add(new Field(valueAndPathFieldType.name(), new BytesRef(valueAndPath), fieldType));
            }

            if (fieldType().hasDocValues()) {
                context.doc().add(new SortedSetDocValuesField(valueFieldType.name(), new BytesRef(getDVPrefix(name()) + value)));
                context.doc()
                    .add(new SortedSetDocValuesField(valueAndPathFieldType.name(), new BytesRef(getDVPrefix(name()) + valueAndPath)));
            }

            pathParts.addAll(Arrays.asList(leafPath.substring(name().length() + 1).split("\\.")));
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

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
