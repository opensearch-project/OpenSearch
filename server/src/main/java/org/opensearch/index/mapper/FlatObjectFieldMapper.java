/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
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
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
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

        /**
         * flat_object leaves are keyword-like (term queries over the {@code _value} /
         * {@code _valueAndPath} sub-fields), so it requests the same search capability keyword does.
         * Without this override {@link MappedFieldType#requestedCapabilities()} throws for any
         * flat_object field in a pluggable-data-format index, because the base implementation has no
         * capability to report for a searchable field.
         */
        @Override
        protected FieldTypeCapabilities.Capability searchCapability() {
            return FieldTypeCapabilities.Capability.FULL_TEXT_SEARCH;
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

    // Pluggable-dataformat indices force derived source on (IndexSettings: derivedSourceEnabled ||
    // pluggableDataFormatEnabled), so every field must satisfy the derive-source create-time contract or
    // the index cannot be created. flat_object is keyword-like (UTF-8 term storage), so it derives like
    // keyword. NOTE: correct reconstruction of the object from the parquet MAP<Utf8,Utf8> column is a
    // read-path concern; this satisfies the create-time contract. The translog derived-source setting
    // defaults off, so this generator does not run during ingest.
    @Override
    protected void canDeriveSourceInternal() {
        // flat_object has no ignore_above/normalizer restrictions that would block derivation.
    }

    @Override
    protected DerivedFieldGenerator derivedFieldGenerator() {
        return new DerivedFieldGenerator(
            mappedFieldType,
            new SortedSetDocValuesFetcher(mappedFieldType, simpleName()),
            new StoredFieldFetcher(mappedFieldType, simpleName())
        );
    }

    @Override
    public FlatObjectFieldType fieldType() {
        return (FlatObjectFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        HashSet<String> pathParts = parseObjectPathParts(context);
        if (pathParts != null) {
            createPathFields(context, pathParts);
        }
    }

    /**
     * Pluggable-data-format path (e.g. parquet composite): instead of exploding the object into
     * per-key leaf columns, emit each leaf as one {@code (key, value)} map entry via
     * {@link ParseContext#documentInput()}. Downstream this becomes a single {@code MAP<Utf8,Utf8>}
     * column, so the open attribute key space is stored losslessly against a static schema.
     * <p>
     * The key is the flattened dotted path relative to this field ({@code http.method}, not
     * {@code LogAttributes.http.method}) — the column name already carries the field prefix that
     * Lucene's {@code _valueAndPath} sub-field has to spell out. Values are stringified. Duplicate keys
     * are preserved, since a parquet MAP is physically a repeated key/value group.
     * <p>
     * Emitting per-leaf (rather than handing over one collection) is what lets the same signal serve a
     * flat_object at the document root and one inside a nested element: the {@code DocumentInput}
     * routes each entry to whichever scope is currently open.
     */
    @Override
    protected void parseCreateFieldForPluggableFormat(ParseContext context) throws IOException {
        XContentParser ctxParser = context.parser();
        if (fieldType().isSearchable() == false && fieldType().isStored() == false && fieldType().hasDocValues() == false) {
            ctxParser.skipChildren();
            return;
        }
        if (ctxParser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return;
        }
        if (ctxParser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                ctxParser.getTokenLocation(),
                "[" + this.name() + "] unexpected token [" + ctxParser.currentToken() + "] in flat_object field value"
            );
        }
        ctxParser.nextToken();
        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        while (ctxParser.currentToken() != XContentParser.Token.END_OBJECT) {
            emitMapEntries(ctxParser, context, path);
        }
    }

    /** Recursively walks the object (mirroring {@link #parseToken}) and emits one map entry per leaf. */
    private void emitMapEntries(XContentParser parser, ParseContext context, Deque<String> path) throws IOException {
        if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
            final String currentFieldName = parser.currentName();
            path.addLast(currentFieldName);
            parser.nextToken();
            emitMapEntries(parser, context, path);
            path.removeLast();
        } else if (parser.currentToken() == XContentParser.Token.START_ARRAY) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
                emitMapEntries(parser, context, path);
            }
            parser.nextToken();
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
            while (parser.currentToken() != XContentParser.Token.END_OBJECT) {
                emitMapEntries(parser, context, path);
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
            // Key relative to this flat_object field: strip the "<fieldName>." prefix.
            final String key = leafPath.substring(name().length() + 1);
            context.documentInput().addMapEntry(fieldType(), key, value);
            parser.nextToken();
        }
    }

    /**
     * Parses the flat_object field value and returns the collected path parts,
     * or {@code null} if the field should be skipped (null value or not searchable/stored/docvalues).
     */
    private HashSet<String> parseObjectPathParts(ParseContext context) throws IOException {
        XContentParser ctxParser = context.parser();
        if (fieldType().isSearchable() == false && fieldType().isStored() == false && fieldType().hasDocValues() == false) {
            ctxParser.skipChildren();
            return null;
        }

        if (ctxParser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        }
        if (ctxParser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                ctxParser.getTokenLocation(),
                "[" + this.name() + "] unexpected token [" + ctxParser.currentToken() + "] in flat_object field value"
            );
        }

        assert ctxParser.currentToken() == XContentParser.Token.START_OBJECT;
        ctxParser.nextToken();

        LinkedList<String> path = new LinkedList<>(Collections.singleton(fieldType().name()));
        HashSet<String> pathParts = new HashSet<>();
        while (ctxParser.currentToken() != XContentParser.Token.END_OBJECT) {
            parseToken(ctxParser, context, path, pathParts);
        }
        return pathParts;
    }

    private void createPathFields(ParseContext context, HashSet<String> pathParts) {
        for (String part : pathParts) {
            final BytesRef value = new BytesRef(name() + DOT_SYMBOL + part);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                context.doc().add(new Field(name(), value, fieldType));
            }
            if (fieldType().hasDocValues()) {
                addDocValueOnlyPathMarker(context.doc(), name(), part);
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

    /**
     * Writes one flat_object-style, doc-values-ONLY leaf entry under {@code rootFieldName} — no
     * indexed terms (matching {@code "index": false}: no inverted index). This is the doc-values
     * half of {@link #parseToken}'s leaf-writing branch, extracted so a caller outside the classic
     * parse walk — e.g. a nested field's coarse Lucene projection, which has no per-leaf indexed
     * terms — can reuse the exact same {@code _value}/{@code _valueAndPath} encoding a real
     * flat_object's own fields use. {@code ParseContext.Document} and Lucene's own {@code Document}
     * share no common supertype with an {@code add(IndexableField)} method, so the two overloads
     * below both funnel into this one implementation via a plain field sink.
     *
     * @param addField         adds one field to whichever document the caller is building
     * @param rootFieldName    the anchor field name — a real flat_object's own name, or a nested
     *                         field's path standing in for one
     * @param leafRelativePath the leaf's dotted path relative to {@code rootFieldName}
     * @param value            the (already-stringified) leaf value
     */
    private static void addDocValueOnlyLeaf(
        java.util.function.Consumer<IndexableField> addField,
        String rootFieldName,
        String leafRelativePath,
        String value
    ) {
        String dvPrefix = getDVPrefix(rootFieldName);
        addField.accept(new SortedSetDocValuesField(rootFieldName + VALUE_SUFFIX, new BytesRef(dvPrefix + value)));
        String valueAndPath = getPathPrefix(rootFieldName + DOT_SYMBOL + leafRelativePath) + value;
        addField.accept(new SortedSetDocValuesField(rootFieldName + VALUE_AND_PATH_SUFFIX, new BytesRef(dvPrefix + valueAndPath)));
    }

    /** {@link #addDocValueOnlyLeaf(java.util.function.Consumer, String, String, String)} for the classic parse-walk document. */
    public static void addDocValueOnlyLeaf(ParseContext.Document doc, String rootFieldName, String leafRelativePath, String value) {
        addDocValueOnlyLeaf(doc::add, rootFieldName, leafRelativePath, value);
    }

    /** {@link #addDocValueOnlyLeaf(java.util.function.Consumer, String, String, String)} for a plain Lucene document. */
    public static void addDocValueOnlyLeaf(Document doc, String rootFieldName, String leafRelativePath, String value) {
        addDocValueOnlyLeaf(doc::add, rootFieldName, leafRelativePath, value);
    }

    /**
     * Writes a flat_object-style, doc-values-ONLY "this path exists" marker under {@code
     * rootFieldName} — the doc-values half of {@link #createPathFields}, so {@link
     * FlatObjectFieldType#existsQuery} (root-field, doc-values branch) can find it via {@code
     * FieldExistsQuery(rootFieldName)}. See {@link #addDocValueOnlyLeaf(java.util.function.Consumer,
     * String, String, String)} for why there are two overloads.
     */
    private static void addDocValueOnlyPathMarker(
        java.util.function.Consumer<IndexableField> addField,
        String rootFieldName,
        String relativePathPart
    ) {
        addField.accept(new SortedSetDocValuesField(rootFieldName, new BytesRef(rootFieldName + DOT_SYMBOL + relativePathPart)));
    }

    /** {@link #addDocValueOnlyPathMarker(java.util.function.Consumer, String, String)} for the classic parse-walk document. */
    public static void addDocValueOnlyPathMarker(ParseContext.Document doc, String rootFieldName, String relativePathPart) {
        addDocValueOnlyPathMarker(doc::add, rootFieldName, relativePathPart);
    }

    /** {@link #addDocValueOnlyPathMarker(java.util.function.Consumer, String, String)} for a plain Lucene document. */
    public static void addDocValueOnlyPathMarker(Document doc, String rootFieldName, String relativePathPart) {
        addDocValueOnlyPathMarker(doc::add, rootFieldName, relativePathPart);
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
            final String relativePath = leafPath.substring(name().length() + 1);
            final String valueAndPath = getPathPrefix(leafPath) + value;
            if (fieldType().isSearchable() || fieldType().isStored()) {
                context.doc().add(new Field(valueFieldType.name(), new BytesRef(value), fieldType));
                context.doc().add(new Field(valueAndPathFieldType.name(), new BytesRef(valueAndPath), fieldType));
            }

            if (fieldType().hasDocValues()) {
                addDocValueOnlyLeaf(context.doc(), name(), relativePath, value);
            }

            pathParts.addAll(Arrays.asList(relativePath.split("\\.")));
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
