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
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Iterators;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.JsonToStringXContentParser;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityProvider;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static org.opensearch.common.xcontent.JsonToStringXContentParser.DOT_SYMBOL;
import static org.opensearch.common.xcontent.JsonToStringXContentParser.EQUAL_SYMBOL;
import static org.opensearch.common.xcontent.JsonToStringXContentParser.VALUE_AND_PATH_SUFFIX;
import static org.opensearch.common.xcontent.JsonToStringXContentParser.VALUE_SUFFIX;
import static org.opensearch.index.mapper.KeywordFieldMapper.Builder.getNormalizerAndSearchAnalyzer;
import static org.opensearch.index.mapper.TypeParsers.DOC_VALUES;
import static org.opensearch.index.mapper.TypeParsers.checkNull;

/**
 * A field mapper for flat_objects.
 * This mapper accepts JSON object and treat as string fields in one index.
 * @opensearch.internal
 */
public final class FlatObjectFieldMapper extends DynamicKeyFieldMapper {

    public static final String CONTENT_TYPE = "flat_object";

    private final String normalizerName;
    private final boolean isSearchable;
    private final boolean hasDocValues;
    private final int ignoreAbove;
    private final SimilarityProvider similarity;
    private final int depthLimit;
    private final IndexAnalyzers indexAnalyzers;

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
        Tuple<NamedAnalyzer, NamedAnalyzer> analyzers = getNormalizerAndSearchAnalyzer(
            this.name() + DOT_SYMBOL + key,
            normalizerName != null ? normalizerName : "default",
            false,
            indexAnalyzers
        );
        return new FlatObjectFieldType(
            this.name() + DOT_SYMBOL + key,
            this.name(),
            (KeywordFieldType) valueFieldMapper.fieldType(),
            (KeywordFieldType) valueAndPathFieldMapper.fieldType(),
            similarity,
            analyzers.v1(),
            analyzers.v2(),
            ignoreAbove
        );
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
        private String normalizerName;
        private int ignoreAbove = Integer.MAX_VALUE;
        private SimilarityProvider similarity;
        private int depthLimit = Integer.MAX_VALUE;
        private final IndexAnalyzers indexAnalyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name, Defaults.FIELD_TYPE);
            builder = this;
            this.indexAnalyzers = indexAnalyzers;
        }

        public void setNormalizer(String normalizer) {
            this.normalizerName = normalizer;
        }

        /**
         * ValueFieldMapper is the subfield type for values in the Json.
         * use a {@link KeywordFieldMapper.KeywordField}
         */
        private ValueFieldMapper buildValueFieldMapper(FieldType fieldType, KeywordFieldType valueFieldType) {
            FieldType vft = new FieldType(fieldType);
            return new ValueFieldMapper(vft, valueFieldType);
        }

        /**
         * ValueAndPathFieldMapper is the subfield type for path=value format in the Json.
         * also use a {@link KeywordFieldMapper.KeywordField}
         */
        private ValueAndPathFieldMapper buildValueAndPathFieldMapper(FieldType fieldType, KeywordFieldType valueAndPathFieldType) {
            FieldType vft = new FieldType(fieldType);
            return new ValueAndPathFieldMapper(vft, valueAndPathFieldType);
        }

        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            Tuple<NamedAnalyzer, NamedAnalyzer> analyzers = getNormalizerAndSearchAnalyzer(
                buildFullName(context),
                normalizerName != null ? normalizerName : "default",
                false,
                indexAnalyzers
            );

            KeywordFieldType valueFieldType = FlatObjectFieldType.getKeywordFieldType(
                buildFullName(context),
                VALUE_SUFFIX,
                indexed,
                hasDocValues,
                similarity,
                analyzers.v1(),
                analyzers.v2()
            );
            KeywordFieldType valueAndPathFieldType = FlatObjectFieldType.getKeywordFieldType(
                buildFullName(context),
                VALUE_AND_PATH_SUFFIX,
                indexed,
                hasDocValues,
                similarity,
                analyzers.v1(),
                analyzers.v2()
            );

            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(indexed, "docs"));

            FlatObjectFieldType fft = new FlatObjectFieldType(
                buildFullName(context),
                null,
                valueFieldType,
                valueAndPathFieldType,
                similarity,
                analyzers.v1(),
                analyzers.v2(),
                ignoreAbove
            );

            return new FlatObjectFieldMapper(
                name,
                fieldtype,
                fft,
                buildValueFieldMapper(Defaults.FIELD_TYPE, valueFieldType),
                buildValueAndPathFieldMapper(Defaults.FIELD_TYPE, valueAndPathFieldType),
                CopyTo.empty(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getIndexAnalyzers()));

    /**
     * Creates a new TypeParser for flatObjectFieldMapper that does not use ParameterizedFieldMapper
     */
    public static class TypeParser implements Mapper.TypeParser {

        static final String INDEXED = "index";
        static final String NORMALIZER = "normalizer";
        static final String IGNORE_ABOVE = "ignore_above";
        static final String SIMILARITY = "similarity";
        static final String DEPTH_LIMIT = "depth_limit";
        private final BiFunction<String, ParserContext, Builder> builderFunction;

        public TypeParser(BiFunction<String, ParserContext, Builder> builderFunction) {
            this.builderFunction = builderFunction;
        }

        @Override
        public Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = builderFunction.apply(name, parserContext);
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String propName = entry.getKey();
                Object propNode = entry.getValue();
                checkNull(propName, propNode);
                switch (propName) {
                    case NORMALIZER: // allow lowercase, and custom normalizer
                        builder.setNormalizer(XContentMapValues.nodeStringValue(propNode));
                        iterator.remove();
                        break;
                    case INDEXED: // allow to set indexed to be false
                        builder.index(XContentMapValues.nodeBooleanValue(propNode));
                        iterator.remove();
                        break;
                    case DOC_VALUES: // allow to set docValues to be false
                        builder.hasDocValues = XContentMapValues.nodeBooleanValue(propNode);
                        iterator.remove();
                        break;
                    case IGNORE_ABOVE:// allow to set if the length of a field is go above certain limit then ignore the document.
                        builder.ignoreAbove = XContentMapValues.nodeIntegerValue(propNode);
                        if (builder.ignoreAbove < 0) {
                            throw new IllegalArgumentException("[ignore_above] must be positive, got " + builder.ignoreAbove);
                        }
                        iterator.remove();
                        break;
                    case SIMILARITY:// allow to set similarity setting
                        builder.similarity = TypeParsers.resolveSimilarity(parserContext, name, propNode);
                        iterator.remove();
                        break;
                    case DEPTH_LIMIT:// allow to set maximum depth limitation to the JSON document
                        builder.depthLimit = XContentMapValues.nodeIntegerValue(propNode);
                        if (builder.depthLimit < 0) {
                            throw new IllegalArgumentException("[depth_limit] must be positive, got " + builder.depthLimit);
                        }
                        iterator.remove();
                        break;
                }
            }
            return builder;
        }
    }

    /**
     * flat_object fields type contains its own fieldType, one valueFieldType and one valueAndPathFieldType
     * @opensearch.internal
     */
    public static final class FlatObjectFieldType extends StringFieldType {

        private final int ignoreAbove;

        private final String mappedFieldTypeName;

        private final KeywordFieldType valueFieldType;

        private final KeywordFieldType valueAndPathFieldType;

        public FlatObjectFieldType(
            String name,
            String mappedFieldTypeName,
            boolean isSearchable,
            boolean hasDocValues,
            int ignoreAbove,
            SimilarityProvider similarity,
            NamedAnalyzer normalizer,
            NamedAnalyzer searchAnalyzer,
            Map<String, String> meta
        ) {
            super(
                name,
                isSearchable,
                false,
                hasDocValues,
                new TextSearchInfo(Defaults.FIELD_TYPE, similarity, normalizer, searchAnalyzer),
                meta
            );

            assert normalizer != null;
            setIndexAnalyzer(normalizer);
            this.ignoreAbove = ignoreAbove;
            this.mappedFieldTypeName = mappedFieldTypeName;
            this.valueFieldType = getKeywordFieldType(
                name,
                VALUE_SUFFIX,
                isSearchable,
                hasDocValues,
                similarity,
                normalizer,
                searchAnalyzer
            );
            this.valueAndPathFieldType = getKeywordFieldType(
                name,
                VALUE_AND_PATH_SUFFIX,
                isSearchable,
                hasDocValues,
                similarity,
                normalizer,
                searchAnalyzer
            );
        }

        public FlatObjectFieldType(
            String name,
            String mappedFieldTypeName,
            KeywordFieldType valueFieldType,
            KeywordFieldType valueAndPathFieldType,
            SimilarityProvider similarity,
            NamedAnalyzer normalizer,
            NamedAnalyzer searchAnalyzer,
            int ignoreAbove
        ) {
            super(
                name,
                valueFieldType.isSearchable(),
                false,
                valueFieldType.hasDocValues(),
                new TextSearchInfo(Defaults.FIELD_TYPE, similarity, normalizer, searchAnalyzer),
                Collections.emptyMap()
            );
            setIndexAnalyzer(normalizer);
            this.ignoreAbove = ignoreAbove;
            this.mappedFieldTypeName = mappedFieldTypeName;
            this.valueFieldType = valueFieldType;
            this.valueAndPathFieldType = valueAndPathFieldType;
        }

        static KeywordFieldType getKeywordFieldType(
            String fullName,
            String valueType,
            boolean isSearchable,
            boolean hasDocValue,
            SimilarityProvider similarity,
            NamedAnalyzer normalizer,
            NamedAnalyzer searchAnalyzer
        ) {
            TextSearchInfo textSearchInfo = new TextSearchInfo(Defaults.FIELD_TYPE, similarity, searchAnalyzer, searchAnalyzer);
            return new KeywordFieldType(
                fullName + valueType,
                isSearchable,
                hasDocValue,
                normalizer,
                textSearchInfo,
                Collections.emptyMap()
            ) {
                @Override
                protected String rewriteForDocValue(Object value) {
                    assert value instanceof String;
                    return fullName + DOT_SYMBOL + value;
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
            return new SortedSetOrdinalsIndexFieldData.Builder(name(), CoreValuesSourceType.BYTES);
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            if (format != null) {
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't support formats.");
            }

            return new SourceValueFetcher(name(), context, null) {
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

        private KeywordFieldType valueFieldType() {
            return (mappedFieldTypeName == null) ? valueFieldType : valueAndPathFieldType;
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
            List<String> parsedValues = new ArrayList<>(values.size());
            for (Object value : values) {
                parsedValues.add(rewriteValue(inputToString(value)));
            }

            return valueFieldType().termsQuery(parsedValues, context);
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

        boolean hasMappedFieldTyeNameInQueryFieldName(String input) {
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
            if (inputValue == null) {
                return null;
            }
            if (inputValue instanceof BytesRef) {
                return (((BytesRef) inputValue).utf8ToString());
            } else {
                return inputValue.toString();
            }
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            return valueFieldType().prefixQuery(rewriteValue(value), method, caseInsensitive, context);
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
            return valueFieldType().regexpQuery(rewriteValue(value), syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
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
            return valueFieldType().fuzzyQuery(
                rewriteValue(inputToString(value)),
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
            return valueFieldType().rangeQuery(
                rewriteValue(inputToString(lowerTerm)),
                rewriteValue(inputToString(upperTerm)),
                includeLower,
                includeUpper,
                context
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
            return valueFieldType().wildcardQuery(rewriteValue(value), method, caseInsensitve, context);
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
        this.normalizerName = builder.normalizerName;
        this.isSearchable = builder.indexed;
        this.hasDocValues = builder.hasDocValues;
        this.ignoreAbove = builder.ignoreAbove;
        this.similarity = builder.similarity;
        this.depthLimit = builder.depthLimit;
        this.indexAnalyzers = builder.indexAnalyzers;
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
    }

    @Override
    protected void mergeOptions(FieldMapper other, List<String> conflicts) {
        FlatObjectFieldMapper mappers = (FlatObjectFieldMapper) other;
        if (!Objects.equals(this.normalizerName, mappers.normalizerName)) {
            conflicts.add("mapper [" + name() + "] has different [normalizer]");
        }
        if (!Objects.equals(this.isSearchable, mappers.isSearchable)) {
            conflicts.add("mapper [" + name() + "] has different [index]");
        }
        if (!Objects.equals(this.hasDocValues, mappers.hasDocValues)) {
            conflicts.add("mapper [" + name() + "] has different [doc_values]");
        }
        if (!Objects.equals(this.ignoreAbove, mappers.ignoreAbove)) {
            conflicts.add("mapper [" + name() + "] has different [ignore_above]");
        }
        if (!Objects.equals(this.similarity, mappers.similarity)) {
            conflicts.add("mapper [" + name() + "] has different [similarity]");
        }
        if (!Objects.equals(this.depthLimit, mappers.depthLimit)) {
            conflicts.add("mapper [" + name() + "] has different [depth_limit]");
        }
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
            XContentParser ctxParser = context.parser();
            if (ctxParser.currentToken() != XContentParser.Token.VALUE_NULL) {
                if (ctxParser.currentToken() != XContentParser.Token.START_OBJECT) {
                    throw new ParsingException(
                        ctxParser.getTokenLocation(),
                        "[" + this.name() + "] unexpected token [" + ctxParser.currentToken() + "] in flat_object field value"
                    );
                }

                JsonToStringXContentParser jsonToStringParser = new JsonToStringXContentParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.IGNORE_DEPRECATIONS,
                    ctxParser,
                    fieldType().name(),
                    depthLimit,
                    ignoreAbove
                );
                /*
                  JsonToStringParser is the main parser class to transform JSON into stringFields in a XContentParser
                  It reads the JSON object and parsed to a list of string
                 */
                XContentParser parser = jsonToStringParser.parseObject();

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

        assert valueFieldMapper != null;
        assert valueAndPathFieldMapper != null;
        if (value == null) {
            return;
        }
        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, name(), value);
        }

        String[] valueTypeList = fieldName.split("\\._");
        String valueType = "._" + valueTypeList[valueTypeList.length - 1];

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(fieldType().name() + DOT_SYMBOL + value);

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {

            if (fieldType().hasDocValues() == false) {
                createFieldNamesField(context);
            }
            if (fieldName.equals(fieldType().name())) {
                Field field = new FlatObjectField(fieldType().name(), binaryValue, fieldType);
                context.doc().add(field);
            } else if (valueType.equals(VALUE_SUFFIX)) {
                valueFieldMapper.addField(context, value);
            } else if (valueType.equals(VALUE_AND_PATH_SUFFIX)) {
                valueAndPathFieldMapper.addField(context, value);
            }
        }

        if (fieldType().hasDocValues()) {
            if (fieldName.equals(fieldType().name())) {
                context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
            } else if (valueType.equals(VALUE_SUFFIX)) {
                context.doc().add(new SortedSetDocValuesField(fieldType().name() + VALUE_SUFFIX, binaryValue));
            } else if (valueType.equals(VALUE_AND_PATH_SUFFIX)) {
                context.doc().add(new SortedSetDocValuesField(fieldType().name() + VALUE_AND_PATH_SUFFIX, binaryValue));
            }
        }
    }

    private static String normalizeValue(NamedAnalyzer normalizer, String field, String value) throws IOException {
        try (TokenStream ts = normalizer.tokenStream(field, value)) {
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            if (ts.incrementToken() == false) {
                throw new IllegalStateException(errorMessage(normalizer, value));
            }
            final String newValue = termAtt.toString();
            if (ts.incrementToken()) {
                throw new IllegalStateException(errorMessage(normalizer, value));
            }
            ts.end();
            return newValue;
        }
    }

    private static String errorMessage(NamedAnalyzer normalizer, String value) {
        return "The normalization token stream is "
            + "expected to produce exactly 1 token, but got 0 for analyzer "
            + normalizer
            + " and input \""
            + value
            + "\"";

    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        builder.field("type", contentType());
        if (includeDefaults || normalizerName != null) {
            builder.field("normalizer", normalizerName);
        }
        if (includeDefaults || isSearchable != true) {
            builder.field("index", isSearchable);
        }
        if (includeDefaults || hasDocValues != true) {
            builder.field("doc_values", hasDocValues);
        }
        if (includeDefaults || ignoreAbove != Integer.MAX_VALUE) {
            builder.field("ignore_above", ignoreAbove);
        }
        if (includeDefaults || similarity != null) {
            builder.field("similarity", similarity.name());
        }
        if (includeDefaults || depthLimit != Integer.MAX_VALUE) {
            builder.field("depth_limit", depthLimit);
        }
    }

    private static final class ValueAndPathFieldMapper extends FieldMapper {

        protected ValueAndPathFieldMapper(FieldType fieldType, KeywordFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            final BytesRef binaryValue = new BytesRef(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                Field field = new KeywordFieldMapper.KeywordField(fieldType().name(), binaryValue, fieldType);

                context.doc().add(field);

                if (fieldType().hasDocValues() == false) {
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
            throw new UnsupportedOperationException();
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

        protected ValueFieldMapper(FieldType fieldType, KeywordFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            final BytesRef binaryValue = new BytesRef(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                Field field = new KeywordFieldMapper.KeywordField(fieldType().name(), binaryValue, fieldType);
                context.doc().add(field);

                if (fieldType().hasDocValues() == false) {
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
            throw new UnsupportedOperationException();
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
