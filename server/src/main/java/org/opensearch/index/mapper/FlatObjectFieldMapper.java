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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.Nullable;
import org.opensearch.common.collect.Iterators;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.xcontent.DeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.JsonToStringXContentParser;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.SortedSetOrdinalsIndexFieldData;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityProvider;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import java.util.logging.Logger;

/**
 * A field mapper for flat-objects. This mapper accepts JSON object and treat as string fields in one index.
 * @opensearch.internal
 */
public final class FlatObjectFieldMapper extends ParametrizedFieldMapper {
    /**
     * logging function:
     * To remove after draft PR
     */

    private static final Logger logger = Logger.getLogger((FlatObjectFieldMapper.class.getName()));

    /**
     * A flat-object mapping contains one parent field itself and two substring fields,
     * field._valueAndPath and field._value
     */

    public static final String CONTENT_TYPE = "flat-object";
    private static final String VALUE_AND_PATH_SUFFIX = "._valueAndPath";
    private static final String VALUE_SUFFIX = "._value";

    /**
     * Default parameters, similar to keyword
     * In flat-object, three fields are treated as keyword fields with the same parameters
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

    /**
     * The flat-object field for the field mapper
     *
     * @opensearch.internal
     */
    public static class FlatObjectField extends Field {

        public FlatObjectField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }

    }

    private static FlatObjectFieldMapper toType(FieldMapper in) {
        return (FlatObjectFieldMapper) in;
    }

    /**
     * The builder for the flat-object field mapper
     * Set the same parameters from keywordFieldMapper.Builder
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Boolean> indexed = Parameter.indexParam(m -> toType(m).indexed, true);

        private final Parameter<Boolean> hasDocValues = Parameter.docValuesParam(m -> toType(m).hasDocValues, true);
        private final Parameter<Boolean> stored = Parameter.storeParam(m -> toType(m).fieldType.stored(), false);

        private final Parameter<String> nullValue = Parameter.stringParam("null_value", false, m -> toType(m).nullValue, null)
            .acceptsNull();

        private final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> toType(m).eagerGlobalOrdinals,
            false
        );
        private final Parameter<Integer> ignoreAbove = Parameter.intParam(
            "ignore_above",
            true,
            m -> toType(m).ignoreAbove,
            Integer.MAX_VALUE
        );

        private final Parameter<String> indexOptions = Parameter.restrictedStringParam(
            "index_options",
            false,
            m -> toType(m).indexOptions,
            "docs",
            "freqs"
        );
        private final Parameter<Boolean> hasNorms = TextParams.norms(false, m -> toType(m).fieldType.omitNorms() == false);
        private final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> toType(m).similarity);

        private final Parameter<String> normalizer = Parameter.stringParam("normalizer", false, m -> toType(m).normalizerName, "default");

        private final Parameter<Boolean> splitQueriesOnWhitespace = Parameter.boolParam(
            "split_queries_on_whitespace",
            true,
            m -> toType(m).splitQueriesOnWhitespace,
            false
        );

        private final Parameter<Map<String, String>> meta = Parameter.metaParam();
        private final Parameter<Float> boost = Parameter.boostParam();
        private final IndexAnalyzers indexAnalyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexAnalyzers = indexAnalyzers;
        }

        public Builder(String name) {
            this(name, null);
        }

        public Builder ignoreAbove(int ignoreAbove) {
            this.ignoreAbove.setValue(ignoreAbove);
            return this;
        }

        Builder normalizer(String normalizerName) {
            this.normalizer.setValue(normalizerName);
            return this;
        }

        Builder nullValue(String nullValue) {
            this.nullValue.setValue(nullValue);
            return this;
        }

        public Builder docValues(boolean hasDocValues) {
            this.hasDocValues.setValue(hasDocValues);
            return this;
        }

        public Builder index(boolean index) {
            return this;
        }

        public Builder store(boolean store) {
            this.stored.setValue(store);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(
                indexed,
                hasDocValues,
                stored,
                nullValue,
                eagerGlobalOrdinals,
                ignoreAbove,
                indexOptions,
                hasNorms,
                similarity,
                normalizer,
                splitQueriesOnWhitespace,
                boost,
                meta
            );
        }

        /**
         * FlatObjectFieldType is the parent field type. the parent field enables KEYWORD_ANALYZER,
         * allows normalizer and splitQueriesOnWhitespace
         */
        private FlatObjectFieldType buildFlatObjectFieldType(BuilderContext context, FieldType fieldType) {
            NamedAnalyzer normalizer = Lucene.KEYWORD_ANALYZER;
            NamedAnalyzer searchAnalyzer = Lucene.KEYWORD_ANALYZER;
            String normalizerName = this.normalizer.getValue();
            if (Objects.equals(normalizerName, "default") == false) {
                assert indexAnalyzers != null;
                normalizer = indexAnalyzers.getNormalizer(normalizerName);
                if (normalizer == null) {
                    throw new MapperParsingException("normalizer [" + normalizerName + "] not found for field [" + name + "]");
                }
                if (splitQueriesOnWhitespace.getValue()) {
                    searchAnalyzer = indexAnalyzers.getWhitespaceNormalizer(normalizerName);
                } else {
                    searchAnalyzer = normalizer;
                }
            } else if (splitQueriesOnWhitespace.getValue()) {
                searchAnalyzer = Lucene.WHITESPACE_ANALYZER;
            }
            return new FlatObjectFieldType(buildFullName(context), fieldType, normalizer, searchAnalyzer, this);
        }

        /**
         * ValueFieldMapper is the sub field type for values in the Json.
         * use a keywordfieldtype
         */
        private ValueFieldMapper buildValueFieldMapper(BuilderContext context, FieldType fieldType, FlatObjectFieldType fft) {
            String fullName = buildFullName(context);
            FieldType vft = new FieldType(fieldType);
            vft.setOmitNorms(this.hasNorms.getValue() == false);
            KeywordFieldMapper.KeywordFieldType valueFieldType = new KeywordFieldMapper.KeywordFieldType(fullName + "._value", vft);
            // TODO: revisit analyzer object
            fft.setValueFieldType(valueFieldType);
            return new ValueFieldMapper(vft, valueFieldType);

        }

        /**
         * ValueAndPathFieldMapper is the sub field type for path=value format in the Json.
         * also use a keywordfieldtype
         */
        private ValueAndPathFieldMapper buildValueAndPathFieldMapper(BuilderContext context, FieldType fieldType, FlatObjectFieldType fft) {

            String fullName = buildFullName(context);
            FieldType vft = new FieldType(fieldType);
            vft.setOmitNorms(this.hasNorms.getValue() == false);
            KeywordFieldMapper.KeywordFieldType ValueAndPathFieldType = new KeywordFieldMapper.KeywordFieldType(
                fullName + "._valueAndPath",
                vft
            );
            // TODO: revisit analyzer object
            fft.setValueAndPathFieldType(ValueAndPathFieldType);
            return new ValueAndPathFieldMapper(vft, ValueAndPathFieldType);
        }

        /**
         * FlatObjectFieldMapper builds the FLatObjectFieldMapper itself, and also build the two sub fieldMappers:
         * ValueFieldMapper and ValueAndPathFieldMapper
         */
        @Override
        public FlatObjectFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            fieldtype.setOmitNorms(this.hasNorms.getValue() == false);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(this.indexed.getValue(), this.indexOptions.getValue()));
            fieldtype.setStored(this.stored.getValue());
            FlatObjectFieldType fft = buildFlatObjectFieldType(context, fieldtype);
            return new FlatObjectFieldMapper(
                name,
                fieldtype,
                fft,
                buildValueFieldMapper(context, fieldtype, fft),
                buildValueAndPathFieldMapper(context, fieldtype, fft),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getIndexAnalyzers()));

    /**
     * Field type for flat-object fields
     * one flat-object fields contains its own fieldType, one valueFieldType and one valueAndPathFieldType
     * @opensearch.internal
     */
    public static final class FlatObjectFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;

        private KeywordFieldMapper.KeywordFieldType valueFieldType;

        private KeywordFieldMapper.KeywordFieldType valueAndPathFieldType;

        public FlatObjectFieldType(
            String name,
            FieldType fieldType,
            NamedAnalyzer normalizer,
            NamedAnalyzer searchAnalyzer,
            Builder builder
        ) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                fieldType.stored(),
                builder.hasDocValues.getValue(),
                new TextSearchInfo(fieldType, builder.similarity.getValue(), searchAnalyzer, searchAnalyzer),
                builder.meta.getValue()
            );
            setEagerGlobalOrdinals(builder.eagerGlobalOrdinals.getValue());
            setIndexAnalyzer(normalizer);
            setBoost(builder.boost.getValue());
            this.ignoreAbove = builder.ignoreAbove.getValue();
            this.nullValue = builder.nullValue.getValue();
        }

        public FlatObjectFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, false, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
        }

        public FlatObjectFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        public FlatObjectFieldType(String name, FieldType fieldType) {
            super(
                name,
                fieldType.indexOptions() != IndexOptions.NONE,
                false,
                false,
                new TextSearchInfo(fieldType, null, Lucene.KEYWORD_ANALYZER, Lucene.KEYWORD_ANALYZER),
                Collections.emptyMap()
            );
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
        }

        public FlatObjectFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, false, true, new TextSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
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
            // flat-objects are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // flat-object analyzer with the default attribute source which encodes terms using UTF8
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

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitve,
            QueryShardContext context
        ) {
            // flat-object field types are always normalized, so ignore case sensitivity and force normalize the wildcard
            // query text
            return super.wildcardQuery(value, method, caseInsensitve, true, context);
        }

    }

    private final boolean indexed;
    private final boolean hasDocValues;
    private final String nullValue;
    private final boolean eagerGlobalOrdinals;
    private final int ignoreAbove;
    private final String indexOptions;
    private final FieldType fieldType;
    private final SimilarityProvider similarity;
    private final String normalizerName;
    private final boolean splitQueriesOnWhitespace;
    private final ValueFieldMapper valueFieldMapper;
    private final ValueAndPathFieldMapper valueAndPathFieldMapper;

    private final IndexAnalyzers indexAnalyzers;

    protected FlatObjectFieldMapper(
        String simpleName,
        FieldType fieldType,
        FlatObjectFieldType mappedFieldType,
        ValueFieldMapper valueFieldMapper,
        ValueAndPathFieldMapper valueAndPathFieldMapper,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.indexed = builder.indexed.getValue();
        this.hasDocValues = builder.hasDocValues.getValue();
        this.nullValue = builder.nullValue.getValue();
        this.eagerGlobalOrdinals = builder.eagerGlobalOrdinals.getValue();
        this.ignoreAbove = builder.ignoreAbove.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.fieldType = fieldType;
        this.similarity = builder.similarity.getValue();
        this.normalizerName = builder.normalizer.getValue();
        this.splitQueriesOnWhitespace = builder.splitQueriesOnWhitespace.getValue();
        this.indexAnalyzers = builder.indexAnalyzers;
        this.valueFieldMapper = valueFieldMapper;
        this.valueAndPathFieldMapper = valueAndPathFieldMapper;
    }

    /**
     * TODO: Placeholder, this is used at keywordfieldmapper, considering to remove ignoreAbove
     * Values that have more chars than the return value of this method will
     *  be skipped at parsing time. */
    public int ignoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected FlatObjectFieldMapper clone() {
        return (FlatObjectFieldMapper) super.clone();
    }

    @Override
    public FlatObjectFieldType fieldType() {
        return (FlatObjectFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value = null;
        String fieldName = null;

        if (context.externalValueSet()) {
            value = context.externalValue().toString();
            ParseValueAddFields(context, value, fieldType().name());
        } else {
            JsonToStringXContentParser JsonToStringParser = new JsonToStringXContentParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                context,
                fieldType().name()
            );
            /**
             * JsonToStringParser is the main parser class to transform JSON into stringFields in a XContentParser
             */
            XContentParser parser = JsonToStringParser.parseObject();

            XContentParser.Token currentToken;
            while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                switch (currentToken) {
                    case FIELD_NAME:
                        fieldName = parser.currentName();
                        logger.info("fieldName: " + fieldName);
                        break;
                    case VALUE_STRING:
                        value = parser.textOrNull();
                        logger.info("value: " + value);
                        ParseValueAddFields(context, value, fieldName);
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

    private void ParseValueAddFields(ParseContext context, String value, String fieldName) throws IOException {
        if (value == null || value.length() > ignoreAbove) {
            return;
        }

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, name(), value);
        }

        String[] valueTypeList = fieldName.split("\\._");
        String valueType = "._" + valueTypeList[valueTypeList.length - 1];
        logger.info("valueType: " + valueType);
        /**
         * the JsonToStringXContentParser returns XContentParser with 3 string fields
         * fieldName, fieldName._value, fieldName._valueAndPath
         */

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            logger.info("FlatObjectField name is " + fieldType().name());
            logger.info("FlatObjectField value is " + value);
            // convert to utf8 only once before feeding postings/dv/stored fields

            final BytesRef binaryValue = new BytesRef(value);
            Field field = new FlatObjectField(fieldType().name(), binaryValue, fieldType);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
            /**
             * Indentified by the stringfield name,
             * fieldName will be store through the parent FlatFieldMapper,which contains all the keys
             * fieldName._value will be store through the valueFieldMapper, which contains the values of the Json Object
             * fieldName._valueAndPath will be store through the valueAndPathFieldMapper, which contains the values of
             * the Json Object.
             */
            if (fieldName.equals(fieldType().name())) {
                context.doc().add(field);
            }
            if (valueType.equals(VALUE_SUFFIX)) {
                if (valueFieldMapper != null) {
                    logger.info("valueFieldMapper value is " + value);
                    valueFieldMapper.addField(context, value);
                }
            }
            if (valueType.equals(VALUE_AND_PATH_SUFFIX)) {
                if (valueAndPathFieldMapper != null) {
                    logger.info("valueAndPathFieldMapper value is " + value);
                    valueAndPathFieldMapper.addField(context, value);
                }
            }

            // TODo: to revisit if flat-object needs docValues.
            if (fieldType().hasDocValues()) {
                if (context.doc().getField(fieldType().name()) == null || !context.doc().getFields(fieldType().name()).equals(field)) {
                    context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
                }
            }

        }

    }

    private static String normalizeValue(NamedAnalyzer normalizer, String field, String value) throws IOException {

        try (TokenStream ts = normalizer.tokenStream(field, value)) {
            final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
            ts.reset();
            if (ts.incrementToken() == false) {
                throw new IllegalStateException(
                    "The normalization token stream is "
                        + "expected to produce exactly 1 token, but got 0 for analyzer "
                        + normalizer
                        + " and input \""
                        + value
                        + "\""
                );
            }
            final String newValue = termAtt.toString();
            if (ts.incrementToken()) {
                throw new IllegalStateException(
                    "The normalization token stream is "
                        + "expected to produce exactly 1 token, but got 2+ for analyzer "
                        + normalizer
                        + " and input \""
                        + value
                        + "\""
                );
            }
            ts.end();
            return newValue;
        }
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), indexAnalyzers).init(this);
    }

    // TODO Further simplify the code by new KeyWordFieldMapper to be ValueAndPathFieldMapper and ValueFieldMapper
    private static final class ValueAndPathFieldMapper extends FieldMapper {

        protected ValueAndPathFieldMapper(FieldType fieldType, KeywordFieldMapper.KeywordFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            // context.doc().add(new Field(fieldType().name(), value, fieldType));
            final BytesRef binaryValue = new BytesRef(value);
            if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
                Field field = new KeywordFieldMapper.KeywordField(fieldType().name(), binaryValue, fieldType);
                // Field field = new (fieldType().name()+VALUE_AND_PATH_SUFFIX, binaryValue, fieldType);

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
