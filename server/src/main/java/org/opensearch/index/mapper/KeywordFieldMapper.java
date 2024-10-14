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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.OpenSearchException;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.xcontent.XContentParser;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

/**
 * A field mapper for keywords. This mapper accepts strings and indexes them as-is.
 *
 * @opensearch.internal
 */
public final class KeywordFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "keyword";

    /**
     * Default parameters
     *
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
     * The keyword field for the field mapper
     *
     * @opensearch.internal
     */
    public static class KeywordField extends Field {

        public KeywordField(String field, BytesRef term, FieldType ft) {
            super(field, term, ft);
        }

    }

    private static KeywordFieldMapper toType(FieldMapper in) {
        return (KeywordFieldMapper) in;
    }

    /**
     * The builder for the field mapper
     *
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

        protected KeywordFieldType buildFieldType(BuilderContext context, FieldType fieldType) {
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
            return new KeywordFieldType(buildFullName(context), fieldType, normalizer, searchAnalyzer, this);
        }

        @Override
        public KeywordFieldMapper build(BuilderContext context) {
            FieldType fieldtype = new FieldType(Defaults.FIELD_TYPE);
            fieldtype.setOmitNorms(this.hasNorms.getValue() == false);
            fieldtype.setIndexOptions(TextParams.toIndexOptions(this.indexed.getValue(), this.indexOptions.getValue()));
            fieldtype.setStored(this.stored.getValue());
            return new KeywordFieldMapper(
                name,
                fieldtype,
                buildFieldType(context, fieldtype),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.getIndexAnalyzers()));

    /**
     * Field type for keyword fields
     *
     * @opensearch.internal
     */
    public static class KeywordFieldType extends StringFieldType {

        private final int ignoreAbove;
        private final String nullValue;

        public KeywordFieldType(String name, FieldType fieldType, NamedAnalyzer normalizer, NamedAnalyzer searchAnalyzer, Builder builder) {
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

        public KeywordFieldType(String name, boolean isSearchable, boolean hasDocValues, Map<String, String> meta) {
            super(name, isSearchable, false, hasDocValues, TextSearchInfo.SIMPLE_MATCH_ONLY, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
        }

        public KeywordFieldType(String name) {
            this(name, true, true, Collections.emptyMap());
        }

        public KeywordFieldType(String name, FieldType fieldType) {
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

        public KeywordFieldType(String name, NamedAnalyzer analyzer) {
            super(name, true, false, true, new TextSearchInfo(Defaults.FIELD_TYPE, null, analyzer, analyzer), Collections.emptyMap());
            this.ignoreAbove = Integer.MAX_VALUE;
            this.nullValue = null;
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
                throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] doesn't " + "support formats.");
            }

            return new SourceValueFetcher(name(), context, nullValue) {
                @Override
                protected String parseSourceValue(Object value) {
                    String keywordValue = value.toString();
                    if (keywordValue.length() > ignoreAbove) {
                        return null;
                    }

                    NamedAnalyzer normalizer = normalizer();
                    if (normalizer == null) {
                        return keywordValue;
                    }

                    try {
                        return normalizeValue(normalizer, name(), keywordValue);
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
            // keywords are internally stored as utf8 bytes
            BytesRef binaryValue = (BytesRef) value;
            return binaryValue.utf8ToString();
        }

        @Override
        protected BytesRef indexedValueForSearch(Object value) {
            if (getTextSearchInfo().getSearchAnalyzer() == Lucene.KEYWORD_ANALYZER) {
                // keyword analyzer with the default attribute source which encodes terms using UTF8
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

        protected Object rewriteForDocValue(Object value) {
            return value;
        }

        @Override
        public Query termsQuery(List<?> values, QueryShardContext context) {
            failIfNotIndexedAndNoDocValues();
            // has index and doc_values enabled
            if (isSearchable() && hasDocValues()) {
                if (!context.keywordFieldIndexOrDocValuesEnabled()) {
                    return super.termsQuery(values, context);
                }
                BytesRef[] iBytesRefs = new BytesRef[values.size()];
                BytesRef[] dVByteRefs = new BytesRef[values.size()];
                for (int i = 0; i < iBytesRefs.length; i++) {
                    iBytesRefs[i] = indexedValueForSearch(values.get(i));
                    dVByteRefs[i] = indexedValueForSearch(rewriteForDocValue(values.get(i)));
                }
                Query indexQuery = new TermInSetQuery(name(), iBytesRefs);
                Query dvQuery = new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, name(), dVByteRefs);
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            // if we only have doc_values enabled, we construct a new query with doc_values re-written
            if (hasDocValues()) {
                BytesRef[] bytesRefs = new BytesRef[values.size()];
                for (int i = 0; i < bytesRefs.length; i++) {
                    bytesRefs[i] = indexedValueForSearch(rewriteForDocValue(values.get(i)));
                }
                return new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, name(), bytesRefs);
            }
            // has index enabled, we're going to return the query as is
            return super.termsQuery(values, context);
        }

        @Override
        public Query prefixQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[prefix] queries cannot be executed when '"
                        + ALLOW_EXPENSIVE_QUERIES.getKey()
                        + "' is set to false. For optimised prefix queries on text "
                        + "fields please enable [index_prefixes]."
                );
            }
            failIfNotIndexedAndNoDocValues();
            if (isSearchable() && hasDocValues()) {
                if (!context.keywordFieldIndexOrDocValuesEnabled()) {
                    return super.prefixQuery(value, method, caseInsensitive, context);
                }
                Query indexQuery = super.prefixQuery(value, method, caseInsensitive, context);
                Query dvQuery = super.prefixQuery(
                    (String) rewriteForDocValue(value),
                    MultiTermQuery.DOC_VALUES_REWRITE,
                    caseInsensitive,
                    context
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            if (hasDocValues()) {
                if (caseInsensitive) {
                    return AutomatonQueries.caseInsensitivePrefixQuery(
                        (new Term(name(), indexedValueForSearch(rewriteForDocValue(value)))),
                        MultiTermQuery.DOC_VALUES_REWRITE
                    );
                }
                return new PrefixQuery(
                    new Term(name(), indexedValueForSearch(rewriteForDocValue(value))),
                    MultiTermQuery.DOC_VALUES_REWRITE
                );
            }
            return super.prefixQuery(value, method, caseInsensitive, context);
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
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[regexp] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to " + "false."
                );
            }
            failIfNotIndexedAndNoDocValues();
            if (isSearchable() && hasDocValues()) {
                if (!context.keywordFieldIndexOrDocValuesEnabled()) {
                    return super.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
                }
                Query indexQuery = super.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
                Query dvQuery = super.regexpQuery(
                    (String) rewriteForDocValue(value),
                    syntaxFlags,
                    matchFlags,
                    maxDeterminizedStates,
                    MultiTermQuery.DOC_VALUES_REWRITE,
                    context
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            if (hasDocValues()) {
                return new RegexpQuery(
                    new Term(name(), indexedValueForSearch(rewriteForDocValue(value))),
                    syntaxFlags,
                    matchFlags,
                    RegexpQuery.DEFAULT_PROVIDER,
                    maxDeterminizedStates,
                    MultiTermQuery.DOC_VALUES_REWRITE
                );
            }
            return super.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
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
            if (isSearchable() && hasDocValues()) {
                Query indexQuery = new TermRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                    upperTerm == null ? null : indexedValueForSearch(upperTerm),
                    includeLower,
                    includeUpper
                );
                Query dvQuery = new TermRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(rewriteForDocValue(lowerTerm)),
                    upperTerm == null ? null : indexedValueForSearch(rewriteForDocValue(upperTerm)),
                    includeLower,
                    includeUpper,
                    MultiTermQuery.DOC_VALUES_REWRITE
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            if (hasDocValues()) {
                return new TermRangeQuery(
                    name(),
                    lowerTerm == null ? null : indexedValueForSearch(rewriteForDocValue(lowerTerm)),
                    upperTerm == null ? null : indexedValueForSearch(rewriteForDocValue(upperTerm)),
                    includeLower,
                    includeUpper,
                    MultiTermQuery.DOC_VALUES_REWRITE
                );
            }
            return new TermRangeQuery(
                name(),
                lowerTerm == null ? null : indexedValueForSearch(lowerTerm),
                upperTerm == null ? null : indexedValueForSearch(upperTerm),
                includeLower,
                includeUpper
            );
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
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[fuzzy] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to " + "false."
                );
            }
            if (isSearchable() && hasDocValues()) {
                if (!context.keywordFieldIndexOrDocValuesEnabled()) {
                    return super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, method, context);
                }
                Query indexQuery = super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, method, context);
                Query dvQuery = super.fuzzyQuery(
                    rewriteForDocValue(value),
                    fuzziness,
                    prefixLength,
                    maxExpansions,
                    transpositions,
                    MultiTermQuery.DOC_VALUES_REWRITE,
                    context
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            if (hasDocValues()) {
                return new FuzzyQuery(
                    new Term(name(), indexedValueForSearch(rewriteForDocValue(value))),
                    fuzziness.asDistance(BytesRefs.toString(rewriteForDocValue(value))),
                    prefixLength,
                    maxExpansions,
                    transpositions,
                    MultiTermQuery.DOC_VALUES_REWRITE
                );
            }
            return super.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, context);
        }

        @Override
        public Query wildcardQuery(
            String value,
            @Nullable MultiTermQuery.RewriteMethod method,
            boolean caseInsensitive,
            QueryShardContext context
        ) {
            if (context.allowExpensiveQueries() == false) {
                throw new OpenSearchException(
                    "[wildcard] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to " + "false."
                );
            }
            failIfNotIndexedAndNoDocValues();
            // keyword field types are always normalized, so ignore case sensitivity and force normalize the
            // wildcard
            // query text
            if (isSearchable() && hasDocValues()) {
                if (!context.keywordFieldIndexOrDocValuesEnabled()) {
                    return super.wildcardQuery(value, method, caseInsensitive, true, context);
                }
                Query indexQuery = super.wildcardQuery(value, method, caseInsensitive, true, context);
                Query dvQuery = super.wildcardQuery(
                    (String) rewriteForDocValue(value),
                    MultiTermQuery.DOC_VALUES_REWRITE,
                    caseInsensitive,
                    true,
                    context
                );
                return new IndexOrDocValuesQuery(indexQuery, dvQuery);
            }
            if (hasDocValues()) {
                Term term;
                value = normalizeWildcardPattern(name(), value, getTextSearchInfo().getSearchAnalyzer());
                term = new Term(name(), (String) rewriteForDocValue(value));
                if (caseInsensitive) {
                    return AutomatonQueries.caseInsensitiveWildcardQuery(term, method);
                }
                return new WildcardQuery(term, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, MultiTermQuery.DOC_VALUES_REWRITE);
            }
            return super.wildcardQuery(value, method, caseInsensitive, true, context);
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

    private final IndexAnalyzers indexAnalyzers;

    protected KeywordFieldMapper(
        String simpleName,
        FieldType fieldType,
        MappedFieldType mappedFieldType,
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
    }

    /**
     * Values that have more chars than the return value of this method will
     * be skipped at parsing time.
     */
    public int ignoreAbove() {
        return ignoreAbove;
    }

    @Override
    protected KeywordFieldMapper clone() {
        return (KeywordFieldMapper) super.clone();
    }

    @Override
    public KeywordFieldType fieldType() {
        return (KeywordFieldType) super.fieldType();
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        String value;
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

        NamedAnalyzer normalizer = fieldType().normalizer();
        if (normalizer != null) {
            value = normalizeValue(normalizer, name(), value);
        }

        // convert to utf8 only once before feeding postings/dv/stored fields
        final BytesRef binaryValue = new BytesRef(value);
        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new KeywordField(fieldType().name(), binaryValue, fieldType);
            context.doc().add(field);

            if (fieldType().hasDocValues() == false && fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
        }

        if (fieldType().hasDocValues()) {
            context.doc().add(new SortedSetDocValuesField(fieldType().name(), binaryValue));
        }
    }

    static String normalizeValue(NamedAnalyzer normalizer, String field, String value) throws IOException {
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
}
