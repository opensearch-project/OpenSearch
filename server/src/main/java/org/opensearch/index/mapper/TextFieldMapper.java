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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.AnalyzerWrapper;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ngram.EdgeNGramTokenFilter;
import org.apache.lucene.analysis.shingle.FixedShingleFilter;
import org.apache.lucene.analysis.tokenattributes.BytesTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.Intervals;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.Version;
import org.opensearch.common.collect.Iterators;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.AutomatonQueries;
import org.opensearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.AnalyzerScope;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.opensearch.index.mapper.Mapper.TypeParser.ParserContext;
import org.opensearch.index.query.IntervalBuilder;
import org.opensearch.index.query.IntervalMode;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityProvider;
import org.opensearch.search.aggregations.support.CoreValuesSourceType;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/**
 * A {@link FieldMapper} for full-text fields.
 *
 * @opensearch.internal
 */
public class TextFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "text";
    protected static final int POSITION_INCREMENT_GAP_USE_ANALYZER = -1;
    private static final String FAST_PHRASE_SUFFIX = "._index_phrase";

    /**
     * Default paramters for text fields
     *
     * @opensearch.internal
     */
    public static class Defaults {
        public static final double FIELDDATA_MIN_FREQUENCY = 0;
        public static final double FIELDDATA_MAX_FREQUENCY = Integer.MAX_VALUE;
        public static final int FIELDDATA_MIN_SEGMENT_SIZE = 0;
        public static final int INDEX_PREFIX_MIN_CHARS = 2;
        public static final int INDEX_PREFIX_MAX_CHARS = 5;

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(true);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setStoreTermVectors(false);
            FIELD_TYPE.setOmitNorms(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            FIELD_TYPE.freeze();
        }

        /**
         * The default position_increment_gap is set to 100 so that phrase
         * queries of reasonably high slop will not match across field values.
         */
        public static final int POSITION_INCREMENT_GAP = 100;
    }

    private static TextFieldMapper toType(FieldMapper in) {
        return (TextFieldMapper) in;
    }

    /**
     * Prefix configuration
     *
     * @opensearch.internal
     */
    protected static final class PrefixConfig implements ToXContent {
        final int minChars;
        final int maxChars;

        PrefixConfig(int minChars, int maxChars) {
            this.minChars = minChars;
            this.maxChars = maxChars;
            if (minChars > maxChars) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be less than max_chars [" + maxChars + "]");
            }
            if (minChars < 1) {
                throw new IllegalArgumentException("min_chars [" + minChars + "] must be greater than zero");
            }
            if (maxChars >= 20) {
                throw new IllegalArgumentException("max_chars [" + maxChars + "] must be less than 20");
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PrefixConfig that = (PrefixConfig) o;
            return minChars == that.minChars && maxChars == that.maxChars;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minChars, maxChars);
        }

        @Override
        public String toString() {
            return "{ min_chars=" + minChars + ", max_chars=" + maxChars + " }";
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min_chars", minChars);
            builder.field("max_chars", maxChars);
            builder.endObject();
            return builder;
        }
    }

    static PrefixConfig parsePrefixConfig(String propName, ParserContext parserContext, Object propNode) {
        if (propNode == null) {
            return null;
        }
        Map<?, ?> indexPrefix = (Map<?, ?>) propNode;
        int minChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("min_chars"), Defaults.INDEX_PREFIX_MIN_CHARS);
        int maxChars = XContentMapValues.nodeIntegerValue(indexPrefix.remove("max_chars"), Defaults.INDEX_PREFIX_MAX_CHARS);
        DocumentMapperParser.checkNoRemainingFields(propName, indexPrefix, parserContext.indexVersionCreated());
        return new PrefixConfig(minChars, maxChars);
    }

    /**
     * Frequency filter for field data
     *
     * @opensearch.internal
     */
    protected static final class FielddataFrequencyFilter implements ToXContent {
        final double minFreq;
        final double maxFreq;
        final int minSegmentSize;

        private FielddataFrequencyFilter(double minFreq, double maxFreq, int minSegmentSize) {
            this.minFreq = minFreq;
            this.maxFreq = maxFreq;
            this.minSegmentSize = minSegmentSize;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FielddataFrequencyFilter that = (FielddataFrequencyFilter) o;
            return Double.compare(that.minFreq, minFreq) == 0
                && Double.compare(that.maxFreq, maxFreq) == 0
                && minSegmentSize == that.minSegmentSize;
        }

        @Override
        public int hashCode() {
            return Objects.hash(minFreq, maxFreq, minSegmentSize);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min", minFreq);
            builder.field("max", maxFreq);
            builder.field("min_segment_size", minSegmentSize);
            builder.endObject();
            return builder;
        }

        @Override
        public String toString() {
            return "{ min=" + minFreq + ", max=" + maxFreq + ", min_segment_size=" + minSegmentSize + " }";
        }
    }

    private static final FielddataFrequencyFilter DEFAULT_FILTER = new FielddataFrequencyFilter(
        Defaults.FIELDDATA_MIN_FREQUENCY,
        Defaults.FIELDDATA_MAX_FREQUENCY,
        Defaults.FIELDDATA_MIN_SEGMENT_SIZE
    );

    private static FielddataFrequencyFilter parseFrequencyFilter(String name, ParserContext parserContext, Object node) {
        Map<?, ?> frequencyFilter = (Map<?, ?>) node;
        double minFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("min"), 0);
        double maxFrequency = XContentMapValues.nodeDoubleValue(frequencyFilter.remove("max"), Integer.MAX_VALUE);
        int minSegmentSize = XContentMapValues.nodeIntegerValue(frequencyFilter.remove("min_segment_size"), 0);
        DocumentMapperParser.checkNoRemainingFields(name, frequencyFilter, parserContext.indexVersionCreated());
        return new FielddataFrequencyFilter(minFrequency, maxFrequency, minSegmentSize);
    }

    /**
     * Builder for text fields
     *
     * @opensearch.internal
     */
    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Version indexCreatedVersion;

        protected final Parameter<Boolean> index = Parameter.indexParam(m -> toType(m).mappedFieldType.isSearchable(), true);
        protected final Parameter<Boolean> store = Parameter.storeParam(m -> toType(m).fieldType.stored(), false);

        final Parameter<SimilarityProvider> similarity = TextParams.similarity(m -> toType(m).similarity);

        final Parameter<String> indexOptions = TextParams.indexOptions(m -> toType(m).indexOptions);
        final Parameter<Boolean> norms = TextParams.norms(true, m -> toType(m).fieldType.omitNorms() == false);
        final Parameter<String> termVectors = TextParams.termVectors(m -> toType(m).termVectors);
        final Parameter<Integer> positionIncrementGap = Parameter.intParam(
            "position_increment_gap",
            false,
            m -> toType(m).positionIncrementGap,
            POSITION_INCREMENT_GAP_USE_ANALYZER
        );

        final Parameter<Boolean> fieldData = Parameter.boolParam(
            "fielddata",
            true,
            m -> ((TextFieldType) toType(m).mappedFieldType).fielddata,
            false
        );
        final Parameter<FielddataFrequencyFilter> freqFilter = new Parameter<>(
            "fielddata_frequency_filter",
            true,
            () -> DEFAULT_FILTER,
            TextFieldMapper::parseFrequencyFilter,
            m -> toType(m).freqFilter
        );
        final Parameter<Boolean> eagerGlobalOrdinals = Parameter.boolParam(
            "eager_global_ordinals",
            true,
            m -> toType(m).mappedFieldType.eagerGlobalOrdinals(),
            false
        );

        final Parameter<Boolean> indexPhrases = Parameter.boolParam(
            "index_phrases",
            false,
            m -> ((TextFieldType) toType(m).mappedFieldType).indexPhrases,
            false
        );
        final Parameter<PrefixConfig> indexPrefixes = new Parameter<>(
            "index_prefixes",
            false,
            () -> null,
            TextFieldMapper::parsePrefixConfig,
            m -> Optional.ofNullable(((TextFieldType) toType(m).mappedFieldType).prefixFieldType)
                .map(p -> new PrefixConfig(p.minChars, p.maxChars))
                .orElse(null)
        ).acceptsNull();

        protected final Parameter<Float> boost = Parameter.boostParam();
        protected final Parameter<Map<String, String>> meta = Parameter.metaParam();

        final TextParams.Analyzers analyzers;

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            this(name, Version.CURRENT, indexAnalyzers);
        }

        public Builder(String name, Version indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name);
            this.indexCreatedVersion = indexCreatedVersion;
            this.analyzers = new TextParams.Analyzers(indexAnalyzers);
        }

        public Builder index(boolean index) {
            this.index.setValue(index);
            return this;
        }

        public Builder store(boolean store) {
            this.store.setValue(store);
            return this;
        }

        public Builder fielddata(boolean fielddata) {
            this.fieldData.setValue(fielddata);
            return this;
        }

        public Builder fielddataFrequencyFilter(double min, double max, int segs) {
            this.freqFilter.setValue(new FielddataFrequencyFilter(min, max, segs));
            return this;
        }

        public Builder addMultiField(Mapper.Builder<?> builder) {
            this.multiFieldsBuilder.add(builder);
            return this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return Arrays.asList(
                index,
                store,
                indexOptions,
                norms,
                termVectors,
                analyzers.indexAnalyzer,
                analyzers.searchAnalyzer,
                analyzers.searchQuoteAnalyzer,
                similarity,
                positionIncrementGap,
                fieldData,
                freqFilter,
                eagerGlobalOrdinals,
                indexPhrases,
                indexPrefixes,
                boost,
                meta
            );
        }

        protected TextFieldType buildFieldType(FieldType fieldType, BuilderContext context) {
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();
            if (positionIncrementGap.get() != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                    throw new IllegalArgumentException(
                        "Cannot set position_increment_gap on field [" + name + "] without positions enabled"
                    );
                }
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer, positionIncrementGap.get());
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer, positionIncrementGap.get());
                searchQuoteAnalyzer = new NamedAnalyzer(searchQuoteAnalyzer, positionIncrementGap.get());
            }
            TextSearchInfo tsi = new TextSearchInfo(fieldType, similarity.getValue(), searchAnalyzer, searchQuoteAnalyzer);
            TextFieldType ft = new TextFieldType(buildFullName(context), index.getValue(), store.getValue(), tsi, meta.getValue());
            ft.setIndexAnalyzer(indexAnalyzer);
            ft.setEagerGlobalOrdinals(eagerGlobalOrdinals.getValue());
            ft.setBoost(boost.getValue());
            if (fieldData.getValue()) {
                ft.setFielddata(true, freqFilter.getValue());
            }
            return ft;
        }

        protected PrefixFieldMapper buildPrefixMapper(BuilderContext context, FieldType fieldType, TextFieldType tft) {
            if (indexPrefixes.get() == null) {
                return null;
            }
            if (index.getValue() == false) {
                throw new IllegalArgumentException("Cannot set index_prefixes on unindexed field [" + name() + "]");
            }
            /*
             * Mappings before v7.2.1 use {@link Builder#name} instead of {@link Builder#fullName}
             * to build prefix field names so we preserve the name that was used at creation time
             * even if it is different from the expected one (in case the field is nested under an object
             * or a multi-field). This way search will continue to work on old indices and new indices
             * will use the expected full name.
             */
            String fullName = buildFullName(context);
            // Copy the index options of the main field to allow phrase queries on
            // the prefix field.
            FieldType pft = new FieldType(fieldType);
            pft.setOmitNorms(true);
            if (fieldType.indexOptions() == IndexOptions.DOCS_AND_FREQS) {
                // frequencies are not needed because prefix queries always use a constant score
                pft.setIndexOptions(IndexOptions.DOCS);
            } else {
                pft.setIndexOptions(fieldType.indexOptions());
            }
            if (fieldType.storeTermVectorOffsets()) {
                pft.setStoreTermVectorOffsets(true);
            }
            PrefixFieldType prefixFieldType = new PrefixFieldType(tft, fullName + "._index_prefix", indexPrefixes.get());
            tft.setPrefixFieldType(prefixFieldType);
            return new PrefixFieldMapper(pft, prefixFieldType);
        }

        protected PhraseFieldMapper buildPhraseMapper(FieldType fieldType, TextFieldType parent) {
            if (indexPhrases.get() == false) {
                return null;
            }
            if (index.get() == false) {
                throw new IllegalArgumentException("Cannot set index_phrases on unindexed field [" + name() + "]");
            }
            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) < 0) {
                throw new IllegalArgumentException("Cannot set index_phrases on field [" + name() + "] if positions are not enabled");
            }
            FieldType phraseFieldType = new FieldType(fieldType);
            parent.setIndexPhrases();
            return new PhraseFieldMapper(phraseFieldType, new PhraseFieldType(parent));
        }

        @Override
        public TextFieldMapper build(BuilderContext context) {
            FieldType fieldType = TextParams.buildFieldType(index, store, indexOptions, norms, termVectors);
            TextFieldType tft = buildFieldType(fieldType, context);
            return new TextFieldMapper(
                name,
                fieldType,
                tft,
                buildPrefixMapper(context, fieldType, tft),
                buildPhraseMapper(fieldType, tft),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                this
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    /**
     * A phrase wrapped field analyzer
     *
     * @opensearch.internal
     */
    private static class PhraseWrappedAnalyzer extends AnalyzerWrapper {

        private final Analyzer delegate;

        PhraseWrappedAnalyzer(Analyzer delegate) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            return new TokenStreamComponents(components.getSource(), new FixedShingleFilter(components.getTokenStream(), 2));
        }
    }

    /**
     * A prefix wrapped analyzer
     *
     * @opensearch.internal
     */
    private static class PrefixWrappedAnalyzer extends AnalyzerWrapper {

        private final int minChars;
        private final int maxChars;
        private final Analyzer delegate;
        private final int positionIncrementGap;

        PrefixWrappedAnalyzer(Analyzer delegate, int minChars, int maxChars, int positionIncrementGap) {
            super(delegate.getReuseStrategy());
            this.delegate = delegate;
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.positionIncrementGap = positionIncrementGap;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            return delegate;
        }

        @Override
        public int getPositionIncrementGap(String fieldName) {
            return positionIncrementGap;
        }

        @Override
        protected TokenStreamComponents wrapComponents(String fieldName, TokenStreamComponents components) {
            TokenFilter filter = new EdgeNGramTokenFilter(components.getTokenStream(), minChars, maxChars, false);
            return new TokenStreamComponents(components.getSource(), filter);
        }
    }

    /**
     * Field type for phrase fields
     *
     * @opensearch.internal
     */
    static final class PhraseFieldType extends StringFieldType {

        final TextFieldType parent;

        PhraseFieldType(TextFieldType parent) {
            super(parent.name() + FAST_PHRASE_SUFFIX, true, false, false, parent.getTextSearchInfo(), Collections.emptyMap());
            setAnalyzer(parent.indexAnalyzer().name(), parent.indexAnalyzer().analyzer());
            this.parent = parent;
        }

        void setAnalyzer(String name, Analyzer delegate) {
            setIndexAnalyzer(new NamedAnalyzer(name, AnalyzerScope.INDEX, new PhraseWrappedAnalyzer(delegate)));
        }

        @Override
        public String typeName() {
            return "phrase";
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // Because this internal field is modelled as a multi-field, SourceValueFetcher will look up its
            // parent field in _source. So we don't need to use the parent field name here.
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Field type for prefix fields
     *
     * @opensearch.internal
     */
    static final class PrefixFieldType extends StringFieldType {

        final int minChars;
        final int maxChars;
        final TextFieldType parent;

        PrefixFieldType(TextFieldType parentField, String name, PrefixConfig config) {
            this(parentField, name, config.minChars, config.maxChars);
        }

        PrefixFieldType(TextFieldType parent, String name, int minChars, int maxChars) {
            super(name, true, false, false, parent.getTextSearchInfo(), Collections.emptyMap());
            this.minChars = minChars;
            this.maxChars = maxChars;
            this.parent = parent;
            setAnalyzer(parent.indexAnalyzer());
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            // Because this internal field is modelled as a multi-field, SourceValueFetcher will look up its
            // parent field in _source. So we don't need to use the parent field name here.
            return SourceValueFetcher.toString(name(), context, format);
        }

        void setAnalyzer(NamedAnalyzer delegate) {
            String analyzerName = delegate.name();
            setIndexAnalyzer(
                new NamedAnalyzer(
                    analyzerName,
                    AnalyzerScope.INDEX,
                    new PrefixWrappedAnalyzer(delegate.analyzer(), minChars, maxChars, delegate.getPositionIncrementGap(analyzerName))
                )
            );
        }

        boolean accept(int length) {
            return length >= minChars - 1 && length <= maxChars;
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            if (value.length() >= minChars) {
                if (caseInsensitive) {
                    return super.termQueryCaseInsensitive(value, context);
                }
                return super.termQuery(value, context);
            }
            List<Automaton> automata = new ArrayList<>();
            if (caseInsensitive) {
                automata.add(AutomatonQueries.toCaseInsensitiveString(value, Integer.MAX_VALUE));
            } else {
                automata.add(Automata.makeString(value));
            }

            for (int i = value.length(); i < minChars; i++) {
                automata.add(Automata.makeAnyChar());
            }
            Automaton automaton = Operations.concatenate(automata);
            AutomatonQuery query = AutomatonQueries.createAutomatonQuery(new Term(name(), value + "*"), automaton, method);
            return new BooleanQuery.Builder().add(query, BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(parent.name(), value)), BooleanClause.Occur.SHOULD)
                .build();
        }

        public IntervalsSource intervals(BytesRef term) {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over a field [" + name() + "] without indexed positions");
            }
            if (term.length > maxChars) {
                return Intervals.prefix(term);
            }
            if (term.length >= minChars) {
                return Intervals.fixField(name(), Intervals.term(term));
            }
            StringBuilder sb = new StringBuilder(term.utf8ToString());
            for (int i = term.length; i < minChars; i++) {
                sb.append("?");
            }
            String wildcardTerm = sb.toString();
            return Intervals.or(Intervals.fixField(name(), Intervals.wildcard(new BytesRef(wildcardTerm))), Intervals.term(term));
        }

        @Override
        public String typeName() {
            return "prefix";
        }

        @Override
        public String toString() {
            return super.toString() + ",prefixChars=" + minChars + ":" + maxChars;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Field mapper for phrase fields
     *
     * @opensearch.internal
     */
    protected static final class PhraseFieldMapper extends FieldMapper {

        PhraseFieldMapper(FieldType fieldType, PhraseFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
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
            return "phrase";
        }
    }

    /**
     * Field mapper for prefix fields
     *
     * @opensearch.internal
     */
    protected static final class PrefixFieldMapper extends FieldMapper {

        protected PrefixFieldMapper(FieldType fieldType, PrefixFieldType mappedFieldType) {
            super(mappedFieldType.name(), fieldType, mappedFieldType, MultiFields.empty(), CopyTo.empty());
        }

        void addField(ParseContext context, String value) {
            context.doc().add(new Field(fieldType().name(), value, fieldType));
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
            return "prefix";
        }

        @Override
        public String toString() {
            return fieldType().toString();
        }
    }

    /**
     * Field type for text fields
     *
     * @opensearch.internal
     */
    public static class TextFieldType extends StringFieldType {

        private boolean fielddata;
        private FielddataFrequencyFilter filter;
        private PrefixFieldType prefixFieldType;
        private boolean indexPhrases = false;

        public TextFieldType(String name, boolean indexed, boolean stored, TextSearchInfo tsi, Map<String, String> meta) {
            super(name, indexed, stored, false, tsi, meta);
            fielddata = false;
        }

        public TextFieldType(String name, boolean indexed, boolean stored, Map<String, String> meta) {
            super(
                name,
                indexed,
                stored,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                meta
            );
            fielddata = false;
        }

        public TextFieldType(String name) {
            this(
                name,
                true,
                false,
                new TextSearchInfo(Defaults.FIELD_TYPE, null, Lucene.STANDARD_ANALYZER, Lucene.STANDARD_ANALYZER),
                Collections.emptyMap()
            );
        }

        public boolean fielddata() {
            return fielddata;
        }

        public void setFielddata(boolean fielddata, FielddataFrequencyFilter filter) {
            this.fielddata = fielddata;
            this.filter = filter;
        }

        public void setFielddata(boolean fielddata) {
            this.setFielddata(fielddata, DEFAULT_FILTER);
        }

        double fielddataMinFrequency() {
            return filter.minFreq;
        }

        double fielddataMaxFrequency() {
            return filter.maxFreq;
        }

        int fielddataMinSegmentSize() {
            return filter.minSegmentSize;
        }

        void setPrefixFieldType(PrefixFieldType prefixFieldType) {
            this.prefixFieldType = prefixFieldType;
        }

        void setIndexPhrases() {
            this.indexPhrases = true;
        }

        public PrefixFieldType getPrefixFieldType() {
            return this.prefixFieldType;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.toString(name(), context, format);
        }

        @Override
        public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitive, QueryShardContext context) {
            if (prefixFieldType == null || prefixFieldType.accept(value.length()) == false) {
                return super.prefixQuery(value, method, caseInsensitive, context);
            }
            Query tq = prefixFieldType.prefixQuery(value, method, caseInsensitive, context);
            if (method == null
                || method == MultiTermQuery.CONSTANT_SCORE_REWRITE
                || method == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
                return new ConstantScoreQuery(tq);
            }
            return tq;
        }

        @Override
        public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
            failIfNotIndexed();
            if (prefixFieldType != null
                && value.length() >= prefixFieldType.minChars
                && value.length() <= prefixFieldType.maxChars
                && prefixFieldType.getTextSearchInfo().hasPositions()) {

                return new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixFieldType.name(), indexedValueForSearch(value))), name());
            } else {
                SpanMultiTermQueryWrapper<?> spanMulti = new SpanMultiTermQueryWrapper<>(
                    new PrefixQuery(new Term(name(), indexedValueForSearch(value)))
                );
                spanMulti.setRewriteMethod(method);
                return spanMulti;
            }
        }

        @Override
        public IntervalsSource intervals(String text, int maxGaps, IntervalMode mode, NamedAnalyzer analyzer, boolean prefix)
            throws IOException {
            if (getTextSearchInfo().hasPositions() == false) {
                throw new IllegalArgumentException("Cannot create intervals over field [" + name() + "] with no positions indexed");
            }
            if (analyzer == null) {
                analyzer = getTextSearchInfo().getSearchAnalyzer();
            }
            if (prefix) {
                BytesRef normalizedTerm = analyzer.normalize(name(), text);
                if (prefixFieldType != null) {
                    return prefixFieldType.intervals(normalizedTerm);
                }
                return Intervals.prefix(normalizedTerm);
            }
            IntervalBuilder builder = new IntervalBuilder(name(), analyzer == null ? getTextSearchInfo().getSearchAnalyzer() : analyzer);
            return builder.analyzeText(text, maxGaps, mode);
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements) throws IOException {
            String field = name();
            // we can't use the index_phrases shortcut with slop, if there are gaps in the stream,
            // or if the incoming token stream is the output of a token graph due to
            // https://issues.apache.org/jira/browse/LUCENE-8916
            if (indexPhrases && slop == 0 && hasGaps(stream) == false && stream.hasAttribute(BytesTermAttribute.class) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            PhraseQuery.Builder builder = new PhraseQuery.Builder();
            builder.setSlop(slop);

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
            int position = -1;

            stream.reset();
            while (stream.incrementToken()) {
                if (termAtt.getBytesRef() == null) {
                    throw new IllegalStateException("Null term while building phrase query");
                }
                if (enablePosIncrements) {
                    position += posIncrAtt.getPositionIncrement();
                } else {
                    position += 1;
                }
                builder.add(new Term(field, termAtt.getBytesRef()), position);
            }

            return builder.build();
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
            String field = name();
            if (indexPhrases && slop == 0 && hasGaps(stream) == false) {
                stream = new FixedShingleFilter(stream, 2);
                field = field + FAST_PHRASE_SUFFIX;
            }
            return createPhraseQuery(stream, field, slop, enablePositionIncrements);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
            return analyzePhrasePrefix(stream, slop, maxExpansions);
        }

        private Query analyzePhrasePrefix(TokenStream stream, int slop, int maxExpansions) throws IOException {
            String prefixField = prefixFieldType == null || slop > 0 ? null : prefixFieldType.name();
            IntPredicate usePrefix = (len) -> len >= prefixFieldType.minChars && len <= prefixFieldType.maxChars;
            return createPhrasePrefixQuery(stream, name(), slop, maxExpansions, prefixField, usePrefix);
        }

        public static boolean hasGaps(TokenStream stream) throws IOException {
            assert stream instanceof CachingTokenFilter;
            PositionIncrementAttribute posIncAtt = stream.getAttribute(PositionIncrementAttribute.class);
            stream.reset();
            while (stream.incrementToken()) {
                if (posIncAtt.getPositionIncrement() > 1) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            if (fielddata == false) {
                throw new IllegalArgumentException(
                    "Text fields are not optimised for operations that require per-document "
                        + "field data like aggregations and sorting, so these operations are disabled by default. Please use a "
                        + "keyword field instead. Alternatively, set fielddata=true on ["
                        + name()
                        + "] in order to load "
                        + "field data by uninverting the inverted index. Note that this can use significant memory."
                );
            }
            return new PagedBytesIndexFieldData.Builder(
                name(),
                filter.minFreq,
                filter.maxFreq,
                filter.minSegmentSize,
                CoreValuesSourceType.BYTES
            );
        }

    }

    protected final FieldType fieldType;
    private final PrefixFieldMapper prefixFieldMapper;
    private final PhraseFieldMapper phraseFieldMapper;
    private final SimilarityProvider similarity;
    private final String indexOptions;
    private final String termVectors;
    private final int positionIncrementGap;
    protected final Version indexCreatedVersion;
    protected final IndexAnalyzers indexAnalyzers;
    private final FielddataFrequencyFilter freqFilter;

    protected TextFieldMapper(
        String simpleName,
        FieldType fieldType,
        TextFieldType mappedFieldType,
        PrefixFieldMapper prefixFieldMapper,
        PhraseFieldMapper phraseFieldMapper,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        assert mappedFieldType.getTextSearchInfo().isTokenized();
        assert mappedFieldType.hasDocValues() == false;
        if (fieldType.indexOptions() == IndexOptions.NONE && fieldType().fielddata()) {
            throw new IllegalArgumentException("Cannot enable fielddata on a [text] field that is not indexed: [" + name() + "]");
        }
        this.fieldType = fieldType;
        this.prefixFieldMapper = prefixFieldMapper;
        this.phraseFieldMapper = phraseFieldMapper;
        this.similarity = builder.similarity.getValue();
        this.indexOptions = builder.indexOptions.getValue();
        this.termVectors = builder.termVectors.getValue();
        this.positionIncrementGap = builder.positionIncrementGap.getValue();
        this.indexCreatedVersion = builder.indexCreatedVersion;
        this.indexAnalyzers = builder.analyzers.indexAnalyzers;
        this.freqFilter = builder.freqFilter.getValue();
    }

    @Override
    protected TextFieldMapper clone() {
        return (TextFieldMapper) super.clone();
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), this.indexCreatedVersion, this.indexAnalyzers).init(this);
    }

    @Override
    protected void parseCreateField(ParseContext context) throws IOException {
        final String value;
        if (context.externalValueSet()) {
            value = context.externalValue().toString();
        } else {
            value = context.parser().textOrNull();
        }

        if (value == null) {
            return;
        }

        if (fieldType.indexOptions() != IndexOptions.NONE || fieldType.stored()) {
            Field field = new Field(fieldType().name(), value, fieldType);
            context.doc().add(field);
            if (fieldType.omitNorms()) {
                createFieldNamesField(context);
            }
            if (prefixFieldMapper != null) {
                prefixFieldMapper.addField(context, value);
            }
            if (phraseFieldMapper != null) {
                context.doc().add(new Field(phraseFieldMapper.fieldType().name(), value, phraseFieldMapper.fieldType));
            }
        }
    }

    @Override
    public Iterator<Mapper> iterator() {
        List<Mapper> subIterators = new ArrayList<>();
        if (prefixFieldMapper != null) {
            subIterators.add(prefixFieldMapper);
        }
        if (phraseFieldMapper != null) {
            subIterators.add(phraseFieldMapper);
        }
        if (subIterators.size() == 0) {
            return super.iterator();
        }
        return Iterators.concat(super.iterator(), subIterators.iterator());
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    public TextFieldType fieldType() {
        return (TextFieldType) super.fieldType();
    }

    public static Query createPhraseQuery(TokenStream stream, String field, int slop, boolean enablePositionIncrements) throws IOException {
        MultiPhraseQuery.Builder mpqb = new MultiPhraseQuery.Builder();
        mpqb.setSlop(slop);

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);

        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
        int position = -1;

        List<Term> multiTerms = new ArrayList<>();
        stream.reset();
        while (stream.incrementToken()) {
            int positionIncrement = posIncrAtt.getPositionIncrement();

            if (positionIncrement > 0 && multiTerms.size() > 0) {
                if (enablePositionIncrements) {
                    mpqb.add(multiTerms.toArray(new Term[0]), position);
                } else {
                    mpqb.add(multiTerms.toArray(new Term[0]));
                }
                multiTerms.clear();
            }
            position += positionIncrement;
            multiTerms.add(new Term(field, termAtt.getBytesRef()));
        }

        if (enablePositionIncrements) {
            mpqb.add(multiTerms.toArray(new Term[0]), position);
        } else {
            mpqb.add(multiTerms.toArray(new Term[0]));
        }
        return mpqb.build();
    }

    public static Query createPhrasePrefixQuery(
        TokenStream stream,
        String field,
        int slop,
        int maxExpansions,
        String prefixField,
        IntPredicate usePrefixField
    ) throws IOException {
        MultiPhrasePrefixQuery builder = new MultiPhrasePrefixQuery(field);
        builder.setSlop(slop);
        builder.setMaxExpansions(maxExpansions);

        List<Term> currentTerms = new ArrayList<>();

        TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
        PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);

        stream.reset();
        int position = -1;
        while (stream.incrementToken()) {
            if (posIncrAtt.getPositionIncrement() != 0) {
                if (currentTerms.isEmpty() == false) {
                    builder.add(currentTerms.toArray(new Term[0]), position);
                }
                position += posIncrAtt.getPositionIncrement();
                currentTerms.clear();
            }
            currentTerms.add(new Term(field, termAtt.getBytesRef()));
        }
        builder.add(currentTerms.toArray(new Term[0]), position);
        if (prefixField == null) {
            return builder;
        }

        int lastPos = builder.getTerms().length - 1;
        final Term[][] terms = builder.getTerms();
        final int[] positions = builder.getPositions();
        for (Term term : terms[lastPos]) {
            String value = term.text();
            if (usePrefixField.test(value.length()) == false) {
                return builder;
            }
        }

        if (terms.length == 1) {
            SynonymQuery.Builder sb = new SynonymQuery.Builder(prefixField);
            Arrays.stream(terms[0]).map(term -> new Term(prefixField, term.bytes())).forEach(sb::addTerm);
            return sb.build();
        }

        SpanNearQuery.Builder spanQuery = new SpanNearQuery.Builder(field, true);
        spanQuery.setSlop(slop);
        int previousPos = -1;
        for (int i = 0; i < terms.length; i++) {
            Term[] posTerms = terms[i];
            int posInc = positions[i] - previousPos;
            previousPos = positions[i];
            if (posInc > 1) {
                spanQuery.addGap(posInc - 1);
            }
            if (i == lastPos) {
                if (posTerms.length == 1) {
                    FieldMaskingSpanQuery fieldMask = new FieldMaskingSpanQuery(
                        new SpanTermQuery(new Term(prefixField, posTerms[0].bytes())),
                        field
                    );
                    spanQuery.addClause(fieldMask);
                } else {
                    SpanQuery[] queries = Arrays.stream(posTerms)
                        .map(term -> new FieldMaskingSpanQuery(new SpanTermQuery(new Term(prefixField, term.bytes())), field))
                        .toArray(SpanQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            } else {
                if (posTerms.length == 1) {
                    spanQuery.addClause(new SpanTermQuery(posTerms[0]));
                } else {
                    SpanTermQuery[] queries = Arrays.stream(posTerms).map(SpanTermQuery::new).toArray(SpanTermQuery[]::new);
                    spanQuery.addClause(new SpanOrQuery(queries));
                }
            }
        }
        return spanQuery.build();
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        // this is a pain, but we have to do this to maintain BWC
        builder.field("type", contentType());
        Builder mapperBuilder = (TextFieldMapper.Builder) getMergeBuilder();
        mapperBuilder.boost.toXContent(builder, includeDefaults);
        mapperBuilder.index.toXContent(builder, includeDefaults);
        mapperBuilder.store.toXContent(builder, includeDefaults);
        this.multiFields.toXContent(builder, params);
        this.copyTo.toXContent(builder, params);
        mapperBuilder.meta.toXContent(builder, includeDefaults);
        mapperBuilder.indexOptions.toXContent(builder, includeDefaults);
        mapperBuilder.termVectors.toXContent(builder, includeDefaults);
        mapperBuilder.norms.toXContent(builder, includeDefaults);
        mapperBuilder.analyzers.indexAnalyzer.toXContent(builder, includeDefaults);
        mapperBuilder.analyzers.searchAnalyzer.toXContent(builder, includeDefaults);
        mapperBuilder.analyzers.searchQuoteAnalyzer.toXContent(builder, includeDefaults);
        mapperBuilder.similarity.toXContent(builder, includeDefaults);
        mapperBuilder.eagerGlobalOrdinals.toXContent(builder, includeDefaults);
        mapperBuilder.positionIncrementGap.toXContent(builder, includeDefaults);
        mapperBuilder.fieldData.toXContent(builder, includeDefaults);
        mapperBuilder.freqFilter.toXContent(builder, includeDefaults);
        mapperBuilder.indexPrefixes.toXContent(builder, includeDefaults);
        mapperBuilder.indexPhrases.toXContent(builder, includeDefaults);
    }
}
