/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.Version;
import org.opensearch.common.lucene.search.MultiPhrasePrefixQuery;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.SourceFieldMatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * A specialized type of TextFieldMapper which disables the positions and norms to save on storage and executes phrase queries, which requires
 * positional data, in a slightly less efficient manner using the {@link  org.opensearch.index.query.SourceFieldMatchQuery}.
 */
public class MatchOnlyTextFieldMapper extends TextFieldMapper {

    public static final FieldType FIELD_TYPE = new FieldType();
    public static final String CONTENT_TYPE = "match_only_text";
    private final String indexOptions = FieldMapper.indexOptionToString(FIELD_TYPE.indexOptions());
    private final boolean norms = FIELD_TYPE.omitNorms() == false;

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    static {
        FIELD_TYPE.setTokenized(true);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.setStoreTermVectors(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
        FIELD_TYPE.freeze();
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n, c.indexVersionCreated(), c.getIndexAnalyzers()));

    protected MatchOnlyTextFieldMapper(
        String simpleName,
        FieldType fieldType,
        MatchOnlyTextFieldType mappedFieldType,
        TextFieldMapper.PrefixFieldMapper prefixFieldMapper,
        TextFieldMapper.PhraseFieldMapper phraseFieldMapper,
        MultiFields multiFields,
        CopyTo copyTo,
        Builder builder
    ) {

        super(simpleName, fieldType, mappedFieldType, prefixFieldMapper, phraseFieldMapper, multiFields, copyTo, builder);
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName(), this.indexCreatedVersion, this.indexAnalyzers).init(this);
    }

    /**
     * Builder class for constructing the MatchOnlyTextFieldMapper.
     */
    public static class Builder extends TextFieldMapper.Builder {
        final Parameter<String> indexOptions = indexOptions(m -> ((MatchOnlyTextFieldMapper) m).indexOptions);

        private static Parameter<String> indexOptions(Function<FieldMapper, String> initializer) {
            return Parameter.restrictedStringParam("index_options", false, initializer, "docs");
        }

        final Parameter<Boolean> norms = norms(m -> ((MatchOnlyTextFieldMapper) m).norms);
        final Parameter<Boolean> indexPhrases = Parameter.boolParam(
            "index_phrases",
            false,
            m -> ((MatchOnlyTextFieldType) m.mappedFieldType).indexPhrases,
            false
        ).setValidator(v -> {
            if (v == true) {
                throw new MapperParsingException("Index phrases cannot be enabled on for match_only_text field. Use text field instead");
            }
        });

        final Parameter<PrefixConfig> indexPrefixes = new Parameter<>(
            "index_prefixes",
            false,
            () -> null,
            TextFieldMapper::parsePrefixConfig,
            m -> Optional.ofNullable(((MatchOnlyTextFieldType) m.mappedFieldType).prefixFieldType)
                .map(p -> new PrefixConfig(p.minChars, p.maxChars))
                .orElse(null)
        ).acceptsNull().setValidator(v -> {
            if (v != null) {
                throw new MapperParsingException("Index prefixes cannot be enabled on for match_only_text field. Use text field instead");
            }
        });

        private static Parameter<Boolean> norms(Function<FieldMapper, Boolean> initializer) {
            return Parameter.boolParam("norms", false, initializer, false)
                .setMergeValidator((o, n) -> o == n || (o && n == false))
                .setValidator(v -> {
                    if (v == true) {
                        throw new MapperParsingException("Norms cannot be enabled on for match_only_text field");
                    }
                });
        }

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name, indexAnalyzers);
        }

        public Builder(String name, Version indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name, indexCreatedVersion, indexAnalyzers);
        }

        @Override
        public MatchOnlyTextFieldMapper build(BuilderContext context) {
            // TODO - disable norms and index-options and validate
            FieldType fieldType = TextParams.buildFieldType(index, store, indexOptions, norms, termVectors);
            MatchOnlyTextFieldType tft = buildFieldType(fieldType, context);
            return new MatchOnlyTextFieldMapper(
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

        @Override
        protected MatchOnlyTextFieldType buildFieldType(FieldType fieldType, BuilderContext context) {
            NamedAnalyzer indexAnalyzer = analyzers.getIndexAnalyzer();
            NamedAnalyzer searchAnalyzer = analyzers.getSearchAnalyzer();
            NamedAnalyzer searchQuoteAnalyzer = analyzers.getSearchQuoteAnalyzer();

            if (fieldType.indexOptions().compareTo(IndexOptions.DOCS) > 0) {
                throw new IllegalArgumentException("Cannot set position_increment_gap on field [" + name + "] without positions enabled");
            }
            if (positionIncrementGap.get() != POSITION_INCREMENT_GAP_USE_ANALYZER) {
                if (fieldType.indexOptions().compareTo(IndexOptions.DOCS) < 0) {
                    throw new IllegalArgumentException(
                        "Cannot set position_increment_gap on field [" + name + "] without indexing enabled"
                    );
                }
                // for index analyzer we don't set positionIncrementGap whereas for search analyzer its set because
                // phrase queries, which make use of it, should work fine as they will directly work on the field value
                // per matched document by reading from _source field.
                indexAnalyzer = new NamedAnalyzer(indexAnalyzer, positionIncrementGap.get());
                searchAnalyzer = new NamedAnalyzer(searchAnalyzer, positionIncrementGap.get());
                searchQuoteAnalyzer = new NamedAnalyzer(searchQuoteAnalyzer, positionIncrementGap.get());
            }
            TextSearchInfo tsi = new TextSearchInfo(fieldType, similarity.getValue(), searchAnalyzer, searchQuoteAnalyzer);
            MatchOnlyTextFieldType ft = new MatchOnlyTextFieldType(
                buildFullName(context),
                index.getValue(),
                fieldType.stored(),
                tsi,
                meta.getValue()
            );
            ft.setIndexAnalyzer(indexAnalyzer);
            ft.setEagerGlobalOrdinals(eagerGlobalOrdinals.getValue());
            ft.setBoost(boost.getValue());
            if (fieldData.getValue()) {
                ft.setFielddata(true, freqFilter.getValue());
            }
            return ft;
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
    }

    /**
     * The specific field type for MatchOnlyTextFieldMapper
     *
     * @opensearch.internal
     */
    public static final class MatchOnlyTextFieldType extends TextFieldMapper.TextFieldType {
        private final boolean indexPhrases = false;

        private PrefixFieldType prefixFieldType;

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public MatchOnlyTextFieldType(String name, boolean indexed, boolean stored, TextSearchInfo tsi, Map<String, String> meta) {
            super(name, indexed, stored, tsi, meta);
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, QueryShardContext context) throws IOException {
            PhraseQuery phraseQuery = (PhraseQuery) super.phraseQuery(stream, slop, enablePosIncrements);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Term term : phraseQuery.getTerms()) {
                builder.add(new TermQuery(term), BooleanClause.Occur.FILTER);
            }
            return new SourceFieldMatchQuery(builder.build(), phraseQuery, this, context);
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context)
            throws IOException {
            MultiPhraseQuery multiPhraseQuery = (MultiPhraseQuery) super.multiPhraseQuery(stream, slop, enablePositionIncrements);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Term[] terms : multiPhraseQuery.getTermArrays()) {
                if (terms.length > 1) {
                    BooleanQuery.Builder disjunctions = new BooleanQuery.Builder();
                    for (Term term : terms) {
                        disjunctions.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
                    }
                    builder.add(disjunctions.build(), BooleanClause.Occur.FILTER);
                } else {
                    builder.add(new TermQuery(terms[0]), BooleanClause.Occur.FILTER);
                }
            }
            return new SourceFieldMatchQuery(builder.build(), multiPhraseQuery, this, context);
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
            Query phrasePrefixQuery = super.phrasePrefixQuery(stream, slop, maxExpansions);
            List<List<Term>> termArray = getTermsFromTokenStream(stream);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (int i = 0; i < termArray.size(); i++) {
                BooleanQuery.Builder disjunctions = new BooleanQuery.Builder();
                for (Term term : termArray.get(i)) {
                    if (i == termArray.size() - 1) {
                        MultiPhrasePrefixQuery mqb = new MultiPhrasePrefixQuery(name());
                        mqb.add(term);
                        disjunctions.add(mqb, BooleanClause.Occur.SHOULD);
                    } else {
                        disjunctions.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
                    }
                }
                builder.add(disjunctions.build(), BooleanClause.Occur.FILTER);
            }
            return new SourceFieldMatchQuery(builder.build(), phrasePrefixQuery, this, context);
        }

        private List<List<Term>> getTermsFromTokenStream(TokenStream stream) throws IOException {
            final List<List<Term>> termArray = new ArrayList<>();
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
            List<Term> currentTerms = new ArrayList<>();
            stream.reset();
            while (stream.incrementToken()) {
                if (posIncrAtt.getPositionIncrement() != 0) {
                    if (currentTerms.isEmpty() == false) {
                        termArray.add(List.copyOf(currentTerms));
                    }
                    currentTerms.clear();
                }
                currentTerms.add(new Term(name(), termAtt.getBytesRef()));
            }
            termArray.add(List.copyOf(currentTerms));
            return termArray;
        }
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults, Params params) throws IOException {
        // this is a pain, but we have to do this to maintain BWC
        builder.field("type", contentType());
        Builder mapperBuilder = (MatchOnlyTextFieldMapper.Builder) getMergeBuilder();
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
