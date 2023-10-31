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
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.SourceFieldMatchQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MatchOnlyTextFieldMapper extends TextFieldMapper {

    public static final FieldType FIELD_TYPE = new FieldType();
    public static final String CONTENT_TYPE = "match_only_text";

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

    protected MatchOnlyTextFieldMapper(String simpleName, FieldType fieldType, MatchOnlyTextFieldType mappedFieldType,
                                       TextFieldMapper.PrefixFieldMapper prefixFieldMapper,
                                       TextFieldMapper.PhraseFieldMapper phraseFieldMapper,
                                       MultiFields multiFields, CopyTo copyTo, Builder builder) {

        super(simpleName, fieldType, mappedFieldType, prefixFieldMapper, phraseFieldMapper, multiFields, copyTo, builder);
    }

    public static class Builder extends TextFieldMapper.Builder {

        public Builder(String name, IndexAnalyzers indexAnalyzers) {
            super(name, indexAnalyzers);
        }

        public Builder(String name, Version indexCreatedVersion, IndexAnalyzers indexAnalyzers) {
            super(name, indexCreatedVersion, indexAnalyzers);
        }

        @Override
        public MatchOnlyTextFieldMapper build(BuilderContext context) {
            FieldType fieldType = FIELD_TYPE;
            MatchOnlyTextFieldType tft = new MatchOnlyTextFieldType(buildFieldType(fieldType, context));
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
    }

    public static final class MatchOnlyTextFieldType extends TextFieldMapper.TextFieldType {

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        public MatchOnlyTextFieldType(TextFieldMapper.TextFieldType tft) {
            super(tft.name(), tft.isSearchable(), tft.isStored(), tft.getTextSearchInfo(), tft.meta());
        }

        @Override
        public Query phraseQuery(TokenStream stream, int slop, boolean enablePosIncrements, QueryShardContext context) throws IOException {
            PhraseQuery phraseQuery = (PhraseQuery) super.phraseQuery(stream, slop, enablePosIncrements);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Term term: phraseQuery.getTerms()) {
                builder.add(new TermQuery(term), BooleanClause.Occur.FILTER);
            }
            return new SourceFieldMatchQuery(builder.build(), phraseQuery, this,
                (SourceValueFetcher) this.valueFetcher(context, context.lookup(), null), context.lookup());
        }

        @Override
        public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context) throws IOException {
            MultiPhraseQuery multiPhraseQuery = (MultiPhraseQuery) super.multiPhraseQuery(stream, slop, enablePositionIncrements);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (Term[] terms : multiPhraseQuery.getTermArrays()) {
                BooleanQuery.Builder disjunctions = new BooleanQuery.Builder();
                for (Term term: terms) {
                    disjunctions.add(new TermQuery(term), BooleanClause.Occur.SHOULD);
                }
                builder.add(disjunctions.build(), BooleanClause.Occur.FILTER);
            }
            return new SourceFieldMatchQuery(builder.build(), multiPhraseQuery, this,
                (SourceValueFetcher) this.valueFetcher(context, context.lookup(), null), context.lookup());
        }

        @Override
        public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
            Query phrasePrefixQuery =  super.phrasePrefixQuery(stream, slop, maxExpansions);
            List<List<Term>> termArray = getTermsFromTokenStream(stream);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            for (int i = 0; i < termArray.size(); i++) {
                BooleanQuery.Builder disjunctions = new BooleanQuery.Builder();
                for (Term term: termArray.get(i)) {
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
            return new SourceFieldMatchQuery(builder.build(), phrasePrefixQuery, this,
                (SourceValueFetcher) this.valueFetcher(context, context.lookup(), null), context.lookup());
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
}
