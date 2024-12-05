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

package org.opensearch.index.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CachingTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.DisableGraphAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.ExtendedCommonTermsQuery;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.QueryBuilder;
import org.apache.lucene.util.graph.GraphTokenStreamFiniteStrings;
import org.opensearch.OpenSearchException;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.lucene.search.SpanBooleanQueryRewriteWithMaxClause;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MatchOnlyTextFieldMapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.support.QueryParsers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.common.lucene.search.Queries.newLenientFieldQuery;
import static org.opensearch.common.lucene.search.Queries.newUnmappedFieldQuery;

/**
 * Foundation match query
 *
 * @opensearch.internal
 */
public class MatchQuery {

    /**
     * Type of the match
     *
     * @opensearch.internal
     */
    public enum Type implements Writeable {
        /**
         * The text is analyzed and terms are added to a boolean query.
         */
        BOOLEAN(0),
        /**
         * The text is analyzed and used as a phrase query.
         */
        PHRASE(1),
        /**
         * The text is analyzed and used in a phrase query, with the last term acting as a prefix.
         */
        PHRASE_PREFIX(2),
        /**
         * The text is analyzed, terms are added to a boolean query with the last term acting as a prefix.
         */
        BOOLEAN_PREFIX(3);

        private final int ordinal;

        Type(int ordinal) {
            this.ordinal = ordinal;
        }

        public static Type readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (Type type : Type.values()) {
                if (type.ordinal == ord) {
                    return type;
                }
            }
            throw new OpenSearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    /**
     * Query with zero terms
     *
     * @opensearch.internal
     */
    public enum ZeroTermsQuery implements Writeable {
        NONE(0),
        ALL(1),
        // this is used internally to make sure that query_string and simple_query_string
        // ignores query part that removes all tokens.
        NULL(2);

        private final int ordinal;

        ZeroTermsQuery(int ordinal) {
            this.ordinal = ordinal;
        }

        public static ZeroTermsQuery readFromStream(StreamInput in) throws IOException {
            int ord = in.readVInt();
            for (ZeroTermsQuery zeroTermsQuery : ZeroTermsQuery.values()) {
                if (zeroTermsQuery.ordinal == ord) {
                    return zeroTermsQuery;
                }
            }
            throw new OpenSearchException("unknown serialized type [" + ord + "]");
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(this.ordinal);
        }
    }

    public static final int DEFAULT_PHRASE_SLOP = 0;

    public static final boolean DEFAULT_LENIENCY = false;

    public static final ZeroTermsQuery DEFAULT_ZERO_TERMS_QUERY = ZeroTermsQuery.NONE;

    protected final QueryShardContext context;

    protected Analyzer analyzer;

    protected BooleanClause.Occur occur = BooleanClause.Occur.SHOULD;

    protected boolean enablePositionIncrements = true;

    protected int phraseSlop = DEFAULT_PHRASE_SLOP;

    protected Fuzziness fuzziness = null;

    protected int fuzzyPrefixLength = FuzzyQuery.defaultPrefixLength;

    protected int maxExpansions = FuzzyQuery.defaultMaxExpansions;

    protected SpanMultiTermQueryWrapper.SpanRewriteMethod spanRewriteMethod = new SpanBooleanQueryRewriteWithMaxClause(
        FuzzyQuery.defaultMaxExpansions,
        false
    );

    protected boolean transpositions = FuzzyQuery.defaultTranspositions;

    protected MultiTermQuery.RewriteMethod fuzzyRewriteMethod;

    protected boolean lenient = DEFAULT_LENIENCY;

    protected ZeroTermsQuery zeroTermsQuery = DEFAULT_ZERO_TERMS_QUERY;

    protected Float commonTermsCutoff = null;

    protected boolean autoGenerateSynonymsPhraseQuery = true;

    public MatchQuery(QueryShardContext context) {
        this.context = context;
    }

    public void setAnalyzer(String analyzerName) {
        this.analyzer = context.getMapperService().getIndexAnalyzers().get(analyzerName);
        if (analyzer == null) {
            throw new IllegalArgumentException("No analyzer found for [" + analyzerName + "]");
        }
    }

    public void setAnalyzer(Analyzer analyzer) {
        this.analyzer = analyzer;
    }

    public void setOccur(BooleanClause.Occur occur) {
        this.occur = occur;
    }

    @Deprecated
    public void setCommonTermsCutoff(Float cutoff) {
        this.commonTermsCutoff = cutoff;
    }

    public void setEnablePositionIncrements(boolean enablePositionIncrements) {
        this.enablePositionIncrements = enablePositionIncrements;
    }

    public void setPhraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
    }

    public void setFuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
    }

    public void setFuzzyPrefixLength(int fuzzyPrefixLength) {
        this.fuzzyPrefixLength = fuzzyPrefixLength;
    }

    public void setMaxExpansions(int maxExpansions) {
        this.maxExpansions = maxExpansions;
        this.spanRewriteMethod = new SpanBooleanQueryRewriteWithMaxClause(maxExpansions, false);
    }

    public void setTranspositions(boolean transpositions) {
        this.transpositions = transpositions;
    }

    public void setFuzzyRewriteMethod(MultiTermQuery.RewriteMethod fuzzyRewriteMethod) {
        this.fuzzyRewriteMethod = fuzzyRewriteMethod;
    }

    public void setLenient(boolean lenient) {
        this.lenient = lenient;
    }

    public void setZeroTermsQuery(ZeroTermsQuery zeroTermsQuery) {
        this.zeroTermsQuery = zeroTermsQuery;
    }

    public void setAutoGenerateSynonymsPhraseQuery(boolean enabled) {
        this.autoGenerateSynonymsPhraseQuery = enabled;
    }

    public Query parse(Type type, String fieldName, Object value) throws IOException {
        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            return newUnmappedFieldQuery(fieldName);
        }
        Set<String> fields = context.simpleMatchToIndexNames(fieldName);
        if (fields.contains(fieldName)) {
            assert fields.size() == 1;
            // this field is a concrete field or an alias so we use the
            // field type name directly
            fieldName = fieldType.name();
        }

        Analyzer analyzer = getAnalyzer(fieldType, type == Type.PHRASE || type == Type.PHRASE_PREFIX);
        assert analyzer != null;

        MatchQueryBuilder builder = new MatchQueryBuilder(analyzer, fieldType, enablePositionIncrements, autoGenerateSynonymsPhraseQuery);

        /*
         * If a keyword analyzer is used, we know that further analysis isn't
         * needed and can immediately return a term query. If the query is a bool
         * prefix query and the field type supports prefix queries, we return
         * a prefix query instead
         */
        if (analyzer == Lucene.KEYWORD_ANALYZER && type != Type.PHRASE_PREFIX) {
            final Term term = new Term(fieldName, value.toString());
            if (type == Type.BOOLEAN_PREFIX
                && (fieldType instanceof TextFieldMapper.TextFieldType || fieldType instanceof KeywordFieldMapper.KeywordFieldType)) {
                return builder.newPrefixQuery(term);
            } else {
                return builder.newTermQuery(term, BoostAttribute.DEFAULT_BOOST);
            }
        }

        return parseInternal(type, fieldName, builder, value);
    }

    protected final Query parseInternal(Type type, String fieldName, MatchQueryBuilder builder, Object value) throws IOException {
        final Query query;
        switch (type) {
            case BOOLEAN:
                if (commonTermsCutoff == null) {
                    query = builder.createBooleanQuery(fieldName, value.toString(), occur);
                } else {
                    query = createCommonTermsQuery(builder, fieldName, value.toString(), occur, occur, commonTermsCutoff);
                }
                break;

            case BOOLEAN_PREFIX:
                query = builder.createBooleanPrefixQuery(fieldName, value.toString(), occur);
                break;

            case PHRASE:
                query = builder.createPhraseQuery(fieldName, value.toString(), phraseSlop);
                break;

            case PHRASE_PREFIX:
                query = builder.createPhrasePrefixQuery(fieldName, value.toString(), phraseSlop);
                break;

            default:
                throw new IllegalStateException("No type found for [" + type + "]");
        }

        return query == null ? zeroTermsQuery() : query;
    }

    private Query createCommonTermsQuery(
        MatchQueryBuilder builder,
        String field,
        String queryText,
        Occur highFreqOccur,
        Occur lowFreqOccur,
        float maxTermFrequency
    ) {
        Query booleanQuery = builder.createBooleanQuery(field, queryText, lowFreqOccur);
        if (booleanQuery != null && booleanQuery instanceof BooleanQuery) {
            BooleanQuery bq = (BooleanQuery) booleanQuery;
            return boolToExtendedCommonTermsQuery(bq, highFreqOccur, lowFreqOccur, maxTermFrequency);
        }
        return booleanQuery;
    }

    private Query boolToExtendedCommonTermsQuery(BooleanQuery bq, Occur highFreqOccur, Occur lowFreqOccur, float maxTermFrequency) {
        ExtendedCommonTermsQuery query = new ExtendedCommonTermsQuery(highFreqOccur, lowFreqOccur, maxTermFrequency);
        for (BooleanClause clause : bq.clauses()) {
            if ((clause.getQuery() instanceof TermQuery) == false) {
                return bq;
            }
            query.add(((TermQuery) clause.getQuery()).getTerm());
        }
        return query;
    }

    protected Analyzer getAnalyzer(MappedFieldType fieldType, boolean quoted) {
        if (analyzer == null) {
            return quoted ? context.getSearchQuoteAnalyzer(fieldType) : context.getSearchAnalyzer(fieldType);
        } else {
            return analyzer;
        }
    }

    protected Query zeroTermsQuery() {
        switch (zeroTermsQuery) {
            case NULL:
                return null;
            case NONE:
                return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
            case ALL:
                return Queries.newMatchAllQuery();
            default:
                throw new IllegalStateException("unknown zeroTermsQuery " + zeroTermsQuery);
        }
    }

    class MatchQueryBuilder extends QueryBuilder {
        private final MappedFieldType fieldType;

        /**
         * Creates a new QueryBuilder using the given analyzer.
         */
        MatchQueryBuilder(
            Analyzer analyzer,
            MappedFieldType fieldType,
            boolean enablePositionIncrements,
            boolean autoGenerateSynonymsPhraseQuery
        ) {
            super(analyzer);
            this.fieldType = fieldType;
            setEnablePositionIncrements(enablePositionIncrements);
            if (fieldType.getTextSearchInfo().hasPositions()) {
                setAutoGenerateMultiTermSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
            } else {
                setAutoGenerateMultiTermSynonymsPhraseQuery(false);
            }
        }

        @Override
        protected Query createFieldQuery(
            Analyzer analyzer,
            BooleanClause.Occur operator,
            String field,
            String queryText,
            boolean quoted,
            int slop
        ) {
            assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;
            Type type = quoted ? Type.PHRASE : Type.BOOLEAN;
            return createQuery(field, queryText, type, operator, slop);
        }

        /**
         * Creates a phrase prefix query from the query text.
         *
         * @param field field name
         * @param queryText text to be passed to the analyzer
         * @return {@code PrefixQuery}, {@code MultiPhrasePrefixQuery}, based on the analysis of {@code queryText}
         */
        protected Query createPhrasePrefixQuery(String field, String queryText, int slop) {
            return createQuery(field, queryText, Type.PHRASE_PREFIX, occur, slop);
        }

        /**
         * Creates a boolean prefix query from the query text.
         *
         * @param field field name
         * @param queryText text to be passed to the analyzer
         * @return {@code PrefixQuery}, {@code BooleanQuery}, based on the analysis of {@code queryText}
         */
        protected Query createBooleanPrefixQuery(String field, String queryText, BooleanClause.Occur occur) {
            return createQuery(field, queryText, Type.BOOLEAN_PREFIX, occur, 0);
        }

        private Query createFieldQuery(TokenStream source, Type type, BooleanClause.Occur operator, String field, int phraseSlop) {
            assert operator == BooleanClause.Occur.SHOULD || operator == BooleanClause.Occur.MUST;

            // Build an appropriate query based on the analysis chain.
            try (CachingTokenFilter stream = new CachingTokenFilter(source)) {

                TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
                PositionIncrementAttribute posIncAtt = stream.addAttribute(PositionIncrementAttribute.class);
                PositionLengthAttribute posLenAtt = stream.addAttribute(PositionLengthAttribute.class);

                if (termAtt == null) {
                    return null;
                }

                // phase 1: read through the stream and assess the situation:
                // counting the number of tokens/positions and marking if we have any synonyms.

                int numTokens = 0;
                int positionCount = 0;
                boolean hasSynonyms = false;
                boolean isGraph = false;

                stream.reset();
                while (stream.incrementToken()) {
                    numTokens++;
                    int positionIncrement = posIncAtt.getPositionIncrement();
                    if (positionIncrement != 0) {
                        positionCount += positionIncrement;
                    } else {
                        hasSynonyms = true;
                    }

                    int positionLength = posLenAtt.getPositionLength();
                    if (enableGraphQueries && positionLength > 1) {
                        isGraph = true;
                    }
                }

                // phase 2: based on token count, presence of synonyms, and options
                // formulate a single term, boolean, or phrase.
                if (numTokens == 0) {
                    return null;
                } else if (numTokens == 1) {
                    // single term
                    if (type == Type.PHRASE_PREFIX) {
                        return analyzePhrasePrefix(field, stream, phraseSlop, positionCount);
                    } else {
                        return analyzeTerm(field, stream, type == Type.BOOLEAN_PREFIX);
                    }
                } else if (isGraph) {
                    // graph
                    if (type == Type.PHRASE || type == Type.PHRASE_PREFIX) {
                        return analyzeGraphPhrase(stream, field, type, phraseSlop);
                    } else {
                        return analyzeGraphBoolean(field, stream, operator, type == Type.BOOLEAN_PREFIX);
                    }
                } else if (type == Type.PHRASE && positionCount > 1) {
                    // phrase
                    if (hasSynonyms) {
                        // complex phrase with synonyms
                        return analyzeMultiPhrase(field, stream, phraseSlop);
                    } else {
                        // simple phrase
                        return analyzePhrase(field, stream, phraseSlop);
                    }
                } else if (type == Type.PHRASE_PREFIX) {
                    // phrase prefix
                    return analyzePhrasePrefix(field, stream, phraseSlop, positionCount);
                } else {
                    // boolean
                    if (positionCount == 1) {
                        // only one position, with synonyms
                        return analyzeBoolean(field, stream);
                    } else {
                        // complex case: multiple positions
                        return analyzeMultiBoolean(field, stream, operator, type == Type.BOOLEAN_PREFIX);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        private Query createQuery(String field, String queryText, Type type, BooleanClause.Occur operator, int phraseSlop) {
            // Use the analyzer to get all the tokens, and then build an appropriate
            // query based on the analysis chain.
            try (TokenStream source = analyzer.tokenStream(field, queryText)) {
                if (source.hasAttribute(DisableGraphAttribute.class)) {
                    /*
                     * A {@link TokenFilter} in this {@link TokenStream} disabled the graph analysis to avoid
                     * paths explosion. See {@link org.opensearch.index.analysis.ShingleTokenFilterFactory} for details.
                     */
                    setEnableGraphQueries(false);
                }
                try {
                    return createFieldQuery(source, type, operator, field, phraseSlop);
                } finally {
                    setEnableGraphQueries(true);
                }
            } catch (IOException e) {
                throw new RuntimeException("Error analyzing query text", e);
            }
        }

        private SpanQuery newSpanQuery(Term[] terms, boolean isPrefix) {
            if (terms.length == 1) {
                return isPrefix ? fieldType.spanPrefixQuery(terms[0].text(), spanRewriteMethod, context) : new SpanTermQuery(terms[0]);
            }
            SpanQuery[] spanQueries = new SpanQuery[terms.length];
            for (int i = 0; i < terms.length; i++) {
                spanQueries[i] = isPrefix
                    ? fieldType.spanPrefixQuery(terms[i].text(), spanRewriteMethod, context)
                    : new SpanTermQuery(terms[i]);
            }
            return new SpanOrQuery(spanQueries);
        }

        private SpanQuery createSpanQuery(TokenStream in, String field, boolean isPrefix) throws IOException {
            TermToBytesRefAttribute termAtt = in.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncAtt = in.getAttribute(PositionIncrementAttribute.class);
            if (termAtt == null) {
                return null;
            }

            SpanNearQuery.Builder builder = new SpanNearQuery.Builder(field, true);
            Term lastTerm = null;
            while (in.incrementToken()) {
                if (posIncAtt.getPositionIncrement() > 1) {
                    builder.addGap(posIncAtt.getPositionIncrement() - 1);
                }
                if (lastTerm != null) {
                    builder.addClause(new SpanTermQuery(lastTerm));
                }
                lastTerm = new Term(field, termAtt.getBytesRef());
            }
            if (lastTerm != null) {
                SpanQuery spanQuery = isPrefix
                    ? fieldType.spanPrefixQuery(lastTerm.text(), spanRewriteMethod, context)
                    : new SpanTermQuery(lastTerm);
                builder.addClause(spanQuery);
            }
            SpanNearQuery query = builder.build();
            SpanQuery[] clauses = query.getClauses();
            if (clauses.length == 1) {
                return clauses[0];
            } else {
                return query;
            }
        }

        @Override
        protected Query newTermQuery(Term term, float boost) {
            Supplier<Query> querySupplier;
            if (fuzziness != null) {
                querySupplier = () -> {
                    Query query = fieldType.fuzzyQuery(term.text(), fuzziness, fuzzyPrefixLength, maxExpansions, transpositions, context);
                    if (query instanceof FuzzyQuery) {
                        QueryParsers.setRewriteMethod((FuzzyQuery) query, fuzzyRewriteMethod);
                    }
                    return query;
                };
            } else {
                querySupplier = () -> fieldType.termQuery(term.bytes(), context);
            }
            try {
                Query query = querySupplier.get();
                return query;
            } catch (RuntimeException e) {
                if (lenient) {
                    return newLenientFieldQuery(fieldType.name(), e);
                } else {
                    throw e;
                }
            }
        }

        /**
         * Builds a new prefix query instance.
         */
        protected Query newPrefixQuery(Term term) {
            try {
                return fieldType.prefixQuery(term.text(), null, context);
            } catch (RuntimeException e) {
                if (lenient) {
                    return newLenientFieldQuery(term.field(), e);
                }
                throw e;
            }
        }

        private Query analyzeTerm(String field, TokenStream stream, boolean isPrefix) throws IOException {
            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);

            stream.reset();
            if (!stream.incrementToken()) {
                throw new AssertionError();
            }
            final Term term = new Term(field, termAtt.getBytesRef());
            int lastOffset = offsetAtt.endOffset();
            stream.end();
            return isPrefix && lastOffset == offsetAtt.endOffset()
                ? newPrefixQuery(term)
                : newTermQuery(term, BoostAttribute.DEFAULT_BOOST);
        }

        private void add(BooleanQuery.Builder q, String field, List<Term> current, BooleanClause.Occur operator, boolean isPrefix) {
            if (current.isEmpty()) {
                return;
            }
            if (current.size() == 1) {
                if (isPrefix) {
                    q.add(newPrefixQuery(current.get(0)), operator);
                } else {
                    q.add(newTermQuery(current.get(0), BoostAttribute.DEFAULT_BOOST), operator);
                }
            } else {
                // We don't apply prefix on synonyms
                final TermAndBoost[] termAndBoosts = current.stream()
                    .map(t -> new TermAndBoost(t.bytes(), BoostAttribute.DEFAULT_BOOST))
                    .toArray(TermAndBoost[]::new);
                q.add(newSynonymQuery(field, termAndBoosts), operator);
            }
        }

        private Query analyzeMultiBoolean(String field, TokenStream stream, BooleanClause.Occur operator, boolean isPrefix)
            throws IOException {
            BooleanQuery.Builder q = newBooleanQuery();
            List<Term> currentQuery = new ArrayList<>();

            TermToBytesRefAttribute termAtt = stream.getAttribute(TermToBytesRefAttribute.class);
            PositionIncrementAttribute posIncrAtt = stream.getAttribute(PositionIncrementAttribute.class);
            OffsetAttribute offsetAtt = stream.addAttribute(OffsetAttribute.class);

            stream.reset();
            int lastOffset = 0;
            while (stream.incrementToken()) {
                if (posIncrAtt.getPositionIncrement() != 0) {
                    add(q, field, currentQuery, operator, false);
                    currentQuery.clear();
                }
                currentQuery.add(new Term(field, termAtt.getBytesRef()));
                lastOffset = offsetAtt.endOffset();
            }
            stream.end();
            add(q, field, currentQuery, operator, isPrefix && lastOffset == offsetAtt.endOffset());
            return q.build();
        }

        @Override
        protected Query analyzePhrase(String field, TokenStream stream, int slop) throws IOException {
            try {
                checkForPositions(field);
                return fieldType.phraseQuery(stream, slop, enablePositionIncrements, context);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        @Override
        protected Query analyzeMultiPhrase(String field, TokenStream stream, int slop) throws IOException {
            try {
                checkForPositions(field);
                return fieldType.multiPhraseQuery(stream, slop, enablePositionIncrements, context);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        private Query analyzePhrasePrefix(String field, TokenStream stream, int slop, int positionCount) throws IOException {
            try {
                if (positionCount > 1) {
                    checkForPositions(field);
                }
                return fieldType.phrasePrefixQuery(stream, slop, maxExpansions, context);
            } catch (IllegalArgumentException | IllegalStateException e) {
                if (lenient) {
                    return newLenientFieldQuery(field, e);
                }
                throw e;
            }
        }

        private Query analyzeGraphBoolean(String field, TokenStream source, BooleanClause.Occur operator, boolean isPrefix)
            throws IOException {
            source.reset();
            GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            int[] articulationPoints = graph.articulationPoints();
            int lastState = 0;
            for (int i = 0; i <= articulationPoints.length; i++) {
                int start = lastState;
                int end = -1;
                if (i < articulationPoints.length) {
                    end = articulationPoints[i];
                }
                lastState = end;
                final Query queryPos;
                boolean usePrefix = isPrefix && end == -1;
                /*
                  check if the GraphTokenStreamFiniteStrings graph is empty
                  return empty BooleanQuery result
                 */
                Iterator<TokenStream> graphIt = graph.getFiniteStrings();
                if (!graphIt.hasNext()) {
                    return builder.build();
                }
                if (graph.hasSidePath(start)) {
                    final Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
                    Iterator<Query> queries = new Iterator<Query>() {
                        @Override
                        public boolean hasNext() {
                            return it.hasNext();
                        }

                        @Override
                        public Query next() {
                            TokenStream ts = it.next();
                            final Type type;
                            if (getAutoGenerateMultiTermSynonymsPhraseQuery()) {
                                type = usePrefix ? Type.PHRASE_PREFIX : Type.PHRASE;
                            } else {
                                type = Type.BOOLEAN;
                            }
                            return createFieldQuery(ts, type, BooleanClause.Occur.MUST, field, 0);
                        }
                    };
                    queryPos = newGraphSynonymQuery(queries);
                } else {
                    Term[] terms = graph.getTerms(field, start);
                    assert terms.length > 0;
                    if (terms.length == 1) {
                        queryPos = usePrefix ? newPrefixQuery(terms[0]) : newTermQuery(terms[0], BoostAttribute.DEFAULT_BOOST);
                    } else {
                        // We don't apply prefix on synonyms
                        final TermAndBoost[] termAndBoosts = Arrays.stream(terms)
                            .map(t -> new TermAndBoost(t.bytes(), BoostAttribute.DEFAULT_BOOST))
                            .toArray(TermAndBoost[]::new);
                        queryPos = newSynonymQuery(field, termAndBoosts);
                    }
                }
                if (queryPos != null) {
                    builder.add(queryPos, operator);
                }
            }
            return builder.build();
        }

        private Query analyzeGraphPhrase(TokenStream source, String field, Type type, int slop) throws IOException {
            assert type == Type.PHRASE_PREFIX || type == Type.PHRASE;

            source.reset();
            GraphTokenStreamFiniteStrings graph = new GraphTokenStreamFiniteStrings(source);
            if (phraseSlop > 0) {
                /*
                 * Creates a boolean query from the graph token stream by extracting all the finite strings from the graph
                 * and using them to create phrase queries with the appropriate slop.
                 */
                BooleanQuery.Builder builder = new BooleanQuery.Builder();
                Iterator<TokenStream> it = graph.getFiniteStrings();
                while (it.hasNext()) {
                    Query query = createFieldQuery(it.next(), type, BooleanClause.Occur.MUST, field, slop);
                    if (query != null) {
                        builder.add(query, BooleanClause.Occur.SHOULD);
                    }
                }
                return builder.build();
            }

            /*
             * Creates a span near (phrase) query from a graph token stream.
             * The articulation points of the graph are visited in order and the queries
             * created at each point are merged in the returned near query.
             */
            List<SpanQuery> clauses = new ArrayList<>();
            int[] articulationPoints = graph.articulationPoints();
            int lastState = 0;
            int maxClauseCount = BooleanQuery.getMaxClauseCount();
            for (int i = 0; i <= articulationPoints.length; i++) {
                int start = lastState;
                int end = -1;
                if (i < articulationPoints.length) {
                    end = articulationPoints[i];
                }
                lastState = end;
                final SpanQuery queryPos;
                boolean usePrefix = end == -1 && type == Type.PHRASE_PREFIX;
                if (graph.hasSidePath(start)) {
                    List<SpanQuery> queries = new ArrayList<>();
                    Iterator<TokenStream> it = graph.getFiniteStrings(start, end);
                    while (it.hasNext()) {
                        TokenStream ts = it.next();
                        SpanQuery q = createSpanQuery(ts, field, usePrefix);
                        if (q != null) {
                            if (queries.size() >= maxClauseCount) {
                                throw new BooleanQuery.TooManyClauses();
                            }
                            queries.add(q);
                        }
                    }
                    if (queries.size() > 0) {
                        queryPos = new SpanOrQuery(queries.toArray(new SpanQuery[0]));
                    } else {
                        queryPos = null;
                    }
                } else {
                    Term[] terms = graph.getTerms(field, start);
                    assert terms.length > 0;
                    if (terms.length >= maxClauseCount) {
                        throw new BooleanQuery.TooManyClauses();
                    }
                    queryPos = newSpanQuery(terms, usePrefix);
                }

                if (queryPos != null) {
                    if (clauses.size() >= maxClauseCount) {
                        throw new BooleanQuery.TooManyClauses();
                    }
                    clauses.add(queryPos);
                }
            }

            if (clauses.isEmpty()) {
                return null;
            } else if (clauses.size() == 1) {
                return clauses.get(0);
            } else {
                return new SpanNearQuery(clauses.toArray(new SpanQuery[0]), 0, true);
            }
        }

        private void checkForPositions(String field) {
            if (fieldType.getTextSearchInfo().hasPositions() == false) {
                if (fieldType instanceof MatchOnlyTextFieldMapper.MatchOnlyTextFieldType) {
                    return;
                }
                throw new IllegalStateException("field:[" + field + "] was indexed without position data; cannot run PhraseQuery");
            }
        }
    }
}
