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
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.Token;
import org.apache.lucene.queryparser.classic.XQueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostAttribute;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiPhraseQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.search.Queries;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.DateFieldMapper.DateFieldType;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.MultiMatchQueryBuilder;
import org.opensearch.index.query.QueryShardContext;

import java.io.IOException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.lucene.search.Queries.fixNegativeQueryIfNeeded;
import static org.opensearch.common.lucene.search.Queries.newLenientFieldQuery;
import static org.opensearch.common.lucene.search.Queries.newUnmappedFieldQuery;
import static org.opensearch.index.search.QueryParserHelper.checkForTooManyFields;
import static org.opensearch.index.search.QueryParserHelper.resolveMappingField;
import static org.opensearch.index.search.QueryParserHelper.resolveMappingFields;

/**
 * A {@link XQueryParser} that uses the {@link MapperService} in order to build smarter
 * queries based on the mapping information.
 * This class uses {@link MultiMatchQuery} to build the text query around operators and {@link XQueryParser}
 * to assemble the result logically.
 *
 * @opensearch.internal
 */
public class QueryStringQueryParser extends XQueryParser {
    private static final String EXISTS_FIELD = "_exists_";

    private final QueryShardContext context;
    private final Map<String, Float> fieldsAndWeights;
    private final boolean lenient;

    private final MultiMatchQuery queryBuilder;
    private MultiMatchQueryBuilder.Type type = MultiMatchQueryBuilder.Type.BEST_FIELDS;
    private Float groupTieBreaker;

    private Analyzer forceAnalyzer;
    private Analyzer forceQuoteAnalyzer;
    private String quoteFieldSuffix;
    private boolean analyzeWildcard;
    private ZoneId timeZone;
    private Fuzziness fuzziness = Fuzziness.AUTO;
    private int fuzzyMaxExpansions = FuzzyQuery.defaultMaxExpansions;
    private MultiTermQuery.RewriteMethod fuzzyRewriteMethod = MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE;
    private boolean fuzzyTranspositions = FuzzyQuery.defaultTranspositions;

    /**
     * @param context The query shard context.
     * @param defaultField The default field for query terms.
     */
    public QueryStringQueryParser(QueryShardContext context, String defaultField) {
        this(context, defaultField, Collections.emptyMap(), false, context.getMapperService().searchAnalyzer());
    }

    /**
     * @param context The query shard context.
     * @param defaultField The default field for query terms.
     * @param lenient If set to `true` will cause format based failures (like providing text to a numeric field) to be ignored.
     */
    public QueryStringQueryParser(QueryShardContext context, String defaultField, boolean lenient) {
        this(context, defaultField, Collections.emptyMap(), lenient, context.getMapperService().searchAnalyzer());
    }

    /**
     * @param context The query shard context
     * @param fieldsAndWeights The default fields and weights expansion for query terms
     */
    public QueryStringQueryParser(QueryShardContext context, Map<String, Float> fieldsAndWeights) {
        this(context, null, fieldsAndWeights, false, context.getMapperService().searchAnalyzer());
    }

    /**
     * @param context The query shard context.
     * @param fieldsAndWeights The default fields and weights expansion for query terms.
     * @param lenient If set to `true` will cause format based failures (like providing text to a numeric field) to be ignored.
     */
    public QueryStringQueryParser(QueryShardContext context, Map<String, Float> fieldsAndWeights, boolean lenient) {
        this(context, null, fieldsAndWeights, lenient, context.getMapperService().searchAnalyzer());
    }

    /**
     * Defaults to all queryable fields extracted from the mapping for query terms
     * @param context The query shard context
     * @param lenient If set to `true` will cause format based failures (like providing text to a numeric field) to be ignored.
     */
    public QueryStringQueryParser(QueryShardContext context, boolean lenient) {
        this(
            context,
            "*",
            resolveMappingField(context, "*", 1.0f, false, false, null),
            lenient,
            context.getMapperService().searchAnalyzer()
        );
    }

    private QueryStringQueryParser(
        QueryShardContext context,
        String defaultField,
        Map<String, Float> fieldsAndWeights,
        boolean lenient,
        Analyzer analyzer
    ) {
        super(defaultField, analyzer);
        this.context = context;
        this.fieldsAndWeights = Collections.unmodifiableMap(fieldsAndWeights);
        this.queryBuilder = new MultiMatchQuery(context);
        queryBuilder.setZeroTermsQuery(MatchQuery.ZeroTermsQuery.NULL);
        queryBuilder.setLenient(lenient);
        this.lenient = lenient;
    }

    @Override
    public void setEnablePositionIncrements(boolean enable) {
        super.setEnablePositionIncrements(enable);
        queryBuilder.setEnablePositionIncrements(enable);
    }

    @Override
    public void setDefaultOperator(Operator op) {
        super.setDefaultOperator(op);
        queryBuilder.setOccur(op == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD);
    }

    @Override
    public void setPhraseSlop(int phraseSlop) {
        super.setPhraseSlop(phraseSlop);
        queryBuilder.setPhraseSlop(phraseSlop);
    }

    /**
     * @param type Sets how multiple fields should be combined to build textual part queries.
     */
    public void setType(MultiMatchQueryBuilder.Type type) {
        this.type = type;
    }

    /**
     * @param fuzziness Sets the default {@link Fuzziness} for fuzzy query.
     * Defaults to {@link Fuzziness#AUTO}.
     */
    public void setFuzziness(Fuzziness fuzziness) {
        this.fuzziness = fuzziness;
    }

    /**
     * @param fuzzyRewriteMethod Sets the default rewrite method for fuzzy query.
     */
    public void setFuzzyRewriteMethod(MultiTermQuery.RewriteMethod fuzzyRewriteMethod) {
        this.fuzzyRewriteMethod = fuzzyRewriteMethod;
    }

    /**
     * @param fuzzyMaxExpansions Sets the maximum number of expansions allowed in a fuzzy query.
     * Defaults to {@link FuzzyQuery#defaultMaxExpansions}.
     */
    public void setFuzzyMaxExpansions(int fuzzyMaxExpansions) {
        this.fuzzyMaxExpansions = fuzzyMaxExpansions;
    }

    /**
     * @param analyzer Force the provided analyzer to be used for all query analysis regardless of the field.
     */
    public void setForceAnalyzer(Analyzer analyzer) {
        this.forceAnalyzer = analyzer;
    }

    /**
     * @param analyzer Force the provided analyzer to be used for all phrase query analysis regardless of the field.
     */
    public void setForceQuoteAnalyzer(Analyzer analyzer) {
        this.forceQuoteAnalyzer = analyzer;
    }

    /**
     * @param quoteFieldSuffix The suffix to append to fields for quoted parts of the query string.
     */
    public void setQuoteFieldSuffix(String quoteFieldSuffix) {
        this.quoteFieldSuffix = quoteFieldSuffix;
    }

    /**
     * @param analyzeWildcard If true, the wildcard operator analyzes the term to build a wildcard query.
     *                        Otherwise the query terms are only normalized.
     */
    public void setAnalyzeWildcard(boolean analyzeWildcard) {
        this.analyzeWildcard = analyzeWildcard;
    }

    /**
     * @param timeZone Time Zone to be applied to any range query related to dates.
     */
    public void setTimeZone(ZoneId timeZone) {
        this.timeZone = timeZone;
    }

    /**
     * @param groupTieBreaker The tie breaker to apply when multiple fields are used.
     */
    public void setGroupTieBreaker(float groupTieBreaker) {
        // Force the tie breaker in the query builder too
        queryBuilder.setTieBreaker(groupTieBreaker);
        this.groupTieBreaker = groupTieBreaker;
    }

    @Override
    public void setAutoGenerateMultiTermSynonymsPhraseQuery(boolean enable) {
        queryBuilder.setAutoGenerateSynonymsPhraseQuery(enable);
    }

    /**
     * @param fuzzyTranspositions Sets whether transpositions are supported in fuzzy queries.
     * Defaults to {@link FuzzyQuery#defaultTranspositions}.
     */
    public void setFuzzyTranspositions(boolean fuzzyTranspositions) {
        this.fuzzyTranspositions = fuzzyTranspositions;
    }

    private Query applyBoost(Query q, Float boost) {
        if (boost != null && boost != 1f) {
            return new BoostQuery(q, boost);
        }
        return q;
    }

    private Map<String, Float> extractMultiFields(String field, boolean quoted) {
        Map<String, Float> extractedFields;
        if (field != null) {
            boolean allFields = Regex.isMatchAllPattern(field);
            if (allFields && this.field != null && this.field.equals(field)) {
                // "*" is the default field
                extractedFields = fieldsAndWeights;
            }
            boolean multiFields = Regex.isSimpleMatchPattern(field);
            // Filters unsupported fields if a pattern is requested
            // Filters metadata fields if all fields are requested
            extractedFields = resolveMappingField(context, field, 1.0f, !allFields, !multiFields, quoted ? quoteFieldSuffix : null);
        } else if (quoted && quoteFieldSuffix != null) {
            extractedFields = resolveMappingFields(context, fieldsAndWeights, quoteFieldSuffix);
        } else {
            extractedFields = fieldsAndWeights;
        }
        checkForTooManyFields(extractedFields.size(), this.context, field);
        return extractedFields;
    }

    @Override
    protected Query newMatchAllDocsQuery() {
        return Queries.newMatchAllQuery();
    }

    @Override
    public Query getFieldQuery(String field, String queryText, boolean quoted) throws ParseException {
        if (field != null && EXISTS_FIELD.equals(field)) {
            return existsQuery(queryText);
        }

        if (quoted) {
            return getFieldQuery(field, queryText, getPhraseSlop());
        }

        // Detects additional operators '<', '<=', '>', '>=' to handle range query with one side unbounded.
        // It is required to use a prefix field operator to enable the detection since they are not treated
        // as logical operator by the query parser (e.g. age:>=10).
        if (field != null) {
            if (queryText.length() > 1) {
                if (queryText.charAt(0) == '>') {
                    if (queryText.length() > 2) {
                        if (queryText.charAt(1) == '=') {
                            return getRangeQuery(field, queryText.substring(2), null, true, true);
                        }
                    }
                    return getRangeQuery(field, queryText.substring(1), null, false, true);
                } else if (queryText.charAt(0) == '<') {
                    if (queryText.length() > 2) {
                        if (queryText.charAt(1) == '=') {
                            return getRangeQuery(field, null, queryText.substring(2), true, true);
                        }
                    }
                    return getRangeQuery(field, null, queryText.substring(1), true, false);
                }
                // if we are querying a single date field, we also create a range query that leverages the time zone setting
                if (context.fieldMapper(field) instanceof DateFieldType && this.timeZone != null) {
                    return getRangeQuery(field, queryText, queryText, true, true);
                }
            }
        }

        Map<String, Float> fields = extractMultiFields(field, quoted);
        if (fields.isEmpty()) {
            // the requested fields do not match any field in the mapping
            // happens for wildcard fields only since we cannot expand to a valid field name
            // if there is no match in the mappings.
            return newUnmappedFieldQuery(field);
        }
        Analyzer oldAnalyzer = queryBuilder.analyzer;
        try {
            if (forceAnalyzer != null) {
                queryBuilder.setAnalyzer(forceAnalyzer);
            }
            return queryBuilder.parse(type, fields, queryText, null);
        } catch (IOException e) {
            throw new ParseException(e.getMessage());
        } finally {
            queryBuilder.setAnalyzer(oldAnalyzer);
        }
    }

    @Override
    protected Query getFieldQuery(String field, String queryText, int slop) throws ParseException {
        if (field != null && EXISTS_FIELD.equals(field)) {
            return existsQuery(queryText);
        }

        Map<String, Float> fields = extractMultiFields(field, true);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(field);
        }
        Analyzer oldAnalyzer = queryBuilder.analyzer;
        int oldSlop = queryBuilder.phraseSlop;
        try {
            if (forceQuoteAnalyzer != null) {
                queryBuilder.setAnalyzer(forceQuoteAnalyzer);
            } else if (forceAnalyzer != null) {
                queryBuilder.setAnalyzer(forceAnalyzer);
            }
            queryBuilder.setPhraseSlop(slop);
            Query query = queryBuilder.parse(MultiMatchQueryBuilder.Type.PHRASE, fields, queryText, null);
            if (query == null) {
                return null;
            }
            return applySlop(query, slop);
        } catch (IOException e) {
            throw new ParseException(e.getMessage());
        } finally {
            queryBuilder.setAnalyzer(oldAnalyzer);
            queryBuilder.setPhraseSlop(oldSlop);
        }
    }

    @Override
    protected Query getRangeQuery(String field, String part1, String part2, boolean startInclusive, boolean endInclusive)
        throws ParseException {
        if ("*".equals(part1)) {
            part1 = null;
        }
        if ("*".equals(part2)) {
            part2 = null;
        }

        Map<String, Float> fields = extractMultiFields(field, false);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(field);
        }

        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            Query q = getRangeQuerySingle(entry.getKey(), part1, part2, startInclusive, endInclusive, context);
            assert q != null;
            queries.add(applyBoost(q, entry.getValue()));
        }

        if (queries.size() == 1) {
            return queries.get(0);
        }
        float tiebreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
        return new DisjunctionMaxQuery(queries, tiebreaker);
    }

    private Query getRangeQuerySingle(
        String field,
        String part1,
        String part2,
        boolean startInclusive,
        boolean endInclusive,
        QueryShardContext context
    ) {
        MappedFieldType currentFieldType = context.fieldMapper(field);
        if (currentFieldType == null) {
            return newUnmappedFieldQuery(field);
        }
        try {
            Analyzer normalizer = forceAnalyzer == null ? queryBuilder.context.getSearchAnalyzer(currentFieldType) : forceAnalyzer;
            BytesRef part1Binary = part1 == null ? null : normalizer.normalize(field, part1);
            BytesRef part2Binary = part2 == null ? null : normalizer.normalize(field, part2);
            Query rangeQuery = currentFieldType.rangeQuery(
                part1Binary,
                part2Binary,
                startInclusive,
                endInclusive,
                null,
                timeZone,
                null,
                context
            );
            return rangeQuery;
        } catch (RuntimeException e) {
            if (lenient) {
                return newLenientFieldQuery(field, e);
            }
            throw e;
        }
    }

    @Override
    protected Query handleBareFuzzy(String field, Token fuzzySlop, String termImage) throws ParseException {
        if (fuzzySlop.image.length() == 1) {
            return getFuzzyQuery(field, termImage, fuzziness.asDistance(termImage));
        }
        float distance = Fuzziness.build(fuzzySlop.image.substring(1)).asDistance(termImage);
        return getFuzzyQuery(field, termImage, distance);
    }

    @Override
    protected Query getFuzzyQuery(String field, String termStr, float minSimilarity) throws ParseException {
        Map<String, Float> fields = extractMultiFields(field, false);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(field);
        }
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            Query q = getFuzzyQuerySingle(entry.getKey(), termStr, minSimilarity);
            assert q != null;
            queries.add(applyBoost(q, entry.getValue()));
        }

        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            float tiebreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
            return new DisjunctionMaxQuery(queries, tiebreaker);
        }
    }

    private Query getFuzzyQuerySingle(String field, String termStr, float minSimilarity) throws ParseException {
        MappedFieldType currentFieldType = context.fieldMapper(field);
        if (currentFieldType == null) {
            return newUnmappedFieldQuery(field);
        }
        try {
            Analyzer normalizer = forceAnalyzer == null ? queryBuilder.context.getSearchAnalyzer(currentFieldType) : forceAnalyzer;
            BytesRef term = termStr == null ? null : normalizer.normalize(field, termStr);
            return currentFieldType.fuzzyQuery(
                term,
                Fuzziness.fromEdits((int) minSimilarity),
                getFuzzyPrefixLength(),
                fuzzyMaxExpansions,
                fuzzyTranspositions,
                fuzzyRewriteMethod,
                context
            );
        } catch (RuntimeException e) {
            if (lenient) {
                return newLenientFieldQuery(field, e);
            }
            throw e;
        }
    }

    @Override
    protected Query newFuzzyQuery(Term term, float minimumSimilarity, int prefixLength) {
        int numEdits = Fuzziness.build(minimumSimilarity).asDistance(term.text());
        return new FuzzyQuery(term, numEdits, prefixLength, fuzzyMaxExpansions, fuzzyTranspositions, fuzzyRewriteMethod);
    }

    @Override
    protected Query getPrefixQuery(String field, String termStr) throws ParseException {
        Map<String, Float> fields = extractMultiFields(field, false);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(termStr);
        }
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            Query q = getPrefixQuerySingle(entry.getKey(), termStr);
            if (q != null) {
                queries.add(applyBoost(q, entry.getValue()));
            }
        }
        if (queries.isEmpty()) {
            return null;
        } else if (queries.size() == 1) {
            return queries.get(0);
        } else {
            float tiebreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
            return new DisjunctionMaxQuery(queries, tiebreaker);
        }
    }

    private Query getPrefixQuerySingle(String field, String termStr) throws ParseException {
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            MappedFieldType currentFieldType = context.fieldMapper(field);
            if (currentFieldType == null || currentFieldType.getTextSearchInfo() == TextSearchInfo.NONE) {
                return newUnmappedFieldQuery(field);
            }
            setAnalyzer(getSearchAnalyzer(currentFieldType));
            Query query = null;
            if (currentFieldType.getTextSearchInfo().isTokenized() == false) {
                query = currentFieldType.prefixQuery(termStr, getMultiTermRewriteMethod(), context);
            } else {
                query = getPossiblyAnalyzedPrefixQuery(currentFieldType.name(), termStr, currentFieldType);
            }
            return query;
        } catch (RuntimeException e) {
            if (lenient) {
                return newLenientFieldQuery(field, e);
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    private Query getPossiblyAnalyzedPrefixQuery(String field, String termStr, MappedFieldType currentFieldType) throws ParseException {
        if (analyzeWildcard == false) {
            return currentFieldType.prefixQuery(
                getAnalyzer().normalize(field, termStr).utf8ToString(),
                getMultiTermRewriteMethod(),
                context
            );
        }
        List<List<String>> tlist;
        // get Analyzer from superclass and tokenize the term
        TokenStream source = null;
        try {
            try {
                source = getAnalyzer().tokenStream(field, termStr);
                source.reset();
            } catch (IOException e) {
                return super.getPrefixQuery(field, termStr);
            }
            tlist = new ArrayList<>();
            List<String> currentPos = new ArrayList<>();
            CharTermAttribute termAtt = source.addAttribute(CharTermAttribute.class);
            PositionIncrementAttribute posAtt = source.addAttribute(PositionIncrementAttribute.class);

            while (true) {
                try {
                    if (!source.incrementToken()) break;
                } catch (IOException e) {
                    break;
                }
                if (currentPos.isEmpty() == false && posAtt.getPositionIncrement() > 0) {
                    tlist.add(currentPos);
                    currentPos = new ArrayList<>();
                }
                currentPos.add(termAtt.toString());
            }
            if (currentPos.isEmpty() == false) {
                tlist.add(currentPos);
            }
        } finally {
            if (source != null) {
                IOUtils.closeWhileHandlingException(source);
            }
        }

        if (tlist.size() == 0) {
            return null;
        }

        if (tlist.size() == 1 && tlist.get(0).size() == 1) {
            return currentFieldType.prefixQuery(tlist.get(0).get(0), getMultiTermRewriteMethod(), context);
        }

        // build a boolean query with prefix on the last position only.
        List<BooleanClause> clauses = new ArrayList<>();
        for (int pos = 0; pos < tlist.size(); pos++) {
            List<String> plist = tlist.get(pos);
            boolean isLastPos = (pos == tlist.size() - 1);
            Query posQuery;
            if (plist.size() == 1) {
                if (isLastPos) {
                    posQuery = currentFieldType.prefixQuery(plist.get(0), getMultiTermRewriteMethod(), context);
                } else {
                    posQuery = newTermQuery(new Term(field, plist.get(0)), BoostAttribute.DEFAULT_BOOST);
                }
            } else if (isLastPos == false) {
                // build a synonym query for terms in the same position.
                SynonymQuery.Builder sb = new SynonymQuery.Builder(field);
                for (String synonym : plist) {
                    sb.addTerm(new Term(field, synonym));
                }
                posQuery = sb.build();
            } else {
                List<BooleanClause> innerClauses = new ArrayList<>();
                for (String token : plist) {
                    innerClauses.add(new BooleanClause(super.getPrefixQuery(field, token), BooleanClause.Occur.SHOULD));
                }
                posQuery = getBooleanQuery(innerClauses);
            }
            clauses.add(
                new BooleanClause(posQuery, getDefaultOperator() == Operator.AND ? BooleanClause.Occur.MUST : BooleanClause.Occur.SHOULD)
            );
        }
        return getBooleanQuery(clauses);
    }

    private Query existsQuery(String fieldName) {
        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType) context
            .getMapperService()
            .fieldType(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldType == null) {
            return new MatchNoDocsQuery("No mappings yet");
        }
        if (fieldNamesFieldType.isEnabled() == false) {
            // The field_names_field is disabled so we switch to a wildcard query that matches all terms
            return new WildcardQuery(new Term(fieldName, "*"));
        }

        return ExistsQueryBuilder.newFilter(context, fieldName, false);
    }

    @Override
    protected Query getWildcardQuery(String field, String termStr) throws ParseException {
        String actualField = field != null ? field : this.field;
        if (termStr.equals("*") && actualField != null) {
            if (Regex.isMatchAllPattern(actualField)) {
                return newMatchAllDocsQuery();
            }
            // effectively, we check if a field exists or not
            return existsQuery(actualField);
        }

        Map<String, Float> fields = extractMultiFields(field, false);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(termStr);
        }
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            Query q = getWildcardQuerySingle(entry.getKey(), termStr);
            assert q != null;
            queries.add(applyBoost(q, entry.getValue()));
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            float tiebreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
            return new DisjunctionMaxQuery(queries, tiebreaker);
        }
    }

    private Query getWildcardQuerySingle(String field, String termStr) throws ParseException {
        if ("*".equals(termStr)) {
            // effectively, we check if a field exists or not
            return existsQuery(field);
        }
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            MappedFieldType currentFieldType = queryBuilder.context.fieldMapper(field);
            if (currentFieldType == null) {
                return newUnmappedFieldQuery(field);
            }
            if (forceAnalyzer != null && (analyzeWildcard || currentFieldType.getTextSearchInfo().isTokenized())) {
                setAnalyzer(forceAnalyzer);
                return super.getWildcardQuery(currentFieldType.name(), termStr);
            }
            if (getAllowLeadingWildcard() == false && (termStr.startsWith("*") || termStr.startsWith("?"))) {
                throw new ParseException("'*' or '?' not allowed as first character in WildcardQuery");
            }
            // query string query is always normalized
            return currentFieldType.normalizedWildcardQuery(termStr, getMultiTermRewriteMethod(), context);
        } catch (RuntimeException e) {
            if (lenient) {
                return newLenientFieldQuery(field, e);
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    private Analyzer getSearchAnalyzer(MappedFieldType currentFieldType) {
        if (forceAnalyzer == null) {
            return queryBuilder.context.getSearchAnalyzer(currentFieldType);
        }
        return forceAnalyzer;
    }

    @Override
    protected Query getRegexpQuery(String field, String termStr) throws ParseException {
        final int maxAllowedRegexLength = context.getIndexSettings().getMaxRegexLength();
        if (termStr.length() > maxAllowedRegexLength) {
            throw new IllegalArgumentException(
                "The length of regex ["
                    + termStr.length()
                    + "] used in the [query_string] has exceeded "
                    + "the allowed maximum of ["
                    + maxAllowedRegexLength
                    + "]. This maximum can be set by changing the ["
                    + IndexSettings.MAX_REGEX_LENGTH_SETTING.getKey()
                    + "] index level setting."
            );
        }
        Map<String, Float> fields = extractMultiFields(field, false);
        if (fields.isEmpty()) {
            return newUnmappedFieldQuery(termStr);
        }
        List<Query> queries = new ArrayList<>();
        for (Map.Entry<String, Float> entry : fields.entrySet()) {
            Query q = getRegexpQuerySingle(entry.getKey(), termStr);
            assert q != null;
            queries.add(applyBoost(q, entry.getValue()));
        }
        if (queries.size() == 1) {
            return queries.get(0);
        } else {
            float tiebreaker = groupTieBreaker == null ? type.tieBreaker() : groupTieBreaker;
            return new DisjunctionMaxQuery(queries, tiebreaker);
        }
    }

    private Query getRegexpQuerySingle(String field, String termStr) throws ParseException {
        Analyzer oldAnalyzer = getAnalyzer();
        try {
            MappedFieldType currentFieldType = queryBuilder.context.fieldMapper(field);
            if (currentFieldType == null) {
                return newUnmappedFieldQuery(field);
            }
            setAnalyzer(getSearchAnalyzer(currentFieldType));
            return super.getRegexpQuery(field, termStr);
        } catch (RuntimeException e) {
            if (lenient) {
                return newLenientFieldQuery(field, e);
            }
            throw e;
        } finally {
            setAnalyzer(oldAnalyzer);
        }
    }

    @Override
    protected Query getBooleanQuery(List<BooleanClause> clauses) throws ParseException {
        Query q = super.getBooleanQuery(clauses);
        if (q == null) {
            return null;
        }
        return fixNegativeQueryIfNeeded(q);
    }

    private Query applySlop(Query q, int slop) {
        if (q instanceof PhraseQuery) {
            // make sure that the boost hasn't been set beforehand, otherwise we'd lose it
            assert q instanceof BoostQuery == false;
            return addSlopToPhrase((PhraseQuery) q, slop);
        } else if (q instanceof MultiPhraseQuery) {
            MultiPhraseQuery.Builder builder = new MultiPhraseQuery.Builder((MultiPhraseQuery) q);
            builder.setSlop(slop);
            return builder.build();
        } else if (q instanceof SpanQuery) {
            return addSlopToSpan((SpanQuery) q, slop);
        } else {
            return q;
        }
    }

    private Query addSlopToSpan(SpanQuery query, int slop) {
        if (query instanceof SpanNearQuery) {
            return new SpanNearQuery(((SpanNearQuery) query).getClauses(), slop, ((SpanNearQuery) query).isInOrder());
        } else if (query instanceof SpanOrQuery) {
            SpanQuery[] clauses = new SpanQuery[((SpanOrQuery) query).getClauses().length];
            int pos = 0;
            for (SpanQuery clause : ((SpanOrQuery) query).getClauses()) {
                clauses[pos++] = (SpanQuery) addSlopToSpan(clause, slop);
            }
            return new SpanOrQuery(clauses);
        } else {
            return query;
        }
    }

    /**
     * Rebuild a phrase query with a slop value
     */
    private PhraseQuery addSlopToPhrase(PhraseQuery query, int slop) {
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(slop);
        final Term[] terms = query.getTerms();
        final int[] positions = query.getPositions();
        for (int i = 0; i < terms.length; ++i) {
            builder.add(terms[i], positions[i]);
        }

        return builder.build();
    }

    @Override
    public Query parse(String query) throws ParseException {
        if (query.trim().isEmpty()) {
            return Queries.newMatchNoDocsQuery("Matching no documents because no terms present");
        }
        return super.parse(query);
    }
}
