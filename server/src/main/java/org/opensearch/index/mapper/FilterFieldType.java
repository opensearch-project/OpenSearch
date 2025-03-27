/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.query.IntervalMode;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Wraps a {@link MappedFieldType}, delegating all methods (except typeName).
 * <p>
 * Subclasses can extend this class to wrap an existing {@link MappedFieldType} to reuse most functionality, while
 * customizing/modifying some specific behavior by overriding the relevant methods.
 */
@PublicApi(since = "3.0.0")
public abstract class FilterFieldType extends MappedFieldType {
    protected final MappedFieldType delegate;

    public FilterFieldType(MappedFieldType delegate) {
        super(
            delegate.name(),
            delegate.isSearchable(),
            delegate.isStored(),
            delegate.hasDocValues(),
            delegate.getTextSearchInfo(),
            delegate.meta()
        );
        this.delegate = delegate;
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
        return delegate.valueFetcher(context, searchLookup, format);
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return delegate.termQuery(value, context);
    }

    @Override
    public String familyTypeName() {
        return delegate.familyTypeName();
    }

    @Override
    public String name() {
        return delegate.name();
    }

    @Override
    public float boost() {
        return delegate.boost();
    }

    @Override
    public void setBoost(float boost) {
        delegate.setBoost(boost);
    }

    @Override
    public boolean hasDocValues() {
        return delegate.hasDocValues();
    }

    @Override
    public NamedAnalyzer indexAnalyzer() {
        return delegate.indexAnalyzer();
    }

    @Override
    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        delegate.setIndexAnalyzer(analyzer);
    }

    @Override
    public Object valueForDisplay(Object value) {
        return delegate.valueForDisplay(value);
    }

    @Override
    public boolean isSearchable() {
        return delegate.isSearchable();
    }

    @Override
    public boolean isStored() {
        return delegate.isStored();
    }

    @Override
    public Function<byte[], Number> pointReaderIfPossible() {
        return delegate.pointReaderIfPossible();
    }

    @Override
    public boolean isAggregatable() {
        return delegate.isAggregatable();
    }

    @Override
    public Query termQueryCaseInsensitive(Object value, QueryShardContext context) {
        return delegate.termQueryCaseInsensitive(value, context);
    }

    @Override
    public Query termsQuery(List<?> values, QueryShardContext context) {
        return delegate.termsQuery(values, context);
    }

    @Override
    public Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ShapeRelation relation,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    ) {
        return delegate.rangeQuery(lowerTerm, upperTerm, includeLower, includeUpper, relation, timeZone, parser, context);
    }

    @Override
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        return delegate.fuzzyQuery(value, fuzziness, prefixLength, maxExpansions, transpositions, method, context);
    }

    @Override
    public Query prefixQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitve, QueryShardContext context) {
        return delegate.prefixQuery(value, method, caseInsensitve, context);
    }

    @Override
    public Query wildcardQuery(String value, MultiTermQuery.RewriteMethod method, boolean caseInsensitve, QueryShardContext context) {
        return delegate.wildcardQuery(value, method, caseInsensitve, context);
    }

    @Override
    public Query normalizedWildcardQuery(String value, MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        return delegate.normalizedWildcardQuery(value, method, context);
    }

    @Override
    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        return delegate.regexpQuery(value, syntaxFlags, matchFlags, maxDeterminizedStates, method, context);
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        return delegate.existsQuery(context);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        return delegate.phraseQuery(stream, slop, enablePositionIncrements);
    }

    @Override
    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context) throws IOException {
        return delegate.phraseQuery(stream, slop, enablePositionIncrements, context);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        return delegate.multiPhraseQuery(stream, slop, enablePositionIncrements);
    }

    @Override
    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context)
        throws IOException {
        return delegate.multiPhraseQuery(stream, slop, enablePositionIncrements, context);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        return delegate.phrasePrefixQuery(stream, slop, maxExpansions);
    }

    @Override
    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
        return delegate.phrasePrefixQuery(stream, slop, maxExpansions, context);
    }

    @Override
    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        return delegate.spanPrefixQuery(value, method, context);
    }

    @Override
    public Query distanceFeatureQuery(Object origin, String pivot, float boost, QueryShardContext context) {
        return delegate.distanceFeatureQuery(origin, pivot, boost, context);
    }

    @Override
    public IntervalsSource intervals(String query, int max_gaps, IntervalMode mode, NamedAnalyzer analyzer, boolean prefix)
        throws IOException {
        return delegate.intervals(query, max_gaps, mode, analyzer, prefix);
    }

    @Override
    public Relation isFieldWithinQuery(
        IndexReader reader,
        Object from,
        Object to,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser dateMathParser,
        QueryRewriteContext context
    ) throws IOException {
        return delegate.isFieldWithinQuery(reader, from, to, includeLower, includeUpper, timeZone, dateMathParser, context);
    }

    @Override
    public boolean eagerGlobalOrdinals() {
        return delegate.eagerGlobalOrdinals();
    }

    @Override
    public void setEagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
        delegate.setEagerGlobalOrdinals(eagerGlobalOrdinals);
    }

    @Override
    public DocValueFormat docValueFormat(String format, ZoneId timeZone) {
        return delegate.docValueFormat(format, timeZone);
    }

    @Override
    public Map<String, String> meta() {
        return delegate.meta();
    }

    @Override
    public TextSearchInfo getTextSearchInfo() {
        return delegate.getTextSearchInfo();
    }

    @Override
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return delegate.fielddataBuilder(fullyQualifiedIndexName, searchLookup);
    }

    @Override
    public MappedFieldType unwrap() {
        return delegate.unwrap();
    }
}
