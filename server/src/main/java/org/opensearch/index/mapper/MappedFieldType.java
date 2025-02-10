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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.intervals.IntervalsSource;
import org.apache.lucene.queries.spans.SpanMultiTermQueryWrapper;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.time.DateMathParser;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.query.DistanceFeatureQueryBuilder;
import org.opensearch.index.query.IntervalMode;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.fetch.subphase.FetchFieldsPhase;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * This defines the core properties and functions to operate on a field.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class MappedFieldType {

    private final String name;
    private final boolean docValues;
    private final boolean isIndexed;
    private final boolean isStored;
    private final TextSearchInfo textSearchInfo;
    private final Map<String, String> meta;
    private float boost;
    private NamedAnalyzer indexAnalyzer;
    private boolean eagerGlobalOrdinals;

    public MappedFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta
    ) {
        setBoost(1.0f);
        this.name = Objects.requireNonNull(name);
        this.isIndexed = isIndexed;
        this.isStored = isStored;
        this.docValues = hasDocValues;
        this.textSearchInfo = Objects.requireNonNull(textSearchInfo);
        this.meta = meta;
    }

    /**
     * Return a fielddata builder for this field
     *
     * @param fullyQualifiedIndexName the name of the index this field-data is build for
     * @param searchLookup a {@link SearchLookup} supplier to allow for accessing other fields values in the context of runtime fields
     * @throws IllegalArgumentException if the fielddata is not supported on this type.
     * An IllegalArgumentException is needed in order to return an http error 400
     * when this error occurs in a request. see: {@link ExceptionsHelper#status}
     */
    public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        throw new IllegalArgumentException("Fielddata is not supported on field [" + name() + "] of type [" + typeName() + "]");
    }

    /**
     * Create a helper class to fetch field values during the {@link FetchFieldsPhase}.
     * <p>
     * New field types must implement this method in order to support the search 'fields' option. Except
     * for metadata fields, field types should not throw {@link UnsupportedOperationException} since this
     * could cause a search retrieving multiple fields (like "fields": ["*"]) to fail.
     */
    public abstract ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, @Nullable String format);

    /** Returns the name of this type, as would be specified in mapping properties */
    public abstract String typeName();

    /** Returns the field family type, as used in field capabilities */
    public String familyTypeName() {
        return typeName();
    }

    public String name() {
        return name;
    }

    public float boost() {
        return boost;
    }

    public void setBoost(float boost) {
        this.boost = boost;
    }

    public boolean hasDocValues() {
        return docValues;
    }

    public NamedAnalyzer indexAnalyzer() {
        return indexAnalyzer;
    }

    public void setIndexAnalyzer(NamedAnalyzer analyzer) {
        this.indexAnalyzer = analyzer;
    }

    /** Given a value that comes from the stored fields API, convert it to the
     *  expected type. For instance a date field would store dates as longs and
     *  format it back to a string in this method. */
    public Object valueForDisplay(Object value) {
        return value;
    }

    /**
     * Returns true if the field is searchable.
     */
    public boolean isSearchable() {
        return isIndexed;
    }

    /**
     * Returns true if the field is stored separately.
     */
    public boolean isStored() {
        return isStored;
    }

    /**
     * If the field supports using the indexed data to speed up operations related to ordering of data, such as sorting or aggs, return
     * a function for doing that.  If it is unsupported for this field type, there is no need to override this method.
     *
     * @return null if the optimization cannot be applied, otherwise a function to use for the optimization
     */
    @Nullable
    public Function<byte[], Number> pointReaderIfPossible() {
        return null;
    }

    /** Returns true if the field is aggregatable.
     *
     */
    public boolean isAggregatable() {
        try {
            fielddataBuilder("", () -> { throw new UnsupportedOperationException("SearchLookup not available"); });
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    /** Generates a query that will only match documents that contain the given value.
     *  The default implementation returns a {@link TermQuery} over the value bytes,
     *  boosted by {@link #boost()}.
     *  @throws IllegalArgumentException if {@code value} cannot be converted to the expected data type or if the field is not searchable
     *      due to the way it is configured (eg. not indexed)
     *  @throws OpenSearchParseException if {@code value} cannot be converted to the expected data type
     *  @throws UnsupportedOperationException if the field is not searchable regardless of options
     *  @throws QueryShardException if the field is not searchable regardless of options
     */
    // TODO: Standardize exception types
    public abstract Query termQuery(Object value, @Nullable QueryShardContext context);

    // Case insensitive form of term query (not supported by all fields so must be overridden to enable)
    public Query termQueryCaseInsensitive(Object value, @Nullable QueryShardContext context) {
        throw new QueryShardException(
            context,
            "[" + name + "] field which is of type [" + typeName() + "], does not support case insensitive term queries"
        );
    }

    /** Build a constant-scoring query that matches all values. The default implementation uses a
     * {@link ConstantScoreQuery} around a {@link BooleanQuery} whose {@link Occur#SHOULD} clauses
     * are generated with {@link #termQuery}. */
    public Query termsQuery(List<?> values, @Nullable QueryShardContext context) {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (Object value : values) {
            builder.add(termQuery(value, context), Occur.SHOULD);
        }
        return new ConstantScoreQuery(builder.build());
    }

    /**
     * Factory method for range queries.
     * @param relation the relation, nulls should be interpreted like INTERSECTS
     */
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
        throw new IllegalArgumentException("Field [" + name + "] of type [" + typeName() + "] does not support range queries");
    }

    // Fuzzy Query with re-write method
    public Query fuzzyQuery(
        Object value,
        Fuzziness fuzziness,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        @Nullable MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException(
            "Can only use fuzzy queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    // Case sensitive form of prefix query
    public final Query prefixQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        return prefixQuery(value, method, false, context);
    }

    public Query prefixQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        QueryShardContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use prefix queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    // Case sensitive form of wildcard query
    public final Query wildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        return wildcardQuery(value, method, false, context);
    }

    /** optionally normalize the wildcard pattern based on the value of {@code caseInsensitive} */
    public Query wildcardQuery(
        String value,
        @Nullable MultiTermQuery.RewriteMethod method,
        boolean caseInsensitve,
        QueryShardContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use wildcard queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /** always normalizes the wildcard pattern to lowercase */
    public Query normalizedWildcardQuery(String value, @Nullable MultiTermQuery.RewriteMethod method, QueryShardContext context) {
        throw new QueryShardException(
            context,
            "Can only use wildcard queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query regexpQuery(
        String value,
        int syntaxFlags,
        int matchFlags,
        int maxDeterminizedStates,
        @Nullable MultiTermQuery.RewriteMethod method,
        QueryShardContext context
    ) {
        throw new QueryShardException(
            context,
            "Can only use regexp queries on keyword and text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query existsQuery(QueryShardContext context) {
        if (hasDocValues()) {
            return new FieldExistsQuery(name());
        } else if (getTextSearchInfo().hasNorms()) {
            return new FieldExistsQuery(name());
        } else {
            return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
        }
    }

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query phraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context) throws IOException {
        return phraseQuery(stream, slop, enablePositionIncrements);
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query multiPhraseQuery(TokenStream stream, int slop, boolean enablePositionIncrements, QueryShardContext context)
        throws IOException {
        return multiPhraseQuery(stream, slop, enablePositionIncrements);
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions) throws IOException {
        throw new IllegalArgumentException(
            "Can only use phrase prefix queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query phrasePrefixQuery(TokenStream stream, int slop, int maxExpansions, QueryShardContext context) throws IOException {
        return phrasePrefixQuery(stream, slop, maxExpansions);
    }

    public SpanQuery spanPrefixQuery(String value, SpanMultiTermQueryWrapper.SpanRewriteMethod method, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Can only use span prefix queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    public Query distanceFeatureQuery(Object origin, String pivot, float boost, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Illegal data type of ["
                + typeName()
                + "]!"
                + "["
                + DistanceFeatureQueryBuilder.NAME
                + "] query can only be run on a date, date_nanos or geo_point field type!"
        );
    }

    /**
     * Create an {@link IntervalsSource} to be used for proximity queries
     */
    public IntervalsSource intervals(String query, int max_gaps, IntervalMode mode, NamedAnalyzer analyzer, boolean prefix)
        throws IOException {
        throw new IllegalArgumentException(
            "Can only use interval queries on text fields - not on [" + name + "] which is of type [" + typeName() + "]"
        );
    }

    /**
     * An enum used to describe the relation between the range of terms in a
     * shard when compared with a query range
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public enum Relation {
        WITHIN,
        INTERSECTS,
        DISJOINT
    }

    /** Return whether all values of the given {@link IndexReader} are within the range,
     *  outside the range or cross the range. The default implementation returns
     *  {@link Relation#INTERSECTS}, which is always fine to return when there is
     *  no way to check whether values are actually within bounds. */
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
        return Relation.INTERSECTS;
    }

    /** @throws IllegalArgumentException if the fielddata is not supported on this type.
     *  An IllegalArgumentException is needed in order to return an http error 400
     *  when this error occurs in a request. see: {@link ExceptionsHelper#status}
     **/
    protected final void failIfNoDocValues() {
        if (hasDocValues() == false) {
            throw new IllegalArgumentException(
                "Can't load fielddata on ["
                    + name()
                    + "] because fielddata is unsupported on fields of type ["
                    + typeName()
                    + "]. Use doc values instead."
            );
        }
    }

    protected final void failIfNotIndexed() {
        if (isIndexed == false) {
            // we throw an IAE rather than an ISE so that it translates to a 4xx code rather than 5xx code on the http layer
            throw new IllegalArgumentException("Cannot search on field [" + name() + "] since it is not indexed.");
        }
    }

    protected final void failIfNotIndexedAndNoDocValues() {
        // we fail if a field is both not indexed and does not have doc_values enabled
        if (isIndexed == false && hasDocValues() == false) {
            throw new IllegalArgumentException(
                "Cannot search on field [" + name() + "] since it is both not indexed," + " and does not have doc_values enabled."
            );
        }
    }

    public boolean eagerGlobalOrdinals() {
        return eagerGlobalOrdinals;
    }

    public void setEagerGlobalOrdinals(boolean eagerGlobalOrdinals) {
        this.eagerGlobalOrdinals = eagerGlobalOrdinals;
    }

    /** Return a {@link DocValueFormat} that can be used to display and parse
     *  values as returned by the fielddata API.
     *  The default implementation returns a {@link DocValueFormat#RAW}. */
    public DocValueFormat docValueFormat(@Nullable String format, ZoneId timeZone) {
        if (format != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom formats");
        }
        if (timeZone != null) {
            throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support custom time zones");
        }
        return DocValueFormat.RAW;
    }

    /**
     * Extract a {@link Term} from a query created with {@link #termQuery} by
     * recursively removing {@link BoostQuery} wrappers.
     * @throws IOException
     * @throws IllegalArgumentException if the wrapped query is not a {@link TermQuery}
     */
    public static Term extractTerm(Query termQuery) throws IOException {
        while (termQuery instanceof BoostQuery) {
            termQuery = ((BoostQuery) termQuery).getQuery();
        }
        if (termQuery instanceof TermInSetQuery) {
            TermInSetQuery tisQuery = (TermInSetQuery) termQuery;
            if (tisQuery.getTermsCount() == 1) {
                BytesRefIterator it = tisQuery.getBytesRefIterator();
                BytesRef term = it.next();
                return new Term(tisQuery.getField(), term);
            }
        }
        if (termQuery instanceof TermQuery == false) {
            throw new IllegalArgumentException("Cannot extract a term from a query of type " + termQuery.getClass() + ": " + termQuery);
        }
        return ((TermQuery) termQuery).getTerm();
    }

    /**
     * Get the metadata associated with this field.
     */
    public Map<String, String> meta() {
        return meta;
    }

    /**
     * Returns information on how any text in this field is indexed
     * <p>
     * Fields that do not support any text-based queries should return
     * {@link TextSearchInfo#NONE}.  Some fields (eg numeric) may support
     * only simple match queries, and can return
     * {@link TextSearchInfo#SIMPLE_MATCH_ONLY}
     */
    public TextSearchInfo getTextSearchInfo() {
        return textSearchInfo;
    }
}
