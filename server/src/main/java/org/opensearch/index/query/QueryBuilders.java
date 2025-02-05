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

package org.opensearch.index.query;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.common.Nullable;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.geometry.Geometry;
import org.opensearch.index.query.DistanceFeatureQueryBuilder.Origin;
import org.opensearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.script.Script;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Utility class to create search queries.
 *
 * @opensearch.internal
 */
public final class QueryBuilders {

    private QueryBuilders() {}

    /**
     * A query that matches on all documents.
     */
    public static MatchAllQueryBuilder matchAllQuery() {
        return new MatchAllQueryBuilder();
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchQueryBuilder matchQuery(String name, Object text) {
        return new MatchQueryBuilder(name, text);
    }

    /**
     * Creates a common query for the provided field name and text.
     *
     * @param fieldName The field name.
     * @param text The query text (to be analyzed).
     *
     * @deprecated See {@link CommonTermsQueryBuilder}
     */
    @Deprecated
    public static CommonTermsQueryBuilder commonTermsQuery(String fieldName, Object text) {
        return new CommonTermsQueryBuilder(fieldName, text);
    }

    /**
     * Creates a match query with type "BOOLEAN" for the provided field name and text.
     *
     * @param fieldNames The field names.
     * @param text       The query text (to be analyzed).
     */
    public static MultiMatchQueryBuilder multiMatchQuery(Object text, String... fieldNames) {
        return new MultiMatchQueryBuilder(text, fieldNames); // BOOLEAN is the default
    }

    /**
     * Creates a text query with type "BOOL_PREFIX" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchBoolPrefixQueryBuilder matchBoolPrefixQuery(String name, Object text) {
        return new MatchBoolPrefixQueryBuilder(name, text);
    }

    /**
     * Creates a text query with type "PHRASE" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchPhraseQueryBuilder matchPhraseQuery(String name, Object text) {
        return new MatchPhraseQueryBuilder(name, text);
    }

    /**
     * Creates a match query with type "PHRASE_PREFIX" for the provided field name and text.
     *
     * @param name The field name.
     * @param text The query text (to be analyzed).
     */
    public static MatchPhrasePrefixQueryBuilder matchPhrasePrefixQuery(String name, Object text) {
        return new MatchPhrasePrefixQueryBuilder(name, text);
    }

    /**
     * A query that generates the union of documents produced by its sub-queries, and that scores each document
     * with the maximum score for that document as produced by any sub-query, plus a tie breaking increment for any
     * additional matching sub-queries.
     */
    public static DisMaxQueryBuilder disMaxQuery() {
        return new DisMaxQueryBuilder();
    }

    /**
     * A query to boost scores based on their proximity to the given origin for date, date_nanos and geo_point field types.
     * @param name The field name
     * @param origin The origin of the distance calculation. Can be a long, string or {@link GeoPoint}, depending on field type.
     * @param pivot The distance from the origin at which relevance scores receive half of the boost value.
     */
    public static DistanceFeatureQueryBuilder distanceFeatureQuery(String name, Origin origin, String pivot) {
        return new DistanceFeatureQueryBuilder(name, origin, pivot);
    }

    /**
     * Constructs a query that will match only specific ids within all types.
     */
    public static IdsQueryBuilder idsQuery() {
        return new IdsQueryBuilder();
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, String value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, int value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, long value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, float value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, double value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, boolean value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing a term.
     *
     * @param name  The name of the field
     * @param value The value of the term
     */
    public static TermQueryBuilder termQuery(String name, Object value) {
        return new TermQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents using fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     *
     * @see #matchQuery(String, Object)
     * @see #rangeQuery(String)
     */
    public static FuzzyQueryBuilder fuzzyQuery(String name, String value) {
        return new FuzzyQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents using fuzzy query.
     *
     * @param name  The name of the field
     * @param value The value of the term
     *
     * @see #matchQuery(String, Object)
     * @see #rangeQuery(String)
     */
    public static FuzzyQueryBuilder fuzzyQuery(String name, Object value) {
        return new FuzzyQueryBuilder(name, value);
    }

    /**
     * A Query that matches documents containing terms with a specified prefix.
     *
     * @param name   The name of the field
     * @param prefix The prefix query
     */
    public static PrefixQueryBuilder prefixQuery(String name, String prefix) {
        return new PrefixQueryBuilder(name, prefix);
    }

    /**
     * A Query that matches documents within an range of terms.
     *
     * @param name The field name
     */
    public static RangeQueryBuilder rangeQuery(String name) {
        return new RangeQueryBuilder(name);
    }

    /**
     * Implements the wildcard search query. Supported wildcards are {@code *}, which
     * matches any character sequence (including the empty one), and {@code ?},
     * which matches any single character. Note this query can be slow, as it
     * needs to iterate over many terms. In order to prevent extremely slow WildcardQueries,
     * a Wildcard term should not start with one of the wildcards {@code *} or
     * {@code ?}. (The wildcard field type however, is optimised for leading wildcards)
     *
     * @param name  The field name
     * @param query The wildcard query string
     */
    public static WildcardQueryBuilder wildcardQuery(String name, String query) {
        return new WildcardQueryBuilder(name, query);
    }

    /**
     * A Query that matches documents containing terms with a specified regular expression.
     *
     * @param name   The name of the field
     * @param regexp The regular expression
     */
    public static RegexpQueryBuilder regexpQuery(String name, String regexp) {
        return new RegexpQueryBuilder(name, regexp);
    }

    /**
     * A query that parses a query string and runs it. There are two modes that this operates. The first,
     * when no field is added (using {@link QueryStringQueryBuilder#field(String)}, will run the query once and non prefixed fields
     * will use the {@link QueryStringQueryBuilder#defaultField(String)} set. The second, when one or more fields are added
     * (using {@link QueryStringQueryBuilder#field(String)}), will run the parsed query against the provided fields, and combine
     * them either using Dismax.
     *
     * @param queryString The query string to run
     */
    public static QueryStringQueryBuilder queryStringQuery(String queryString) {
        return new QueryStringQueryBuilder(queryString);
    }

    /**
     * A query that acts similar to a query_string query, but won't throw
     * exceptions for any weird string syntax. See
     * {@link org.apache.lucene.queryparser.simple.SimpleQueryParser} for the full
     * supported syntax.
     */
    public static SimpleQueryStringBuilder simpleQueryStringQuery(String queryString) {
        return new SimpleQueryStringBuilder(queryString);
    }

    /**
     * The BoostingQuery class can be used to effectively demote results that match a given query.
     * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
     * but reduces their overall score:
     */
    public static BoostingQueryBuilder boostingQuery(QueryBuilder positiveQuery, QueryBuilder negativeQuery) {
        return new BoostingQueryBuilder(positiveQuery, negativeQuery);
    }

    /**
     * A Query that matches documents matching boolean combinations of other queries.
     */
    public static BoolQueryBuilder boolQuery() {
        return new BoolQueryBuilder();
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, String value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, int value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, long value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, float value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanTermQueryBuilder spanTermQuery(String name, double value) {
        return new SpanTermQueryBuilder(name, value);
    }

    public static SpanFirstQueryBuilder spanFirstQuery(SpanQueryBuilder match, int end) {
        return new SpanFirstQueryBuilder(match, end);
    }

    public static SpanNearQueryBuilder spanNearQuery(SpanQueryBuilder initialClause, int slop) {
        return new SpanNearQueryBuilder(initialClause, slop);
    }

    public static SpanNotQueryBuilder spanNotQuery(SpanQueryBuilder include, SpanQueryBuilder exclude) {
        return new SpanNotQueryBuilder(include, exclude);
    }

    public static SpanOrQueryBuilder spanOrQuery(SpanQueryBuilder initialClause) {
        return new SpanOrQueryBuilder(initialClause);
    }

    /** Creates a new {@code span_within} builder.
    * @param big the big clause, it must enclose {@code little} for a match.
    * @param little the little clause, it must be contained within {@code big} for a match.
    */
    public static SpanWithinQueryBuilder spanWithinQuery(SpanQueryBuilder big, SpanQueryBuilder little) {
        return new SpanWithinQueryBuilder(big, little);
    }

    /**
     * Creates a new {@code span_containing} builder.
     * @param big the big clause, it must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public static SpanContainingQueryBuilder spanContainingQuery(SpanQueryBuilder big, SpanQueryBuilder little) {
        return new SpanContainingQueryBuilder(big, little);
    }

    /**
     * Creates a {@link SpanQueryBuilder} which allows having a sub query
     * which implements {@link MultiTermQueryBuilder}. This is useful for
     * having e.g. wildcard or fuzzy queries inside spans.
     *
     * @param multiTermQueryBuilder The {@link MultiTermQueryBuilder} that
     *                              backs the created builder.
     */

    public static SpanMultiTermQueryBuilder spanMultiTermQueryBuilder(MultiTermQueryBuilder multiTermQueryBuilder) {
        return new SpanMultiTermQueryBuilder(multiTermQueryBuilder);
    }

    public static FieldMaskingSpanQueryBuilder fieldMaskingSpanQuery(SpanQueryBuilder query, String field) {
        return new FieldMaskingSpanQueryBuilder(query, field);
    }

    /**
     * A query that wraps another query and simply returns a constant score equal to the
     * query boost for every document in the query.
     *
     * @param queryBuilder The query to wrap in a constant score query
     */
    public static ConstantScoreQueryBuilder constantScoreQuery(QueryBuilder queryBuilder) {
        return new ConstantScoreQueryBuilder(queryBuilder);
    }

    /**
     * A function_score query with no functions.
     *
     * @param queryBuilder The query to custom score
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(QueryBuilder queryBuilder) {
        return new FunctionScoreQueryBuilder(queryBuilder);
    }

    /**
     * A query that allows to define a custom scoring function
     *
     * @param queryBuilder The query to custom score
     * @param filterFunctionBuilders the filters and functions to execute
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(
        QueryBuilder queryBuilder,
        FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders
    ) {
        return new FunctionScoreQueryBuilder(queryBuilder, filterFunctionBuilders);
    }

    /**
     * A query that allows to define a custom scoring function
     *
     * @param filterFunctionBuilders the filters and functions to execute
     * @return the function score query
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(FunctionScoreQueryBuilder.FilterFunctionBuilder[] filterFunctionBuilders) {
        return new FunctionScoreQueryBuilder(filterFunctionBuilders);
    }

    /**
     * A query that allows to define a custom scoring function.
     *
     * @param function The function builder used to custom score
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(ScoreFunctionBuilder function) {
        return functionScoreQuery(function, null);
    }

    /**
     * A query that allows to define a custom scoring function.
     *
     * @param function The function builder used to custom score
     * @param queryName The query name
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(ScoreFunctionBuilder function, @Nullable String queryName) {
        return new FunctionScoreQueryBuilder(function, queryName);
    }

    /**
     * A query that allows to define a custom scoring function.
     *
     * @param queryBuilder The query to custom score
     * @param function     The function builder used to custom score
     */
    public static FunctionScoreQueryBuilder functionScoreQuery(QueryBuilder queryBuilder, ScoreFunctionBuilder function) {
        return (new FunctionScoreQueryBuilder(queryBuilder, function));
    }

    /**
     * A query that allows to define a custom scoring function through script.
     *
     * @param queryBuilder The query to custom score
     * @param script       The script used to score the query
     */
    public static ScriptScoreQueryBuilder scriptScoreQuery(QueryBuilder queryBuilder, Script script) {
        return new ScriptScoreQueryBuilder(queryBuilder, script);
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts or documents
     * which is checked against the fields the query is constructed with.
     *
     * @param fields the field names that will be used when generating the 'More Like This' query.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] fields, String[] likeTexts, Item[] likeItems) {
        return new MoreLikeThisQueryBuilder(fields, likeTexts, likeItems);
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts or documents
     * which is checked against the "_all" field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] likeTexts, Item[] likeItems) {
        return moreLikeThisQuery(null, likeTexts, likeItems);
    }

    /**
     * A more like this query that finds documents that are "like" the provided texts
     * which is checked against the "_all" field.
     * @param likeTexts the text to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(String[] likeTexts) {
        return moreLikeThisQuery(null, likeTexts, null);
    }

    /**
     * A more like this query that finds documents that are "like" the provided documents
     * which is checked against the "_all" field.
     * @param likeItems the documents to use when generating the 'More Like This' query.
     */
    public static MoreLikeThisQueryBuilder moreLikeThisQuery(Item[] likeItems) {
        return moreLikeThisQuery(null, null, likeItems);
    }

    public static NestedQueryBuilder nestedQuery(String path, QueryBuilder query, ScoreMode scoreMode) {
        return new NestedQueryBuilder(path, query, scoreMode);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, String... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, int... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, long... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, float... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, double... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, Object... values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A filter for a field based on several terms matching on any of them.
     *
     * @param name   The field name
     * @param values The terms
     */
    public static TermsQueryBuilder termsQuery(String name, Collection<?> values) {
        return new TermsQueryBuilder(name, values);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(String source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(BytesReference source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * A Query builder which allows building a query thanks to a JSON string or binary data.
     */
    public static WrapperQueryBuilder wrapperQuery(byte[] source) {
        return new WrapperQueryBuilder(source);
    }

    /**
     * A terms query that can extract the terms from another doc in an index.
     */
    public static TermsQueryBuilder termsLookupQuery(String name, TermsLookup termsLookup) {
        return new TermsQueryBuilder(name, termsLookup);
    }

    /**
     * A builder for filter based on a script.
     *
     * @param script The script to filter by.
     */
    public static ScriptQueryBuilder scriptQuery(Script script) {
        return new ScriptQueryBuilder(script);
    }

    /**
     * A filter to filter based on a specific distance from a specific geo location / point.
     *
     * @param name The location field name.
     */
    public static GeoDistanceQueryBuilder geoDistanceQuery(String name) {
        return new GeoDistanceQueryBuilder(name);
    }

    /**
     * A filter to filter based on a bounding box defined by top left and bottom right locations / points
     *
     * @param name The location field name.
     */
    public static GeoBoundingBoxQueryBuilder geoBoundingBoxQuery(String name) {
        return new GeoBoundingBoxQueryBuilder(name);
    }

    /**
     * A filter to filter based on a polygon defined by a set of locations  / points.
     *
     * @param name The location field name.
     */
    public static GeoPolygonQueryBuilder geoPolygonQuery(String name, List<GeoPoint> points) {
        return new GeoPolygonQueryBuilder(name, points);
    }

    /**
     * A filter based on the relationship of a shape and indexed shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoShapeQuery(String name, Geometry shape) throws IOException {
        return new GeoShapeQueryBuilder(name, shape);
    }

    /**
     * @deprecated use {@link #geoShapeQuery(String, Geometry)} instead
     */
    @Deprecated
    public static GeoShapeQueryBuilder geoShapeQuery(String name, ShapeBuilder shape) throws IOException {
        return new GeoShapeQueryBuilder(name, shape);
    }

    public static GeoShapeQueryBuilder geoShapeQuery(String name, String indexedShapeId) {
        return new GeoShapeQueryBuilder(name, indexedShapeId);
    }

    /**
     * A filter to filter indexed shapes intersecting with shapes
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, Geometry shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    /**
     * @deprecated use {@link #geoIntersectionQuery(String, Geometry)} instead
     */
    @Deprecated
    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    public static GeoShapeQueryBuilder geoIntersectionQuery(String name, String indexedShapeId) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId);
        builder.relation(ShapeRelation.INTERSECTS);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are contained by a shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoWithinQuery(String name, Geometry shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    /**
     * @deprecated use {@link #geoWithinQuery(String, Geometry)} instead
     */
    @Deprecated
    public static GeoShapeQueryBuilder geoWithinQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    public static GeoShapeQueryBuilder geoWithinQuery(String name, String indexedShapeId) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId);
        builder.relation(ShapeRelation.WITHIN);
        return builder;
    }

    /**
     * A filter to filter indexed shapes that are not intersection with the query shape
     *
     * @param name  The shape field name
     * @param shape Shape to use in the filter
     */
    public static GeoShapeQueryBuilder geoDisjointQuery(String name, Geometry shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    /**
     * @deprecated use {@link #geoDisjointQuery(String, Geometry)} instead
     */
    @Deprecated
    public static GeoShapeQueryBuilder geoDisjointQuery(String name, ShapeBuilder shape) throws IOException {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, shape);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    public static GeoShapeQueryBuilder geoDisjointQuery(String name, String indexedShapeId) {
        GeoShapeQueryBuilder builder = geoShapeQuery(name, indexedShapeId);
        builder.relation(ShapeRelation.DISJOINT);
        return builder;
    }

    /**
     * A filter to filter only documents where a field exists in them.
     *
     * @param name The name of the field
     */
    public static ExistsQueryBuilder existsQuery(String name) {
        return new ExistsQueryBuilder(name);
    }

    /**
     *  A query that contains a template with holder that should be resolved by search processors
     *
     * @param content The content of the template
     */
    public static TemplateQueryBuilder templateQuery(Map<String, Object> content) {
        return new TemplateQueryBuilder(content);
    }
}
