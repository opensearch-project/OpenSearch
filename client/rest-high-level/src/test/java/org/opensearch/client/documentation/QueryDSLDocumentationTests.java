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

package org.opensearch.client.documentation;

import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.CoordinatesBuilder;
import org.opensearch.common.geo.builders.MultiPointBuilder;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.RankFeatureQueryBuilders;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.opensearch.join.query.JoinQueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.boostingQuery;
import static org.opensearch.index.query.QueryBuilders.commonTermsQuery;
import static org.opensearch.index.query.QueryBuilders.constantScoreQuery;
import static org.opensearch.index.query.QueryBuilders.disMaxQuery;
import static org.opensearch.index.query.QueryBuilders.existsQuery;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.fuzzyQuery;
import static org.opensearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.opensearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.opensearch.index.query.QueryBuilders.geoPolygonQuery;
import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.opensearch.index.query.QueryBuilders.idsQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.moreLikeThisQuery;
import static org.opensearch.index.query.QueryBuilders.multiMatchQuery;
import static org.opensearch.index.query.QueryBuilders.nestedQuery;
import static org.opensearch.index.query.QueryBuilders.prefixQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.regexpQuery;
import static org.opensearch.index.query.QueryBuilders.scriptQuery;
import static org.opensearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.opensearch.index.query.QueryBuilders.spanContainingQuery;
import static org.opensearch.index.query.QueryBuilders.spanFirstQuery;
import static org.opensearch.index.query.QueryBuilders.spanMultiTermQueryBuilder;
import static org.opensearch.index.query.QueryBuilders.spanNearQuery;
import static org.opensearch.index.query.QueryBuilders.spanNotQuery;
import static org.opensearch.index.query.QueryBuilders.spanOrQuery;
import static org.opensearch.index.query.QueryBuilders.spanTermQuery;
import static org.opensearch.index.query.QueryBuilders.spanWithinQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;
import static org.opensearch.index.query.QueryBuilders.wildcardQuery;
import static org.opensearch.index.query.QueryBuilders.wrapperQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.exponentialDecayFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;

/**
 * Examples of using the transport client that are imported into the transport client documentation.
 * There are no assertions here because we're mostly concerned with making sure that the examples
 * compile and don't throw weird runtime exceptions. Assertions and example data would be nice, but
 * that is secondary.
 */
public class QueryDSLDocumentationTests extends OpenSearchTestCase {
    public void testBool() {
        // tag::bool
        boolQuery()
                .must(termQuery("content", "test1"))                 // <1>
                .must(termQuery("content", "test4"))                 // <1>
                .mustNot(termQuery("content", "test2"))              // <2>
                .should(termQuery("content", "test3"))               // <3>
                .filter(termQuery("content", "test5"));              // <4>
        // end::bool
    }

    public void testBoosting() {
        // tag::boosting
        boostingQuery(
                    termQuery("name","foobar"),                      // <1>
                    termQuery("name","qux"))                    // <2>
                .negativeBoost(0.2f);                                // <3>
        // end::boosting
    }

    public void testCommonTerms() {
        // tag::common_terms
        commonTermsQuery("name",                                     // <1>
                         "foobar");                                  // <2>
        // end::common_terms
    }

    public void testConstantScore() {
        // tag::constant_score
        constantScoreQuery(
                termQuery("name","foobar"))                          // <1>
            .boost(2.0f);                                            // <2>
        // end::constant_score
    }

    public void testDisMax() {
        // tag::dis_max
        disMaxQuery()
                .add(termQuery("name", "foobar"))                    // <1>
                .add(termQuery("name", "opensearch"))             // <2>
                .boost(1.2f)                                         // <3>
                .tieBreaker(0.7f);                                   // <4>
        // end::dis_max
    }

    public void testExists() {
        // tag::exists
        existsQuery("name");                                         // <1>
        // end::exists
    }

    public void testFunctionScore() {
        // tag::function_score
        FilterFunctionBuilder[] functions = {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        matchQuery("name", "foobar"),                // <1>
                        randomFunction()),                           // <2>
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(
                        exponentialDecayFunction("age", 0L, 1L))     // <3>
        };
        functionScoreQuery(functions);
        // end::function_score
    }

    public void testFuzzy() {
        // tag::fuzzy
        fuzzyQuery(
                "name",                                              // <1>
                "foobar");                                           // <2>
        // end::fuzzy
    }

    public void testGeoBoundingBox() {
        // tag::geo_bounding_box
        geoBoundingBoxQuery("pin.location")                          // <1>
            .setCorners(40.73, -74.1,                                // <2>
                        40.717, -73.99);                             // <3>
        // end::geo_bounding_box
    }

    public void testGeoDistance() {
        // tag::geo_distance
        geoDistanceQuery("pin.location")                             // <1>
            .point(40, -70)                                          // <2>
            .distance(200, DistanceUnit.KILOMETERS);                 // <3>
        // end::geo_distance
    }

    public void testGeoPolygon() {
        // tag::geo_polygon
        List<GeoPoint> points = new ArrayList<>();           // <1>
        points.add(new GeoPoint(40, -70));
        points.add(new GeoPoint(30, -80));
        points.add(new GeoPoint(20, -90));
        geoPolygonQuery("pin.location", points);                     // <2>
        // end::geo_polygon
    }

    public void testGeoShape() throws IOException {
        {
            // tag::geo_shape
            GeoShapeQueryBuilder qb = geoShapeQuery(
                    "pin.location",                                      // <1>
                    new MultiPointBuilder(                         // <2>
                            new CoordinatesBuilder()
                        .coordinate(0, 0)
                        .coordinate(0, 10)
                        .coordinate(10, 10)
                        .coordinate(10, 0)
                        .coordinate(0, 0)
                        .build()));
            qb.relation(ShapeRelation.WITHIN);                           // <3>
            // end::geo_shape
        }

        {
            // tag::indexed_geo_shape
            // Using pre-indexed shapes
            GeoShapeQueryBuilder qb = geoShapeQuery(
                        "pin.location",                                  // <1>
                        "DEU");                                  // <2>
            qb.relation(ShapeRelation.WITHIN)                            // <3>
                .indexedShapeIndex("shapes")                             // <4>
                .indexedShapePath("location");                           // <5>
            // end::indexed_geo_shape
        }
    }

    public void testHasChild() {
        // tag::has_child
        JoinQueryBuilders.hasChildQuery(
                "blog_tag",                                          // <1>
                termQuery("tag","something"),                        // <2>
                ScoreMode.None);                                     // <3>
        // end::has_child
    }

    public void testHasParent() {
        // tag::has_parent
        JoinQueryBuilders.hasParentQuery(
            "blog",                                                  // <1>
            termQuery("tag","something"),                            // <2>
            false);                                                  // <3>
        // end::has_parent
    }

    public void testIds() {
        // tag::ids
        idsQuery()                                                   // <1>
                .addIds("1", "4", "100");
        // end::ids
    }

    public void testMatchAll() {
        // tag::match_all
        matchAllQuery();
        // end::match_all
    }

    public void testMatch() {
        // tag::match
        matchQuery(
                "name",                                              // <1>
                "foobar opensearch");                             // <2>
        // end::match
    }

    public void testMoreLikeThis() {
        // tag::more_like_this
        String[] fields = {"name.first", "name.last"};               // <1>
        String[] texts = {"text like this one"};                     // <2>

        moreLikeThisQuery(fields, texts, null)
            .minTermFreq(1)                                          // <3>
            .maxQueryTerms(12);                                      // <4>
        // end::more_like_this
    }

    public void testMultiMatch() {
        // tag::multi_match
        multiMatchQuery(
                "foobar opensearch",                              // <1>
                "user", "message");                                  // <2>
        // end::multi_match
    }

    public void testNested() {
        // tag::nested
        nestedQuery(
                "obj1",                                              // <1>
                boolQuery()                                          // <2>
                        .must(matchQuery("obj1.name", "blue"))
                        .must(rangeQuery("obj1.count").gt(5)),
                ScoreMode.Avg);                                      // <3>
        // end::nested
    }

    public void testPrefix() {
        // tag::prefix
        prefixQuery(
                "brand",                                             // <1>
                "heine");                                            // <2>
        // end::prefix
    }

    public void testQueryString() {
        // tag::query_string
        queryStringQuery("+foobar -opensearch");
        // end::query_string
    }

    public void testRange() {
        // tag::range
        rangeQuery("price")                                          // <1>
            .from(5)                                                 // <2>
            .to(10)                                                  // <3>
            .includeLower(true)                                      // <4>
            .includeUpper(false);                                    // <5>
        // end::range

        // tag::range_simplified
        // A simplified form using gte, gt, lt or lte
        rangeQuery("age")                                             // <1>
            .gte("10")                                                // <2>
            .lt("20");                                                // <3>
        // end::range_simplified
    }

    public void testRegExp() {
        // tag::regexp
        regexpQuery(
                "name.first",                                        // <1>
                "s.*y");                                             // <2>
        // end::regexp
    }

    public void testScript() {
        // tag::script_inline
        scriptQuery(
                new Script("doc['num1'].value > 1")                  // <1>
            );
        // end::script_inline

        // tag::script_file
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("param1", 5);
        scriptQuery(new Script(
                ScriptType.STORED,                                   // <1>
                null,                                          // <2>
                "myscript",                                          // <3>
                singletonMap("param1", 5)));                         // <4>
        // end::script_file
    }

    public void testSimpleQueryString() {
        // tag::simple_query_string
        simpleQueryStringQuery("+foobar -opensearch");
        // end::simple_query_string
    }

    public void testSpanContaining() {
        // tag::span_containing
        spanContainingQuery(
                spanNearQuery(spanTermQuery("field1","bar"), 5)      // <1>
                    .addClause(spanTermQuery("field1","baz"))
                    .inOrder(true),
                spanTermQuery("field1","foo"));                      // <2>
        // end::span_containing
    }

    public void testSpanFirst() {
        // tag::span_first
        spanFirstQuery(
                spanTermQuery("user", "foobar"),                     // <1>
                3                                                    // <2>
            );
        // end::span_first
    }

    public void testSpanMultiTerm() {
        // tag::span_multi
        spanMultiTermQueryBuilder(
                prefixQuery("user", "ki"));                          // <1>
        // end::span_multi
    }

    public void testSpanNear() {
        // tag::span_near
        spanNearQuery(
                spanTermQuery("field","value1"),                     // <1>
                12)                                                  // <2>
                    .addClause(spanTermQuery("field","value2"))      // <1>
                    .addClause(spanTermQuery("field","value3"))      // <1>
                    .inOrder(false);                                 // <3>
        // end::span_near
    }

    public void testSpanNot() {
        // tag::span_not
        spanNotQuery(
                spanTermQuery("field","value1"),                     // <1>
                spanTermQuery("field","value2"));                    // <2>
        // end::span_not
    }

    public void testSpanOr() {
        // tag::span_or
        spanOrQuery(spanTermQuery("field","value1"))                 // <1>
            .addClause(spanTermQuery("field","value2"))              // <1>
            .addClause(spanTermQuery("field","value3"));             // <1>
        // end::span_or
    }

    public void testSpanTerm() {
        // tag::span_term
        spanTermQuery(
                "user",       // <1>
                "foobar");    // <2>
        // end::span_term
    }

    public void testSpanWithin() {
        // tag::span_within
        spanWithinQuery(
                spanNearQuery(spanTermQuery("field1", "bar"), 5)     // <1>
                    .addClause(spanTermQuery("field1", "baz"))
                    .inOrder(true),
                spanTermQuery("field1", "foo"));                     // <2>
        // end::span_within
    }

    public void testTerm() {
        // tag::term
        termQuery(
                "name",                                              // <1>
                "foobar");                                           // <2>
        // end::term
    }

    public void testTerms() {
        // tag::terms
        termsQuery("tags",                                           // <1>
                "blue", "pill");                                     // <2>
        // end::terms
    }

    public void testWildcard() {
        // tag::wildcard
        wildcardQuery(
                "user",                                              // <1>
                "k?mch*");                                           // <2>
        // end::wildcard
    }

    public void testWrapper() {
        // tag::wrapper
        String query = "{\"term\": {\"user\": \"foobar\"}}"; // <1>
        wrapperQuery(query);
        // end::wrapper
    }

    public void testRankFeatureSaturation() {
        RankFeatureQueryBuilders.saturation("pagerank");
    }

    public void testRankFeatureSaturationPivot() {
        RankFeatureQueryBuilders.saturation("pagerank", 8);
    }

    public void testRankFeatureLog() {
        RankFeatureQueryBuilders.log("pagerank", 4f);
    }

    public void testRankFeatureSigmoid() {
        RankFeatureQueryBuilders.sigmoid("pagerank", 7, 0.6f);
    }

    public void testRankFeatureLinear() {
        RankFeatureQueryBuilders.linear("pagerank");
    }
}
