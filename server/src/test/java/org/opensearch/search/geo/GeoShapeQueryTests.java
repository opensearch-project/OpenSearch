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

package org.opensearch.search.geo;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.tests.geo.GeoTestUtil;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.SpatialStrategy;
import org.opensearch.common.geo.builders.CircleBuilder;
import org.opensearch.common.geo.builders.CoordinatesBuilder;
import org.opensearch.common.geo.builders.EnvelopeBuilder;
import org.opensearch.common.geo.builders.GeometryCollectionBuilder;
import org.opensearch.common.geo.builders.LineStringBuilder;
import org.opensearch.common.geo.builders.MultiPointBuilder;
import org.opensearch.common.geo.builders.PointBuilder;
import org.opensearch.common.geo.builders.PolygonBuilder;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.index.query.ExistsQueryBuilder;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.geo.RandomShapeGenerator;

import java.io.IOException;
import java.util.Locale;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Rectangle;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.test.geo.RandomShapeGenerator.createGeometryCollectionWithin;
import static org.opensearch.test.geo.RandomShapeGenerator.xRandomPoint;
import static org.opensearch.test.geo.RandomShapeGenerator.xRandomRectangle;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static com.carrotsearch.randomizedtesting.RandomizedTest.assumeTrue;

public class GeoShapeQueryTests extends GeoQueryTests {
    protected static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE };

    @Override
    protected XContentBuilder createTypedMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
                .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject().endObject();

        return xcb;
    }

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        return xcb;
    }

    protected XContentBuilder createPrefixTreeMapping(String tree) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape")
            .field("tree", tree)
            .endObject()
            .endObject()
            .endObject();
        return xcb;
    }

    protected XContentBuilder createRandomMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
                .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject();

        return xcb;
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape").get();

        String location = "\"geo\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex("shapes")
            .setId("1")
            .setSource(
                String.format(Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location),
                MediaTypeRegistry.JSON
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject("geo")
                    .field("type", "polygon")
                    .startArray("coordinates")
                    .startArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .endArray()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery("geo", "1").indexedShapeIndex("shapes").indexedShapePath("geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1").indexedShapeIndex("shapes").indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1").indexedShapeIndex("shapes").indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1").indexedShapeIndex("shapes").indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();

        assumeTrue(
            "Skipping the check for the polygon with a degenerated dimension",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 && randomPoly.maxLon - randomPoly.minLon > 8.4e-8
        );

        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        XContentBuilder mapping = createRandomMapping();
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        client().admin().indices().prepareCreate("test").setMapping(mapping).setSettings(settings).get();
        ensureGreen();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollectionBuilder queryCollection = RandomShapeGenerator.createGeometryCollection(random());
        queryCollection.shape(new PolygonBuilder(cb));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", queryCollection);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertThat(result.getHits().getHits().length, greaterThan(0));
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        String doc1 = "{\"geo\": {\r\n"
            + "\"coordinates\": [\r\n"
            + "-33.918711,\r\n"
            + "18.847685\r\n"
            + "],\r\n"
            + "\"type\": \"Point\"\r\n"
            + "}}";
        client().index(new IndexRequest("test").id("1").source(doc1, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = "{\"geo\": {\r\n"
            + "\"coordinates\": [\r\n"
            + "-49.0,\r\n"
            + "18.847685\r\n"
            + "],\r\n"
            + "\"type\": \"Point\"\r\n"
            + "}}";
        client().index(new IndexRequest("test").id("2").source(doc2, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = "{\"geo\": {\r\n"
            + "\"coordinates\": [\r\n"
            + "49.0,\r\n"
            + "18.847685\r\n"
            + "],\r\n"
            + "\"type\": \"Point\"\r\n"
            + "}}";
        client().index(new IndexRequest("test").id("3").source(doc3, MediaTypeRegistry.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked")
        CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> QueryBuilders.geoShapeQuery("geo", new EnvelopeBuilder(new Coordinate(-21, 44), new Coordinate(-39, 9)))
                .relation(ShapeRelation.WITHIN),
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("geo")
                    .startObject("shape")
                    .field("type", "envelope")
                    .startArray("coordinates")
                    .startArray()
                    .value(-21)
                    .value(44)
                    .endArray()
                    .startArray()
                    .value(-39)
                    .value(9)
                    .endArray()
                    .endArray()
                    .endObject()
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)) {
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            },
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("geo")
                    .field("shape", "BBOX (-21, -39, 44, 9)")
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)) {
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            }
        );

        SearchResponse response = client().prepareSearch("test").setQuery(querySupplier.get()).get();
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), not(equalTo("1")));
        assertThat(response.getHits().getAt(1).getId(), not(equalTo("1")));
    }

    public void testGeometryCollectionRelations() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).build(), "doc", mapping);

        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(-10, 10), new Coordinate(10, -10));

        client().index(
            new IndexRequest("test").source(jsonBuilder().startObject().field("geo", envelopeBuilder).endObject())
                .setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(1, 2));
            builder.shape(new PointBuilder(-2, -1));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 0);
        }
        // A geometry collection that is partially within the indexed shape
        {
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(1, 2));
            builder.shape(new PointBuilder(20, 30));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 0);
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            GeometryCollectionBuilder builder = new GeometryCollectionBuilder();
            builder.shape(new PointBuilder(-20, -30));
            builder.shape(new PointBuilder(20, 30));
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", builder.buildGeometry()).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 1);
        }
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        String mapping = xcb.toString();
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex("test")
            .setId("blakely")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Blakely Island")
                    .startObject("geo")
                    .field("type", "polygon")
                    .startArray("coordinates")
                    .startArray()
                    .startArray()
                    .value(-122.83)
                    .value(48.57)
                    .endArray()
                    .startArray()
                    .value(-122.77)
                    .value(48.56)
                    .endArray()
                    .startArray()
                    .value(-122.79)
                    .value(48.53)
                    .endArray()
                    .startArray()
                    .value(-122.83)
                    .value(48.57)
                    .endArray() // close the polygon
                    .endArray()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        EnvelopeBuilder query = new EnvelopeBuilder(new Coordinate(-122.88, 48.62), new Coordinate(-122.82, 48.54));

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoIntersectionQuery("geo", query)).get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes")
            .setId("Big_Rectangle")
            .setSource(jsonBuilder().startObject().field("shape", shape).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        try {
            client().prepareSearch("test").setQuery(geoIntersectionQuery("geo", "Big_Rectangle")).get();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("source disabled"));
        }
    }

    public void testReusableBuilder() throws IOException {
        PolygonBuilder polygon = new PolygonBuilder(
            new CoordinatesBuilder().coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close()
        ).hole(
            new LineStringBuilder(
                new CoordinatesBuilder().coordinate(175, -5).coordinate(185, -5).coordinate(185, 5).coordinate(175, 5).close()
            )
        );
        assertUnmodified(polygon);

        LineStringBuilder linestring = new LineStringBuilder(
            new CoordinatesBuilder().coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close()
        );
        assertUnmodified(linestring);
    }

    private void assertUnmodified(ShapeBuilder builder) throws IOException {
        String before = jsonBuilder().startObject().field("area", builder).endObject().toString();
        builder.buildS4J();
        String after = jsonBuilder().startObject().field("area", builder).endObject().toString();
        assertThat(before, equalTo(after));
    }

    /** tests querying a random geometry collection with a point */
    public void testPointQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        double[] pt = new double[] { GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude() };
        PointBuilder pb = new PointBuilder(pt[0], pt[1]);
        gcb.shape(pb);
        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape").execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape,tree=quadtree").execute().actionGet();
        }
        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", pb);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        // Create a random geometry collection.
        Rectangle mbr = xRandomRectangle(random(), xRandomPoint(random()), true);
        boolean usePrefixTrees = randomBoolean();
        GeometryCollectionBuilder gcb;
        if (usePrefixTrees) {
            gcb = createGeometryCollectionWithin(random(), mbr);
        } else {
            // vector strategy does not yet support multipoint queries
            gcb = new GeometryCollectionBuilder();
            int numShapes = RandomNumbers.randomIntBetween(random(), 1, 4);
            for (int i = 0; i < numShapes; ++i) {
                ShapeBuilder shape;
                do {
                    shape = RandomShapeGenerator.createShapeWithin(random(), mbr);
                } while (shape instanceof MultiPointBuilder);
                gcb.shape(shape);
            }
        }

        if (usePrefixTrees) {
            client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape,tree=quadtree").execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape").execute().actionGet();
        }

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        EnvelopeBuilder env = new EnvelopeBuilder(
            new Coordinate(mbr.getMinX(), mbr.getMaxY()),
            new Coordinate(mbr.getMaxX(), mbr.getMinY())
        );
        docSource = env.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("2").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", filterShape).relation(ShapeRelation.CONTAINS);

        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(response);
        assertThat(response.getHits().getHits().length, greaterThan(0));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());

        XContentBuilder builder = createRandomMapping();
        client().admin().indices().prepareCreate("test").setMapping(builder).execute().actionGet();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = QueryBuilders.existsQuery("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testPointsOnly() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        ShapeBuilder shape = RandomShapeGenerator.createShape(random());
        try {
            client().prepareIndex("geo_points_only")
                .setId("1")
                .setSource(jsonBuilder().startObject().field("location", shape).endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();
        } catch (MapperParsingException e) {
            // RandomShapeGenerator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getCause().getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setQuery(geoIntersectionQuery("location", shape)).get();

        assertHitCount(response, 1);
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geo")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        client().admin().indices().prepareCreate("geo_points_only").setMapping(mapping).get();
        ensureGreen();

        // MULTIPOINT
        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("geo_points_only")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("geo", shape).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // POINT
        shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POINT);
        client().prepareIndex("geo_points_only")
            .setId("2")
            .setSource(jsonBuilder().startObject().field("geo", shape).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setQuery(matchAllQuery()).get();

        assertHitCount(response, 2);
    }

    public void testIndexedShapeReference() throws Exception {
        String mapping = createDefaultMapping().toString();
        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        createIndex("shapes");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes")
            .setId("Big_Rectangle")
            .setSource(jsonBuilder().startObject().field("shape", shape).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Document 1")
                    .startObject("geo")
                    .field("type", "point")
                    .startArray("coordinates")
                    .value(-30)
                    .value(-30)
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoIntersectionQuery("geo", "Big_Rectangle")).get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("geo", "Big_Rectangle")).get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testFieldAlias() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .endObject()
            .startObject("alias")
            .field("type", "alias")
            .field("path", "location")
            .endObject()
            .endObject()
            .endObject();

        createIndex("test", Settings.EMPTY, "type", mapping);

        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("location", shape).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch("test").setQuery(geoShapeQuery("alias", shape)).get();
        assertHitCount(response, 1);
    }

    public void testQueryRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();
        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        XContentBuilder builder = createRandomMapping();
        client().admin().indices().prepareCreate("test").setMapping(builder).get();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(gcb.numShapes() - 1));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assumeTrue(
            "Skipping the check for the polygon with a degenerated dimension until "
                + " https://issues.apache.org/jira/browse/LUCENE-8634 is fixed",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 && randomPoly.maxLon - randomPoly.minLon > 8.4e-8
        );
        assertHitCount(result, 1);
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").setMapping("geo", "type=geo_shape,tree=quadtree").get();

        XContentBuilder docSource = jsonBuilder().startObject()
            .startObject("geo")
            .field("type", "geometrycollection")
            .startArray("geometries")
            .startObject()
            .field("type", "point")
            .startArray("coordinates")
            .value(100.0)
            .value(0.0)
            .endArray()
            .endObject()
            .startObject()
            .field("type", "linestring")
            .startArray("coordinates")
            .startArray()
            .value(101.0)
            .value(0.0)
            .endArray()
            .startArray()
            .value(102.0)
            .value(1.0)
            .endArray()
            .endArray()
            .endObject()
            .endArray()
            .endObject()
            .endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder().polygon(
                new PolygonBuilder(
                    new CoordinatesBuilder().coordinate(99.0, -1.0)
                        .coordinate(99.0, 3.0)
                        .coordinate(103.0, 3.0)
                        .coordinate(103.0, -1.0)
                        .coordinate(99.0, -1.0)
                )
            )
        ).relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder().polygon(
                new PolygonBuilder(
                    new CoordinatesBuilder().coordinate(199.0, -11.0)
                        .coordinate(199.0, 13.0)
                        .coordinate(193.0, 13.0)
                        .coordinate(193.0, -11.0)
                        .coordinate(199.0, -11.0)
                )
            )
        ).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery(
            "geo",
            new GeometryCollectionBuilder().polygon(
                new PolygonBuilder(
                    new CoordinatesBuilder().coordinate(99.0, -1.0)
                        .coordinate(99.0, 3.0)
                        .coordinate(103.0, 3.0)
                        .coordinate(103.0, -1.0)
                        .coordinate(99.0, -1.0)
                )
            )
                .polygon(
                    new PolygonBuilder(
                        new CoordinatesBuilder().coordinate(199.0, -11.0)
                            .coordinate(199.0, 13.0)
                            .coordinate(193.0, 13.0)
                            .coordinate(193.0, -11.0)
                            .coordinate(199.0, -11.0)
                    )
                )
        ).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        // no shape
        filter = QueryBuilders.geoShapeQuery("geo", new GeometryCollectionBuilder());
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
    }

    public void testDistanceQuery() throws Exception {
        String mapping = createRandomMapping().toString();
        client().admin().indices().prepareCreate("test_distance").setMapping(mapping).get();
        ensureGreen();

        CircleBuilder circleBuilder = new CircleBuilder().center(new Coordinate(1, 0)).radius(350, DistanceUnit.KILOMETERS);

        client().index(
            new IndexRequest("test_distance").source(jsonBuilder().startObject().field("geo", new PointBuilder(2, 2)).endObject())
                .setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(jsonBuilder().startObject().field("geo", new PointBuilder(3, 1)).endObject())
                .setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(jsonBuilder().startObject().field("geo", new PointBuilder(-20, -30)).endObject())
                .setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest("test_distance").source(jsonBuilder().startObject().field("geo", new PointBuilder(20, 30)).endObject())
                .setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        SearchResponse response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circleBuilder.buildGeometry()).relation(ShapeRelation.WITHIN))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value());
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circleBuilder.buildGeometry()).relation(ShapeRelation.INTERSECTS))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value());
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circleBuilder.buildGeometry()).relation(ShapeRelation.DISJOINT))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value());
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circleBuilder.buildGeometry()).relation(ShapeRelation.CONTAINS))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value());
    }

    public void testIndexRectangleSpanningDateLine() throws Exception {
        String mapping = createRandomMapping().toString();

        client().admin().indices().prepareCreate("test").setMapping(mapping).get();
        ensureGreen();

        EnvelopeBuilder envelopeBuilder = new EnvelopeBuilder(new Coordinate(178, 10), new Coordinate(-178, -10));

        XContentBuilder docSource = envelopeBuilder.toXContent(jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = new PointBuilder(179, 0);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }
}
