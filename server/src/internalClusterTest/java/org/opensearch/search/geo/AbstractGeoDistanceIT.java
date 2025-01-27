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

import org.opensearch.Version;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.bucket.range.InternalGeoDistance;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/** base class for testing geo_distance queries on geo_ field types */
abstract class AbstractGeoDistanceIT extends OpenSearchIntegTestCase {

    private static final double src_lat = 32.798;
    private static final double src_lon = -117.151;
    private static final double tgt_lat = 32.81;
    private static final double tgt_lon = -117.21;
    private static final String tgt_geohash = Geohash.stringEncode(tgt_lon, tgt_lat);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("arcDistance", vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat, tgt_lon)));
            scripts.put(
                "arcDistanceGeoUtils",
                vars -> distanceScript(vars, location -> GeoUtils.arcDistance(location.getLat(), location.getLon(), tgt_lat, tgt_lon))
            );
            scripts.put("planeDistance", vars -> distanceScript(vars, location -> location.planeDistance(tgt_lat, tgt_lon)));
            scripts.put("geohashDistance", vars -> distanceScript(vars, location -> location.geohashDistance(tgt_geohash)));
            scripts.put(
                "arcDistance(lat, lon + 360)/1000d",
                vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat, tgt_lon + 360) / 1000d)
            );
            scripts.put(
                "arcDistance(lat + 360, lon)/1000d",
                vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat + 360, tgt_lon) / 1000d)
            );

            return scripts;
        }

        static Double distanceScript(Map<String, Object> vars, Function<ScriptDocValues.GeoPoints, Double> distance) {
            Map<?, ?> doc = (Map) vars.get("doc");
            return distance.apply((ScriptDocValues.GeoPoints) doc.get("location"));
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    protected void indexSetup() throws IOException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = getMapping();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("name", "New York").field("location", "POINT(-74.0059731 40.7143528)").endObject())
            .get();

        // to NY: 5.286 km
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject().field("name", "Times Square").field("location", "POINT(-73.9844722 40.759011)").endObject()
            )
            .get();

        // to NY: 0.4621 km
        client().prepareIndex("test")
            .setId("3")
            .setSource(jsonBuilder().startObject().field("name", "Tribeca").field("location", "POINT(-74.007819 40.718266)").endObject())
            .get();

        // to NY: 1.055 km
        client().prepareIndex("test")
            .setId("4")
            .setSource(
                jsonBuilder().startObject().field("name", "Wall Street").field("location", "POINT(-74.0088305 40.7051157)").endObject()
            )
            .get();

        // to NY: 1.258 km
        client().prepareIndex("test")
            .setId("5")
            .setSource(jsonBuilder().startObject().field("name", "Soho").field("location", "POINT(-74 40.7247222)").endObject())
            .get();

        // to NY: 2.029 km
        client().prepareIndex("test")
            .setId("6")
            .setSource(
                jsonBuilder().startObject().field("name", "Greenwich Village").field("location", "POINT(-73.9962255 40.731033)").endObject()
            )
            .get();

        // to NY: 8.572 km
        client().prepareIndex("test")
            .setId("7")
            .setSource(jsonBuilder().startObject().field("name", "Brooklyn").field("location", "POINT(-73.95 40.65)").endObject())
            .get();

        client().admin().indices().prepareRefresh().get();
    }

    public abstract XContentBuilder addGeoMapping(XContentBuilder parentMapping) throws IOException;

    public XContentBuilder getMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping = addGeoMapping(mapping);
        return mapping.endObject().endObject();
    }

    public void testSimpleDistanceQuery() {
        SearchResponse searchResponse = client().prepareSearch() // from NY
            .setQuery(QueryBuilders.geoDistanceQuery("location").point(40.5, -73.9).distance(25, DistanceUnit.KILOMETERS))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("7"), equalTo("4")));
        }
    }

    public void testDistanceScript() throws Exception {
        client().prepareIndex("test")
            .setId("8")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "TestPosition")
                    .field("location", "POINT(" + src_lon + " " + src_lat + ")")
                    .endObject()
            )
            .get();

        refresh();

        // Test doc['location'].arcDistance(lat, lon)
        SearchResponse searchResponse1 = client().prepareSearch()
            .setQuery(new IdsQueryBuilder().addIds("8"))
            .addStoredField("_source")
            .addScriptField("distance", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance", Collections.emptyMap()))
            .get();
        Double resultDistance1 = searchResponse1.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance1, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon), 0.01d));

        // Test doc['location'].planeDistance(lat, lon)
        SearchResponse searchResponse2 = client().prepareSearch()
            .setQuery(new IdsQueryBuilder().addIds("8"))
            .addStoredField("_source")
            .addScriptField("distance", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "planeDistance", Collections.emptyMap()))
            .get();
        Double resultDistance2 = searchResponse2.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance2, closeTo(GeoUtils.planeDistance(src_lat, src_lon, tgt_lat, tgt_lon), 0.01d));

        // Test doc['location'].geohashDistance(lat, lon)
        SearchResponse searchResponse4 = client().prepareSearch()
            .setQuery(new IdsQueryBuilder().addIds("8"))
            .addStoredField("_source")
            .addScriptField("distance", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "geohashDistance", Collections.emptyMap()))
            .get();
        Double resultDistance4 = searchResponse4.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(
            resultDistance4,
            closeTo(
                GeoUtils.arcDistance(src_lat, src_lon, Geohash.decodeLatitude(tgt_geohash), Geohash.decodeLongitude(tgt_geohash)),
                0.01d
            )
        );

        // Test doc['location'].arcDistance(lat, lon + 360)/1000d
        SearchResponse searchResponse5 = client().prepareSearch()
            .setQuery(new IdsQueryBuilder().addIds("8"))
            .addStoredField("_source")
            .addScriptField(
                "distance",
                new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance(lat, lon + 360)/1000d", Collections.emptyMap())
            )
            .get();
        Double resultArcDistance5 = searchResponse5.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance5, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon) / 1000d, 0.01d));

        // Test doc['location'].arcDistance(lat + 360, lon)/1000d
        SearchResponse searchResponse6 = client().prepareSearch()
            .setQuery(new IdsQueryBuilder().addIds("8"))
            .addStoredField("_source")
            .addScriptField(
                "distance",
                new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance(lat + 360, lon)/1000d", Collections.emptyMap())
            )
            .get();
        Double resultArcDistance6 = searchResponse6.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance6, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon) / 1000d, 0.01d));
    }

    public void testGeoDistanceAggregation() throws IOException {
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "TestPosition")
                    .field("location", "POINT(" + src_lon + " " + src_lat + ")")
                    .endObject()
            )
            .get();

        refresh();

        SearchRequestBuilder search = client().prepareSearch("test");
        String name = "TestPosition";

        search.setQuery(QueryBuilders.matchAllQuery())
            .addAggregation(
                AggregationBuilders.geoDistance(name, new GeoPoint(tgt_lat, tgt_lon))
                    .field("location")
                    .unit(DistanceUnit.MILES)
                    .addRange(0, 25000) // limits the distance (expected to omit one point outside this range)
            );

        search.setSize(0); // no hits please

        SearchResponse response = search.get();
        Aggregations aggregations = response.getAggregations();
        assertNotNull(aggregations);
        InternalGeoDistance geoDistance = aggregations.get(name);
        assertNotNull(geoDistance);

        List<? extends Range.Bucket> buckets = ((Range) geoDistance).getBuckets();
        assertNotNull("Buckets should not be null", buckets);
        assertEquals("Unexpected number of buckets", 1, buckets.size());
        assertEquals("Unexpected doc count for geo distance", 7, buckets.get(0).getDocCount());
    }
}
