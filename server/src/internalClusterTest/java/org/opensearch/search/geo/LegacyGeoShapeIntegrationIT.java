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

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.geometry.Circle;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LegacyGeoShapeIntegrationIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public LegacyGeoShapeIntegrationIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    /**
     * Test that orientation parameter correctly persists across cluster restart
     */
    public void testOrientationPersistence() throws Exception {
        String idxName = "orientation";
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("orientation", "left")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        // create index
        assertAcked(prepareCreate(idxName).setMapping(mapping));

        mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_shape")
            .field("tree", "quadtree")
            .field("orientation", "right")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(prepareCreate(idxName + "2").setMapping(mapping));
        ensureGreen(idxName, idxName + "2");

        internalCluster().fullRestart();
        ensureGreen(idxName, idxName + "2");

        // left orientation test
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
        IndexService indexService = indicesService.indexService(resolveIndex(idxName));
        MappedFieldType fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(LegacyGeoShapeFieldMapper.GeoShapeFieldType.class));

        LegacyGeoShapeFieldMapper.GeoShapeFieldType gsfm = (LegacyGeoShapeFieldMapper.GeoShapeFieldType) fieldType;
        ShapeBuilder.Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName + "2"));
        indexService = indicesService.indexService(resolveIndex((idxName + "2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(LegacyGeoShapeFieldMapper.GeoShapeFieldType.class));

        gsfm = (LegacyGeoShapeFieldMapper.GeoShapeFieldType) fieldType;
        orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() throws Exception {
        // create index
        assertAcked(
            client().admin().indices().prepareCreate("test").setMapping("shape", "type=geo_shape,tree=quadtree,ignore_malformed=true").get()
        );
        ensureGreen();

        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = XContentFactory.jsonBuilder()
            .startObject()
            .field("type", "Polygon")
            .startArray("coordinates")
            .startArray()
            .startArray()
            .value(176.0)
            .value(15.0)
            .endArray()
            .startArray()
            .value(-177.0)
            .value(10.0)
            .endArray()
            .startArray()
            .value(-177.0)
            .value(-10.0)
            .endArray()
            .startArray()
            .value(176.0)
            .value(-15.0)
            .endArray()
            .startArray()
            .value(-177.0)
            .value(15.0)
            .endArray()
            .startArray()
            .value(172.0)
            .value(0.0)
            .endArray()
            .startArray()
            .value(176.0)
            .value(15.0)
            .endArray()
            .endArray()
            .endArray()
            .endObject()
            .toString();

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", polygonGeoJson));
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() throws Exception {
        String mapping = "{\n"
            + "    \"_routing\": {\n"
            + "      \"required\": true\n"
            + "    },\n"
            + "    \"properties\": {\n"
            + "      \"shape\": {\n"
            + "        \"type\": \"geo_shape\",\n"
            + "        \"tree\" : \"quadtree\"\n"
            + "      }\n"
            + "    }\n"
            + "  }";

        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping).get());
        ensureGreen();

        String source = "{\n"
            + "    \"shape\" : {\n"
            + "        \"type\" : \"bbox\",\n"
            + "        \"coordinates\" : [[-45.0, 45.0], [45.0, -45.0]]\n"
            + "    }\n"
            + "}";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, MediaTypeRegistry.JSON).setRouting("ABC"));

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoShapeQuery("shape", "0").indexedShapeIndex("test").indexedShapeRouting("ABC"))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the circle is still supported for the legacy shapes
     */
    public void testLegacyCircle() throws Exception {
        // create index
        assertAcked(
            client().admin().indices().prepareCreate("test").setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash").get()
        );
        ensureGreen();

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
            builder.startObject()
                .field("type", "circle")
                .startArray("coordinates")
                .value(30)
                .value(50)
                .endArray()
                .field("radius", "77km")
                .endObject();
            return builder;
        }));

        // test self crossing of circles
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testDisallowExpensiveQueries() throws InterruptedException, IOException {
        try {
            // create index
            assertAcked(
                client().admin().indices().prepareCreate("test").setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash").get()
            );
            ensureGreen();

            indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
                builder.startObject()
                    .field("type", "circle")
                    .startArray("coordinates")
                    .value(30)
                    .value(50)
                    .endArray()
                    .field("radius", "77km")
                    .endObject();
                return builder;
            }));
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000)))
                .get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            // Set search.allow_expensive_queries to "false" => assert failure
            OpenSearchException e = expectThrows(
                OpenSearchException.class,
                () -> client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000))).get()
            );
            assertEquals(
                "[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when "
                    + "'search.allow_expensive_queries' is set to false.",
                e.getCause().getMessage()
            );

            // Set search.allow_expensive_queries to "true" => success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape", new Circle(30, 50, 77000))).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }
}
