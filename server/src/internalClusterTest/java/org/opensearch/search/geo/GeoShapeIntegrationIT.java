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

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.geo.builders.PointBuilder;
import org.opensearch.common.geo.builders.ShapeBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexService;
import org.opensearch.index.mapper.GeoShapeFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.indices.IndicesService;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeIntegrationIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public GeoShapeIntegrationIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            // Check that only geo-shape queries on legacy PrefixTree based
            // geo shapes are disallowed.
            .put("search.allow_expensive_queries", false)
            .put(super.nodeSettings(nodeOrdinal))
            .build();
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
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        GeoShapeFieldMapper.GeoShapeFieldType gsfm = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;
        ShapeBuilder.Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName + "2"));
        indexService = indicesService.indexService(resolveIndex((idxName + "2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        gsfm = (GeoShapeFieldMapper.GeoShapeFieldType) fieldType;
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
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("shape", "type=geo_shape,ignore_malformed=true").get());
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

    public void testMappingUpdate() throws Exception {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("shape", "type=geo_shape").get());
        ensureGreen();

        String update = "{\n"
            + "  \"properties\": {\n"
            + "    \"shape\": {\n"
            + "      \"type\": \"geo_shape\",\n"
            + "      \"strategy\": \"recursive\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping("test").setSource(update, MediaTypeRegistry.JSON).get()
        );
        assertThat(e.getMessage(), containsString("using [BKD] strategy cannot be merged with"));
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
            + "        \"type\": \"geo_shape\"\n"
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

    public void testIndexPolygonDateLine() throws Exception {
        String mappingVector = "{\n"
            + "    \"properties\": {\n"
            + "      \"shape\": {\n"
            + "        \"type\": \"geo_shape\"\n"
            + "      }\n"
            + "    }\n"
            + "  }";

        String mappingQuad = "{\n"
            + "    \"properties\": {\n"
            + "      \"shape\": {\n"
            + "        \"type\": \"geo_shape\",\n"
            + "        \"tree\": \"quadtree\"\n"
            + "      }\n"
            + "    }\n"
            + "  }";

        // create index
        assertAcked(client().admin().indices().prepareCreate("vector").setMapping(mappingVector).get());
        ensureGreen();

        assertAcked(client().admin().indices().prepareCreate("quad").setMapping(mappingQuad).get());
        ensureGreen();

        String source = "{\n" + "    \"shape\" : \"POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))\"" + "}";

        indexRandom(true, client().prepareIndex("quad").setId("0").setSource(source, MediaTypeRegistry.JSON));
        indexRandom(true, client().prepareIndex("vector").setId("0").setSource(source, MediaTypeRegistry.JSON));

        try {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            SearchResponse searchResponse = client().prepareSearch("quad")
                .setQuery(geoShapeQuery("shape", new PointBuilder(-179.75, 1)))
                .get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            searchResponse = client().prepareSearch("quad").setQuery(geoShapeQuery("shape", new PointBuilder(90, 1))).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

            searchResponse = client().prepareSearch("quad").setQuery(geoShapeQuery("shape", new PointBuilder(-180, 1))).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            searchResponse = client().prepareSearch("quad").setQuery(geoShapeQuery("shape", new PointBuilder(180, 1))).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }

        SearchResponse searchResponse = client().prepareSearch("vector").setQuery(geoShapeQuery("shape", new PointBuilder(90, 1))).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("vector").setQuery(geoShapeQuery("shape", new PointBuilder(-179.75, 1))).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("vector").setQuery(geoShapeQuery("shape", new PointBuilder(-180, 1))).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("vector").setQuery(geoShapeQuery("shape", new PointBuilder(180, 1))).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }
}
