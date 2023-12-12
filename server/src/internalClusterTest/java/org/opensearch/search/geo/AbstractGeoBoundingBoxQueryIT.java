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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.GeoValidationMethod;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;

import static org.opensearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;

abstract class AbstractGeoBoundingBoxQueryIT extends OpenSearchIntegTestCase {

    public abstract XContentBuilder addGeoMapping(XContentBuilder parentMapping) throws IOException;

    public XContentBuilder getMapping() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject().startObject("properties");
        mapping = addGeoMapping(mapping);
        return mapping.endObject().endObject();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testSimpleBoundingBoxTest() throws Exception {
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

        SearchResponse searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }

        searchResponse = client().prepareSearch() // from NY
            .setQuery(geoBoundingBoxQuery("location").setCorners(40.73, -74.1, 40.717, -73.99).type("indexed"))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(2));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), anyOf(equalTo("1"), equalTo("3"), equalTo("5")));
        }
    }

    public void testLimit2BoundingBox() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = getMapping();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("userid", 880)
                    .field("title", "Place in Stockholm")
                    .field("location", "POINT(59.328355000000002 18.036842)")
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("userid", 534)
                    .field("title", "Place in Montreal")
                    .field("location", "POINT(-73.570986000000005 45.509526999999999)")
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(termQuery("userid", 880))
                    .filter(geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875))
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(termQuery("userid", 880))
                    .filter(
                        geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875)
                            .type("indexed")
                    )
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(termQuery("userid", 534))
                    .filter(geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875))
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(
                boolQuery().must(termQuery("userid", 534))
                    .filter(
                        geoBoundingBoxQuery("location").setCorners(74.579421999999994, 143.5, -66.668903999999998, 113.96875)
                            .type("indexed")
                    )
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testCompleteLonRange() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = getMapping();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("userid", 880)
                    .field("title", "Place in Stockholm")
                    .field("location", "POINT(18.036842 59.328355000000002)")
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("userid", 534)
                    .field("title", "Place in Montreal")
                    .field("location", "POINT(-73.570986000000005 45.509526999999999)")
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, -180, -50, 180))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(
                geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE)
                    .setCorners(50, -180, -50, 180)
                    .type("indexed")
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, -180, -90, 180))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        searchResponse = client().prepareSearch()
            .setQuery(
                geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE)
                    .setCorners(90, -180, -90, 180)
                    .type("indexed")
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));

        searchResponse = client().prepareSearch()
            .setQuery(geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(
                geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(50, 0, -50, 360).type("indexed")
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        searchResponse = client().prepareSearch()
            .setQuery(geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360))
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
        searchResponse = client().prepareSearch()
            .setQuery(
                geoBoundingBoxQuery("location").setValidationMethod(GeoValidationMethod.COERCE).setCorners(90, 0, -90, 360).type("indexed")
            )
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(2L));
    }
}
