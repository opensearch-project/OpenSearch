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

package org.opensearch.search.sort;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.geo.GeoDistance;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.ParameterizedOpenSearchIntegTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFirstHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertOrderedSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchHits;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.hasId;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class GeoDistanceIT extends ParameterizedOpenSearchIntegTestCase {

    public GeoDistanceIT(Settings dynamicSettings) {
        super(dynamicSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    public void testDistanceSortingMVFields() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.field("ignore_malformed", true).endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("names", "New York")
                    .startObject("locations")
                    .field("lat", 40.7143528)
                    .field("lon", -74.0059731)
                    .endObject()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .field("names", "New York 2")
                    .startObject("locations")
                    .field("lat", 400.7143528)
                    .field("lon", 285.9990269)
                    .endObject()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("4")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Wall Street", "Soho")
                    .startArray("locations")
                    // to NY: 1.055 km
                    .startObject()
                    .field("lat", 40.7051157)
                    .field("lon", -74.0088305)
                    .endObject()
                    // to NY: 1.258 km
                    .startObject()
                    .field("lat", 40.7247222)
                    .field("lon", -74)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("5")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Greenwich Village", "Brooklyn")
                    .startArray("locations")
                    // to NY: 2.029 km
                    .startObject()
                    .field("lat", 40.731033)
                    .field("lon", -73.9962255)
                    .endObject()
                    // to NY: 8.572 km
                    .startObject()
                    .field("lat", 40.65)
                    .field("lon", -73.95)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().admin().indices().prepareRefresh().get();
        indexRandomForConcurrentSearch("test");

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC).sortMode(SortMode.MAX))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC).sortMode(SortMode.MIN))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "1", "2", "4", "3", "5");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(2874d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(5301d, 10d));

        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.AVG).order(SortOrder.DESC))
            .get();

        assertHitCount(searchResponse, 5);
        assertOrderedSearchHits(searchResponse, "5", "3", "4", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(421.2d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(4).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        try {
            client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).sortMode(SortMode.SUM));
            fail("sum should not be supported for sorting by geo distance");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    public void testDistanceSortingWithMissingGeoPoint() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().prepareIndex("test")
            .setId("2")
            .setSource(jsonBuilder().startObject().array("names", "Wall Street", "Soho").endObject())
            .get();

        refresh();
        indexRandomForConcurrentSearch("test");

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "1", "2");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));

        // Order: Desc
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC))
            .get();

        // Doc with missing geo point is first, is consistent with 0.20.x
        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286d, 10d));
    }

    public void testDistanceSortingNestedFields() throws Exception {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .startObject("branches")
            .field("type", "nested")
            .startObject("properties")
            .startObject("name")
            .field("type", "text")
            .endObject()
            .startObject("location")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject().endObject();

        assertAcked(prepareCreate("companies").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();

        indexRandom(
            true,
            client().prepareIndex("companies")
                .setId("1")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 1")
                        .startArray("branches")
                        .startObject()
                        .field("name", "New York")
                        .startObject("location")
                        .field("lat", 40.7143528)
                        .field("lon", -74.0059731)
                        .endObject()
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            client().prepareIndex("companies")
                .setId("2")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 2")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Times Square")
                        .startObject("location")
                        .field("lat", 40.759011)
                        .field("lon", -73.9844722)
                        .endObject() // to NY: 5.286 km
                        .endObject()
                        .startObject()
                        .field("name", "Tribeca")
                        .startObject("location")
                        .field("lat", 40.718266)
                        .field("lon", -74.007819)
                        .endObject() // to NY:
                                     // 0.4621
                                     // km
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            client().prepareIndex("companies")
                .setId("3")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 3")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Wall Street")
                        .startObject("location")
                        .field("lat", 40.7051157)
                        .field("lon", -74.0088305)
                        .endObject() // to NY: 1.055 km
                        .endObject()
                        .startObject()
                        .field("name", "Soho")
                        .startObject("location")
                        .field("lat", 40.7247222)
                        .field("lon", -74)
                        .endObject() // to NY: 1.258
                                     // km
                        .endObject()
                        .endArray()
                        .endObject()
                ),
            client().prepareIndex("companies")
                .setId("4")
                .setSource(
                    jsonBuilder().startObject()
                        .field("name", "company 4")
                        .startArray("branches")
                        .startObject()
                        .field("name", "Greenwich Village")
                        .startObject("location")
                        .field("lat", 40.731033)
                        .field("lon", -73.9962255)
                        .endObject() // to NY:
                                     // 2.029
                                     // km
                        .endObject()
                        .startObject()
                        .field("name", "Brooklyn")
                        .startObject("location")
                        .field("lat", 40.65)
                        .field("lon", -73.95)
                        .endObject() // to NY:
                                     // 8.572 km
                        .endObject()
                        .endArray()
                        .endObject()
                )
        );

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731).order(SortOrder.ASC).setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));

        // Order: Asc, Mode: max
        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                    .order(SortOrder.ASC)
                    .sortMode(SortMode.MAX)
                    .setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));

        // Order: Desc
        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731).order(SortOrder.DESC).setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1258.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        // Order: Desc, Mode: min
        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                    .order(SortOrder.DESC)
                    .sortMode(SortMode.MIN)
                    .setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "3", "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(2029.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1055.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
                    .setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "1", "3", "2", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));

        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                    .setNestedPath("branches")
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.DESC)
                    .setNestedPath("branches")
            )
            .get();

        assertHitCount(searchResponse, 4);
        assertOrderedSearchHits(searchResponse, "4", "2", "3", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(5301.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(2874.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), closeTo(1157.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), closeTo(0d, 10d));

        searchResponse = client().prepareSearch("companies")
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                    .setNestedFilter(termQuery("branches.name", "brooklyn"))
                    .sortMode(SortMode.AVG)
                    .order(SortOrder.ASC)
                    .setNestedPath("branches")
            )
            .get();
        assertHitCount(searchResponse, 4);
        assertFirstHit(searchResponse, hasId("4"));
        assertSearchHits(searchResponse, "1", "2", "3", "4");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(8572.0d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(((Number) searchResponse.getHits().getAt(3).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));

        try {
            client().prepareSearch("companies")
                .setQuery(matchAllQuery())
                .addSort(
                    SortBuilders.geoDistanceSort("branches.location", 40.7143528, -74.0059731)
                        .sortMode(SortMode.SUM)
                        .setNestedPath("branches")
                );
            fail("Sum should not be allowed as sort mode");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    /**
     * Issue 3073
     */
    public void testGeoDistanceFilter() throws IOException, InterruptedException {
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        double lat = 40.720611;
        double lon = -73.998776;

        XContentBuilder mapping = JsonXContent.contentBuilder()
            .startObject()
            .startObject("properties")
            .startObject("pin")
            .field("type", "geo_point");
        mapping.endObject().endObject().endObject();

        XContentBuilder source = JsonXContent.contentBuilder().startObject().field("pin", Geohash.stringEncode(lon, lat)).endObject();

        assertAcked(prepareCreate("locations").setSettings(settings).setMapping(mapping));
        client().prepareIndex("locations").setId("1").setCreate(true).setSource(source).get();
        refresh();
        indexRandomForConcurrentSearch("locations");
        client().prepareGet("locations", "1").get();

        SearchResponse result = client().prepareSearch("locations")
            .setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(QueryBuilders.geoDistanceQuery("pin").geoDistance(GeoDistance.ARC).point(lat, lon).distance("1m"))
            .get();

        assertHitCount(result, 1);
    }

    public void testDistanceSortingWithUnmappedField() throws Exception {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("locations")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject();
        assertAcked(prepareCreate("test1").setMapping(xContentBuilder));
        assertAcked(prepareCreate("test2"));
        ensureGreen();

        client().prepareIndex("test1")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .array("names", "Times Square", "Tribeca")
                    .startArray("locations")
                    // to NY: 5.286 km
                    .startObject()
                    .field("lat", 40.759011)
                    .field("lon", -73.9844722)
                    .endObject()
                    // to NY: 0.4621 km
                    .startObject()
                    .field("lat", 40.718266)
                    .field("lon", -74.007819)
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();

        client().prepareIndex("test2")
            .setId("2")
            .setSource(jsonBuilder().startObject().array("names", "Wall Street", "Soho").endObject())
            .get();

        refresh();
        indexRandomForConcurrentSearch("test1", "test2");

        // Order: Asc
        SearchResponse searchResponse = client().prepareSearch("test1", "test2")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).ignoreUnmapped(true).order(SortOrder.ASC))
            .get();

        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "1", "2");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), closeTo(462.1d, 10d));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));

        // Order: Desc
        searchResponse = client().prepareSearch("test1", "test2")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).ignoreUnmapped(true).order(SortOrder.DESC))
            .get();

        // Doc with missing geo point is first, is consistent with 0.20.x
        assertHitCount(searchResponse, 2);
        assertOrderedSearchHits(searchResponse, "2", "1");
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), closeTo(5286d, 10d));

        // Make sure that by default the unmapped fields continue to fail
        searchResponse = client().prepareSearch("test1", "test2")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.geoDistanceSort("locations", 40.7143528, -74.0059731).order(SortOrder.DESC))
            .get();
        assertThat(searchResponse.getFailedShards(), greaterThan(0));
        assertHitCount(searchResponse, 1);
    }

}
