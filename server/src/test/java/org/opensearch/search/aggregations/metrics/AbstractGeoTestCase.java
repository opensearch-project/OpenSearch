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

package org.opensearch.search.aggregations.metrics;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.document.DocumentField;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.opensearch.test.geo.RandomGeoGenerator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public abstract class AbstractGeoTestCase extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    protected static final String SINGLE_VALUED_FIELD_NAME = "geo_value";
    protected static final String MULTI_VALUED_FIELD_NAME = "geo_values";
    protected static final String NUMBER_FIELD_NAME = "l_values";
    protected static final String UNMAPPED_IDX_NAME = "idx_unmapped";
    protected static final String IDX_NAME = "idx";
    protected static final String EMPTY_IDX_NAME = "empty_idx";
    protected static final String DATELINE_IDX_NAME = "dateline_idx";
    protected static final String HIGH_CARD_IDX_NAME = "high_card_idx";
    protected static final String IDX_ZERO_NAME = "idx_zero";
    protected static int numDocs;
    protected static int numUniqueGeoPoints;
    protected static GeoPoint[] singleValues, multiValues;
    protected static GeoPoint singleTopLeft, singleBottomRight, multiTopLeft, multiBottomRight, singleCentroid, multiCentroid,
        unmappedCentroid;
    protected static Map<String, Integer> expectedDocCountsForGeoHash = null;
    protected static Map<String, GeoPoint> expectedCentroidsForGeoHash = null;
    protected static final double GEOHASH_TOLERANCE = 1E-5D;

    public AbstractGeoTestCase(Settings dynamicSettings) {
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
    public void setupSuiteScopeCluster() throws Exception {
        createIndex(UNMAPPED_IDX_NAME);
        assertAcked(
            prepareCreate(IDX_NAME).setMapping(
                SINGLE_VALUED_FIELD_NAME,
                "type=geo_point",
                MULTI_VALUED_FIELD_NAME,
                "type=geo_point",
                NUMBER_FIELD_NAME,
                "type=long",
                "tag",
                "type=keyword"
            )
        );

        singleTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        singleBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        multiTopLeft = new GeoPoint(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
        multiBottomRight = new GeoPoint(Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY);
        singleCentroid = new GeoPoint(0, 0);
        multiCentroid = new GeoPoint(0, 0);
        unmappedCentroid = new GeoPoint(0, 0);

        numDocs = randomIntBetween(6, 20);
        numUniqueGeoPoints = randomIntBetween(1, numDocs);
        expectedDocCountsForGeoHash = new HashMap<>(numDocs * 2);
        expectedCentroidsForGeoHash = new HashMap<>(numDocs * 2);

        singleValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0; i < singleValues.length; i++) {
            singleValues[i] = RandomGeoGenerator.randomPoint(random());
            updateBoundsTopLeft(singleValues[i], singleTopLeft);
            updateBoundsBottomRight(singleValues[i], singleBottomRight);
        }

        multiValues = new GeoPoint[numUniqueGeoPoints];
        for (int i = 0; i < multiValues.length; i++) {
            multiValues[i] = RandomGeoGenerator.randomPoint(random());
            updateBoundsTopLeft(multiValues[i], multiTopLeft);
            updateBoundsBottomRight(multiValues[i], multiBottomRight);
        }

        List<IndexRequestBuilder> builders = new ArrayList<>();

        GeoPoint singleVal;
        final GeoPoint[] multiVal = new GeoPoint[2];
        double newMVLat, newMVLon;
        for (int i = 0; i < numDocs; i++) {
            singleVal = singleValues[i % numUniqueGeoPoints];
            multiVal[0] = multiValues[i % numUniqueGeoPoints];
            multiVal[1] = multiValues[(i + 1) % numUniqueGeoPoints];
            builders.add(
                client().prepareIndex(IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, singleVal.lon(), singleVal.lat())
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .startArray()
                            .value(multiVal[0].lon())
                            .value(multiVal[0].lat())
                            .endArray()
                            .startArray()
                            .value(multiVal[1].lon())
                            .value(multiVal[1].lat())
                            .endArray()
                            .endArray()
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
            singleCentroid = singleCentroid.reset(
                singleCentroid.lat() + (singleVal.lat() - singleCentroid.lat()) / (i + 1),
                singleCentroid.lon() + (singleVal.lon() - singleCentroid.lon()) / (i + 1)
            );
            newMVLat = (multiVal[0].lat() + multiVal[1].lat()) / 2d;
            newMVLon = (multiVal[0].lon() + multiVal[1].lon()) / 2d;
            multiCentroid = multiCentroid.reset(
                multiCentroid.lat() + (newMVLat - multiCentroid.lat()) / (i + 1),
                multiCentroid.lon() + (newMVLon - multiCentroid.lon()) / (i + 1)
            );
        }

        assertAcked(prepareCreate(EMPTY_IDX_NAME).setMapping(SINGLE_VALUED_FIELD_NAME, "type=geo_point"));

        assertAcked(
            prepareCreate(DATELINE_IDX_NAME).setMapping(
                SINGLE_VALUED_FIELD_NAME,
                "type=geo_point",
                MULTI_VALUED_FIELD_NAME,
                "type=geo_point",
                NUMBER_FIELD_NAME,
                "type=long",
                "tag",
                "type=keyword"
            )
        );

        GeoPoint[] geoValues = new GeoPoint[5];
        geoValues[0] = new GeoPoint(38, 178);
        geoValues[1] = new GeoPoint(12, -179);
        geoValues[2] = new GeoPoint(-24, 170);
        geoValues[3] = new GeoPoint(32, -175);
        geoValues[4] = new GeoPoint(-11, 178);

        for (int i = 0; i < 5; i++) {
            builders.add(
                client().prepareIndex(DATELINE_IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, geoValues[i].lon(), geoValues[i].lat())
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
        }
        assertAcked(
            prepareCreate(HIGH_CARD_IDX_NAME).setSettings(Settings.builder().put("number_of_shards", 2))
                .setMapping(
                    SINGLE_VALUED_FIELD_NAME,
                    "type=geo_point",
                    MULTI_VALUED_FIELD_NAME,
                    "type=geo_point",
                    NUMBER_FIELD_NAME,
                    "type=long,store=true",
                    "tag",
                    "type=keyword"
                )
        );

        for (int i = 0; i < 2000; i++) {
            singleVal = singleValues[i % numUniqueGeoPoints];
            builders.add(
                client().prepareIndex(HIGH_CARD_IDX_NAME)
                    .setSource(
                        jsonBuilder().startObject()
                            .array(SINGLE_VALUED_FIELD_NAME, singleVal.lon(), singleVal.lat())
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .startArray()
                            .value(multiValues[i % numUniqueGeoPoints].lon())
                            .value(multiValues[i % numUniqueGeoPoints].lat())
                            .endArray()
                            .startArray()
                            .value(multiValues[(i + 1) % numUniqueGeoPoints].lon())
                            .value(multiValues[(i + 1) % numUniqueGeoPoints].lat())
                            .endArray()
                            .endArray()
                            .field(NUMBER_FIELD_NAME, i)
                            .field("tag", "tag" + i)
                            .endObject()
                    )
            );
            updateGeohashBucketsCentroid(singleVal);
        }

        builders.add(
            client().prepareIndex(IDX_ZERO_NAME)
                .setSource(jsonBuilder().startObject().array(SINGLE_VALUED_FIELD_NAME, 0.0, 1.0).endObject())
        );
        assertAcked(prepareCreate(IDX_ZERO_NAME).setMapping(SINGLE_VALUED_FIELD_NAME, "type=geo_point"));

        indexRandom(true, builders);
        ensureSearchable();

        // Added to debug a test failure where the terms aggregation seems to be reporting two documents with the same
        // value for NUMBER_FIELD_NAME. This will check that after random indexing each document only has 1 value for
        // NUMBER_FIELD_NAME and it is the correct value. Following this initial change its seems that this call was getting
        // more that 2000 hits (actual value was 2059) so now it will also check to ensure all hits have the correct index and type.
        SearchResponse response = client().prepareSearch(HIGH_CARD_IDX_NAME)
            .addStoredField(NUMBER_FIELD_NAME)
            .addSort(SortBuilders.fieldSort(NUMBER_FIELD_NAME).order(SortOrder.ASC))
            .setSize(5000)
            .get();
        assertSearchResponse(response);
        long totalHits = response.getHits().getTotalHits().value();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        logger.info("Full high_card_idx Response Content:\n{ {} }", builder.toString());
        for (int i = 0; i < totalHits; i++) {
            SearchHit searchHit = response.getHits().getAt(i);
            assertThat("Hit " + i + " with id: " + searchHit.getId(), searchHit.getIndex(), equalTo("high_card_idx"));
            DocumentField hitField = searchHit.field(NUMBER_FIELD_NAME);

            assertThat("Hit " + i + " has wrong number of values", hitField.getValues().size(), equalTo(1));
            Long value = hitField.getValue();
            assertThat("Hit " + i + " has wrong value", value.intValue(), equalTo(i));
        }
        assertThat(totalHits, equalTo(2000L));
    }

    private void updateGeohashBucketsCentroid(final GeoPoint location) {
        String hash = Geohash.stringEncode(location.lon(), location.lat(), Geohash.PRECISION);
        for (int precision = Geohash.PRECISION; precision > 0; --precision) {
            final String h = hash.substring(0, precision);
            expectedDocCountsForGeoHash.put(h, expectedDocCountsForGeoHash.getOrDefault(h, 0) + 1);
            expectedCentroidsForGeoHash.put(h, updateHashCentroid(h, location));
        }
    }

    private GeoPoint updateHashCentroid(String hash, final GeoPoint location) {
        GeoPoint centroid = expectedCentroidsForGeoHash.getOrDefault(hash, null);
        if (centroid == null) {
            return new GeoPoint(location.lat(), location.lon());
        }
        final int docCount = expectedDocCountsForGeoHash.get(hash);
        final double newLon = centroid.lon() + (location.lon() - centroid.lon()) / docCount;
        final double newLat = centroid.lat() + (location.lat() - centroid.lat()) / docCount;
        return centroid.reset(newLat, newLon);
    }

    private void updateBoundsBottomRight(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() < currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() > currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }

    private void updateBoundsTopLeft(GeoPoint geoPoint, GeoPoint currentBound) {
        if (geoPoint.lat() > currentBound.lat()) {
            currentBound.resetLat(geoPoint.lat());
        }
        if (geoPoint.lon() < currentBound.lon()) {
            currentBound.resetLon(geoPoint.lon());
        }
    }
}
