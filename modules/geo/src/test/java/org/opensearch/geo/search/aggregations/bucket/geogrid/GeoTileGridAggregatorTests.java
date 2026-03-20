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

package org.opensearch.geo.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonShape;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.opensearch.common.geo.GeoShapeUtils;
import org.opensearch.geometry.Line;
import org.opensearch.index.mapper.GeoShapeFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;

import java.io.IOException;

public class GeoTileGridAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoTileGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return GeoTileUtils.stringEncode(GeoTileUtils.longEncode(lng, lat, precision));
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoTileGridAggregationBuilder(name);
    }

    public void testPrecision() {
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        int precision = randomIntBetween(0, 29);
        builder.precision(precision);
        assertEquals(precision, builder.precision());
    }

    /**
     * Test that a LineString spanning large distances doesn't cause excessive tile generation.
     * This reproduces bug #20413 where a LineString with a large bounding box can generate
     * millions of tiles at high precision, causing CPU to max out and the cluster to stall.
     */
    public void testLineStringWithLargeBoundingBoxAtHighPrecision() {
        final String fieldName = "location";
        // Create a LineString spanning nearly the entire world diagonally.
        // The diagonal span ensures a large bounding box in both x and y tile dimensions.
        Line line = new Line(new double[] { -170.0, 170.0 }, new double[] { -60.0, 60.0 });

        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(LatLonShape.createDocValueField(fieldName, GeoShapeUtils.toLuceneLine(line)));
            writer.addDocument(doc);

            MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType(fieldName);

            // At precision 15 (2^15 = 32768 tiles/axis), this diagonal line's bounding box
            // spans ~31000 x-tiles and many y-tiles, well exceeding MAX_TILES_PER_SHAPE (65536)
            GeoTileGridAggregationBuilder aggBuilder = new GeoTileGridAggregationBuilder("geotile_grid");
            aggBuilder.field(fieldName).precision(15);

            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                // With the fix, this should throw an IllegalArgumentException
                // instead of hanging indefinitely
                Exception exception = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType)
                );
                assertTrue("Exception should mention tile limit", exception.getMessage().contains("would generate too many tiles"));
            }
        } catch (IOException e) {
            fail("IOException during test: " + e.getMessage());
        }
    }

    /**
     * Test that a LineString with moderate size works correctly at reasonable precision.
     */
    public void testLineStringWithModerateBoundingBox() throws IOException {
        final String fieldName = "location";
        // Create a smaller LineString that should work fine
        Line line = new Line(new double[] { -1.0, 1.0 }, new double[] { 0.0, 0.0 });

        try (Directory dir = newDirectory(); RandomIndexWriter writer = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(LatLonShape.createDocValueField(fieldName, GeoShapeUtils.toLuceneLine(line)));
            writer.addDocument(doc);

            MappedFieldType fieldType = new GeoShapeFieldMapper.GeoShapeFieldType(fieldName);

            // This should work fine even at high precision
            GeoTileGridAggregationBuilder aggBuilder = new GeoTileGridAggregationBuilder("geotile_grid");
            aggBuilder.field(fieldName).precision(10);

            try (IndexReader reader = writer.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                GeoTileGrid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

                // Should successfully generate buckets
                assertNotNull(result);
                assertTrue("Expected buckets to be generated", result.getBuckets().size() > 0);
            }
        }
    }
}
