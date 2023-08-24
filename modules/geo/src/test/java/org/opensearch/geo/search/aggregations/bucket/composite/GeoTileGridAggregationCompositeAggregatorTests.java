/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.bucket.composite;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.geo.GeoModulePlugin;
import org.opensearch.geo.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.opensearch.index.mapper.GeoPointFieldMapper;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.aggregations.bucket.GeoTileUtils;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.opensearch.search.aggregations.composite.BaseCompositeAggregatorTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Testing the geo tile grid as part of CompositeAggregation.
 */
public class GeoTileGridAggregationCompositeAggregatorTests extends BaseCompositeAggregatorTestCase {

    protected List<SearchPlugin> getSearchPlugins() {
        return Collections.singletonList(new GeoModulePlugin());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FIELD_TYPES.add(new GeoPointFieldMapper.GeoPointFieldType("geo_point"));
    }

    public void testUnmappedFieldWithGeopoint() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        final String mappedFieldName = "geo_point";
        dataset.addAll(
            Arrays.asList(
                createDocument(mappedFieldName, new GeoPoint(48.934059, 41.610741)),
                createDocument(mappedFieldName, new GeoPoint(-23.065941, 113.610741)),
                createDocument(mappedFieldName, new GeoPoint(90.0, 0.0)),
                createDocument(mappedFieldName, new GeoPoint(37.2343, -115.8067)),
                createDocument(mappedFieldName, new GeoPoint(90.0, 0.0))
            )
        );

        // just unmapped = no results
        testSearchCase(
            Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(mappedFieldName)),
            dataset,
            () -> new CompositeAggregationBuilder("name", Arrays.asList(new GeoTileGridValuesSourceBuilder("unmapped").field("unmapped"))),
            (result) -> assertEquals(0, result.getBuckets().size())
        );

        // unmapped missing bucket = one result
        testSearchCase(
            Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(mappedFieldName)),
            dataset,
            () -> new CompositeAggregationBuilder(
                "name",
                Arrays.asList(new GeoTileGridValuesSourceBuilder("unmapped").field("unmapped").missingBucket(true))
            ),
            (result) -> {
                assertEquals(1, result.getBuckets().size());
                assertEquals("{unmapped=null}", result.afterKey().toString());
                assertEquals("{unmapped=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(5L, result.getBuckets().get(0).getDocCount());
            }
        );

        // field + unmapped, no missing bucket = no results
        testSearchCase(
            Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(mappedFieldName)),
            dataset,
            () -> new CompositeAggregationBuilder(
                "name",
                Arrays.asList(
                    new GeoTileGridValuesSourceBuilder(mappedFieldName).field(mappedFieldName),
                    new GeoTileGridValuesSourceBuilder("unmapped").field("unmapped")
                )
            ),
            (result) -> assertEquals(0, result.getBuckets().size())
        );

        // field + unmapped with missing bucket = multiple results
        testSearchCase(
            Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery(mappedFieldName)),
            dataset,
            () -> new CompositeAggregationBuilder(
                "name",
                Arrays.asList(
                    new GeoTileGridValuesSourceBuilder(mappedFieldName).field(mappedFieldName),
                    new GeoTileGridValuesSourceBuilder("unmapped").field("unmapped").missingBucket(true)
                )
            ),
            (result) -> {
                assertEquals(2, result.getBuckets().size());
                assertEquals("{geo_point=7/64/56, unmapped=null}", result.afterKey().toString());
                assertEquals("{geo_point=7/32/56, unmapped=null}", result.getBuckets().get(0).getKeyAsString());
                assertEquals(2L, result.getBuckets().get(0).getDocCount());
                assertEquals("{geo_point=7/64/56, unmapped=null}", result.getBuckets().get(1).getKeyAsString());
                assertEquals(3L, result.getBuckets().get(1).getDocCount());
            }
        );

    }

    public void testWithGeoPoint() throws Exception {
        final List<Map<String, List<Object>>> dataset = new ArrayList<>();
        dataset.addAll(
            Arrays.asList(
                createDocument("geo_point", new GeoPoint(48.934059, 41.610741)),
                createDocument("geo_point", new GeoPoint(-23.065941, 113.610741)),
                createDocument("geo_point", new GeoPoint(90.0, 0.0)),
                createDocument("geo_point", new GeoPoint(37.2343, -115.8067)),
                createDocument("geo_point", new GeoPoint(90.0, 0.0))
            )
        );
        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("geo_point")), dataset, () -> {
            GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder("geo_point").field("geo_point");
            return new CompositeAggregationBuilder("name", Collections.singletonList(geoTile));
        }, (result) -> {
            assertEquals(2, result.getBuckets().size());
            assertEquals("{geo_point=7/64/56}", result.afterKey().toString());
            assertEquals("{geo_point=7/32/56}", result.getBuckets().get(0).getKeyAsString());
            assertEquals(2L, result.getBuckets().get(0).getDocCount());
            assertEquals("{geo_point=7/64/56}", result.getBuckets().get(1).getKeyAsString());
            assertEquals(3L, result.getBuckets().get(1).getDocCount());
        });

        testSearchCase(Arrays.asList(new MatchAllDocsQuery(), new DocValuesFieldExistsQuery("geo_point")), dataset, () -> {
            GeoTileGridValuesSourceBuilder geoTile = new GeoTileGridValuesSourceBuilder("geo_point").field("geo_point");
            return new CompositeAggregationBuilder("name", Collections.singletonList(geoTile)).aggregateAfter(
                Collections.singletonMap("geo_point", "7/32/56")
            );
        }, (result) -> {
            assertEquals(1, result.getBuckets().size());
            assertEquals("{geo_point=7/64/56}", result.afterKey().toString());
            assertEquals("{geo_point=7/64/56}", result.getBuckets().get(0).getKeyAsString());
            assertEquals(3L, result.getBuckets().get(0).getDocCount());
        });
    }

    @Override
    protected boolean addValueToDocument(final Document doc, final String name, final Object value) {
        if (value instanceof GeoPoint) {
            GeoPoint point = (GeoPoint) value;
            doc.add(
                new SortedNumericDocValuesField(
                    name,
                    GeoTileUtils.longEncode(point.lon(), point.lat(), GeoTileGridAggregationBuilder.DEFAULT_PRECISION)
                )
            );
            doc.add(new LatLonPoint(name, point.lat(), point.lon()));
            return true;
        }
        return false;
    }
}
