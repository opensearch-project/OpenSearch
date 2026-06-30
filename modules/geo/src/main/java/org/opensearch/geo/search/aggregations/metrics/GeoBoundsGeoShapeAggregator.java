/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.geo.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.GeoShapeValue;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate all docs into a geographic bounds for field geo_shape.
 *
 * @opensearch.internal
 */
public final class GeoBoundsGeoShapeAggregator extends AbstractGeoBoundsAggregator<ValuesSource.GeoShape> {
    private static final Logger LOGGER = LogManager.getLogger(GeoBoundsGeoShapeAggregator.class);

    public GeoBoundsGeoShapeAggregator(
        String name,
        SearchContext searchContext,
        Aggregator aggregator,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metaData
    ) throws IOException {
        super(name, searchContext, aggregator, valuesSourceConfig, wrapLongitude, metaData);
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector leafBucketCollector) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final GeoShapeValue values = valuesSource.getGeoShapeValues(ctx);
        return new LeafBucketCollectorBase(leafBucketCollector, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                setBucketSize(bucket, bigArrays);
                if (values.advanceExact(doc)) {
                    final GeoShapeDocValue value = values.nextValue();
                    final GeoShapeDocValue.BoundingRectangle boundingBox = value.getBoundingRectangle();
                    if (boundingBox != null) {
                        double top = tops.get(bucket);
                        if (boundingBox.getMaxLatitude() > top) {
                            top = boundingBox.getMaxLatitude();
                        }

                        double bottom = bottoms.get(bucket);
                        if (boundingBox.getMinLatitude() < bottom) {
                            bottom = boundingBox.getMinLatitude();
                        }

                        double posLeft = posLefts.get(bucket);
                        if (boundingBox.getMinLongitude() >= 0 && boundingBox.getMinLongitude() < posLeft) {
                            posLeft = boundingBox.getMinLongitude();
                        }
                        if (boundingBox.getMaxLongitude() >= 0 && boundingBox.getMaxLongitude() < posLeft) {
                            posLeft = boundingBox.getMaxLongitude();
                        }

                        double posRight = posRights.get(bucket);
                        if (boundingBox.getMaxLongitude() >= 0 && boundingBox.getMaxLongitude() > posRight) {
                            posRight = boundingBox.getMaxLongitude();
                        }
                        if (boundingBox.getMinLongitude() >= 0 && boundingBox.getMinLongitude() > posRight) {
                            posRight = boundingBox.getMinLongitude();
                        }

                        double negLeft = negLefts.get(bucket);
                        if (boundingBox.getMinLongitude() < 0 && boundingBox.getMinLongitude() < negLeft) {
                            negLeft = boundingBox.getMinLongitude();
                        }
                        if (boundingBox.getMaxLongitude() < 0 && boundingBox.getMaxLongitude() < negLeft) {
                            negLeft = boundingBox.getMaxLongitude();
                        }

                        double negRight = negRights.get(bucket);
                        if (boundingBox.getMaxLongitude() < 0 && boundingBox.getMaxLongitude() > negRight) {
                            negRight = boundingBox.getMaxLongitude();
                        }
                        if (boundingBox.getMinLongitude() < 0 && boundingBox.getMinLongitude() > negRight) {
                            negRight = boundingBox.getMinLongitude();
                        }

                        tops.set(bucket, top);
                        bottoms.set(bucket, bottom);
                        posLefts.set(bucket, posLeft);
                        posRights.set(bucket, posRight);
                        negLefts.set(bucket, negLeft);
                        negRights.set(bucket, negRight);
                    } else {
                        LOGGER.error("The bounding box was null for the Doc id {}", doc);
                    }
                }
            }
        };
    }
}
