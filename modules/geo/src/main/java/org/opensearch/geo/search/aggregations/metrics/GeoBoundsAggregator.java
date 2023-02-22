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

package org.opensearch.geo.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregate all docs into a geographic bounds for field GeoPoint.
 *
 * @opensearch.internal
 */
final class GeoBoundsAggregator extends AbstractGeoBoundsAggregator<ValuesSource.GeoPoint> {

    GeoBoundsAggregator(
        String name,
        SearchContext aggregationContext,
        Aggregator parent,
        ValuesSourceConfig valuesSourceConfig,
        boolean wrapLongitude,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, valuesSourceConfig, wrapLongitude, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final MultiGeoPointValues values = valuesSource.geoPointValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                setBucketSize(bucket, bigArrays);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    for (int i = 0; i < valuesCount; ++i) {
                        GeoPoint value = values.nextValue();
                        double top = tops.get(bucket);
                        if (value.lat() > top) {
                            top = value.lat();
                        }
                        double bottom = bottoms.get(bucket);
                        if (value.lat() < bottom) {
                            bottom = value.lat();
                        }
                        double posLeft = posLefts.get(bucket);
                        if (value.lon() >= 0 && value.lon() < posLeft) {
                            posLeft = value.lon();
                        }
                        double posRight = posRights.get(bucket);
                        if (value.lon() >= 0 && value.lon() > posRight) {
                            posRight = value.lon();
                        }
                        double negLeft = negLefts.get(bucket);
                        if (value.lon() < 0 && value.lon() < negLeft) {
                            negLeft = value.lon();
                        }
                        double negRight = negRights.get(bucket);
                        if (value.lon() < 0 && value.lon() > negRight) {
                            negRight = value.lon();
                        }
                        tops.set(bucket, top);
                        bottoms.set(bucket, bottom);
                        posLefts.set(bucket, posLeft);
                        posRights.set(bucket, posRight);
                        negLefts.set(bucket, negLeft);
                        negRights.set(bucket, negRight);
                    }
                }
            }
        };
    }
}
