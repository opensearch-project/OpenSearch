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

package org.opensearch.geo.search.aggregations.bucket.geogrid.cells;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.aggregations.support.ValuesSource;

/**
 * Wrapper class to help convert {@link MultiGeoPointValues}
 * to numeric long values for bucketing.
 *
 * @opensearch.api
 */
public class CellIdSource extends ValuesSource.Numeric {
    private final ValuesSource.GeoPoint valuesSource;
    private final int precision;
    private final GeoPointLongEncoder encoder;
    private final GeoBoundingBox geoBoundingBox;

    public CellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox, GeoPointLongEncoder encoder) {
        this.valuesSource = valuesSource;
        // different GeoPoints could map to the same or different hashing cells.
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
        this.encoder = encoder;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        if (geoBoundingBox.isUnbounded()) {
            return new UnboundedCellValues(valuesSource.geoPointValues(ctx), precision, encoder);
        }
        return new BoundedCellValues(valuesSource.geoPointValues(ctx), precision, encoder, geoBoundingBox);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    /**
     * The encoder to use to convert a geopoint's (lon, lat, precision) into
     * a long-encoded bucket key for aggregating.
     *
     * @opensearch.internal
     */
    @FunctionalInterface
    public interface GeoPointLongEncoder {
        long encode(double lon, double lat, int precision);
    }

}
