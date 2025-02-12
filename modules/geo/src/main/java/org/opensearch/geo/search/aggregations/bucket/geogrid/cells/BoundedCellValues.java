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

import org.opensearch.common.geo.GeoBoundingBox;
import org.opensearch.index.fielddata.MultiGeoPointValues;

/**
 * Class representing {@link CellValues} whose values are filtered
 * according to whether they are within the specified {@link GeoBoundingBox}.
 * <p>
 * The specified bounding box is assumed to be bounded.
 *
 * @opensearch.internal
 */
class BoundedCellValues extends CellValues {

    private final GeoBoundingBox geoBoundingBox;

    protected BoundedCellValues(
        MultiGeoPointValues geoValues,
        int precision,
        CellIdSource.GeoPointLongEncoder encoder,
        GeoBoundingBox geoBoundingBox
    ) {
        super(geoValues, precision, encoder);
        this.geoBoundingBox = geoBoundingBox;
    }

    @Override
    int advanceValue(org.opensearch.common.geo.GeoPoint target, int valuesIdx) {
        if (geoBoundingBox.pointInBounds(target.getLon(), target.getLat())) {
            values[valuesIdx] = encoder.encode(target.getLon(), target.getLat(), precision);
            return valuesIdx + 1;
        }
        return valuesIdx;
    }
}
