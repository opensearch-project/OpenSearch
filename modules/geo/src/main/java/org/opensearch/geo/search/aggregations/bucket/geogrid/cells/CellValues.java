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

import org.opensearch.index.fielddata.AbstractSortingNumericDocValues;
import org.opensearch.index.fielddata.MultiGeoPointValues;

import java.io.IOException;

/**
 * Class representing the long-encoded grid-cells belonging to
 * the geo-doc-values. Class must encode the values and then
 * sort them in order to account for the cells correctly.
 *
 * @opensearch.internal
 */
abstract class CellValues extends AbstractSortingNumericDocValues {
    private MultiGeoPointValues geoValues;
    protected int precision;
    protected CellIdSource.GeoPointLongEncoder encoder;

    protected CellValues(MultiGeoPointValues geoValues, int precision, CellIdSource.GeoPointLongEncoder encoder) {
        this.geoValues = geoValues;
        this.precision = precision;
        this.encoder = encoder;
    }

    // we will need a way to clear the values array, as it is storing the states
    @Override
    public boolean advanceExact(int docId) throws IOException {
        if (geoValues.advanceExact(docId)) {
            int docValueCount = geoValues.docValueCount();
            resize(docValueCount);
            int j = 0;
            // we will need a way to convert the shape into a set of GeoTiles. Now, rather than iterating over the
            // points as in GeoPoints, we need to add the list of tiles which shape is intersecting with like we are
            // doing for the points.
            for (int i = 0; i < docValueCount; i++) {
                j = advanceValue(geoValues.nextValue(), j);
            }
            resize(j);
            sort();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Sets the appropriate long-encoded value for <code>target</code>
     * in <code>values</code>.
     *
     * @param target    the geo-value to encode
     * @param valuesIdx the index into <code>values</code> to set
     * @return          valuesIdx + 1 if value was set, valuesIdx otherwise.
     */
    abstract int advanceValue(org.opensearch.common.geo.GeoPoint target, int valuesIdx);
}
