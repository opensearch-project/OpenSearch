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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.StringField;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;

import java.io.IOException;

import static org.opensearch.test.geo.RandomShapeGenerator.randomPoint;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public abstract class AbstractGeoFieldDataTestCase extends AbstractFieldDataImplTestCase {
    @Override
    protected abstract String getFieldDataType();

    protected Field randomGeoPointField(String fieldName, Field.Store store) {
        GeoPoint point = randomPoint(random());
        return new LatLonDocValuesField(fieldName, point.lat(), point.lon());
    }

    @Override
    protected boolean hasDocValues() {
        return true;
    }

    @Override
    protected long minRamBytesUsed() {
        return 0;
    }

    @Override
    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Override
    public void testSortMultiValuesFields() {
        assumeFalse("Only test on non geo_point fields", getFieldDataType().equals("geo_point"));
    }

    protected void assertValues(MultiGeoPointValues values, int docId) throws IOException {
        assertValues(values, docId, false);
    }

    protected void assertMissing(MultiGeoPointValues values, int docId) throws IOException {
        assertValues(values, docId, true);
    }

    private void assertValues(MultiGeoPointValues values, int docId, boolean missing) throws IOException {
        assertEquals(missing == false, values.advanceExact(docId));
        if (missing == false) {
            final int docCount = values.docValueCount();
            for (int i = 0; i < docCount; ++i) {
                final GeoPoint point = values.nextValue();
                assertThat(point.lat(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LAT), lessThanOrEqualTo(GeoUtils.MAX_LAT)));
                assertThat(point.lon(), allOf(greaterThanOrEqualTo(GeoUtils.MIN_LON), lessThanOrEqualTo(GeoUtils.MAX_LON)));
            }
        }
    }
}
