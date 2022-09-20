/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.common.geo.GeoShapeDocValue;
import org.opensearch.geometry.Point;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class GeoShapeValueTests extends OpenSearchTestCase {

    public void testMissingGeoShapeValue() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final GeoShapeDocValue[][] values = new GeoShapeDocValue[numDocs][];

        for (int i = 0; i < numDocs; ++i) {
            values[i] = new GeoShapeDocValue[1];
            int number = TestUtil.nextInt(random(), 1, 2);
            if (number == 1) {
                values[i][0] = GeoShapeDocValue.createGeometryDocValue(new Point(randomDouble() * 90, randomDouble() * 180));
            } else {
                values[i][0] = null;
            }

        }
        final GeoShapeValue asGeoValues = new GeoShapeValue() {
            int doc;

            @Override
            public boolean advanceExact(int docId) {
                doc = docId;
                return values[doc][0] != null;
            }

            @Override
            public GeoShapeDocValue nextValue() {
                return values[doc][0];
            }

        };
        final Point missing = new Point(randomDouble() * 90, randomDouble() * 180);
        final GeoShapeValue withMissingReplaced = new GeoShapeValue.MissingGeoShapeValue(asGeoValues, missing);
        final GeoShapeDocValue missingValue = GeoShapeDocValue.createGeometryDocValue(missing);
        for (int i = 0; i < numDocs; i++) {
            assertTrue(withMissingReplaced.advanceExact(i));
            if (values[i][0] != null) {
                assertEquals(values[i][0], withMissingReplaced.nextValue());
            } else {
                assertEquals(missingValue, withMissingReplaced.nextValue());
            }
        }
    }
}
