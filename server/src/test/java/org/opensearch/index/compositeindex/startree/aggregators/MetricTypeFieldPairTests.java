/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.startree.aggregators;

import org.opensearch.index.compositeindex.MetricType;
import org.opensearch.test.OpenSearchTestCase;

public class MetricTypeFieldPairTests extends OpenSearchTestCase {

    public void testConstructor() {
        MetricTypeFieldPair pair = new MetricTypeFieldPair(MetricType.SUM, "column1");
        assertEquals(MetricType.SUM, pair.getMetricType());
        assertEquals("column1", pair.getField());
    }

    public void testCountStarConstructor() {
        MetricTypeFieldPair pair = new MetricTypeFieldPair(MetricType.COUNT, "anything");
        assertEquals(MetricType.COUNT, pair.getMetricType());
        assertEquals("*", pair.getField());
    }

    public void testToFieldName() {
        MetricTypeFieldPair pair = new MetricTypeFieldPair(MetricType.AVG, "column2");
        assertEquals("avg__column2", pair.toFieldName());
    }

    public void testFromFieldName() {
        MetricTypeFieldPair pair = MetricTypeFieldPair.fromFieldName("max__column3");
        assertEquals(MetricType.MAX, pair.getMetricType());
        assertEquals("column3", pair.getField());
    }

    public void testCountStarFromFieldName() {
        MetricTypeFieldPair pair = MetricTypeFieldPair.fromFieldName("count__*");
        assertEquals(MetricType.COUNT, pair.getMetricType());
        assertEquals("*", pair.getField());
        assertSame(MetricTypeFieldPair.COUNT_STAR, pair);
    }

    public void testEquals() {
        MetricTypeFieldPair pair1 = new MetricTypeFieldPair(MetricType.SUM, "column1");
        MetricTypeFieldPair pair2 = new MetricTypeFieldPair(MetricType.SUM, "column1");
        assertEquals(pair1, pair2);
        assertNotEquals(pair1, new MetricTypeFieldPair(MetricType.AVG, "column1"));
        assertNotEquals(pair1, new MetricTypeFieldPair(MetricType.SUM, "column2"));
    }

    public void testHashCode() {
        MetricTypeFieldPair pair1 = new MetricTypeFieldPair(MetricType.SUM, "column1");
        MetricTypeFieldPair pair2 = new MetricTypeFieldPair(MetricType.SUM, "column1");
        assertEquals(pair1.hashCode(), pair2.hashCode());
    }

    public void testCompareTo() {
        MetricTypeFieldPair pair1 = new MetricTypeFieldPair(MetricType.SUM, "column1");
        MetricTypeFieldPair pair2 = new MetricTypeFieldPair(MetricType.SUM, "column2");
        MetricTypeFieldPair pair3 = new MetricTypeFieldPair(MetricType.AVG, "column1");
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair3.compareTo(pair1) < 0);
    }
}
