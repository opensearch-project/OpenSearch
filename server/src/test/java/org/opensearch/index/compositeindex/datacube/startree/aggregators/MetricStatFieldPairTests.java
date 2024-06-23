/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.test.OpenSearchTestCase;

public class MetricStatFieldPairTests extends OpenSearchTestCase {

    public void testConstructor() {
        MetricStatFieldPair pair = new MetricStatFieldPair(MetricStat.SUM, "column1");
        assertEquals(MetricStat.SUM, pair.getMetricStat());
        assertEquals("column1", pair.getField());
    }

    public void testCountStarConstructor() {
        MetricStatFieldPair pair = new MetricStatFieldPair(MetricStat.COUNT, "anything");
        assertEquals(MetricStat.COUNT, pair.getMetricStat());
        assertEquals("*", pair.getField());
    }

    public void testToFieldName() {
        MetricStatFieldPair pair = new MetricStatFieldPair(MetricStat.AVG, "column2");
        assertEquals("avg__column2", pair.toFieldName());
    }

    public void testFromFieldName() {
        MetricStatFieldPair pair = MetricStatFieldPair.fromFieldName("max__column3");
        assertEquals(MetricStat.MAX, pair.getMetricStat());
        assertEquals("column3", pair.getField());
    }

    public void testCountStarFromFieldName() {
        MetricStatFieldPair pair = MetricStatFieldPair.fromFieldName("count__*");
        assertEquals(MetricStat.COUNT, pair.getMetricStat());
        assertEquals("*", pair.getField());
        assertSame(MetricStatFieldPair.COUNT_STAR, pair);
    }

    public void testEquals() {
        MetricStatFieldPair pair1 = new MetricStatFieldPair(MetricStat.SUM, "column1");
        MetricStatFieldPair pair2 = new MetricStatFieldPair(MetricStat.SUM, "column1");
        assertEquals(pair1, pair2);
        assertNotEquals(pair1, new MetricStatFieldPair(MetricStat.AVG, "column1"));
        assertNotEquals(pair1, new MetricStatFieldPair(MetricStat.SUM, "column2"));
    }

    public void testHashCode() {
        MetricStatFieldPair pair1 = new MetricStatFieldPair(MetricStat.SUM, "column1");
        MetricStatFieldPair pair2 = new MetricStatFieldPair(MetricStat.SUM, "column1");
        assertEquals(pair1.hashCode(), pair2.hashCode());
    }

    public void testCompareTo() {
        MetricStatFieldPair pair1 = new MetricStatFieldPair(MetricStat.SUM, "column1");
        MetricStatFieldPair pair2 = new MetricStatFieldPair(MetricStat.SUM, "column2");
        MetricStatFieldPair pair3 = new MetricStatFieldPair(MetricStat.AVG, "column1");
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair3.compareTo(pair1) < 0);
    }
}
