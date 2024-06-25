/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.aggregators;

import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.fielddata.IndexNumericFieldData;
import org.opensearch.test.OpenSearchTestCase;

public class MetricAggregationDescriptorTests extends OpenSearchTestCase {

    public void testConstructor() {
        MetricAggregationDescriptor pair = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(MetricStat.SUM, pair.getMetricStat());
        assertEquals("column1", pair.getField());
    }

    public void testCountStarConstructor() {
        MetricAggregationDescriptor pair = new MetricAggregationDescriptor(
            MetricStat.COUNT,
            "anything",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(MetricStat.COUNT, pair.getMetricStat());
        assertEquals("*", pair.getField());
    }

    public void testToFieldName() {
        MetricAggregationDescriptor pair = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column2",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals("sum__column2", pair.toFieldName());
    }

    public void testEquals() {
        MetricAggregationDescriptor pair1 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregationDescriptor pair2 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(pair1, pair2);
        assertNotEquals(
            pair1,
            new MetricAggregationDescriptor(MetricStat.COUNT, "column1", IndexNumericFieldData.NumericType.DOUBLE, null)
        );
        assertNotEquals(pair1, new MetricAggregationDescriptor(MetricStat.SUM, "column2", IndexNumericFieldData.NumericType.DOUBLE, null));
    }

    public void testHashCode() {
        MetricAggregationDescriptor pair1 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregationDescriptor pair2 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertEquals(pair1.hashCode(), pair2.hashCode());
    }

    public void testCompareTo() {
        MetricAggregationDescriptor pair1 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregationDescriptor pair2 = new MetricAggregationDescriptor(
            MetricStat.SUM,
            "column2",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        MetricAggregationDescriptor pair3 = new MetricAggregationDescriptor(
            MetricStat.COUNT,
            "column1",
            IndexNumericFieldData.NumericType.DOUBLE,
            null
        );
        assertTrue(pair1.compareTo(pair2) < 0);
        assertTrue(pair2.compareTo(pair1) > 0);
        assertTrue(pair1.compareTo(pair3) > 0);
        assertTrue(pair3.compareTo(pair1) < 0);
    }
}
